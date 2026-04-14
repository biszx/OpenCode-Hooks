#!/usr/bin/env python3
"""
Universal snapshot autocommit hook.

Reads any harness-specific PostToolUse / file-change JSON on stdin, normalizes
it, captures an immutable git blob snapshot for each changed file, inserts one
event row into a SQLite database living inside this worktree's private git
dir, then spawns the singleton worker.

Design
------
- DB is truth. The hidden-ref worker is replaced by plain journal-first events.
- DB lives at <git-dir>/ai-snapshotd/snapshotd.db. Because git rev-parse
  --absolute-git-dir returns the worktree's private git dir, every worktree
  (main or linked) automatically gets its own isolated DB and its own worker.
- path_tail chains repeated edits to the same file so 10 fast edits produce
  10 replayable commits in the same order.
- The hook does only fast work: parse, snapshot, insert, spawn. The worker
  handles quiet-window batching, commit-tree construction, and CAS publish.

Supported harnesses (all via stdin JSON):
- Claude Code PostToolUse: tool_input.file_path for Write|Edit|MultiEdit|NotebookEdit
- OpenCode file.changed plugin events: changes[] / files[]
- Codex / generic: best-effort extraction from common payload shapes

Env:
- SNAPSHOTD_DEBUG=1                enables /tmp/snapshotd-hook.log
- SNAPSHOTD_WORKER_PATH=<path>     override the worker script path
"""

from __future__ import annotations

import errno
import json
import os
import sqlite3
import stat
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


DB_SUBPATH = "ai-snapshotd/snapshotd.db"
DEBUG_LOG = Path("/tmp/snapshotd-hook.log")
DEBUG = os.environ.get("SNAPSHOTD_DEBUG", "").lower() not in {"", "0", "false", "no"}


SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA busy_timeout=5000;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS events (
  seq          INTEGER PRIMARY KEY AUTOINCREMENT,
  branch_ref   TEXT NOT NULL,
  base_head    TEXT NOT NULL,
  session_id   TEXT,
  tool_name    TEXT,
  source       TEXT,
  captured_ts  REAL NOT NULL,
  state        TEXT NOT NULL DEFAULT 'pending',
  commit_oid   TEXT,
  settled_ts   REAL,
  error        TEXT
);

CREATE INDEX IF NOT EXISTS idx_events_state_seq ON events(state, seq);
CREATE INDEX IF NOT EXISTS idx_events_branch     ON events(branch_ref, state, seq);

CREATE TABLE IF NOT EXISTS event_ops (
  event_seq   INTEGER NOT NULL,
  ord         INTEGER NOT NULL,
  op          TEXT NOT NULL,
  path        TEXT NOT NULL,
  old_path    TEXT,
  before_oid  TEXT,
  before_mode TEXT,
  after_oid   TEXT,
  after_mode  TEXT,
  PRIMARY KEY (event_seq, ord),
  FOREIGN KEY (event_seq) REFERENCES events(seq) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_ops_path ON event_ops(path);

CREATE TABLE IF NOT EXISTS path_tail (
  branch_ref  TEXT NOT NULL,
  path        TEXT NOT NULL,
  tail_oid    TEXT,
  tail_mode   TEXT,
  source_seq  INTEGER NOT NULL,
  PRIMARY KEY (branch_ref, path)
);

CREATE TABLE IF NOT EXISTS worker_state (
  id              INTEGER PRIMARY KEY CHECK (id = 1),
  pid             INTEGER,
  heartbeat_ts    REAL,
  last_enqueue_ts REAL,
  started_ts      REAL
);

INSERT OR IGNORE INTO worker_state(id, pid, heartbeat_ts, last_enqueue_ts, started_ts)
VALUES (1, 0, 0, 0, 0);
"""


def debug(message: str) -> None:
    if not DEBUG:
        return
    try:
        DEBUG_LOG.parent.mkdir(parents=True, exist_ok=True)
        with DEBUG_LOG.open("a", encoding="utf-8") as fh:
            fh.write(f"[{time.strftime('%H:%M:%S')}] pid={os.getpid()} {message}\n")
    except Exception:
        pass


def run_git(cwd: Path, *args: str, input_bytes: Optional[bytes] = None) -> str:
    proc = subprocess.run(
        ["git", *args],
        cwd=str(cwd),
        input=input_bytes,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            proc.stderr.decode("utf-8", errors="replace").strip()
            or f"git {' '.join(args)} failed"
        )
    return proc.stdout.decode("utf-8", errors="replace").rstrip("\n")


def resolve_cwd(payload: Dict[str, Any]) -> Path:
    for key in ("cwd", "CLAUDE_PROJECT_DIR", "project_dir"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return Path(value).expanduser()
    for env_key in ("CLAUDE_PROJECT_DIR", "OPENCODE_PROJECT_DIR"):
        value = os.environ.get(env_key)
        if value:
            return Path(value).expanduser()
    return Path(os.getcwd())


def resolve_repo(cwd: Path) -> Tuple[Path, Path]:
    repo_root = Path(run_git(cwd, "rev-parse", "--show-toplevel"))
    git_dir = Path(run_git(cwd, "rev-parse", "--absolute-git-dir"))
    return repo_root, git_dir


def rel_path(repo_root: Path, candidate: str) -> Optional[str]:
    p = Path(candidate)
    if not p.is_absolute():
        p = (repo_root / p)
    try:
        resolved = p.resolve(strict=False)
        return resolved.relative_to(repo_root.resolve()).as_posix()
    except ValueError:
        return None


def extract_changes(payload: Dict[str, Any], repo_root: Path) -> List[Dict[str, Any]]:
    """Return normalized ops: [{op, path[, old_path]}, ...]"""
    ops: List[Dict[str, Any]] = []
    seen: set = set()

    def add(op_kind: str, path: Optional[str], old_path: Optional[str] = None) -> None:
        if not path:
            return
        rel = rel_path(repo_root, path)
        if not rel:
            return
        old_rel = rel_path(repo_root, old_path) if old_path else None
        key = (op_kind, rel, old_rel)
        if key in seen:
            return
        seen.add(key)
        entry: Dict[str, Any] = {"op": op_kind, "path": rel}
        if old_rel:
            entry["old_path"] = old_rel
        ops.append(entry)

    # OpenCode file.changed
    if payload.get("event") == "file.changed":
        for item in payload.get("changes") or []:
            if not isinstance(item, dict):
                continue
            op = item.get("operation")
            if op == "rename":
                add("rename", item.get("toPath"), item.get("fromPath"))
            elif op in {"create", "modify", "delete"} and isinstance(item.get("path"), str):
                add(op, item["path"])
        for f in payload.get("files") or []:
            if isinstance(f, str):
                add("modify", f)
        return ops

    # Claude Code PostToolUse style
    tool_name = str(payload.get("tool_name") or "").strip()
    tool_input = payload.get("tool_input") or {}
    if tool_name and isinstance(tool_input, dict):
        lower = tool_name.lower()
        if lower in {"write", "edit", "multiedit"}:
            fp = tool_input.get("file_path")
            if isinstance(fp, str):
                add("modify", fp)
        elif lower == "notebookedit":
            fp = tool_input.get("notebook_path") or tool_input.get("file_path")
            if isinstance(fp, str):
                add("modify", fp)
        elif lower in {"move", "rename"}:
            add("rename", tool_input.get("to_path") or tool_input.get("destination"),
                tool_input.get("from_path") or tool_input.get("source"))

    if ops:
        return ops

    # Generic fallback: scan common fields
    for key in ("file_path", "path", "filepath", "filename"):
        value = payload.get(key)
        if isinstance(value, str):
            add("modify", value)
    for key in ("files", "paths", "changed_files"):
        for item in payload.get(key) or []:
            if isinstance(item, str):
                add("modify", item)
    return ops


def git_mode_for(abs_path: Path) -> Optional[str]:
    try:
        st = abs_path.lstat()
    except FileNotFoundError:
        return None
    if stat.S_ISLNK(st.st_mode):
        return "120000"
    if st.st_mode & 0o111:
        return "100755"
    return "100644"


def hash_object(repo_root: Path, abs_path: Path) -> Tuple[Optional[str], Optional[str]]:
    mode = git_mode_for(abs_path)
    if mode is None:
        return None, None
    if mode == "120000":
        target = os.readlink(abs_path)
        oid = run_git(repo_root, "hash-object", "-w", "--stdin",
                      input_bytes=target.encode("utf-8"))
        return oid, mode
    oid = run_git(repo_root, "hash-object", "-w", str(abs_path))
    return oid, mode


def ls_tree_path(repo_root: Path, rev: str, rel: str) -> Tuple[Optional[str], Optional[str]]:
    proc = subprocess.run(
        ["git", "ls-tree", rev, "--", rel],
        cwd=str(repo_root),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if proc.returncode != 0:
        return None, None
    out = proc.stdout.decode("utf-8", errors="replace").strip()
    if not out:
        return None, None
    meta, _tab, path_part = out.splitlines()[0].partition("\t")
    if path_part != rel:
        return None, None
    parts = meta.split()
    if len(parts) < 3:
        return None, None
    return parts[2], parts[0]


def open_db(git_dir: Path) -> sqlite3.Connection:
    db_path = git_dir / DB_SUBPATH
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=10.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)
    return conn


def read_tail(conn: sqlite3.Connection, branch: str, path: str) -> Tuple[Optional[str], Optional[str]]:
    row = conn.execute(
        "SELECT tail_oid, tail_mode FROM path_tail WHERE branch_ref=? AND path=?",
        (branch, path),
    ).fetchone()
    if row is None:
        return None, None
    return row["tail_oid"], row["tail_mode"]


def snapshot_op(
    repo_root: Path,
    conn: sqlite3.Connection,
    branch: str,
    base_head: str,
    op: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    kind = op["op"]
    path = op["path"]
    abs_path = repo_root / path

    # Compute "before" from unpublished tail, else from base_head
    before_source = "tail"
    before_oid, before_mode = read_tail(conn, branch, path)
    if before_oid is None and before_mode is None:
        before_source = "head"
        before_oid, before_mode = ls_tree_path(repo_root, base_head, path)

    if kind in {"create", "modify"}:
        after_oid, after_mode = hash_object(repo_root, abs_path)
        if after_oid is None:
            debug(f"skip missing {kind}: {path}")
            return None
        effective_kind = "create" if before_oid is None else "modify"
        return {
            "op": effective_kind,
            "path": path,
            "before_oid": before_oid,
            "before_mode": before_mode,
            "after_oid": after_oid,
            "after_mode": after_mode,
        }

    if kind == "delete":
        if before_oid is None:
            debug(f"skip delete with no prior state: {path}")
            return None
        return {
            "op": "delete",
            "path": path,
            "before_oid": before_oid,
            "before_mode": before_mode,
            "after_oid": None,
            "after_mode": None,
        }

    if kind == "rename":
        old_path = op.get("old_path")
        if not old_path:
            return None
        old_before_oid, old_before_mode = read_tail(conn, branch, old_path)
        if old_before_oid is None and old_before_mode is None:
            old_before_oid, old_before_mode = ls_tree_path(repo_root, base_head, old_path)
        if old_before_oid is None:
            debug(f"skip rename from missing source: {old_path} -> {path}")
            return None
        after_oid, after_mode = hash_object(repo_root, abs_path)
        if after_oid is None:
            return None
        return {
            "op": "rename",
            "path": path,
            "old_path": old_path,
            "before_oid": old_before_oid,
            "before_mode": old_before_mode,
            "after_oid": after_oid,
            "after_mode": after_mode,
        }

    _ = before_source
    debug(f"unsupported op: {kind}")
    return None


def insert_event(
    conn: sqlite3.Connection,
    branch: str,
    base_head: str,
    session_id: str,
    tool_name: str,
    source: str,
    ops: List[Dict[str, Any]],
) -> int:
    now = time.time()
    cur = conn.execute(
        """INSERT INTO events(branch_ref, base_head, session_id, tool_name, source, captured_ts, state)
           VALUES (?, ?, ?, ?, ?, ?, 'pending')""",
        (branch, base_head, session_id, tool_name, source, now),
    )
    seq = cur.lastrowid
    for ord_idx, op in enumerate(ops):
        conn.execute(
            """INSERT INTO event_ops(event_seq, ord, op, path, old_path,
                                      before_oid, before_mode, after_oid, after_mode)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                seq,
                ord_idx,
                op["op"],
                op["path"],
                op.get("old_path"),
                op.get("before_oid"),
                op.get("before_mode"),
                op.get("after_oid"),
                op.get("after_mode"),
            ),
        )
        # Update tail to the new after-state for this path
        target_path = op["path"]
        conn.execute(
            """INSERT INTO path_tail(branch_ref, path, tail_oid, tail_mode, source_seq)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(branch_ref, path) DO UPDATE SET
                 tail_oid=excluded.tail_oid,
                 tail_mode=excluded.tail_mode,
                 source_seq=excluded.source_seq""",
            (branch, target_path, op.get("after_oid"), op.get("after_mode"), seq),
        )
        # For rename: old_path becomes absent
        if op["op"] == "rename" and op.get("old_path"):
            conn.execute(
                """INSERT INTO path_tail(branch_ref, path, tail_oid, tail_mode, source_seq)
                   VALUES (?, ?, NULL, NULL, ?)
                   ON CONFLICT(branch_ref, path) DO UPDATE SET
                     tail_oid=NULL, tail_mode=NULL, source_seq=excluded.source_seq""",
                (branch, op["old_path"], seq),
            )
    conn.execute(
        "UPDATE worker_state SET last_enqueue_ts=? WHERE id=1",
        (now,),
    )
    return seq


def spawn_worker(git_dir: Path, repo_root: Path) -> None:
    worker_path = os.environ.get("SNAPSHOTD_WORKER_PATH")
    if not worker_path:
        worker_path = str(Path(__file__).resolve().with_name("snapshot-worker.py"))
    try:
        subprocess.Popen(
            [sys.executable, worker_path, "--repo", str(repo_root), "--git-dir", str(git_dir)],
            cwd=str(repo_root),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
            env=os.environ.copy(),
        )
    except OSError as exc:
        debug(f"failed to spawn worker: {exc}")


def detect_source(payload: Dict[str, Any]) -> str:
    if payload.get("event") == "file.changed":
        return "opencode"
    if "tool_name" in payload and "tool_input" in payload:
        if "hook_event_name" in payload or "transcript_path" in payload:
            return "claude"
        return "tool-hook"
    return "generic"


def handle_payload(payload: Dict[str, Any]) -> int:
    cwd = resolve_cwd(payload)
    try:
        repo_root, git_dir = resolve_repo(cwd)
    except RuntimeError as exc:
        debug(f"not a git repo: {cwd}: {exc}")
        return 0

    # Resolve branch + base head once per payload
    try:
        branch = run_git(repo_root, "symbolic-ref", "-q", "HEAD").strip()
    except RuntimeError:
        branch = ""
    if not branch:
        debug("detached HEAD, skipping")
        return 0
    try:
        base_head = run_git(repo_root, "rev-parse", "HEAD").strip()
    except RuntimeError as exc:
        debug(f"rev-parse HEAD failed: {exc}")
        return 0

    changes = extract_changes(payload, repo_root)
    if not changes:
        debug("no changes extracted")
        return 0

    conn = open_db(git_dir)
    try:
        ops: List[Dict[str, Any]] = []
        for change in changes:
            op = snapshot_op(repo_root, conn, branch, base_head, change)
            if op is not None:
                ops.append(op)
        if not ops:
            debug("no valid ops after snapshot")
            return 0

        session_id = str(payload.get("session_id") or "")
        tool_name = str(payload.get("tool_name") or "")
        source = detect_source(payload)

        conn.execute("BEGIN IMMEDIATE")
        try:
            seq = insert_event(conn, branch, base_head, session_id, tool_name, source, ops)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        debug(f"queued event seq={seq} ops={len(ops)} branch={branch}")
    finally:
        conn.close()

    spawn_worker(git_dir, repo_root)
    return 0


def main() -> int:
    raw = sys.stdin.read()
    if not raw.strip():
        return 0
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        debug(f"invalid JSON: {exc}")
        return 0
    if not isinstance(payload, dict):
        return 0
    try:
        return handle_payload(payload)
    except Exception as exc:  # noqa: BLE001
        debug(f"hook error: {exc}")
        return 0


if __name__ == "__main__":
    sys.exit(main())
