#!/usr/bin/env python3
"""Capture file edits and queue them for replay.

This hook reads PostToolUse or file-change JSON from stdin, normalizes it into
file operations, snapshots the current file contents into git blobs, and writes
one queued event into the worktree-local SQLite database.

It does the cheap part only. The actual replay and commit creation happen in
``snapshot-worker.py`` after edits settle down.

Concurrency matters here. All ``path_tail`` reads and writes happen inside one
``BEGIN IMMEDIATE`` transaction, so two hooks racing on the same path cannot
capture the same ``before`` state.
"""

from __future__ import annotations

import errno
import json
import os
import signal
import sqlite3
import stat
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


STATE_SUBDIR = "ai-snapshotd"
DB_SUBPATH = f"{STATE_SUBDIR}/snapshotd.db"
LOG_SUBPATH = f"{STATE_SUBDIR}/logs/hook.log"
LOG_MAX_BYTES = int(os.environ.get("SNAPSHOTD_LOG_MAX_BYTES", str(2 * 1024 * 1024)))
LOG_KEEP = int(os.environ.get("SNAPSHOTD_LOG_KEEP", "3"))
WORKER_HEARTBEAT_STALE = float(os.environ.get("SNAPSHOTD_HEARTBEAT_STALE", "15"))
DEBUG = os.environ.get("SNAPSHOTD_DEBUG", "").lower() not in {"", "0", "false", "no"}


SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA busy_timeout=5000;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS events (
  seq               INTEGER PRIMARY KEY AUTOINCREMENT,
  branch_ref        TEXT NOT NULL,
  base_head         TEXT NOT NULL,
  session_id        TEXT,
  tool_name         TEXT,
  source            TEXT,
  captured_ts       REAL NOT NULL,
  state             TEXT NOT NULL DEFAULT 'pending',
  commit_oid        TEXT,
  target_commit_oid TEXT,
  message           TEXT,
  settled_ts        REAL,
  error             TEXT
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

CREATE TABLE IF NOT EXISTS reconcile_pending (
  branch_ref  TEXT NOT NULL,
  path        TEXT NOT NULL,
  pre_mode    TEXT,
  pre_oid     TEXT,
  created_ts  REAL NOT NULL,
  PRIMARY KEY (branch_ref, path)
);

INSERT OR IGNORE INTO worker_state(id, pid, heartbeat_ts, last_enqueue_ts, started_ts)
VALUES (1, 0, 0, 0, 0);
"""


def _rotate_log(path: Path) -> None:
    try:
        if path.stat().st_size < LOG_MAX_BYTES:
            return
    except OSError:
        return
    try:
        for i in range(LOG_KEEP, 0, -1):
            src = path.with_suffix(path.suffix + f".{i}")
            dst = path.with_suffix(path.suffix + f".{i + 1}")
            if src.exists():
                if i == LOG_KEEP:
                    src.unlink(missing_ok=True)
                else:
                    src.replace(dst)
        path.replace(path.with_suffix(path.suffix + ".1"))
    except OSError:
        pass


def _log(log_path: Optional[Path], message: str) -> None:
    if not DEBUG:
        return
    target = log_path
    if target is None:
        return
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        _rotate_log(target)
        with target.open("a", encoding="utf-8") as fh:
            fh.write(f"[{time.strftime('%H:%M:%S')}] pid={os.getpid()} {message}\n")
    except Exception:
        pass


_LOG_PATH: Optional[Path] = None


def debug(message: str) -> None:
    _log(_LOG_PATH, message)


def migrate_schema(conn: sqlite3.Connection) -> None:
    existing = {row[1] for row in conn.execute("PRAGMA table_info(events)")}
    if "target_commit_oid" not in existing:
        try:
            conn.execute("ALTER TABLE events ADD COLUMN target_commit_oid TEXT")
        except sqlite3.OperationalError:
            pass
    if "message" not in existing:
        try:
            conn.execute("ALTER TABLE events ADD COLUMN message TEXT")
        except sqlite3.OperationalError:
            pass


def open_db(git_dir: Path) -> sqlite3.Connection:
    db_path = git_dir / DB_SUBPATH
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=10.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)
    migrate_schema(conn)
    return conn


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


def maybe_git(cwd: Path, *args: str) -> Tuple[int, str, str]:
    proc = subprocess.run(
        ["git", *args],
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return (
        proc.returncode,
        proc.stdout.decode("utf-8", errors="replace").rstrip("\n"),
        proc.stderr.decode("utf-8", errors="replace").rstrip("\n"),
    )


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
        p = repo_root / p
    try:
        resolved = p.resolve(strict=False)
        return resolved.relative_to(repo_root.resolve()).as_posix()
    except ValueError:
        return None


def extract_changes(payload: Dict[str, Any], repo_root: Path) -> List[Dict[str, Any]]:
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

    if payload.get("event") == "file.changed":
        for item in payload.get("changes") or []:
            if not isinstance(item, dict):
                continue
            op = item.get("operation")
            if op == "rename":
                add("rename", item.get("toPath"), item.get("fromPath"))
            elif op in {"create", "modify", "delete"} and isinstance(
                item.get("path"), str
            ):
                add(op, item["path"])
        for f in payload.get("files") or []:
            if isinstance(f, str):
                add("modify", f)
        return ops

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
            add(
                "rename",
                tool_input.get("to_path") or tool_input.get("destination"),
                tool_input.get("from_path") or tool_input.get("source"),
            )

    if ops:
        return ops

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
        try:
            target = os.readlink(abs_path)
        except OSError as exc:
            debug(f"readlink failed for {abs_path}: {exc}")
            return None, None
        try:
            oid = run_git(
                repo_root,
                "hash-object",
                "-w",
                "--stdin",
                input_bytes=target.encode("utf-8", errors="replace"),
            )
        except RuntimeError as exc:
            debug(f"hash-object symlink failed for {abs_path}: {exc}")
            return None, None
        return oid, mode
    try:
        oid = run_git(repo_root, "hash-object", "-w", str(abs_path))
    except RuntimeError as exc:
        debug(f"hash-object failed for {abs_path}: {exc}")
        return None, None
    return oid, mode


def batch_ls_tree(
    repo_root: Path, rev: str, paths: List[str]
) -> Dict[str, Tuple[str, str]]:
    """One `git ls-tree -z` call for many paths. Returns {path: (oid, mode)}."""
    if not paths:
        return {}
    out: Dict[str, Tuple[str, str]] = {}
    try:
        code, stdout, _err = maybe_git(repo_root, "ls-tree", "-z", rev, "--", *paths)
        if code != 0 or not stdout:
            return out
    except Exception as exc:  # noqa: BLE001
        debug(f"ls-tree batch failed: {exc}")
        return out
    for entry in stdout.split("\x00"):
        if not entry:
            continue
        meta, _tab, path_part = entry.partition("\t")
        parts = meta.split()
        if len(parts) < 3:
            continue
        mode, _kind, oid = parts[0], parts[1], parts[2]
        out[path_part] = (oid, mode)
    return out


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError as exc:
        return exc.errno == errno.EPERM


def wake_or_spawn_worker(
    conn: sqlite3.Connection, git_dir: Path, repo_root: Path
) -> None:
    """Prefer signalling an already-alive worker over spawning a new one."""
    row = conn.execute(
        "SELECT pid, heartbeat_ts FROM worker_state WHERE id=1"
    ).fetchone()
    if row:
        pid = int(row["pid"] or 0)
        hb = float(row["heartbeat_ts"] or 0.0)
        if pid > 0 and time.time() - hb < WORKER_HEARTBEAT_STALE and _pid_alive(pid):
            try:
                os.kill(pid, signal.SIGUSR1)
                debug(f"signalled existing worker pid={pid}")
                return
            except OSError as exc:
                debug(f"signal to {pid} failed: {exc}; will spawn")

    worker_path = os.environ.get("SNAPSHOTD_WORKER_PATH")
    if not worker_path:
        worker_path = str(Path(__file__).resolve().with_name("snapshot-worker.py"))
    try:
        subprocess.Popen(
            [
                sys.executable,
                worker_path,
                "--repo",
                str(repo_root),
                "--git-dir",
                str(git_dir),
            ],
            cwd=str(repo_root),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
            env=os.environ.copy(),
        )
        debug("spawned worker")
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


def _resolve_before(
    conn: sqlite3.Connection,
    branch: str,
    path: str,
    head_entries: Dict[str, Tuple[str, str]],
) -> Tuple[Optional[str], Optional[str], Optional[int]]:
    """Return (before_oid, before_mode, tail_source_seq)."""
    row = conn.execute(
        "SELECT tail_oid, tail_mode, source_seq FROM path_tail WHERE branch_ref=? AND path=?",
        (branch, path),
    ).fetchone()
    if row is not None:
        return row["tail_oid"], row["tail_mode"], int(row["source_seq"])
    head = head_entries.get(path)
    if head is None:
        return None, None, None
    return head[0], head[1], None


def _build_op(
    conn: sqlite3.Connection,
    branch: str,
    change: Dict[str, Any],
    hashes: Dict[str, Tuple[str, str]],
    head_entries: Dict[str, Tuple[str, str]],
) -> Optional[Tuple[Dict[str, Any], List[int]]]:
    """Return (op_row, observed_tail_source_seqs) for CAS tracking."""
    kind = change["op"]
    path = change["path"]
    observed: List[int] = []

    if kind in {"create", "modify"}:
        after = hashes.get(path)
        if after is None:
            return None
        before_oid, before_mode, tail_seq = _resolve_before(
            conn, branch, path, head_entries
        )
        if tail_seq is not None:
            observed.append(tail_seq)
        effective = "create" if before_oid is None else "modify"
        return (
            {
                "op": effective,
                "path": path,
                "before_oid": before_oid,
                "before_mode": before_mode,
                "after_oid": after[0],
                "after_mode": after[1],
            },
            observed,
        )

    if kind == "delete":
        before_oid, before_mode, tail_seq = _resolve_before(
            conn, branch, path, head_entries
        )
        if tail_seq is not None:
            observed.append(tail_seq)
        if before_oid is None:
            return None
        return (
            {
                "op": "delete",
                "path": path,
                "before_oid": before_oid,
                "before_mode": before_mode,
                "after_oid": None,
                "after_mode": None,
            },
            observed,
        )

    if kind == "rename":
        old_path = change.get("old_path")
        if not old_path:
            return None
        before_oid, before_mode, tail_seq = _resolve_before(
            conn, branch, old_path, head_entries
        )
        if tail_seq is not None:
            observed.append(tail_seq)
        if before_oid is None:
            return None
        after = hashes.get(path)
        if after is None:
            return None
        # Also track target-path tail, in case a prior event referenced it.
        _tgt_oid, _tgt_mode, tgt_seq = _resolve_before(conn, branch, path, head_entries)
        if tgt_seq is not None:
            observed.append(tgt_seq)
        return (
            {
                "op": "rename",
                "path": path,
                "old_path": old_path,
                "before_oid": before_oid,
                "before_mode": before_mode,
                "after_oid": after[0],
                "after_mode": after[1],
            },
            observed,
        )

    debug(f"unsupported op: {kind}")
    return None


def insert_event_and_tails(
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
        """INSERT INTO events(branch_ref, base_head, session_id, tool_name, source,
                              captured_ts, state)
           VALUES (?, ?, ?, ?, ?, ?, 'pending')""",
        (branch, base_head, session_id, tool_name, source, now),
    )
    seq = int(cur.lastrowid)
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
        conn.execute(
            """INSERT INTO path_tail(branch_ref, path, tail_oid, tail_mode, source_seq)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(branch_ref, path) DO UPDATE SET
                 tail_oid=excluded.tail_oid,
                 tail_mode=excluded.tail_mode,
                 source_seq=excluded.source_seq""",
            (branch, op["path"], op.get("after_oid"), op.get("after_mode"), seq),
        )
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


def handle_payload(payload: Dict[str, Any]) -> int:
    global _LOG_PATH
    cwd = resolve_cwd(payload)
    try:
        repo_root, git_dir = resolve_repo(cwd)
    except RuntimeError as exc:
        debug(f"not a git repo: {cwd}: {exc}")
        return 0
    _LOG_PATH = git_dir / LOG_SUBPATH

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

    # Hash all files (expensive, outside tx). Skip individual failures.
    hashes: Dict[str, Tuple[str, str]] = {}
    survivors: List[Dict[str, Any]] = []
    for change in changes:
        try:
            target = change["path"]
            if change["op"] in {"create", "modify", "rename"}:
                result = hash_object(repo_root, repo_root / target)
                if result[0] is None:
                    if change["op"] == "rename":
                        debug(f"skip rename (missing target): {target}")
                    else:
                        debug(f"skip missing path: {target}")
                    continue
                hashes[target] = result
            survivors.append(change)
        except Exception as exc:  # noqa: BLE001
            debug(f"per-file capture failed for {change.get('path')}: {exc}")
            continue

    if not survivors:
        return 0

    # Batch HEAD lookups for all candidate paths (source + rename source).
    head_paths: List[str] = []
    seen_paths: set = set()
    for change in survivors:
        for key in ("path", "old_path"):
            value = change.get(key)
            if isinstance(value, str) and value not in seen_paths:
                seen_paths.add(value)
                head_paths.append(value)
    head_entries = batch_ls_tree(repo_root, base_head, head_paths)

    conn = open_db(git_dir)
    try:
        session_id = str(payload.get("session_id") or "")
        tool_name = str(payload.get("tool_name") or "")
        source = detect_source(payload)

        # CAS loop: re-read path_tail under BEGIN IMMEDIATE; if a concurrent
        # hook bumped source_seq for any observed tail, retry from scratch.
        for attempt in range(6):
            conn.execute("BEGIN IMMEDIATE")
            try:
                ops: List[Dict[str, Any]] = []
                observed: List[int] = []
                for change in survivors:
                    built = _build_op(conn, branch, change, hashes, head_entries)
                    if built is None:
                        continue
                    ops.append(built[0])
                    observed.extend(built[1])
                if not ops:
                    conn.execute("ROLLBACK")
                    return 0

                # Verify observed source_seqs are still current before we commit.
                stale = False
                for seq in observed:
                    check = conn.execute(
                        "SELECT 1 FROM path_tail WHERE source_seq=?",
                        (seq,),
                    ).fetchone()
                    if check is None:
                        stale = True
                        break
                if stale:
                    conn.execute("ROLLBACK")
                    debug(f"path_tail shifted under us, retry {attempt + 1}")
                    time.sleep(0.005 * (attempt + 1))
                    continue

                new_seq = insert_event_and_tails(
                    conn, branch, base_head, session_id, tool_name, source, ops
                )
                conn.execute("COMMIT")
                debug(f"queued event seq={new_seq} ops={len(ops)} branch={branch}")
                break
            except sqlite3.OperationalError as exc:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.OperationalError:
                    pass
                debug(f"sqlite busy (attempt {attempt + 1}): {exc}")
                time.sleep(0.01 * (attempt + 1))
                continue
            except Exception:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.OperationalError:
                    pass
                raise
        else:
            debug("gave up after repeated path_tail conflicts")
            return 0

        wake_or_spawn_worker(conn, git_dir, repo_root)
    finally:
        conn.close()
    return 0


def main() -> int:
    raw = sys.stdin.read()
    if not raw.strip():
        return 0
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        _log(_LOG_PATH, f"invalid JSON: {exc}")
        return 0
    if not isinstance(payload, dict):
        return 0
    try:
        return handle_payload(payload)
    except Exception as exc:  # noqa: BLE001
        _log(_LOG_PATH, f"hook error: {exc}")
        return 0


if __name__ == "__main__":
    sys.exit(main())
