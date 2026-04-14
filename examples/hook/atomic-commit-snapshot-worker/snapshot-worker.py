#!/usr/bin/env python3
"""
Singleton snapshot worker.

Runs per worktree/git-dir. Processes pending events from the SQLite journal,
replays them onto the current branch tip, publishes with compare-and-swap via
`git update-ref`, and exits after an idle window.

Lifecycle:
- Spawned by snapshot-hook.py after each captured event.
- Acquires an exclusive flock on <git-dir>/ai-snapshotd/worker.lock.
  If it cannot acquire within a brief retry window, another worker is already
  running and this process exits cleanly.
- Loops:
    * update heartbeat
    * wait for quiet window (no enqueue for QUIET_SECONDS)
    * read pending events for the current branch
    * replay onto current HEAD using a temp index + commit-tree
    * CAS publish with update-ref <branch> <new> <old>
    * mark events published or blocked_conflict
- Exits when: no pending events AND last_enqueue_ts is older than IDLE_SECONDS.

Env:
- SNAPSHOTD_QUIET_SECONDS    default 1.0
- SNAPSHOTD_IDLE_SECONDS     default 30.0
- SNAPSHOTD_POLL_SECONDS     default 0.35
- SNAPSHOTD_DEBUG            enables /tmp/snapshotd-worker.log
- SNAPSHOTD_COMMIT_MESSAGE_CMD  optional external command that reads JSON on
                                stdin and prints a commit message on stdout.
                                Called per prepared commit.
- OPENAI_API_KEY / OPENAI_BASE_URL / OPENAI_MODEL  if set, used for AI commit
                                                   messages as a fallback when
                                                   SNAPSHOTD_COMMIT_MESSAGE_CMD
                                                   is not set.
"""

from __future__ import annotations

import argparse
import difflib
import errno
import fcntl
import json
import os
import re
import signal
import sqlite3
import subprocess
import sys
import textwrap
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib import error as urllib_error
from urllib import request as urllib_request


DB_SUBPATH = "ai-snapshotd/snapshotd.db"
LOCK_SUBPATH = "ai-snapshotd/worker.lock"
INDEX_SUBPATH = "ai-snapshotd/worker.index"

QUIET_SECONDS = float(os.environ.get("SNAPSHOTD_QUIET_SECONDS", "1.0"))
IDLE_SECONDS = float(os.environ.get("SNAPSHOTD_IDLE_SECONDS", "30.0"))
POLL_SECONDS = float(os.environ.get("SNAPSHOTD_POLL_SECONDS", "0.35"))

DEBUG_LOG = Path("/tmp/snapshotd-worker.log")
DEBUG = os.environ.get("SNAPSHOTD_DEBUG", "").lower() not in {"", "0", "false", "no"}

COMMIT_CMD = os.environ.get("SNAPSHOTD_COMMIT_MESSAGE_CMD", "").strip()
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
OPENAI_BASE_URL = os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1")
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-5.4-mini")
OPENAI_API_TIMEOUT = float(os.environ.get("OPENAI_API_TIMEOUT", "15"))

AI_SYSTEM_PROMPT = (
    "You are a git commit message generator.\n"
    "Line 1: imperative subject, max 50 chars, no trailing period.\n"
    "Blank line, then body bullets starting with '- ', wrapped at 72 chars.\n"
    "Describe WHAT changed and WHY. No questions, no preamble.\n"
    "Output only the commit message."
)


def debug(message: str) -> None:
    if not DEBUG:
        return
    try:
        DEBUG_LOG.parent.mkdir(parents=True, exist_ok=True)
        with DEBUG_LOG.open("a", encoding="utf-8") as fh:
            fh.write(f"[{time.strftime('%H:%M:%S')}] pid={os.getpid()} {message}\n")
    except Exception:
        pass


# --------------------------------------------------------------------------- #
# Signals / singleton lock
# --------------------------------------------------------------------------- #


_wake_flag = False


def _on_wake(signum, frame) -> None:  # noqa: ARG001
    global _wake_flag
    _wake_flag = True


def consume_wake() -> bool:
    global _wake_flag
    if _wake_flag:
        _wake_flag = False
        return True
    return False


class Singleton:
    def __init__(self, lock_path: Path) -> None:
        self.lock_path = lock_path
        self._fh: Optional[Any] = None

    def acquire(self, attempts: int = 10, sleep: float = 0.05) -> bool:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.lock_path.open("a+")
        for _ in range(max(1, attempts)):
            try:
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                return True
            except OSError as exc:
                if exc.errno not in {errno.EAGAIN, errno.EACCES, errno.EWOULDBLOCK}:
                    raise
                time.sleep(sleep)
        self._fh.close()
        self._fh = None
        return False

    def release(self) -> None:
        if self._fh is None:
            return
        try:
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
        finally:
            self._fh.close()
            self._fh = None


# --------------------------------------------------------------------------- #
# Git helpers
# --------------------------------------------------------------------------- #


def run_git(
    repo_root: Path,
    *args: str,
    input_bytes: Optional[bytes] = None,
    env: Optional[Dict[str, str]] = None,
) -> str:
    proc = subprocess.run(
        ["git", *args],
        cwd=str(repo_root),
        input=input_bytes,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            proc.stderr.decode("utf-8", errors="replace").strip()
            or f"git {' '.join(args)} failed"
        )
    return proc.stdout.decode("utf-8", errors="replace").rstrip("\n")


def maybe_git(
    repo_root: Path,
    *args: str,
    env: Optional[Dict[str, str]] = None,
) -> Tuple[int, str, str]:
    proc = subprocess.run(
        ["git", *args],
        cwd=str(repo_root),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )
    return (
        proc.returncode,
        proc.stdout.decode("utf-8", errors="replace").rstrip("\n"),
        proc.stderr.decode("utf-8", errors="replace").rstrip("\n"),
    )


def current_branch(repo_root: Path) -> Optional[str]:
    code, out, _ = maybe_git(repo_root, "symbolic-ref", "-q", "HEAD")
    if code != 0:
        return None
    return out.strip() or None


def current_head(repo_root: Path) -> Optional[str]:
    code, out, _ = maybe_git(repo_root, "rev-parse", "HEAD")
    if code != 0:
        return None
    return out.strip() or None


def repo_special_state(git_dir: Path) -> Optional[str]:
    markers = {
        "MERGE_HEAD": "merge",
        "rebase-apply": "rebase",
        "rebase-merge": "rebase",
        "CHERRY_PICK_HEAD": "cherry-pick",
        "REVERT_HEAD": "revert",
        "BISECT_LOG": "bisect",
    }
    for name, label in markers.items():
        if (git_dir / name).exists():
            return label
    return None


def ls_tree_path(repo_root: Path, rev: str, rel: str) -> Tuple[Optional[str], Optional[str]]:
    code, out, _ = maybe_git(repo_root, "ls-tree", rev, "--", rel)
    if code != 0 or not out.strip():
        return None, None
    meta, _tab, path_part = out.splitlines()[0].partition("\t")
    if path_part != rel:
        return None, None
    parts = meta.split()
    if len(parts) < 3:
        return None, None
    return parts[2], parts[0]


def index_entry(repo_root: Path, rel: str, env: Dict[str, str]) -> Tuple[Optional[str], Optional[str]]:
    code, out, _ = maybe_git(repo_root, "ls-files", "-s", "--", rel, env=env)
    if code != 0 or not out.strip():
        return None, None
    first = out.splitlines()[0]
    meta, _tab, path_part = first.partition("\t")
    if path_part != rel:
        return None, None
    parts = meta.split()
    if len(parts) < 2:
        return None, None
    return parts[1], parts[0]


def apply_op_to_index(repo_root: Path, op: Dict[str, Any], env: Dict[str, str]) -> None:
    kind = op["op"]
    path = op["path"]
    if kind in {"create", "modify"}:
        run_git(
            repo_root, "update-index", "--add", "--cacheinfo",
            f"{op['after_mode']},{op['after_oid']},{path}",
            env=env,
        )
    elif kind == "delete":
        run_git(repo_root, "update-index", "--force-remove", "--", path, env=env)
    elif kind == "rename":
        old_path = op.get("old_path")
        if old_path:
            run_git(repo_root, "update-index", "--force-remove", "--", old_path, env=env)
        run_git(
            repo_root, "update-index", "--add", "--cacheinfo",
            f"{op['after_mode']},{op['after_oid']},{path}",
            env=env,
        )
    else:
        raise RuntimeError(f"unsupported op: {kind}")


def reconcile_live_index(repo_root: Path, paths: List[str]) -> None:
    """After update-ref moves the branch, the live (non-worker) index still
    reflects the old HEAD. Reset those paths in the live index so `git status`
    is consistent. Working tree files are left untouched.
    """
    if not paths:
        return
    # git reset [--] <paths> resets index entries to match HEAD for those paths.
    cmd = ["reset", "-q", "--"] + sorted(set(paths))
    code, _out, err = maybe_git(repo_root, *cmd)
    if code != 0:
        debug(f"reconcile_live_index soft-failed: {err}")


# --------------------------------------------------------------------------- #
# DB helpers
# --------------------------------------------------------------------------- #


def open_db(git_dir: Path) -> sqlite3.Connection:
    db_path = git_dir / DB_SUBPATH
    if not db_path.exists():
        raise RuntimeError(f"no snapshot database at {db_path}")
    conn = sqlite3.connect(str(db_path), timeout=10.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def fetch_pending(conn: sqlite3.Connection, branch: str) -> List[sqlite3.Row]:
    return conn.execute(
        """SELECT seq, branch_ref, base_head, session_id, tool_name, source, captured_ts
           FROM events
           WHERE state='pending' AND branch_ref=?
           ORDER BY seq""",
        (branch,),
    ).fetchall()


def fetch_ops(conn: sqlite3.Connection, event_seq: int) -> List[sqlite3.Row]:
    return conn.execute(
        """SELECT ord, op, path, old_path, before_oid, before_mode, after_oid, after_mode
           FROM event_ops WHERE event_seq=? ORDER BY ord""",
        (event_seq,),
    ).fetchall()


def latest_enqueue(conn: sqlite3.Connection) -> float:
    row = conn.execute(
        "SELECT last_enqueue_ts FROM worker_state WHERE id=1"
    ).fetchone()
    return float(row["last_enqueue_ts"] or 0.0) if row else 0.0


def update_heartbeat(conn: sqlite3.Connection, pid: int) -> None:
    conn.execute(
        "UPDATE worker_state SET pid=?, heartbeat_ts=? WHERE id=1",
        (pid, time.time()),
    )


def clear_worker_state(conn: sqlite3.Connection) -> None:
    try:
        conn.execute(
            "UPDATE worker_state SET pid=0, heartbeat_ts=? WHERE id=1",
            (time.time(),),
        )
    except Exception:  # noqa: BLE001
        pass


def mark_published(conn: sqlite3.Connection, seq: int, commit_oid: str) -> None:
    conn.execute(
        "UPDATE events SET state='published', commit_oid=?, settled_ts=? WHERE seq=?",
        (commit_oid, time.time(), seq),
    )


def mark_blocked(conn: sqlite3.Connection, seq: int, reason: str) -> None:
    conn.execute(
        "UPDATE events SET state='blocked_conflict', error=?, settled_ts=? WHERE seq=?",
        (reason, time.time(), seq),
    )


def mark_failed(conn: sqlite3.Connection, seq: int, reason: str) -> None:
    conn.execute(
        "UPDATE events SET state='failed', error=?, settled_ts=? WHERE seq=?",
        (reason, time.time(), seq),
    )


def reset_tails_for_paths(conn: sqlite3.Connection, branch: str, paths: List[str]) -> None:
    for path in paths:
        conn.execute(
            "DELETE FROM path_tail WHERE branch_ref=? AND path=?",
            (branch, path),
        )


# --------------------------------------------------------------------------- #
# Commit message generation
# --------------------------------------------------------------------------- #


def decode_blob_text(data: bytes) -> Optional[str]:
    if b"\x00" in data:
        return None
    return data.decode("utf-8", errors="replace")


def blob_bytes(repo_root: Path, oid: Optional[str]) -> bytes:
    if not oid:
        return b""
    proc = subprocess.run(
        ["git", "cat-file", "-p", oid],
        cwd=str(repo_root),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if proc.returncode != 0:
        return b""
    return proc.stdout


def op_diff_text(repo_root: Path, op: Dict[str, Any]) -> str:
    kind = op["op"]
    if kind == "create":
        before_label, after_label = "/dev/null", op["path"]
        before_bytes, after_bytes = b"", blob_bytes(repo_root, op.get("after_oid"))
    elif kind == "modify":
        before_label = after_label = op["path"]
        before_bytes = blob_bytes(repo_root, op.get("before_oid"))
        after_bytes = blob_bytes(repo_root, op.get("after_oid"))
    elif kind == "delete":
        before_label, after_label = op["path"], "/dev/null"
        before_bytes = blob_bytes(repo_root, op.get("before_oid"))
        after_bytes = b""
    else:  # rename
        before_label = op.get("old_path") or op["path"]
        after_label = op["path"]
        before_bytes = blob_bytes(repo_root, op.get("before_oid"))
        after_bytes = blob_bytes(repo_root, op.get("after_oid"))

    before_text = decode_blob_text(before_bytes)
    after_text = decode_blob_text(after_bytes)
    if before_text is None or after_text is None:
        return "<binary content changed>"

    diff = list(
        difflib.unified_diff(
            before_text.splitlines(),
            after_text.splitlines(),
            fromfile=before_label,
            tofile=after_label,
            lineterm="",
            n=3,
        )
    )
    if not diff:
        return "<no textual diff>"
    return "\n".join(diff)[:4000]


def deterministic_message(event: sqlite3.Row, ops: List[Dict[str, Any]]) -> str:
    if len(ops) == 1:
        op = ops[0]
        kind = op["op"]
        if kind == "create":
            subject = f"Add {op['path']}"
        elif kind == "modify":
            subject = f"Update {op['path']}"
        elif kind == "delete":
            subject = f"Remove {op['path']}"
        else:
            subject = f"Rename {op.get('old_path')} to {op['path']}"
    else:
        subject = f"Update {len(ops)} files"
    subject = subject[:50].rstrip()
    lines = [subject, ""]
    for op in ops[:10]:
        if op["op"] == "rename":
            lines.append(f"- Rename {op.get('old_path')} -> {op['path']}")
        else:
            lines.append(f"- {op['op'].title()} {op['path']}")
    tool = event["tool_name"] or "unknown"
    lines.append(f"- Snapshot seq: {event['seq']} tool: {tool}")
    return "\n".join(lines)


def sanitize_message(text: str) -> str:
    raw = [line.rstrip() for line in text.splitlines()]
    lines = [line for line in raw if line.strip()]
    if not lines:
        return "Update files"
    subject = re.sub(r"^[\-*\s]+", "", lines[0]).strip().rstrip(".")
    subject = subject[:50].rstrip() or "Update files"
    body: List[str] = []
    current: Optional[str] = None
    for line in lines[1:]:
        stripped = line.strip()
        if not stripped:
            continue
        if re.match(r"^[\-*]\s+", stripped):
            if current:
                body.append(current)
            current = re.sub(r"^[\-*\s]+", "", stripped).strip()
        else:
            current = f"{current} {stripped}".strip() if current else stripped
    if current:
        body.append(current)
    if not body:
        return subject
    wrapped: List[str] = []
    for bullet in body:
        wrapped.extend(
            textwrap.wrap(
                bullet, width=72, initial_indent="- ", subsequent_indent="  ",
                break_long_words=False, break_on_hyphens=False,
            )
        )
    return subject + "\n\n" + "\n".join(wrapped)


def ai_message_via_command(
    repo_root: Path,
    event: sqlite3.Row,
    ops: List[Dict[str, Any]],
) -> Optional[str]:
    if not COMMIT_CMD:
        return None
    payload = {
        "seq": event["seq"],
        "branch_ref": event["branch_ref"],
        "tool_name": event["tool_name"] or "",
        "source": event["source"] or "",
        "ops": [
            {
                "op": op["op"],
                "path": op["path"],
                "old_path": op.get("old_path"),
                "diff": op_diff_text(repo_root, op),
            }
            for op in ops
        ],
    }
    try:
        proc = subprocess.run(
            COMMIT_CMD,
            shell=True,
            input=json.dumps(payload).encode("utf-8"),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=OPENAI_API_TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        debug("commit message command timed out")
        return None
    if proc.returncode != 0:
        debug(f"commit message command failed: {proc.stderr.decode('utf-8','replace')[:200]}")
        return None
    text = proc.stdout.decode("utf-8", errors="replace").strip()
    if not text:
        return None
    return sanitize_message(text)


def ai_message_via_openai(
    repo_root: Path,
    event: sqlite3.Row,
    ops: List[Dict[str, Any]],
) -> Optional[str]:
    if not OPENAI_API_KEY:
        return None
    diffs = "\n\n".join(
        f"### {op['op']} {op['path']}\n{op_diff_text(repo_root, op)}" for op in ops[:5]
    )
    user_prompt = (
        f"Tool: {event['tool_name'] or 'unknown'}\n"
        f"Branch: {event['branch_ref']}\n"
        f"Paths: {', '.join(op['path'] for op in ops)}\n\n"
        f"Diffs:\n{diffs}\n\nGenerate the commit message."
    )
    payload = {
        "model": OPENAI_MODEL,
        "messages": [
            {"role": "system", "content": AI_SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.3,
        "max_tokens": 220,
    }
    req = urllib_request.Request(
        OPENAI_BASE_URL.rstrip("/") + "/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {OPENAI_API_KEY}",
        },
        method="POST",
    )
    try:
        with urllib_request.urlopen(req, timeout=OPENAI_API_TIMEOUT) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except (urllib_error.URLError, TimeoutError) as exc:
        debug(f"openai request failed: {exc}")
        return None
    try:
        parsed = json.loads(raw)
        content = parsed["choices"][0]["message"]["content"]
    except (KeyError, IndexError, json.JSONDecodeError) as exc:
        debug(f"openai response parse failed: {exc}")
        return None
    return sanitize_message(content)


def build_message(
    repo_root: Path,
    event: sqlite3.Row,
    ops: List[Dict[str, Any]],
) -> str:
    for provider in (ai_message_via_command, ai_message_via_openai):
        try:
            msg = provider(repo_root, event, ops)
            if msg:
                return msg
        except Exception as exc:  # noqa: BLE001
            debug(f"{provider.__name__} errored: {exc}")
    return deterministic_message(event, ops)


# --------------------------------------------------------------------------- #
# Replay / publish
# --------------------------------------------------------------------------- #


def ops_as_dicts(rows: List[sqlite3.Row]) -> List[Dict[str, Any]]:
    return [
        {
            "ord": r["ord"],
            "op": r["op"],
            "path": r["path"],
            "old_path": r["old_path"],
            "before_oid": r["before_oid"],
            "before_mode": r["before_mode"],
            "after_oid": r["after_oid"],
            "after_mode": r["after_mode"],
        }
        for r in rows
    ]


def verify_op_applies(
    repo_root: Path, op: Dict[str, Any], env: Dict[str, str]
) -> Optional[str]:
    kind = op["op"]
    if kind == "create":
        cur_oid, _mode = index_entry(repo_root, op["path"], env)
        if cur_oid is not None and cur_oid != op["after_oid"]:
            return f"create target already exists with different content: {op['path']}"
        return None
    if kind == "modify":
        cur_oid, cur_mode = index_entry(repo_root, op["path"], env)
        if cur_oid != op["before_oid"] or cur_mode != op["before_mode"]:
            return f"modify before-state mismatch for {op['path']}"
        return None
    if kind == "delete":
        cur_oid, _mode = index_entry(repo_root, op["path"], env)
        if cur_oid != op["before_oid"]:
            return f"delete before-state mismatch for {op['path']}"
        return None
    if kind == "rename":
        old_oid, old_mode = index_entry(repo_root, op["old_path"] or "", env)
        if old_oid != op["before_oid"] or old_mode != op["before_mode"]:
            return f"rename source mismatch for {op.get('old_path')}"
        new_oid, _mode = index_entry(repo_root, op["path"], env)
        if new_oid is not None:
            return f"rename target already present: {op['path']}"
        return None
    return f"unknown op: {kind}"


def replay_batch(
    conn: sqlite3.Connection,
    repo_root: Path,
    git_dir: Path,
    branch: str,
) -> int:
    """Replay all pending events for the branch onto current HEAD. Returns the
    number of commits published. Returns -1 if a CAS conflict suggests retry.
    """
    events = fetch_pending(conn, branch)
    if not events:
        return 0

    if repo_special_state(git_dir):
        debug("repo in special state; deferring")
        return 0

    head = current_head(repo_root)
    if head is None:
        debug("could not read HEAD; deferring")
        return 0
    live_branch = current_branch(repo_root)
    if live_branch != branch:
        debug(f"branch changed from {branch} to {live_branch}; deferring this branch")
        return 0

    index_file = git_dir / INDEX_SUBPATH
    index_file.parent.mkdir(parents=True, exist_ok=True)
    if index_file.exists():
        index_file.unlink()
    env = os.environ.copy()
    env["GIT_INDEX_FILE"] = str(index_file)

    try:
        run_git(repo_root, "read-tree", head, env=env)
    except RuntimeError as exc:
        debug(f"read-tree failed: {exc}")
        return 0

    commits: List[Tuple[int, str, List[Dict[str, Any]]]] = []
    blocked: List[Tuple[int, str, List[Dict[str, Any]]]] = []
    failed: List[Tuple[int, str]] = []
    parent = head

    for event in events:
        op_rows = fetch_ops(conn, event["seq"])
        ops = ops_as_dicts(op_rows)
        if not ops:
            failed.append((event["seq"], "no ops"))
            continue

        reason: Optional[str] = None
        for op in ops:
            reason = verify_op_applies(repo_root, op, env)
            if reason is not None:
                break
        if reason is not None:
            blocked.append((event["seq"], reason, ops))
            continue

        try:
            for op in ops:
                apply_op_to_index(repo_root, op, env)
            tree = run_git(repo_root, "write-tree", env=env).strip()
            message = build_message(repo_root, event, ops)
            commit_oid = run_git(
                repo_root, "commit-tree", tree, "-p", parent,
                input_bytes=message.encode("utf-8"),
                env=env,
            ).strip()
        except RuntimeError as exc:
            failed.append((event["seq"], str(exc)))
            # Reset index back to parent for next event
            try:
                run_git(repo_root, "read-tree", parent, env=env)
            except RuntimeError:
                pass
            continue

        commits.append((event["seq"], commit_oid, ops))
        parent = commit_oid

    try:
        index_file.unlink()
    except OSError:
        pass

    if not commits:
        if blocked or failed:
            settle_results(conn, branch, [], blocked, failed)
        return 0

    code, _out, err = maybe_git(repo_root, "update-ref", branch, parent, head)
    if code != 0:
        debug(f"update-ref CAS failed ({err}); will retry")
        return -1

    settle_results(conn, branch, commits, blocked, failed)
    paths_to_reset: List[str] = []
    for _seq, _oid, ops in commits:
        for op in ops:
            paths_to_reset.append(op["path"])
            if op["op"] == "rename" and op.get("old_path"):
                paths_to_reset.append(op["old_path"])
    reconcile_live_index(repo_root, paths_to_reset)
    debug(f"published {len(commits)} commit(s) to {branch}: tip={parent}")
    return len(commits)


def settle_results(
    conn: sqlite3.Connection,
    branch: str,
    commits: List[Tuple[int, str, List[Dict[str, Any]]]],
    blocked: List[Tuple[int, str, List[Dict[str, Any]]]],
    failed: List[Tuple[int, str]],
) -> None:
    conn.execute("BEGIN IMMEDIATE")
    try:
        for seq, commit_oid, _ops in commits:
            mark_published(conn, seq, commit_oid)
        blocked_paths: List[str] = []
        for seq, reason, ops in blocked:
            mark_blocked(conn, seq, reason)
            for op in ops:
                blocked_paths.append(op["path"])
                if op["op"] == "rename" and op.get("old_path"):
                    blocked_paths.append(op["old_path"])
        for seq, reason in failed:
            mark_failed(conn, seq, reason)
        if blocked_paths:
            reset_tails_for_paths(conn, branch, blocked_paths)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


# --------------------------------------------------------------------------- #
# Main loop
# --------------------------------------------------------------------------- #


def pending_count(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM events WHERE state='pending'"
    ).fetchone()
    return int(row["n"] if row else 0)


def worker_loop(repo_root: Path, git_dir: Path) -> int:
    conn = open_db(git_dir)
    try:
        update_heartbeat(conn, os.getpid())
        idle_since: Optional[float] = None

        while True:
            update_heartbeat(conn, os.getpid())
            now = time.time()
            last_enq = latest_enqueue(conn)

            # Quiet-window gate: don't process until enqueues have settled.
            if now - last_enq < QUIET_SECONDS:
                time.sleep(POLL_SECONDS)
                idle_since = None
                continue

            branch = current_branch(repo_root)
            if branch is None:
                time.sleep(POLL_SECONDS)
                continue

            pending = fetch_pending(conn, branch)
            if not pending:
                if pending_count(conn) == 0:
                    if idle_since is None:
                        idle_since = now
                    if now - idle_since >= IDLE_SECONDS and now - last_enq >= IDLE_SECONDS:
                        debug("idle timeout reached, exiting")
                        return 0
                time.sleep(POLL_SECONDS)
                continue

            idle_since = None
            result = replay_batch(conn, repo_root, git_dir, branch)
            if result == -1:
                time.sleep(POLL_SECONDS)
                continue

            # Brief pause to batch subsequent events together.
            time.sleep(POLL_SECONDS)
    finally:
        clear_worker_state(conn)
        conn.close()


def run_worker(repo_root: Path, git_dir: Path) -> int:
    lock = Singleton(git_dir / LOCK_SUBPATH)
    if not lock.acquire(attempts=10, sleep=0.05):
        debug("another worker holds the lock; exiting")
        return 0
    try:
        signal.signal(signal.SIGUSR1, _on_wake)
        signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
        return worker_loop(repo_root, git_dir)
    finally:
        lock.release()


# --------------------------------------------------------------------------- #
# Commands: status / flush
# --------------------------------------------------------------------------- #


def cmd_status(git_dir: Path) -> int:
    conn = open_db(git_dir)
    try:
        counts = {
            state: conn.execute(
                "SELECT COUNT(*) AS n FROM events WHERE state=?", (state,)
            ).fetchone()["n"]
            for state in ("pending", "published", "blocked_conflict", "failed")
        }
        worker_row = conn.execute(
            "SELECT pid, heartbeat_ts, last_enqueue_ts FROM worker_state WHERE id=1"
        ).fetchone()
        tails = conn.execute("SELECT COUNT(*) AS n FROM path_tail").fetchone()["n"]
        print(json.dumps({
            "db": str(git_dir / DB_SUBPATH),
            "counts": counts,
            "path_tails": tails,
            "worker": {
                "pid": worker_row["pid"] if worker_row else 0,
                "heartbeat_ts": worker_row["heartbeat_ts"] if worker_row else 0,
                "last_enqueue_ts": worker_row["last_enqueue_ts"] if worker_row else 0,
            },
        }, indent=2))
        return 0
    finally:
        conn.close()


def cmd_flush(repo_root: Path, git_dir: Path) -> int:
    lock = Singleton(git_dir / LOCK_SUBPATH)
    if not lock.acquire(attempts=20, sleep=0.1):
        print("another worker is running; could not acquire lock", file=sys.stderr)
        return 2
    try:
        conn = open_db(git_dir)
        try:
            branch = current_branch(repo_root)
            if branch is None:
                return 1
            for _ in range(10):
                result = replay_batch(conn, repo_root, git_dir, branch)
                if result <= 0:
                    break
            return 0
        finally:
            conn.close()
    finally:
        lock.release()


def resolve_git_dir(repo: Path, explicit_git_dir: Optional[Path]) -> Path:
    if explicit_git_dir is not None:
        return explicit_git_dir
    out = run_git(repo, "rev-parse", "--absolute-git-dir")
    return Path(out.strip())


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Snapshot autocommit worker")
    parser.add_argument("--repo", required=False, help="repo path (working directory)")
    parser.add_argument("--git-dir", required=False, help="explicit git dir override")
    parser.add_argument("--status", action="store_true", help="print queue status and exit")
    parser.add_argument("--flush", action="store_true", help="drain queue immediately and exit")
    args = parser.parse_args(argv)

    repo_input = Path(args.repo).expanduser() if args.repo else Path(os.getcwd())
    try:
        repo_root = Path(run_git(repo_input, "rev-parse", "--show-toplevel")).resolve()
        git_dir = (
            Path(args.git_dir).expanduser().resolve() if args.git_dir
            else resolve_git_dir(repo_input, None)
        )
    except RuntimeError as exc:
        print(f"not a git repository: {exc}", file=sys.stderr)
        return 1

    try:
        if args.status:
            return cmd_status(git_dir)
        if args.flush:
            return cmd_flush(repo_root, git_dir)
        return run_worker(repo_root, git_dir)
    except Exception as exc:  # noqa: BLE001
        debug(f"worker fatal: {exc}")
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
