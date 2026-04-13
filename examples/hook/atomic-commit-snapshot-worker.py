#!/usr/bin/env python3
"""
Reliable snapshot-based async autocommit example for OpenCode hooks.

What it does
------------
- Hook mode (`file.changed` on stdin):
  - captures immutable per-change snapshots immediately
  - stores them in a per-repo/worktree spool
  - starts a singleton background worker for that repo/worktree
- Worker mode:
  - drains queued snapshots sequentially under a repo-wide lock
  - creates commits from stored git blobs using a temporary index
  - never stages from the live working tree
  - leaves the user's working tree and index untouched

Why this example exists
-----------------------
This solves the main failure mode of path-based autocommit hooks:
if a file changes several times before the worker can commit, each captured
snapshot still remains committable later because the worker replays stored blob
snapshots rather than reading the current file contents.

Usage
-----
Hook mode (recommended):

  hooks:
    - id: snapshot-atomic-commit
      event: file.changed
      async: true
      scope: main
      conditions: [matchesCodeFiles]
      actions:
        - bash: 'python3 "$HOME/.config/opencode/hook/atomic-commit-snapshot-worker.py"'

Manual commands:

  python3 atomic-commit-snapshot-worker.py --status /path/to/repo
  python3 atomic-commit-snapshot-worker.py --flush /path/to/repo
  python3 atomic-commit-snapshot-worker.py --worker /path/to/repo

Environment
-----------
- OPENCODE_SNAPSHOT_SPOOL_DIR         default: /tmp/opencode-atomic-snapshot
- OPENCODE_SNAPSHOT_QUIET_SECONDS     default: 1.0
- OPENCODE_SNAPSHOT_IDLE_SECONDS      default: 30.0
- OPENCODE_SNAPSHOT_INFLIGHT_TTL      default: 120.0
- OPENCODE_SNAPSHOT_DEBUG             default: off

Notes
-----
- This example intentionally uses deterministic commit messages for reliability
  and speed. It can be extended later to use AI for message generation.
- `rename` events are committed as one deterministic remove+add snapshot unit.
- The worker updates the checked-out branch ref directly via `git update-ref`
  using compare-and-swap semantics. The worktree/index are not rewritten.
"""

from __future__ import annotations

import argparse
import errno
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple


SPOOL_ROOT = Path(
    os.environ.get("OPENCODE_SNAPSHOT_SPOOL_DIR", "/tmp/opencode-atomic-snapshot")
)
QUIET_SECONDS = float(os.environ.get("OPENCODE_SNAPSHOT_QUIET_SECONDS", "1.0"))
IDLE_SECONDS = float(os.environ.get("OPENCODE_SNAPSHOT_IDLE_SECONDS", "30.0"))
INFLIGHT_TTL = float(os.environ.get("OPENCODE_SNAPSHOT_INFLIGHT_TTL", "120.0"))
DEBUG_ENABLED = os.environ.get("OPENCODE_SNAPSHOT_DEBUG", "").lower() not in {
    "",
    "0",
    "false",
    "no",
}
DEBUG_LOG = Path("/tmp/opencode-atomic-snapshot.log")


def debug(message: str) -> None:
    if not DEBUG_ENABLED:
        return
    DEBUG_LOG.parent.mkdir(parents=True, exist_ok=True)
    with DEBUG_LOG.open("a", encoding="utf-8") as fh:
        fh.write(f"[{time.strftime('%H:%M:%S')}] {message}\n")


class GitError(RuntimeError):
    pass


class LockFile:
    def __init__(self, path: Path, blocking: bool = True) -> None:
        self.path = path
        self.blocking = blocking
        self._fh: Optional[Any] = None

    def __enter__(self) -> Any:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.path.open("a+")
        flags = fcntl.LOCK_EX
        if not self.blocking:
            flags |= fcntl.LOCK_NB
        fcntl.flock(self._fh.fileno(), flags)
        return self._fh

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._fh is not None:
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
            self._fh.close()


def run_git(
    repo_root: Path,
    *args: str,
    input_bytes: Optional[bytes] = None,
    env: Optional[Dict[str, str]] = None,
) -> str:
    cmd = ["git", *args]
    proc = subprocess.run(
        cmd,
        cwd=str(repo_root),
        input=input_bytes,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )
    if proc.returncode != 0:
        raise GitError(
            proc.stderr.decode("utf-8", errors="replace").strip()
            or f"git {' '.join(args)} failed"
        )
    return proc.stdout.decode("utf-8", errors="replace").strip()


def maybe_run_git(
    repo_root: Path, *args: str, env: Optional[Dict[str, str]] = None
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
        proc.stdout.decode("utf-8", errors="replace").strip(),
        proc.stderr.decode("utf-8", errors="replace").strip(),
    )


def json_dump(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    tmp.replace(path)


def json_load(path: Path, default: Dict[str, Any]) -> Dict[str, Any]:
    if not path.exists():
        return default.copy()
    return json.loads(path.read_text(encoding="utf-8"))


def monotonic_now() -> float:
    return time.time()


def rel_repo_path(repo_root: Path, candidate: str) -> str:
    candidate_path = Path(candidate)
    if candidate_path.is_absolute():
        resolved = candidate_path.resolve()
    else:
        resolved = (repo_root / candidate_path).resolve()
    return resolved.relative_to(repo_root.resolve()).as_posix()


def git_mode_for_path(path: Path) -> str:
    st = path.lstat()
    if stat.S_ISLNK(st.st_mode):
        return "120000"
    if st.st_mode & 0o111:
        return "100755"
    return "100644"


def store_blob(repo_root: Path, abs_path: Path) -> Tuple[str, str]:
    mode = git_mode_for_path(abs_path)
    if mode == "120000":
        target = os.readlink(abs_path)
        oid = run_git(
            repo_root,
            "hash-object",
            "-w",
            "--stdin",
            input_bytes=target.encode("utf-8"),
        )
        return oid, mode
    oid = run_git(repo_root, "hash-object", "-w", str(abs_path))
    return oid, mode


def ls_tree_entry(
    repo_root: Path, rev: str, rel_path: str
) -> Optional[Tuple[str, str]]:
    code, out, _err = maybe_run_git(repo_root, "ls-tree", rev, "--", rel_path)
    if code != 0 or not out.strip():
        return None
    first = out.splitlines()[0]
    meta, _tab, path_part = first.partition("\t")
    if path_part != rel_path:
        return None
    parts = meta.split()
    if len(parts) < 3:
        return None
    return parts[2], parts[0]


def current_branch_ref(repo_root: Path) -> str:
    ref = run_git(repo_root, "symbolic-ref", "-q", "HEAD")
    if not ref:
        raise GitError("Detached HEAD is not supported by this example")
    return ref


def current_head(repo_root: Path) -> str:
    return run_git(repo_root, "rev-parse", "HEAD")


def repo_paths(repo_root: Path) -> Tuple[Path, str, Path]:
    repo_root = Path(run_git(repo_root, "rev-parse", "--show-toplevel"))
    git_dir = run_git(repo_root, "rev-parse", "--absolute-git-dir")
    key = hashlib.sha256(f"{repo_root}\0{git_dir}".encode("utf-8")).hexdigest()[:24]
    spool = SPOOL_ROOT / key
    return repo_root, git_dir, spool


def init_spool(repo_root: Path) -> Tuple[Path, Dict[str, Any]]:
    repo_root, git_dir, spool = repo_paths(repo_root)
    for sub in ("pending", "inflight", "done", "failed", "tmp-index"):
        (spool / sub).mkdir(parents=True, exist_ok=True)
    meta = {
        "repo_root": str(repo_root),
        "git_dir": git_dir,
        "created_at": monotonic_now(),
    }
    meta_path = spool / "meta.json"
    if not meta_path.exists():
        json_dump(meta_path, meta)
    state_path = spool / "state.json"
    if not state_path.exists():
        json_dump(
            state_path,
            {
                "last_seq": 0,
                "last_enqueue_ts": 0.0,
                "last_worker_heartbeat": 0.0,
            },
        )
    return spool, meta


@dataclass
class SnapshotEntry:
    seq: int
    ts: float
    repo_root: str
    branch_ref: str
    base_head: str
    tool_name: str
    session_id: str
    change: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "seq": self.seq,
            "ts": self.ts,
            "repo_root": self.repo_root,
            "branch_ref": self.branch_ref,
            "base_head": self.base_head,
            "tool_name": self.tool_name,
            "session_id": self.session_id,
            "change": self.change,
        }


def parse_file_changed_payload(
    repo_root: Path, payload: Dict[str, Any]
) -> List[Dict[str, Any]]:
    changes = payload.get("changes") or []
    normalized: List[Dict[str, Any]] = []
    if changes:
        for item in changes:
            if not isinstance(item, dict):
                continue
            op = item.get("operation")
            if op in {"create", "modify", "delete"} and isinstance(
                item.get("path"), str
            ):
                normalized.append(
                    {"operation": op, "path": rel_repo_path(repo_root, item["path"])}
                )
            elif (
                op == "rename"
                and isinstance(item.get("fromPath"), str)
                and isinstance(item.get("toPath"), str)
            ):
                normalized.append(
                    {
                        "operation": "rename",
                        "fromPath": rel_repo_path(repo_root, item["fromPath"]),
                        "toPath": rel_repo_path(repo_root, item["toPath"]),
                    }
                )
    if normalized:
        return normalized

    files = payload.get("files") or []
    seen = set()
    for path in files:
        if not isinstance(path, str):
            continue
        rel = rel_repo_path(repo_root, path)
        if rel in seen:
            continue
        seen.add(rel)
        normalized.append({"operation": "modify", "path": rel})
    return normalized


def snapshot_change(
    repo_root: Path, base_head: str, change: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    op = change["operation"]
    if op in {"create", "modify"}:
        rel = change["path"]
        abs_path = repo_root / rel
        if not abs_path.exists() and not abs_path.is_symlink():
            debug(f"skip missing path for {op}: {rel}")
            return None
        blob_oid, mode = store_blob(repo_root, abs_path)
        previous = ls_tree_entry(repo_root, base_head, rel)
        return {
            "operation": op,
            "path": rel,
            "blob_oid": blob_oid,
            "mode": mode,
            "previous_blob_oid": previous[0] if previous else None,
            "previous_mode": previous[1] if previous else None,
        }

    if op == "delete":
        rel = change["path"]
        previous = ls_tree_entry(repo_root, base_head, rel)
        return {
            "operation": "delete",
            "path": rel,
            "previous_blob_oid": previous[0] if previous else None,
            "previous_mode": previous[1] if previous else None,
        }

    if op == "rename":
        from_path = change["fromPath"]
        to_path = change["toPath"]
        abs_to = repo_root / to_path
        if not abs_to.exists() and not abs_to.is_symlink():
            debug(f"skip rename with missing target: {from_path} -> {to_path}")
            return None
        old_entry = ls_tree_entry(repo_root, base_head, from_path)
        new_blob_oid, new_mode = store_blob(repo_root, abs_to)
        return {
            "operation": "rename",
            "from_path": from_path,
            "to_path": to_path,
            "old_blob_oid": old_entry[0] if old_entry else None,
            "old_mode": old_entry[1] if old_entry else None,
            "new_blob_oid": new_blob_oid,
            "new_mode": new_mode,
        }

    debug(f"unsupported operation: {op}")
    return None


def deterministic_commit_message(entry: SnapshotEntry) -> str:
    change = entry.change
    op = change["operation"]
    if op == "create":
        title = f"Add {change['path']}"
        body = "- Capture the created file snapshot exactly as reported"
    elif op == "modify":
        title = f"Update {change['path']}"
        body = "- Preserve this file snapshot as its own atomic change"
    elif op == "delete":
        title = f"Remove {change['path']}"
        body = "- Preserve the file deletion as its own atomic change"
    else:
        title = f"Rename {change['from_path']} to {change['to_path']}"
        body = "- Preserve the rename snapshot without reading live files"
    title = title[:72].rstrip()
    return f"{title}\n\n{body}\n- Snapshot seq: {entry.seq}\n- Tool: {entry.tool_name or 'unknown'}"


def enqueue_from_payload(payload: Dict[str, Any]) -> int:
    event = payload.get("event")
    if event != "file.changed":
        debug(f"ignore unsupported event: {event!r}")
        return 0

    cwd = payload.get("cwd") or os.environ.get("OPENCODE_PROJECT_DIR") or os.getcwd()
    repo_root = Path(run_git(Path(cwd), "rev-parse", "--show-toplevel"))
    spool, _meta = init_spool(repo_root)
    branch_ref = current_branch_ref(repo_root)
    base_head = current_head(repo_root)
    changes = parse_file_changed_payload(repo_root, payload)
    if not changes:
        debug("no changes to snapshot")
        return 0

    state_path = spool / "state.json"
    queued = 0
    session_id = str(payload.get("session_id") or "")
    tool_name = str(payload.get("tool_name") or "")

    with LockFile(spool / "enqueue.lock"):
        state = json_load(
            state_path,
            {"last_seq": 0, "last_enqueue_ts": 0.0, "last_worker_heartbeat": 0.0},
        )
        seq = int(state.get("last_seq", 0))

        for item in changes:
            snap = snapshot_change(repo_root, base_head, item)
            if snap is None:
                continue
            seq += 1
            entry = SnapshotEntry(
                seq=seq,
                ts=monotonic_now(),
                repo_root=str(repo_root),
                branch_ref=branch_ref,
                base_head=base_head,
                tool_name=tool_name,
                session_id=session_id,
                change=snap,
            )
            json_dump(spool / "pending" / f"{seq:020d}.json", entry.to_dict())
            queued += 1

        state["last_seq"] = seq
        state["last_enqueue_ts"] = monotonic_now()
        json_dump(state_path, state)

        ensure_worker_running(repo_root, spool)

    debug(f"queued {queued} snapshot item(s)")
    return queued


def pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError as exc:
        return exc.errno == errno.EPERM


def ensure_worker_running(repo_root: Path, spool: Path) -> None:
    pid_path = spool / "worker.pid"
    if pid_path.exists():
        try:
            pid = int(pid_path.read_text(encoding="utf-8").strip())
        except ValueError:
            pid = 0
        if pid_is_alive(pid):
            return

    debug(f"starting worker for {repo_root}")
    proc = subprocess.Popen(
        [sys.executable, str(Path(__file__).resolve()), "--worker", str(repo_root)],
        cwd=str(repo_root),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
        env=os.environ.copy(),
    )
    pid_path.write_text(str(proc.pid), encoding="utf-8")


def list_seq_files(path: Path) -> List[Path]:
    if not path.exists():
        return []
    return sorted([p for p in path.iterdir() if p.suffix == ".json"])


def recover_stale_inflight(spool: Path) -> None:
    now = monotonic_now()
    for path in list_seq_files(spool / "inflight"):
        payload = json.loads(path.read_text(encoding="utf-8"))
        inflight_ts = float(payload.get("inflight_ts") or 0.0)
        if now - inflight_ts < INFLIGHT_TTL:
            continue
        payload.pop("inflight_ts", None)
        json_dump(spool / "pending" / path.name, payload)
        path.unlink(missing_ok=True)
        debug(f"recovered stale inflight item {path.name}")


def first_pending(spool: Path) -> Optional[Path]:
    files = list_seq_files(spool / "pending")
    return files[0] if files else None


def load_entry(path: Path) -> SnapshotEntry:
    data = json.loads(path.read_text(encoding="utf-8"))
    return SnapshotEntry(
        seq=int(data["seq"]),
        ts=float(data["ts"]),
        repo_root=str(data["repo_root"]),
        branch_ref=str(data["branch_ref"]),
        base_head=str(data["base_head"]),
        tool_name=str(data.get("tool_name") or ""),
        session_id=str(data.get("session_id") or ""),
        change=dict(data["change"]),
    )


def mark_done(spool: Path, src: Path, payload: Dict[str, Any]) -> None:
    json_dump(spool / "done" / src.name, payload)
    src.unlink(missing_ok=True)


def mark_failed(spool: Path, src: Path, payload: Dict[str, Any], error: str) -> None:
    payload["error"] = error
    payload["failed_ts"] = monotonic_now()
    json_dump(spool / "failed" / src.name, payload)
    src.unlink(missing_ok=True)


def build_commit(repo_root: Path, spool: Path, entry: SnapshotEntry) -> str:
    index_file = spool / "tmp-index" / f"index-{os.getpid()}"
    index_file.parent.mkdir(parents=True, exist_ok=True)
    if index_file.exists():
        index_file.unlink()
    env = os.environ.copy()
    env["GIT_INDEX_FILE"] = str(index_file)

    branch_ref = current_branch_ref(repo_root)
    if branch_ref != entry.branch_ref:
        raise GitError(
            f"Branch changed from {entry.branch_ref} to {branch_ref}; refusing to replay stale snapshot"
        )

    parent = current_head(repo_root)
    run_git(repo_root, "read-tree", parent, env=env)

    change = entry.change
    op = change["operation"]
    if op in {"create", "modify"}:
        run_git(
            repo_root,
            "update-index",
            "--add",
            "--cacheinfo",
            change["mode"],
            change["blob_oid"],
            change["path"],
            env=env,
        )
    elif op == "delete":
        run_git(
            repo_root, "update-index", "--force-remove", "--", change["path"], env=env
        )
    elif op == "rename":
        run_git(
            repo_root,
            "update-index",
            "--force-remove",
            "--",
            change["from_path"],
            env=env,
        )
        run_git(
            repo_root,
            "update-index",
            "--add",
            "--cacheinfo",
            change["new_mode"],
            change["new_blob_oid"],
            change["to_path"],
            env=env,
        )
    else:
        raise GitError(f"Unsupported operation: {op}")

    tree_oid = run_git(repo_root, "write-tree", env=env)
    commit_message = deterministic_commit_message(entry)
    commit_oid = run_git(
        repo_root,
        "commit-tree",
        tree_oid,
        "-p",
        parent,
        input_bytes=commit_message.encode("utf-8"),
        env=env,
    )
    run_git(repo_root, "update-ref", entry.branch_ref, commit_oid, parent)
    index_file.unlink(missing_ok=True)
    return commit_oid


def worker(repo_root: Path, flush_only: bool = False) -> int:
    spool, _meta = init_spool(repo_root)
    state_path = spool / "state.json"

    try:
        worker_lock = LockFile(spool / "worker.lock", blocking=False)
        worker_lock.__enter__()
    except OSError:
        debug("worker already running")
        return 0

    try:
        (spool / "worker.pid").write_text(str(os.getpid()), encoding="utf-8")
        draining = flush_only
        while True:
            recover_stale_inflight(spool)
            state = json_load(
                state_path,
                {"last_seq": 0, "last_enqueue_ts": 0.0, "last_worker_heartbeat": 0.0},
            )
            state["last_worker_heartbeat"] = monotonic_now()
            json_dump(state_path, state)

            pending_path = first_pending(spool)
            if pending_path is None:
                if (
                    monotonic_now() - float(state.get("last_enqueue_ts") or 0.0)
                    >= IDLE_SECONDS
                ):
                    debug("worker idle timeout reached")
                    return 0
                if flush_only:
                    return 0
                time.sleep(0.35)
                draining = False
                continue

            if not draining and not flush_only:
                newest_enqueue = float(state.get("last_enqueue_ts") or 0.0)
                if monotonic_now() - newest_enqueue < QUIET_SECONDS:
                    time.sleep(0.2)
                    continue
                draining = True

            inflight_path = spool / "inflight" / pending_path.name
            payload = json.loads(pending_path.read_text(encoding="utf-8"))
            payload["inflight_ts"] = monotonic_now()
            json_dump(inflight_path, payload)
            pending_path.unlink(missing_ok=True)

            try:
                entry = load_entry(inflight_path)
                commit_oid = build_commit(repo_root, spool, entry)
                payload["commit_oid"] = commit_oid
                payload["done_ts"] = monotonic_now()
                mark_done(spool, inflight_path, payload)
                debug(f"committed seq={entry.seq} commit={commit_oid}")
            except Exception as exc:  # noqa: BLE001
                debug(f"failed processing {inflight_path.name}: {exc}")
                mark_failed(spool, inflight_path, payload, str(exc))
                if flush_only:
                    return 1
                time.sleep(0.5)
    finally:
        try:
            (spool / "worker.pid").unlink(missing_ok=True)
        except Exception:  # noqa: BLE001
            pass
        worker_lock.__exit__(None, None, None)


def status(repo_root: Path) -> int:
    spool, _meta = init_spool(repo_root)
    pending = len(list_seq_files(spool / "pending"))
    inflight = len(list_seq_files(spool / "inflight"))
    done = len(list_seq_files(spool / "done"))
    failed = len(list_seq_files(spool / "failed"))
    state = json_load(
        spool / "state.json",
        {"last_seq": 0, "last_enqueue_ts": 0.0, "last_worker_heartbeat": 0.0},
    )
    print(
        json.dumps(
            {
                "spool": str(spool),
                "pending": pending,
                "inflight": inflight,
                "done": done,
                "failed": failed,
                "state": state,
            },
            indent=2,
        )
    )
    return 0


def handle_hook_stdin() -> int:
    raw = sys.stdin.read()
    if not raw.strip():
        debug("empty stdin in hook mode")
        return 0
    payload = json.loads(raw)
    queued = enqueue_from_payload(payload)
    debug(f"hook mode complete: queued={queued}")
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Reliable snapshot-based autocommit example"
    )
    parser.add_argument(
        "--worker", metavar="REPO", help="run singleton worker for repo"
    )
    parser.add_argument(
        "--flush", metavar="REPO", help="process pending queue immediately and exit"
    )
    parser.add_argument("--status", metavar="REPO", help="show queue status for repo")
    args = parser.parse_args(argv)

    try:
        if args.worker:
            return worker(Path(args.worker), flush_only=False)
        if args.flush:
            return worker(Path(args.flush), flush_only=True)
        if args.status:
            return status(Path(args.status))
        return handle_hook_stdin()
    except json.JSONDecodeError as exc:
        print(f"Invalid JSON payload: {exc}", file=sys.stderr)
        return 1
    except GitError as exc:
        print(f"git error: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001
        print(f"unexpected error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
