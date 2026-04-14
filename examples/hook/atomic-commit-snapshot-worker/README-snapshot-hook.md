# Snapshot autocommit — SQLite version

Two files, one SQLite database per worktree, one singleton worker per
worktree. Works the same way regardless of which harness (Claude Code,
OpenCode, Codex, …) fires the hook, as long as the harness sends a tool
payload to stdin.

## What the two files do

- `snapshot-hook.py` — ingress. Reads the harness's JSON on stdin, extracts
  changed file paths, hashes each file into a git blob, and atomically
  inserts an event row + updates `path_tail` inside one `BEGIN IMMEDIATE`
  transaction. Then either sends `SIGUSR1` to the live worker or spawns a
  new one.
- `snapshot-worker.py` — egress. Singleton per worktree. Waits for a quiet
  window, replays pending events onto the current branch tip by batch-
  applying ops to a temp git index, publishes with a compare-and-swap
  `git update-ref`, and exits after an idle timeout. Publish is two-phase:
  events are marked `publishing` with a target commit OID before the
  branch is moved, so a crash in the middle can be reconciled on restart.

## Where files live

### Scripts

Put `snapshot-hook.py` and `snapshot-worker.py` in the same directory. The
hook finds the worker via `__file__` by default. You can override with
`SNAPSHOTD_WORKER_PATH=/abs/path/to/snapshot-worker.py`.

Make them executable:

```bash
chmod +x snapshot-hook.py snapshot-worker.py
```

### Per-worktree state

Each worktree gets its own isolated state directory inside its private git
dir. `git rev-parse --absolute-git-dir` returns:

- main worktree: `/path/to/repo/.git`
- linked worktree: `/path/to/repo/.git/worktrees/<name>`

The hook and worker auto-create:

- `<git-dir>/ai-snapshotd/snapshotd.db`      — SQLite journal (WAL mode)
- `<git-dir>/ai-snapshotd/worker.lock`       — singleton flock
- `<git-dir>/ai-snapshotd/worker.index`      — scratch git index
- `<git-dir>/ai-snapshotd/logs/hook.log`     — hook debug log (rotated)
- `<git-dir>/ai-snapshotd/logs/worker.log`   — worker debug log (rotated)

Open 5 projects × 3 worktrees and you automatically get 15 independent
journals and 15 independent workers.

## States an event can be in

| State              | Meaning                                                                                       |
|--------------------|-----------------------------------------------------------------------------------------------|
| `pending`          | Captured, not yet replayed                                                                    |
| `publishing`       | Commit-tree objects built, about to call update-ref. Transient. Reconciled on worker restart. |
| `published`        | Branch has been moved to include this event                                                    |
| `blocked_conflict` | Can't replay cleanly: `before` state doesn't match current index for one of its paths          |
| `failed`           | Hard error during build (commit-tree failed, etc.). Path tails cleared.                        |

## Concurrency guarantees

- **Concurrent hooks for the same path** — serialized. All path_tail reads
  and writes happen inside one `BEGIN IMMEDIATE`, with a CAS retry on the
  observed `source_seq`. Two hooks racing on the same file cannot capture
  the same `before` state.
- **Concurrent worker invocations** — one wins the flock, others exit. The
  hook prefers `SIGUSR1` to an already-alive worker (heartbeat within
  `SNAPSHOTD_HEARTBEAT_STALE` seconds) over spawning a new Python process.
- **Multiple worktrees** — separate git dirs → separate DBs → separate
  workers. Zero cross-contention.
- **Branch switching** — worker only processes events for the current
  branch, and its idle exit condition only considers current-branch
  pending. Events on other branches stay `pending` until someone checks
  out that branch. Events on branches that no longer exist get marked
  `blocked_conflict` at startup.
- **Crash between update-ref and DB settlement** — leftover `publishing`
  rows are reconciled on worker restart: if the target commit is an
  ancestor of the branch tip, mark `published`; otherwise back to
  `pending`.

## Wiring

### Claude Code

`~/.claude/settings.json` or `.claude/settings.json` in the project:

```json
{
  "hooks": {
    "PostToolUse": [
      {
        "matcher": "Write|Edit|MultiEdit|NotebookEdit",
        "hooks": [
          {
            "type": "command",
            "command": "python3 /abs/path/to/snapshot-hook.py",
            "timeout": 15
          }
        ]
      }
    ]
  }
}
```

### OpenCode

`~/.config/opencode/opencode.json`:

```json
{
  "hooks": [
    {
      "id": "snapshot-autocommit",
      "event": "file.changed",
      "async": true,
      "actions": [
        { "bash": "python3 $HOOK_DIR/snapshot-hook.py" }
      ]
    }
  ]
}
```

### Codex

Codex CLI hooks currently emit only `Bash` PostToolUse. Wire the hook
there if you want best-effort scanning of shell-driven edits. Exact per-
edit capture needs Codex app-server or a shared MCP edit surface.

## Environment knobs

| Variable                              | Default                  | Purpose                                                              |
|---------------------------------------|--------------------------|----------------------------------------------------------------------|
| `SNAPSHOTD_QUIET_SECONDS`             | `1.0`                    | Wait this long after the last enqueue before replaying               |
| `SNAPSHOTD_IDLE_SECONDS`              | `30.0`                   | Worker exits after this much idle time on the current branch         |
| `SNAPSHOTD_POLL_SECONDS`              | `0.35`                   | Worker poll interval                                                 |
| `SNAPSHOTD_HEARTBEAT_STALE`           | `15.0`                   | Older heartbeats are treated as dead workers                         |
| `SNAPSHOTD_AI_ENABLE`                 | off                      | Explicit opt-in for the built-in OpenAI commit-message path         |
| `SNAPSHOTD_AI_MAX_QUEUE_DEPTH`        | `2`                      | AI messages are skipped when pending depth exceeds this              |
| `SNAPSHOTD_COMMIT_MESSAGE_CMD`        | unset                    | argv-style command; reads event JSON on stdin, prints a message. No shell. |
| `SNAPSHOTD_SENSITIVE_GLOBS`           | `.env,*.pem,*.key,…`     | Comma-separated globs whose diffs are redacted before AI            |
| `SNAPSHOTD_RETENTION_SECONDS`         | `604800` (7 days)        | Published/blocked/failed rows older than this are pruned on startup |
| `SNAPSHOTD_LOG_MAX_BYTES` / `_KEEP`   | `2 MiB` / `3`            | Log rotation threshold and keep count                                |
| `SNAPSHOTD_DEBUG`                     | off                      | Write to `<git-dir>/ai-snapshotd/logs/*.log`                         |
| `SNAPSHOTD_WORKER_PATH`               | sibling                  | Override the worker script path                                      |
| `OPENAI_API_KEY` / `OPENAI_BASE_URL`  | unset / OpenAI default   | Required for built-in AI path. Base URL must be `https://`.          |
| `OPENAI_MODEL`                        | `gpt-5.4-mini`           | Model used when AI is enabled                                        |
| `OPENAI_API_TIMEOUT`                  | `15`                     | Seconds                                                              |

If neither a custom command nor `SNAPSHOTD_AI_ENABLE=1` is set, the worker
writes a clean deterministic message (imperative subject + bullet body).

## Operating commands

```bash
# Queue status for a given worktree
python3 snapshot-worker.py --status --repo /path/to/repo

# Drain pending events synchronously.
# Exits 0 if the current-branch queue is empty, 2 if events remain.
python3 snapshot-worker.py --flush --repo /path/to/repo

# Foreground worker (normally invoked automatically by the hook)
python3 snapshot-worker.py --repo /path/to/repo
```

## Debugging

```bash
GIT_DIR=$(git rev-parse --absolute-git-dir)
tail -n 200 "$GIT_DIR/ai-snapshotd/logs/hook.log"
tail -n 200 "$GIT_DIR/ai-snapshotd/logs/worker.log"
python3 snapshot-worker.py --status --repo .
sqlite3 "$GIT_DIR/ai-snapshotd/snapshotd.db" \
  "SELECT seq, state, branch_ref, tool_name, substr(commit_oid,1,8), error FROM events ORDER BY seq DESC LIMIT 20;"
```

## Notes on security and exfiltration

- `SNAPSHOTD_COMMIT_MESSAGE_CMD` is `shlex.split` + `subprocess.run` with
  a plain argv. No shell.
- The built-in OpenAI path is off by default. To turn it on you must set
  both `OPENAI_API_KEY` and `SNAPSHOTD_AI_ENABLE=1`. Base URL must be
  HTTPS.
- Diffs for paths matching `SNAPSHOTD_SENSITIVE_GLOBS` (`.env`, `*.pem`,
  `*.key`, `secrets/*`, `credentials*`, and so on by default) are
  replaced with a redaction marker before any network call.
- AI messages are also skipped entirely when the backlog depth exceeds
  `SNAPSHOTD_AI_MAX_QUEUE_DEPTH`, both for cost and for not hanging the
  drain on network latency.

## Known limitations

- Unix only. Uses advisory `fcntl` locks.
- Harnesses that don't tell the hook which file changed (e.g. Codex CLI
  PostToolUse for non-Bash tools) cannot be captured exactly.
- `blocked_conflict` events are recorded but not auto-retried. Their path
  tails are cleared so subsequent edits start fresh from HEAD.
- After a successful publish, the worker resets only the paths it just
  committed in the live index, and only when the live index still matches
  the pre-publish HEAD. If you have unrelated staged content for the same
  paths, it is left alone.
