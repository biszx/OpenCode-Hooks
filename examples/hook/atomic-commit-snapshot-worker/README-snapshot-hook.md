# Snapshot autocommit, SQLite version

This setup gives you one hook, one worker, and one SQLite queue per worktree.
It watches file edits, snapshots them into git objects, then replays them as
real commits after a short quiet window.

If you want the short version:

- `snapshot-hook.py` captures file changes and queues them.
- `snapshot-worker.py` drains the queue and publishes commits.
- Each worktree gets its own isolated state under its private git dir.
- Built-in AI commit messages are optional. If AI is off or skipped, the worker
  falls back to deterministic messages.

## What it does

There are two files:

- `snapshot-hook.py`
  - Reads hook payload JSON from stdin
  - Figures out which files changed
  - Hashes current file contents into git blobs
  - Writes an event plus `path_tail` updates in one `BEGIN IMMEDIATE`
    transaction
  - Wakes the live worker with `SIGUSR1`, or spawns one if needed

- `snapshot-worker.py`
  - Waits for a quiet window
  - Replays pending events onto the current branch tip using a temporary git
    index
  - Publishes with `git update-ref` compare-and-swap
  - Uses a two-phase publish so crashes can be recovered on restart

Why split it this way? The hook stays cheap and fast. It just captures state.
The worker does the expensive part later, once edits settle down.

## Where state lives

Put `snapshot-hook.py` and `snapshot-worker.py` in the same directory. By
default, the hook finds the worker via `__file__`. If you need to override it,
set:

```bash
export SNAPSHOTD_WORKER_PATH=/abs/path/to/snapshot-worker.py
```

Make both scripts executable:

```bash
chmod +x snapshot-hook.py snapshot-worker.py
```

Each worktree stores its own state inside its private git dir.

Examples:

- main worktree: `/path/to/repo/.git`
- linked worktree: `/path/to/repo/.git/worktrees/<name>`

The hook and worker create these paths automatically:

| Path | Purpose |
|------|---------|
| `<git-dir>/ai-snapshotd/snapshotd.db` | SQLite queue, WAL mode |
| `<git-dir>/ai-snapshotd/worker.lock` | Singleton worker lock |
| `<git-dir>/ai-snapshotd/worker.index` | Temporary git index for replay |
| `<git-dir>/ai-snapshotd/logs/hook.log` | Rotated hook debug log |
| `<git-dir>/ai-snapshotd/logs/worker.log` | Rotated worker debug log |

This isolation matters. Five projects with three worktrees each means fifteen
independent queues and fifteen independent workers, not one shared bottleneck.

## Event states

| State | Meaning |
|------|---------|
| `pending` | Captured and waiting to replay |
| `publishing` | Commit objects are built and `update-ref` is next |
| `published` | The branch now includes the event |
| `blocked_conflict` | Replay could not apply cleanly against current state |
| `failed` | Hard error during replay or commit creation |

`publishing` is intentionally transient. If the worker crashes between
`update-ref` and DB settlement, startup recovery checks whether the target
commit made it into history and fixes the state from there.

## Concurrency guarantees

- **Same-path hooks are serialized**
  - `path_tail` reads and writes happen inside one `BEGIN IMMEDIATE`
    transaction.
  - Two hooks racing on the same file do not capture the same `before` state.

- **Only one worker drains a worktree**
  - The worker takes a flock on `worker.lock`.
  - If another worker starts, it exits.
  - The hook prefers signalling a live worker over spawning a new one.

- **Worktrees stay isolated**
  - Different git dirs mean different DBs, locks, logs, and workers.
  - No cross-worktree contention.

- **Branch-specific replay**
  - The worker only processes pending events for the currently checked out
    branch.
  - Events for other branches stay pending until that branch is active again.
  - If a branch is gone, pending events for it become `blocked_conflict`.

- **Crash recovery is explicit**
  - Leftover `publishing` rows are reconciled on startup.
  - If the target commit is already an ancestor of the branch tip, the event is
    marked `published`.
  - Otherwise it goes back to `pending`.

## Wiring

### Claude Code

Add this to `~/.claude/settings.json` or `.claude/settings.json` in the repo:

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

Add this to `~/.config/opencode/opencode.json`:

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

Codex CLI currently emits `Bash` PostToolUse events only. That means this hook
can do best-effort capture for shell-driven edits, but not exact per-edit
capture for every tool. If you need exact per-edit capture, use Codex
app-server or a shared MCP edit surface.

## Environment variables

| Variable | Default | What it controls |
|----------|---------|------------------|
| `SNAPSHOTD_QUIET_SECONDS` | `1.0` | How long the worker waits after the last enqueue before replay starts |
| `SNAPSHOTD_IDLE_SECONDS` | `30.0` | How long the worker stays alive with no work on the current branch |
| `SNAPSHOTD_POLL_SECONDS` | `0.35` | Poll interval while waiting |
| `SNAPSHOTD_HEARTBEAT_STALE` | `15.0` | Age after which a worker heartbeat is treated as dead |
| `SNAPSHOTD_AI_ENABLE` | off | Enables built-in AI commit messages |
| `SNAPSHOTD_AI_MAX_QUEUE_DEPTH` | `2` | Backlog depth above which built-in AI batching is skipped |
| `SNAPSHOTD_AI_CHUNK_SIZE` | `20` | Max events per structured-output AI request, clamped to `1..100` |
| `SNAPSHOTD_COMMIT_MESSAGE_CMD` | unset | Custom argv-style message command, run per event |
| `SNAPSHOTD_SENSITIVE_GLOBS` | `.env,*.pem,*.key,…` | Paths whose diffs are redacted before any network call |
| `SNAPSHOTD_RETENTION_SECONDS` | `604800` | How long settled rows are kept before pruning |
| `SNAPSHOTD_LOG_MAX_BYTES` / `_KEEP` | `2 MiB` / `3` | Log rotation threshold and retained files |
| `SNAPSHOTD_DEBUG` | off | Writes debug logs under `<git-dir>/ai-snapshotd/logs/` |
| `SNAPSHOTD_WORKER_PATH` | sibling file | Override worker script path |
| `OPENAI_API_KEY` / `OPENAI_BASE_URL` | unset / OpenAI default | Required for built-in AI mode. Base URL must be `https://` |
| `OPENAI_MODEL` | `gpt-5.4-mini` | Model used for built-in AI mode |
| `OPENAI_API_TIMEOUT` | `15` | Network timeout in seconds |

If neither `SNAPSHOTD_COMMIT_MESSAGE_CMD` nor `SNAPSHOTD_AI_ENABLE=1` is set,
the worker writes deterministic commit messages.

## Batch message generation

Built-in AI now works as a pre-pass, not a per-event network call.

When AI is enabled and the backlog is at or below
`SNAPSHOTD_AI_MAX_QUEUE_DEPTH`, the worker does this before replaying commits:

1. Find events whose `events.message` is still `NULL`
2. Split them into chunks of `SNAPSHOTD_AI_CHUNK_SIZE`
3. Send one structured-output request per chunk
4. Persist returned messages into `events.message`
5. Build commits from stored messages, falling back to deterministic messages
   for any event whose chunk failed

Why this design is better:

- It cuts prompt overhead on bursts because one request covers many events.
- It keeps replay predictable because generated messages are stored before the
  commit loop.
- It degrades cleanly. One bad chunk falls back to deterministic messages for
  that chunk only.

Each request uses a JSON schema. The model has to return a `messages` array,
keyed by event `seq`. Sensitive-path redaction still happens per event before
anything leaves the machine.

### Tuning guidance

| Setting | Good starting point | Why |
|---------|---------------------|-----|
| `SNAPSHOTD_AI_CHUNK_SIZE` | `20` | Large enough to amortize prompt overhead, small enough to keep responses manageable |
| `SNAPSHOTD_AI_MAX_QUEUE_DEPTH` | `50` or `100` | With chunking, 100 queued events means 5 requests at chunk size 20, not 100 requests |
| `SNAPSHOTD_COMMIT_MESSAGE_CMD` | leave unset unless you need custom logic | Custom command generation still runs once per event, so it does not benefit from batching |

If you are still tuning the system, start with built-in batching. It is cheaper
and simpler than a custom per-event command.

## Operating commands

```bash
# Show queue status for a repo
python3 snapshot-worker.py --status --repo /path/to/repo

# Drain pending events synchronously
# Exit 0 if the current-branch queue is empty, 2 if events remain
python3 snapshot-worker.py --flush --repo /path/to/repo

# Run the worker in the foreground
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

## Security notes

- `SNAPSHOTD_COMMIT_MESSAGE_CMD` is parsed with `shlex.split` and executed as a
  plain argv list. No shell involved.
- Built-in OpenAI mode is off by default. To enable it, set both
  `OPENAI_API_KEY` and `SNAPSHOTD_AI_ENABLE=1`.
- `OPENAI_BASE_URL` must use `https://`.
- Diffs for paths matching `SNAPSHOTD_SENSITIVE_GLOBS` are replaced with a
  redaction marker before any network request.
- If backlog depth exceeds `SNAPSHOTD_AI_MAX_QUEUE_DEPTH`, built-in AI is
  skipped for that drain cycle.

The opinionated recommendation here is simple: keep sensitive globs broad, keep
the base URL HTTPS-only, and do not try to be clever about redaction.

## Known limitations

- Unix only. The worker uses advisory `fcntl` locks.
- Some harnesses do not report exact changed files for every tool. In those
  cases, capture is best-effort.
- `blocked_conflict` events are recorded, not auto-retried.
- After a successful publish, the worker resets only the paths it just
  committed, and only when the live index still matches the pre-publish HEAD.
  If you staged different content for the same paths, it leaves that alone.

## Related files

- `snapshot-hook.py`
- `snapshot-worker.py`
