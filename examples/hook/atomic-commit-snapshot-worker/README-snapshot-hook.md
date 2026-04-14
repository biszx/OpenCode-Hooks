# Snapshot autocommit — simple SQLite version

Two files, one SQLite database per worktree, one singleton worker per
worktree. Works the same way regardless of which harness (Claude Code,
OpenCode, Codex, …) fires the hook, as long as the harness sends its tool
payload to stdin.

## What the two files do

- `snapshot-hook.py` — ingress. Reads the harness's JSON on stdin, extracts
  changed file paths, snapshots each file into a git blob, inserts an
  immutable event row into SQLite, and spawns the worker (fire-and-forget).
  It is deliberately fast: parse, snapshot, insert, spawn, exit.
- `snapshot-worker.py` — egress. Singleton per worktree. Waits for a quiet
  window, replays pending events onto the current branch tip by building
  commits with `git commit-tree`, publishes them with a compare-and-swap
  `git update-ref`, and exits after an idle timeout. If the visible branch
  has moved in a way that invalidates a captured event, the event is marked
  `blocked_conflict` rather than being silently dropped.

## Where files live

### Scripts

Put `snapshot-hook.py` and `snapshot-worker.py` in the same directory. The
hook discovers the worker via `__file__` by default. You can also override
with `SNAPSHOTD_WORKER_PATH=/abs/path/to/snapshot-worker.py`.

Common locations:

- Inside this repo: `examples/hook/snapshot-hook.py` +
  `examples/hook/snapshot-worker.py` (where they already are).
- Your dotfiles: `~/.config/claude-code/hooks/`,
  `~/.config/opencode/hook/`, or wherever you keep harness scripts.

Make them executable:

```bash
chmod +x snapshot-hook.py snapshot-worker.py
```

### Per-worktree state

Each worktree gets its own isolated state directory inside its private git
dir. `git rev-parse --absolute-git-dir` returns:

- main worktree: `/path/to/repo/.git`
- linked worktree: `/path/to/repo/.git/worktrees/<name>`

So the hook and worker automatically create:

- `<git-dir>/ai-snapshotd/snapshotd.db`   — SQLite journal (WAL mode)
- `<git-dir>/ai-snapshotd/worker.lock`    — singleton flock
- `<git-dir>/ai-snapshotd/worker.index`   — scratch git index

You never need to configure paths per project. Open 5 projects × 3
worktrees and you automatically get 15 independent journals and 15
independent workers. They share nothing except the git object store, and
that's what Git's concurrency model is already designed for.

## Concurrency model

- **Multiple subagents editing the same file in one worktree** → they all
  hit the same SQLite DB. Each `Write`/`Edit` fires the hook, which
  captures a fresh blob and chains the `before` state off the last
  unpublished snapshot via the `path_tail` table. 10 edits produce 10
  events, and the worker replays them as 10 commits in capture order.
- **Multiple worktrees of the same repo** → separate git dirs →
  separate DBs → separate workers. Zero cross-contention.
- **Multiple projects** → completely independent.
- **Worker already running** → the next hook's spawned worker can't
  acquire the flock, so it exits within ~0.5 s. Cheap.
- **Worker idle** → exits after `SNAPSHOTD_IDLE_SECONDS` (default 30 s)
  with no pending events and no recent enqueues. The next hook respawns
  a fresh worker.
- **Branch moves externally while events are pending** → the affected
  events are marked `blocked_conflict`, their `path_tail` entries are
  cleared, and future captures chain cleanly from the new HEAD.

## Wiring

### OpenCode

OpenCode's plugin events ship richer payloads than its `file.edited`
event, but for a universal script this hook accepts the simpler
`file.changed` shape too. The hook will read `changes[]`, `files[]`, and
`cwd`.

`~/.config/opencode/opencode.json`:

```json
{
  "hooks": [
    {
      "id": "snapshot-autocommit",
      "event": "file.changed",
      "async": true,
      "actions": [
        { "bash": "python3 $HOME/.config/opencode/hook/atomic-commit-snapshot-worker/snapshot-hook.py" }
      ]
    }
  ]
}
```

## Environment knobs

| Variable                            | Default                       | Purpose                                         |
|-------------------------------------|-------------------------------|-------------------------------------------------|
| `SNAPSHOTD_QUIET_SECONDS`           | `1.0`                         | Wait this long after the last enqueue before replaying |
| `SNAPSHOTD_IDLE_SECONDS`            | `30.0`                        | Worker exits after this much idle time          |
| `SNAPSHOTD_POLL_SECONDS`            | `0.35`                        | Worker poll interval                            |
| `SNAPSHOTD_DEBUG`                   | off                           | Logs to `/tmp/snapshotd-hook.log` + `/tmp/snapshotd-worker.log` |
| `SNAPSHOTD_WORKER_PATH`             | sibling file                  | Override the worker script path                 |
| `SNAPSHOTD_COMMIT_MESSAGE_CMD`      | unset                         | Shell command; reads event JSON on stdin, prints a commit message |
| `OPENAI_API_KEY` / `OPENAI_BASE_URL` / `OPENAI_MODEL` | OpenAI defaults / `gpt-5.4-mini` | Used for AI commit messages when no custom command is set |

If neither a custom command nor `OPENAI_API_KEY` is set, the worker writes
a clean deterministic message (imperative subject + bullet body).

## Operating commands

```bash
# Queue status for a given worktree
python3 snapshot-worker.py --status --repo /path/to/repo

# Drain pending events synchronously (useful before switching branches)
python3 snapshot-worker.py --flush --repo /path/to/repo

# Run the worker in the foreground (normally you never do this)
python3 snapshot-worker.py --repo /path/to/repo
```

## What "blocked_conflict" means

An event is `blocked_conflict` when, at replay time, the current index
state for one of its paths doesn't match what the hook captured as
`before`. That happens when:

- someone committed manually on top of the branch,
- another worker on a different worktree published a conflicting change,
- the agent's `Edit` tool was given a `before` value that didn't reflect
  reality by the time the worker replayed.

`blocked_conflict` events are not retried. Their path tails are cleared
so new edits from the same path start fresh against the real HEAD. The
event row stays in the DB for inspection.

## Debugging checklist

```bash
tail -n 200 /tmp/snapshotd-hook.log
tail -n 200 /tmp/snapshotd-worker.log
python3 snapshot-worker.py --status --repo .
sqlite3 "$(git rev-parse --absolute-git-dir)/ai-snapshotd/snapshotd.db" \
  "SELECT seq, state, branch_ref, tool_name, commit_oid FROM events ORDER BY seq DESC LIMIT 20;"
```

## Known limitations

- Advisory `fcntl` locks → Unix-only. No Windows support.
- Hooks that don't tell the script which file changed (stock Codex CLI
  PostToolUse for non-Bash tools) cannot be captured exactly.
- `blocked_conflict` events are recorded but not auto-recovered. Manual
  inspection is expected when that state shows up frequently.
- The worker reconciles the live index for published paths with
  `git reset -- <paths>`. The working tree is never touched.
