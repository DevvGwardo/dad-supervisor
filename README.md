# DAD Supervisor

> **"Dad watches the kids so you don't have to."**

A Hermes Agent port of **Grok Build's DAD plugin** — supervises long-running agent workflows with 3-tier observation loops, evidence-based verification, lease-based recovery, and watchdog monitoring.

## Credits

- **Original concept & design**: [xAI's Grok Build](https://x.ai) — DAD plugin for the Grok agent framework
- **Hermes Agent port**: [DevGwardo](https://github.com/DevvGwardo)

The architecture, supervision tiers, evidence gates, and lease system are derived directly from Grok Build's DAD implementation. This project ports that battle-tested pattern to the Hermes Agent ecosystem.

## What DAD Does

| DAD Feature | Hermes Equivalent |
|---|---|
| Dad pane (supervisor) | `dad_supervisor.py` background process |
| Son pane (worker) | `hermes chat -q` subprocess |
| 2min stall detection | Internal loop — checks process alive, file changes, output flow |
| 12min claim verification | Git diff evidence, parses worker claims |
| 30min deep repair | Full status report, restart if stuck |
| Evidence gates | Git diff + file stat monitoring + test output detection |
| Lease-based recovery | `~/.hermes/dad/workflows/<id>/lease.lock` |
| Watchdog | `dad_watchdog.py` — cron-friendly monitor for supervisor health |

## Architecture

```
┌─────────────────────────────────────────────┐
│  DAD Supervisor (dad_supervisor.py)         │
│                                              │
│  ┌── 2min loop ──────────────────────────┐  │
│  │  • process alive?                      │  │
│  │  • recent file changes?                │  │
│  │  • output log flowing?                 │  │
│  └────────────────────────────────────────┘  │
│  ┌── 12min loop (every 6th tick) ────────┐  │
│  │  • verify claims vs git diff           │  │
│  │  • check for task drift                │  │
│  │  • assess progress rate                │  │
│  └────────────────────────────────────────┘  │
│  ┌── 30min loop (every 15th tick) ───────┐  │
│  │  • comprehensive status report         │  │
│  │  • test evidence verification          │  │
│  │  • recover/restart if needed           │  │
│  └────────────────────────────────────────┘  │
│                                              │
│  spawns/monitors                             │
│       ▼                                      │
│  ┌───────────────────────────────────────┐   │
│  │  Son Worker (hermes chat -q)          │   │
│  │  • reads task from task.md            │   │
│  │  • works in specified workdir         │   │
│  │  • output captured to output log      │   │
│  └───────────────────────────────────────┘   │
└─────────────────────────────────────────────┘
```

### File layout

```
~/.hermes/dad/workflows/<workflow-id>/
├── lease.lock          # PID of active supervisor (prevents duplicates)
├── state.json          # Current status, metrics, history
├── task.md             # Task description for the worker
├── worker-output.log   # Worker stdout/stderr
└── evidence/           # Verification artifacts (git diffs, test output)
```

## Quick Start

### Supervise a coding task

```bash
python3 dad_supervisor.py \
  --workflow-id build-auth-api \
  --task "Build a FastAPI authentication service with JWT and tests" \
  --workdir ~/project \
  --max-runtime 4h
```

### With Telegram notifications

```bash
python3 dad_supervisor.py \
  --workflow-id refactor-backend \
  --task "Refactor backend to use dependency injection. Run all tests." \
  --workdir ~/myapp \
  --max-runtime 8h \
  --notify-telegram -1001234567890
```

### Watchdog (cron)

```bash
# Run every 5 minutes — detects dead supervisors, reaps orphaned workers
python3 dad_watchdog.py --notify-telegram -1001234567890
```

## Supervision Tiers

### 2-Minute Check
- Worker process alive?
- Recent file changes in workdir?
- Output log growing?
- **Noise detection** — parses last 30 lines for spinners (`\|/-`, braille `⠋⠙⠹`), dots, retry loops vs meaningful content

### 12-Minute Check
- Git diff snapshot as evidence artifact
- Parses worker output for claims (`done`, `fixed`, `implemented`)
- Monitors for test file modifications
- Task drift detection

### 30-Minute Check
- Comprehensive status report to Telegram
- Full evidence verification
- Recovery trigger if worker is stuck
- Output log size tracking

## Evidence Gates

The supervisor won't accept "done" without real proof:

- **`file_changes`** — `git diff --stat HEAD` must show changes
- **`test_output`** — Test files must have been modified
- Custom evidence types can be added by extending `_verify_evidence()`

## Recovery

| Trigger | Action |
|---|---|
| 3+ consecutive stalls (no file changes, noise-only output) | Kill worker, restart |
| Worker process dies | Detected next 2min check, restart |
| Rate limit detected | Exponential backoff (30s → 60s → 120s... cap 10min) |
| Max restarts reached | Give up, notify, mark failed |

## Options

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `--workflow-id` | ✓ | — | Unique workflow identifier |
| `--task` | ✓ | — | Task description for worker |
| `--workdir` | ✓ | — | Working directory (git repo) |
| `--max-runtime` | — | `8h` | Max runtime (h/m/s suffixes) |
| `--notify-telegram` | — | — | Telegram chat ID for updates |
| `--max-restarts` | — | `3` | Max worker restarts |
| `--evidence` | — | `file_changes,test_output` | Required evidence types |
| `--model` | — | `deepseek-v4-flash` | Model for worker |
| `--provider` | — | `opencode-go` | Provider for worker |

## How It Compares to `delegate_task`

| | `delegate_task` | DAD Supervisor |
|---|---|---|
| Duration | Minutes (600s timeout) | Hours/days |
| Supervision | None (fire and forget) | 3-tier loops with recovery |
| Evidence | Self-reported summary | Git diff, file changes, test output |
| Restarts | No | Auto-restart up to N times |
| Status updates | One final message | Periodic Telegram notifications |
| Use case | Quick parallel subtasks | Long autonomous missions |

## Requirements

- Hermes Agent installed (`~/.hermes/hermes-agent/venv/bin/hermes`)
- Python 3.9+
- `TELEGRAM_BOT_TOKEN` in `~/.hermes/.env` (for notifications)
- Git repo as workdir (for evidence verification)

## License

MIT — see [LICENSE](LICENSE)
