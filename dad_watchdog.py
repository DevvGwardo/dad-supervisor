#!/usr/bin/env python3
"""
DAD Watchdog — monitors DAD supervisor processes, reaps orphaned sons.

Runs as a cron job (no_agent=true) every 2-5 minutes. Scans all active
DAD workflows, checks if the supervisor is still alive, and cleans up
orphaned workers if the supervisor died unexpectedly.

Usage:
  python3 dad_watchdog.py                           # check all workflows, report
  python3 dad_watchdog.py --notify-telegram -100X   # also send TG alerts
  python3 dad_watchdog.py --json                    # JSON output for scripting
"""

import json
import os
import signal
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

DAD_HOME = Path.home() / ".hermes" / "dad"
WORKFLOWS_DIR = DAD_HOME / "workflows"
HERMES_ENV = Path.home() / ".hermes" / ".env"

# ── Utilities ────────────────────────────────────────────────────────────────

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except (OSError, ProcessLookupError):
        return False


def signal_pg(pid: int, sig: int):
    """Kill entire process group."""
    try:
        pgid = os.getpgid(pid)
        os.killpg(pgid, sig)
    except (OSError, ProcessLookupError):
        pass


def load_env() -> dict:
    env = {}
    if HERMES_ENV.exists():
        for line in HERMES_ENV.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def telegram_send(chat_id: str, text: str) -> bool:
    env = load_env()
    token = env.get("TELEGRAM_BOT_TOKEN")
    if not token:
        print("[watchdog] no TELEGRAM_BOT_TOKEN — can't send notification", file=sys.stderr)
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = json.dumps({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }).encode()
    try:
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10)
        return True
    except Exception as e:
        print(f"[watchdog] telegram send failed: {e}", file=sys.stderr)
        return False


# ── Core Logic ───────────────────────────────────────────────────────────────

def find_active_workflows() -> list[Path]:
    """Find all workflow dirs with state.json where status is 'running'."""
    active = []
    if not WORKFLOWS_DIR.exists():
        return active
    for wf_dir in WORKFLOWS_DIR.iterdir():
        if not wf_dir.is_dir():
            continue
        state_file = wf_dir / "state.json"
        if not state_file.exists():
            continue
        try:
            state = json.loads(state_file.read_text())
        except (json.JSONDecodeError, OSError):
            continue
        if state.get("status") == "running":
            active.append(wf_dir)
    return active


def check_workflow(wf_dir: Path) -> Optional[dict]:
    """
    Check a single workflow. Returns a report dict if action was taken.
    """
    state_file = wf_dir / "state.json"
    lease_file = wf_dir / "lease.lock"

    try:
        state = json.loads(state_file.read_text())
    except Exception:
        return None

    if state.get("status") != "running":
        return None

    supervisor_pid = None
    if lease_file.exists():
        try:
            raw = lease_file.read_text().strip()
            if raw.isdigit():
                supervisor_pid = int(raw)
        except Exception:
            pass

    worker_pid = state.get("worker_pid")

    report = {
        "workflow_id": wf_dir.name,
        "supervisor_pid": supervisor_pid,
        "worker_pid": worker_pid,
        "action": None,
    }

    # ── Case 1: Supervisor alive, everything normal ──
    if supervisor_pid and pid_alive(supervisor_pid):
        # Check if worker is also alive
        worker_ok = worker_pid and pid_alive(worker_pid)
        if not worker_ok and worker_pid:
            # Worker died but supervisor hasn't noticed yet — normal,
            # supervisor loop will catch it next cycle
            pass
        report["action"] = "ok"
        return report

    # ── Case 2: No lease file but state says running ──
    if supervisor_pid is None:
        # Stale state — supervisor never wrote lease or cleaned up
        mark_failed(wf_dir, state_file, state, "no_lease_file")
        report["action"] = "marked_failed_no_lease"
        return report

    # ── Case 3: Supervisor is DEAD ──
    if not pid_alive(supervisor_pid):
        # Check for orphaned worker
        orphan = worker_pid and pid_alive(worker_pid)

        if orphan:
            # Reap the orphan
            print(f"[watchdog] supervisor {supervisor_pid} dead, reaping orphan worker {worker_pid}")
            signal_pg(worker_pid, signal.SIGTERM)
            time.sleep(1)
            if pid_alive(worker_pid):
                signal_pg(worker_pid, signal.SIGKILL)
            report["action"] = "reaped_orphan"
        else:
            report["action"] = "supervisor_dead_no_orphan"

        # Mark workflow as failed
        mark_failed(wf_dir, state_file, state, "supervisor_died")
        report["action"] = report["action"] + "+marked_failed"

        # Notify if Telegram is configured
        notify_chat = state.get("notify_telegram")
        if notify_chat:
            elapsed = ""
            if state.get("worker_started"):
                try:
                    started = datetime.fromisoformat(state["worker_started"].replace("Z", "+00:00"))
                    elapsed = f" after {datetime.now(timezone.utc) - started}"
                except Exception:
                    pass
            msg = (
                f"🪦 *DAD Supervisor died*{elapsed}\n"
                f"Workflow: `{wf_dir.name}`\n"
                f"Task: {state.get('task', 'unknown')[:150]}\n"
                f"Orphan worker: {'reaped' if orphan else 'already dead'}\n"
                f"State: `{wf_dir / 'state.json'}`"
            )
            telegram_send(notify_chat, msg)

        return report

    return None


def mark_failed(wf_dir: Path, state_file: Path, state: dict, reason: str):
    """Update state to reflect failure, write tombstone."""
    state["status"] = f"failed_{reason}"
    state["worker_pid"] = None
    state["failure_at"] = now_iso()
    state["failure_reason"] = reason
    state.setdefault("history", []).append(f"[{now_iso()}] watchdog: marked failed — {reason}")
    try:
        state_file.write_text(json.dumps(state, indent=2))
    except Exception as e:
        print(f"[watchdog] failed to write state for {wf_dir.name}: {e}", file=sys.stderr)


def run_all(notify_telegram: str = None, json_output: bool = False):
    """Check all active workflows."""
    workflows = find_active_workflows()
    reports = []

    if not workflows:
        if json_output:
            print(json.dumps({"status": "ok", "workflows": 0, "reports": []}))
        return 0

    for wf_dir in workflows:
        report = check_workflow(wf_dir)
        if report:
            reports.append(report)

    if json_output:
        print(json.dumps({
            "status": "ok",
            "workflows_checked": len(workflows),
            "actions_taken": sum(1 for r in reports if r.get("action") and r["action"] != "ok"),
            "reports": reports,
        }, indent=2))
    else:
        for r in reports:
            action = r.get("action", "unknown")
            wid = r.get("workflow_id", "?")
            if action == "ok":
                print(f"[watchdog] {wid}: OK (supervisor PID {r['supervisor_pid']})")
            else:
                print(f"[watchdog] {wid}: {action}")

    return 0


# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    import argparse
    p = argparse.ArgumentParser(description="DAD Watchdog — monitors DAD supervisors, reaps orphans")
    p.add_argument("--notify-telegram", help="Telegram chat ID for alerts")
    p.add_argument("--json", action="store_true", help="JSON output")
    args = p.parse_args()

    return run_all(
        notify_telegram=args.notify_telegram,
        json_output=args.json,
    )


if __name__ == "__main__":
    sys.exit(main())
