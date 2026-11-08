#!/usr/bin/env python3
"""
DAD Supervisor — Hermes Agent port of Grok Build's DAD plugin.

Supervises long-running agent workflows with 3-tier observation loops,
evidence-based verification, lease-based recovery, and watchdog monitoring.

Usage:
  python3 dad_supervisor.py \\
    --workflow-id my-build \\
    --task "Build a REST API for user management with tests" \\
    --workdir ~/project \\
    --max-runtime 4h \\
    --notify-telegram -1001234567890

Architecture:
  Dad (this script) ──spawns──▶ Son (hermes chat -q worker)
       │
       ├── 2min loop: process alive? recent file changes? output flowing?
       ├── 12min loop: verify claims against git diff, test results
       └── 30min loop: full repair — restart if stuck, comprehensive report

Lease: ~/.hermes/dad/workflows/<id>/lease.lock — prevents duplicate supervisors
State: ~/.hermes/dad/workflows/<id>/state.json — current status, metrics
Evidence: ~/.hermes/dad/workflows/<id>/evidence/ — verification artifacts
"""

import argparse
import json
import os
import signal
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

DAD_HOME = Path.home() / ".hermes" / "dad"
WORKFLOWS_DIR = DAD_HOME / "workflows"
HERMES_ENV = Path.home() / ".hermes" / ".env"
HERMES_BIN = Path.home() / ".hermes" / "hermes-agent" / "venv" / "bin" / "hermes"
MESH_BIN = Path.home() / ".local" / "bin" / "mesh"

# ── CLI ──────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="DAD Supervisor for Hermes Agent")
    p.add_argument("--workflow-id", required=True, help="Unique workflow identifier")
    p.add_argument("--task", required=True, help="Task description for the worker agent")
    p.add_argument("--workdir", required=True, help="Working directory for the worker")
    p.add_argument("--max-runtime", default="8h", help="Max runtime (e.g. 4h, 120m, 3600s)")
    p.add_argument("--notify-telegram", help="Telegram chat ID for status updates")
    p.add_argument("--max-restarts", type=int, default=3, help="Max worker restarts before giving up")
    p.add_argument("--evidence", default="file_changes,test_output", help="Comma-separated evidence types required")
    p.add_argument("--model", default="deepseek-v4-flash", help="Model for the worker")
    p.add_argument("--provider", default="opencode-go", help="Provider for the worker")
    return p.parse_args()


# ── Utilities ────────────────────────────────────────────────────────────────

def parse_duration(s: str) -> int:
    """Parse duration string to seconds. Supports h, m, s suffixes."""
    s = s.strip().lower()
    if s.endswith("h"):
        return int(s[:-1]) * 3600
    elif s.endswith("m"):
        return int(s[:-1]) * 60
    elif s.endswith("s"):
        return int(s[:-1])
    else:
        return int(s)  # assume seconds


def now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def load_env() -> dict:
    """Load KEY=VALUE pairs from ~/.hermes/.env"""
    env = {}
    if HERMES_ENV.exists():
        for line in HERMES_ENV.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def telegram_send(chat_id: str, text: str) -> bool:
    """Send a message via Telegram Bot API. Returns True on success."""
    env = load_env()
    token = env.get("TELEGRAM_BOT_TOKEN")
    if not token:
        print("[dad] no TELEGRAM_BOT_TOKEN in .env — can't send notification")
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
        print(f"[dad] telegram send failed: {e}")
        return False


def run(cmd: str, cwd: Optional[str] = None, timeout: int = 30) -> subprocess.CompletedProcess:
    """Run a shell command, return CompletedProcess."""
    return subprocess.run(
        cmd, shell=True, cwd=cwd, capture_output=True, text=True, timeout=timeout
    )


def pid_alive(pid: int) -> bool:
    """Check if a process is still running."""
    try:
        os.kill(pid, 0)
        return True
    except (OSError, ProcessLookupError):
        return False


def mesh_publish(workflow_id: str, type_: str, content: str, tags: list, importance: int = 2):
    """Best-effort publish to agentic-mesh. Silent on failure — never breaks DAD."""
    if not MESH_BIN.exists():
        return
    env = {
        **os.environ,
        "MESH_AGENT_ID": f"dad-{workflow_id}",
        "MESH_AGENT_NAME": f"DAD ({workflow_id})",
        "MESH_RUNTIME": "dad-supervisor",
    }
    try:
        subprocess.run(
            [str(MESH_BIN), "publish",
             "--type", type_,
             "--content", content,
             "--tags", ",".join(["dad", workflow_id, *tags]),
             "--importance", str(importance)],
            env=env, capture_output=True, text=True, timeout=10, check=False,
        )
    except Exception:
        pass  # mesh is optional; DAD must not fail because of it


# ── Workflow State ───────────────────────────────────────────────────────────

class WorkflowState:
    """Persistent state for a DAD workflow."""

    def __init__(self, workflow_id: str):
        self.wf_dir = WORKFLOWS_DIR / workflow_id
        self.state_file = self.wf_dir / "state.json"
        self.lease_file = self.wf_dir / "lease.lock"
        self.evidence_dir = self.wf_dir / "evidence"
        self.task_file = self.wf_dir / "task.md"
        self.output_log = self.wf_dir / "worker-output.log"

    def setup(self, task: str, workdir: str, evidence_types: list, max_runtime: int):
        self.wf_dir.mkdir(parents=True, exist_ok=True)
        self.evidence_dir.mkdir(exist_ok=True)
        self.task_file.write_text(task)
        self.state = {
            "workflow_id": str(self.wf_dir.name),
            "task": task,
            "workdir": workdir,
            "evidence_types": evidence_types,
            "max_runtime": max_runtime,
            "status": "initializing",
            "worker_pid": None,
            "worker_started": None,
            "restart_count": 0,
            "max_restarts": 0,  # set from args
            "last_check_2min": None,
            "last_check_12min": None,
            "last_check_30min": None,
            "checks_2min_total": 0,
            "checks_12min_total": 0,
            "checks_30min_total": 0,
            "stall_count": 0,
            "last_file_change": None,
            "last_output_line": None,
            "claims_verified": 0,
            "claims_failed": 0,
            "history": [],
            "created": now_iso(),
            "notify_telegram": None,
        }
        self.save()

    def load(self):
        if self.state_file.exists():
            self.state = json.loads(self.state_file.read_text())
        else:
            raise FileNotFoundError(f"no state file at {self.state_file}")

    def save(self):
        self.state_file.write_text(json.dumps(self.state, indent=2))

    def log(self, msg: str):
        entry = f"[{now_iso()}] {msg}"
        self.state.setdefault("history", []).append(entry)
        print(entry)

    def acquire_lease(self) -> bool:
        """Try to acquire the lease. Returns True if successful."""
        self.wf_dir.mkdir(parents=True, exist_ok=True)
        if self.lease_file.exists():
            old_pid = self.lease_file.read_text().strip()
            if old_pid.isdigit() and pid_alive(int(old_pid)):
                print(f"[dad] lease held by live process {old_pid} — aborting")
                return False
            else:
                print(f"[dad] stale lease from dead PID {old_pid} — taking over")
        self.lease_file.write_text(str(os.getpid()))
        return True

    def release_lease(self):
        if self.lease_file.exists():
            self.lease_file.unlink()

    def update_field(self, **kwargs):
        self.state.update(kwargs)
        self.save()


# ── Supervision Logic ────────────────────────────────────────────────────────

class Supervisor:
    def __init__(self, args):
        self.args = args
        self.wf = WorkflowState(args.workflow_id)
        self.max_runtime = parse_duration(args.max_runtime)
        self.evidence_types = [e.strip() for e in args.evidence.split(",")]
        self.notify_chat = args.notify_telegram
        self.max_restarts = args.max_restarts
        self.start_time = time.time()
        self._notify_cooldown = 0  # prevent spam

    def run(self):
        # Setup
        print(f"[dad] DAD Supervisor starting for workflow: {self.args.workflow_id}")
        print(f"[dad] task: {self.args.task}")
        print(f"[dad] workdir: {self.args.workdir}")
        print(f"[dad] max runtime: {self.args.max_runtime}")

        # Acquire lease
        if not self.wf.acquire_lease():
            sys.exit(1)

        # Setup state
        self.wf.setup(
            task=self.args.task,
            workdir=self.args.workdir,
            evidence_types=self.evidence_types,
            max_runtime=self.max_runtime,
        )
        self.wf.update_field(
            max_restarts=self.max_restarts,
            notify_telegram=self.notify_chat,
        )

        # Spawn worker
        self._spawn_worker()

        # Main supervision loop
        self._supervise()

    def _spawn_worker(self):
        """Spawn the worker Hermes process."""
        self.wf.log("spawning worker agent...")

        task_text = self.args.task
        workdir = os.path.expanduser(self.args.workdir)

        # Write task to file for the worker to read
        task_file = self.wf.task_file
        task_file.write_text(task_text)

        # Build command
        cmd = (
            f"cd {workdir} && "
            f"{HERMES_BIN} chat -q '{task_text}' "
            f"--model {self.args.model} "
            f"--provider {self.args.provider} "
            f"> {self.wf.output_log} 2>&1"
        )

        self.wf.log(f"worker command: {cmd[:200]}...")

        proc = subprocess.Popen(
            cmd, shell=True, cwd=workdir,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            preexec_fn=os.setpgrp,  # detach from parent
        )

        pid = proc.pid
        self.wf.update_field(
            worker_pid=pid,
            worker_started=now_iso(),
            status="running",
        )
        self.wf.log(f"worker spawned with PID {pid}")
        self._maybe_notify(f"🚀 *DAD Supervisor started*\nWorkflow: `{self.wf.wf_dir.name}`\nTask: {task_text[:200]}\nPID: `{pid}`")
        mesh_publish(
            self.wf.wf_dir.name, "log",
            f"DAD started. Task: {task_text[:300]}",
            tags=["start"], importance=2,
        )

    def _supervise(self):
        """Main supervision loop — 2min, 12min, 30min tiers."""
        loop_count = 0
        stall_streak = 0

        while True:
            elapsed = time.time() - self.start_time

            # Timeout check
            if elapsed > self.max_runtime:
                self.wf.log(f"max runtime ({self.args.max_runtime}) exceeded — terminating")
                self._terminate_worker("max_runtime_exceeded")
                break

            # Wait 2 minutes between checks
            time.sleep(120)
            loop_count += 1

            # ── 2-MINUTE CHECK ──────────────────────────────────────────
            self._check_2min()

            # Stall detection
            if self._is_stalled():
                stall_streak += 1
                self.wf.log(f"stall detected — streak {stall_streak}")
                if stall_streak >= 3:
                    self.wf.log("3 consecutive stalls — triggering recovery")
                    self._recover_worker(f"stalled for {stall_streak * 2} minutes")
                    stall_streak = 0
            else:
                stall_streak = 0

            # ── 12-MINUTE CHECK (every 6th loop) ────────────────────────
            if loop_count % 6 == 0:
                self._check_12min()

            # ── 30-MINUTE CHECK (every 15th loop) ───────────────────────
            if loop_count % 15 == 0:
                self._check_30min()

            # Check if worker completed
            pid = self.wf.state.get("worker_pid")
            if pid and not pid_alive(pid):
                self.wf.log(f"worker PID {pid} has exited")
                self._on_worker_exit()
                break

        # Cleanup
        self.wf.release_lease()
        self.wf.log("supervisor exiting")

    def _check_2min(self):
        """Quick health check — process alive, recent changes, output flowing."""
        self.wf.update_field(
            last_check_2min=now_iso(),
            checks_2min_total=self.wf.state["checks_2min_total"] + 1,
        )

        pid = self.wf.state.get("worker_pid")
        workdir = os.path.expanduser(self.wf.state["workdir"])

        # 1. Process alive?
        if pid and not pid_alive(pid):
            self.wf.log("worker process is dead!")
            return  # handled in main loop

        # 2. Recent file changes in workdir?
        try:
            r = run(f"find {workdir} -type f -newer {self.wf.output_log} 2>/dev/null | head -20", timeout=10)
            changed = r.stdout.strip()
            if changed:
                self.wf.update_field(last_file_change=now_iso())
        except Exception:
            pass

        # 3. Recent output?
        if self.wf.output_log.exists():
            try:
                r = run(f"tail -5 {self.wf.output_log}", timeout=5)
                if r.stdout.strip():
                    self.wf.update_field(last_output_line=r.stdout.strip().split("\n")[-1][:200])
            except Exception:
                pass

    def _check_12min(self):
        """Medium check — verify claims, check drift, assess progress."""
        self.wf.update_field(
            last_check_12min=now_iso(),
            checks_12min_total=self.wf.state["checks_12min_total"] + 1,
        )

        workdir = os.path.expanduser(self.wf.state["workdir"])

        # 1. Git diff summary
        try:
            r = run(f"cd {workdir} && git diff --stat HEAD 2>/dev/null", timeout=15)
            diff_stat = r.stdout.strip()
            if diff_stat:
                self.wf.log(f"git changes:\n{diff_stat[:500]}")
                # Save evidence
                evidence_file = self.wf.evidence_dir / f"git-diff-{int(time.time())}.txt"
                run(f"cd {workdir} && git diff HEAD > {evidence_file} 2>/dev/null", timeout=15)
        except Exception as e:
            self.wf.log(f"git diff failed: {e}")

        # 2. Parse worker output for claims
        if self.wf.output_log.exists():
            try:
                r = run(f"grep -i 'done\\|fixed\\|added\\|created\\|implemented\\|completed' {self.wf.output_log} 2>/dev/null | tail -10", timeout=5)
                claims = r.stdout.strip()
                if claims:
                    self.wf.log(f"worker claims:\n{claims[:500]}")
            except Exception:
                pass

        # 3. Check for test output
        if "test_output" in self.evidence_types:
            try:
                r = run(f"cd {workdir} && find . -name '*.py' -newer {self.wf.output_log} | head -5", timeout=10)
                if r.stdout.strip():
                    self.wf.update_field(claims_verified=self.wf.state["claims_verified"] + 1)
            except Exception:
                pass

    def _check_30min(self):
        """Deep check — comprehensive verification, repair if needed, status report."""
        self.wf.update_field(
            last_check_30min=now_iso(),
            checks_30min_total=self.wf.state["checks_30min_total"] + 1,
        )

        workdir = os.path.expanduser(self.wf.state["workdir"])
        pid = self.wf.state.get("worker_pid")
        elapsed = time.time() - self.start_time

        # Status report
        status_lines = [
            f"📊 *30-Minute Status Report*",
            f"Workflow: `{self.wf.wf_dir.name}`",
            f"Runtime: `{timedelta(seconds=int(elapsed))}`",
            f"Worker PID: `{pid}`",
            f"Restarts: `{self.wf.state['restart_count']}/{self.max_restarts}`",
            f"Checks: `{self.wf.state['checks_2min_total']} quick / {self.wf.state['checks_12min_total']} medium / {self.wf.state['checks_30min_total']} deep`",
            f"Stalls: `{self.wf.state['stall_count']}`",
        ]

        # Git status
        try:
            r = run(f"cd {workdir} && git diff --stat HEAD 2>/dev/null | tail -1", timeout=10)
            if r.stdout.strip():
                status_lines.append(f"Changes: `{r.stdout.strip()}`")
            else:
                status_lines.append("Changes: _none yet_")
        except Exception:
            status_lines.append("Changes: _git error_")

        # Output log size
        if self.wf.output_log.exists():
            size = self.wf.output_log.stat().st_size
            status_lines.append(f"Output log: `{size:,} bytes`")

        report = "\n".join(status_lines)
        self._maybe_notify(report)
        mesh_publish(
            self.wf.wf_dir.name, "log", report,
            tags=["heartbeat"], importance=1,
        )

    def _is_stalled(self) -> bool:
        """Detect if the worker is stalled (no real progress).

        Two signals:
        1. No file changes in 6+ minutes (existing check)
        2. Output is flowing but it's just noise — spinners, dots, retry loops
        """
        last_change = self.wf.state.get("last_file_change")

        # Check 1: file change age
        file_stalled = False
        if last_change:
            try:
                last_dt = datetime.fromisoformat(last_change.replace("Z", "+00:00"))
                now = datetime.utcnow()
                if last_dt.tzinfo:
                    last_dt = last_dt.replace(tzinfo=None)
                # Stalled if no file changes in 6+ minutes
                file_stalled = (now - last_dt).total_seconds() > 360
            except Exception:
                pass

        if file_stalled:
            return True

        # Check 2: output quality — is it just noise?
        if self._output_is_noise():
            # If we've had noise for 3+ consecutive checks (6+ minutes of noise), that's a stall
            noise_streak = self.wf.state.setdefault("_noise_streak", 0) + 1
            self.wf.state["_noise_streak"] = noise_streak
            self.wf.save()
            if noise_streak >= 3:
                self.wf.log(f"stall: {noise_streak} consecutive noise-only output checks")
                return True
        else:
            self.wf.state["_noise_streak"] = 0
            self.wf.save()

        return False

    def _output_is_noise(self) -> bool:
        """Check if recent worker output is just noise (spinners, dots, retries).

        Returns True if output exists but contains no meaningful progress markers.
        """
        if not self.wf.output_log.exists():
            return False

        try:
            r = run(f"tail -30 {self.wf.output_log}", timeout=5)
            output = r.stdout.strip()
            if not output:
                return False
        except Exception:
            return False

        lines = [l.strip() for l in output.split("\n") if l.strip()]
        if not lines:
            return False

        # Noise patterns
        spinner_chars = set("\\|/-⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
        noise_score = 0
        meaningful_score = 0

        for line in lines:
            # Pure spinner/dots lines
            stripped = line.strip()
            if all(c in spinner_chars or c == "." for c in stripped) and len(stripped) < 20:
                noise_score += 2
                continue

            # Retry/rate limit noise
            if any(phrase in line.lower() for phrase in [
                "retrying", "retry", "attempt", "backoff",
                "rate limit", "too many requests", "429",
                "throttled", "try again", "waiting",
            ]):
                noise_score += 2
                continue

            # Progress bar fragments (=====> or [###])
            if stripped.startswith("[") and all(c in "#=- >" for c in stripped.lstrip("[").rstrip("]")):
                noise_score += 1
                continue

            # Empty-ish lines
            if len(stripped) < 3:
                noise_score += 1
                continue

            # Meaningful markers
            if any(kw in line.lower() for kw in [
                "done", "completed", "finished", "success",
                "created", "written", "saved", "compiled",
                "error", "failed", "traceback", "exception",
                "def ", "class ", "import ", "function",
                "file", "test", "build", "commit",
            ]):
                meaningful_score += 3
                continue

            # Any line with actual content (more than just noise chars)
            meaningful_score += 1

        # High noise-to-signal ratio = noise output
        if meaningful_score == 0 and noise_score > 0:
            return True
        if noise_score > meaningful_score * 2:
            return True

        return False

    def _recover_worker(self, reason: str):
        """Restart the worker after a failure. Applies rate limit backoff if detected."""
        restarts = self.wf.state["restart_count"]
        if restarts >= self.max_restarts:
            self.wf.log(f"max restarts ({self.max_restarts}) reached — giving up")
            self._maybe_notify(f"❌ *DAD: Max restarts reached*\nWorkflow `{self.wf.wf_dir.name}` has failed after {restarts} restarts.\nReason: {reason}")
            self._terminate_worker("max_restarts")
            return

        # Check for rate limits in worker output
        rate_limit_hits = self.wf.state.setdefault("rate_limit_hits", 0)
        if self._detect_rate_limit():
            rate_limit_hits += 1
            self.wf.state["rate_limit_hits"] = rate_limit_hits
            self.wf.save()
            backoff_secs = self._rate_limit_backoff(rate_limit_hits)
            self.wf.log(f"rate limit detected (hit #{rate_limit_hits}) — backing off {backoff_secs}s")
            self._maybe_notify(
                f"⏳ *DAD: Rate limit backoff*\n"
                f"Workflow: `{self.wf.wf_dir.name}`\n"
                f"Hit #{rate_limit_hits} — waiting {backoff_secs}s before restart"
            )
            time.sleep(backoff_secs)

        self.wf.log(f"recovering worker — reason: {reason}")
        self._maybe_notify(f"🔄 *DAD: Restarting worker*\nWorkflow: `{self.wf.wf_dir.name}`\nReason: {reason}\nAttempt: {restarts + 1}/{self.max_restarts}")
        mesh_publish(
            self.wf.wf_dir.name, "finding",
            f"Worker stalled — restarting (attempt {restarts + 1}/{self.max_restarts}). Reason: {reason}",
            tags=["stall", "restart"], importance=3,
        )

        # Kill old worker
        old_pid = self.wf.state.get("worker_pid")
        if old_pid and pid_alive(old_pid):
            try:
                os.kill(old_pid, signal.SIGTERM)
                time.sleep(2)
                if pid_alive(old_pid):
                    os.kill(old_pid, signal.SIGKILL)
            except Exception:
                pass

        # Spawn new worker
        self.wf.update_field(restart_count=restarts + 1, stall_count=self.wf.state["stall_count"] + 1)
        self._spawn_worker()

    def _detect_rate_limit(self) -> bool:
        """Scan worker output for rate limit / API cap errors."""
        if not self.wf.output_log.exists():
            return False
        try:
            r = run(f"tail -50 {self.wf.output_log}", timeout=5)
            output = r.stdout.lower()
        except Exception:
            return False

        rate_limit_patterns = [
            "429", "rate limit", "too many requests",
            "throttled", "api cap", "quota exceeded",
            "rate exceeded", "try again later",
            "model overloaded", "server busy",
            "capacity", "backoff",
        ]
        return any(p in output for p in rate_limit_patterns)

    def _rate_limit_backoff(self, hit_count: int) -> int:
        """Exponential backoff: 30s → 60s → 120s → 240s → 480s (cap at 10min)."""
        base = 30
        backoff = base * (2 ** (hit_count - 1))
        return min(backoff, 600)  # cap at 10 minutes

    def _terminate_worker(self, reason: str):
        """Kill the worker and mark workflow as done."""
        pid = self.wf.state.get("worker_pid")
        if pid and pid_alive(pid):
            try:
                os.killpg(os.getpgid(pid), signal.SIGTERM)
                time.sleep(1)
                if pid_alive(pid):
                    os.killpg(os.getpgid(pid), signal.SIGKILL)
            except Exception:
                pass

        self.wf.update_field(status=reason, worker_pid=None)

    def _on_worker_exit(self):
        """Handle worker process exiting."""
        self.wf.log("worker process completed")

        # Check exit — look at output log for success indicators
        success = False
        output = ""
        if self.wf.output_log.exists():
            output = self.wf.output_log.read_text()[-2000:]  # last 2000 chars
            if any(kw in output.lower() for kw in ["completed", "done", "success", "finished"]):
                success = True

        # Evidence gate
        evidence_ok = self._verify_evidence()

        if success and evidence_ok:
            self.wf.update_field(status="completed_success")
            self._maybe_notify(f"✅ *DAD: Workflow completed*\nWorkflow: `{self.wf.wf_dir.name}`\nEvidence: {'✅ verified' if evidence_ok else '⚠️ partial'}\nOutput log: `{self.wf.output_log}`")
            self.wf.log("✅ workflow completed successfully")
            mesh_publish(
                self.wf.wf_dir.name, "finding",
                f"Workflow completed successfully. Evidence verified. Task: {self.wf.state.get('task','')[:200]}",
                tags=["complete", "success"], importance=3,
            )
        else:
            self.wf.update_field(status="completed_unverified")
            self._maybe_notify(f"⚠️ *DAD: Workflow finished but unverified*\nWorkflow: `{self.wf.wf_dir.name}`\nSuccess signals: {'found' if success else 'none'}\nEvidence: {'✅ verified' if evidence_ok else '❌ insufficient'}")
            self.wf.log("⚠️ workflow completed but evidence insufficient")
            mesh_publish(
                self.wf.wf_dir.name, "finding",
                f"Workflow finished but UNVERIFIED. Success signals: {'found' if success else 'none'}. Evidence: {'verified' if evidence_ok else 'insufficient'}.",
                tags=["complete", "unverified"], importance=3,
            )

    def _verify_evidence(self) -> bool:
        """Check if required evidence types are satisfied."""
        workdir = os.path.expanduser(self.wf.state["workdir"])
        passed = True

        if "file_changes" in self.evidence_types:
            try:
                r = run(f"cd {workdir} && git diff --stat HEAD 2>/dev/null", timeout=10)
                if not r.stdout.strip():
                    self.wf.log("evidence FAIL: no file changes detected")
                    passed = False
                else:
                    self.wf.log(f"evidence PASS: file changes found")
            except Exception:
                passed = False

        if "test_output" in self.evidence_types:
            try:
                r = run(f"cd {workdir} && find . -name 'test_*.py' -o -name '*_test.py' -o -name '*.test.*' -newer {self.wf.output_log} 2>/dev/null | head -3", timeout=10)
                if not r.stdout.strip():
                    self.wf.log("evidence FAIL: no test files modified")
                    passed = False
                else:
                    self.wf.log("evidence PASS: test files found")
            except Exception:
                passed = False

        return passed

    def _maybe_notify(self, text: str):
        """Send a telegram notification, with cooldown to prevent spam."""
        if not self.notify_chat:
            return

        now = time.time()
        if now - self._notify_cooldown < 60:  # max 1 notification per minute
            return

        self._notify_cooldown = now
        telegram_send(self.notify_chat, text)


# ── Entry Point ──────────────────────────────────────────────────────────────

def main():
    args = parse_args()

    # Resolve workdir
    args.workdir = os.path.expanduser(args.workdir)
    if not os.path.isdir(args.workdir):
        print(f"[dad] ERROR: workdir does not exist: {args.workdir}")
        sys.exit(1)

    # Check hermes binary
    if not HERMES_BIN.exists():
        print(f"[dad] ERROR: hermes binary not found at {HERMES_BIN}")
        sys.exit(1)

    supervisor = Supervisor(args)
    supervisor.run()


if __name__ == "__main__":
    main()
