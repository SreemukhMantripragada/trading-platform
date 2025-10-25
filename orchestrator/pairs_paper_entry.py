#!/usr/bin/env python3
"""
orchestrator/pairs_paper_entry.py

Single entry point to launch the pairs paper-trading stack:
  - Zerodha websocket ingestor
  - 1-second bar builder
  - 1m→multi timeframe bar aggregator
  - pair watcher (with hydration)
  - pairs executor (writes to OMS + state file)
  - risk manager v2
  - paper gateway matcher

The script keeps subprocesses running, tails them into per-service log files,
and propagates termination (Ctrl+C) to all children. If any child exits
unexpectedly, the runner shuts everything down and reports the failing service.
"""
from __future__ import annotations

import asyncio
import os
import signal
import sys
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple, IO
import contextlib

try:
    from dotenv import load_dotenv  # type: ignore
except ImportError as exc:  # pragma: no cover
    raise SystemExit("python-dotenv is required. Install with `pip install python-dotenv`.") from exc

ROOT_DIR = Path(__file__).resolve().parents[1]
VENV_PY = ROOT_DIR / ".venv" / "bin" / "python"
if VENV_PY.exists():
    PYTHON_BIN = str(VENV_PY)
else:
    PYTHON_BIN = sys.executable or shutil.which("python3") or "python3"

SERVICE_CMDS = [
    ("zerodha-ws", [PYTHON_BIN, "ingestion/zerodha_ws.py"]),
    ("bar-builder-1s", [PYTHON_BIN, "compute/bar_builder_1s.py"]),
    ("bar-agg-multi", [PYTHON_BIN, "compute/bar_aggregator_1m_to_multi.py"]),
    ("pair-watch", [PYTHON_BIN, "compute/pair_watch_producer.py"]),
    ("pairs-exec", [PYTHON_BIN, "execution/pairs_executor.py"]),
    ("risk-manager", [PYTHON_BIN, "risk/manager_v2.py"]),
    ("paper-matcher", [PYTHON_BIN, "execution/paper_gateway_matcher.py"]),
]

SERVICE_METRICS_PORT = {
    "zerodha-ws": "8111",
    "bar-builder-1s": "8112",
    "bar-agg-multi": "8113",
    "pair-watch": "8114",
    "pairs-exec": "8115",
    "risk-manager": "8116",
    "paper-matcher": "8117",
}


async def spawn_service(
    name: str, cmd: List[str], env: Dict[str, str], log_dir: Path
) -> Tuple[asyncio.subprocess.Process, Path, IO[str]]:
    log_dir.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)
    log_path = log_dir / f"{now:%Y%m%d_%H%M%S}_{name}.log"
    log_file = log_path.open("a", encoding="utf-8")
    log_file.write(f"[entry] starting {name}: {' '.join(cmd)}\n")
    log_file.flush()
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=log_file, stderr=asyncio.subprocess.STDOUT, env=env, cwd=str(ROOT_DIR)
    )
    return proc, log_path, log_file


async def main() -> None:
    load_dotenv(ROOT_DIR / ".env", override=False)
    load_dotenv(ROOT_DIR / "infra" / ".env", override=False)

    base_env = os.environ.copy()

    venv_bin = VENV_PY.parent
    if venv_bin.exists():
        base_env["PATH"] = f"{venv_bin}{os.pathsep}{base_env.get('PATH','')}"
        base_env.setdefault("VIRTUAL_ENV", str(venv_bin.parent))

    base_env.setdefault("LIVE_TRADING", "0")
    base_env.setdefault("AUTO_SWITCH_TO_LIVE", "0")
    base_env.setdefault("PAIR_HYDRATE_SOURCE", "kite")
    base_env.setdefault("PAIRS_STATE_FILE", "data/runtime/pairs_state.json")
    existing_py = base_env.get("PYTHONPATH")
    path_str = str(ROOT_DIR)
    if existing_py:
        if path_str not in existing_py.split(os.pathsep):
            base_env["PYTHONPATH"] = os.pathsep.join([path_str, existing_py])
    else:
        base_env["PYTHONPATH"] = path_str

    services = list(SERVICE_CMDS)

    log_root = Path(os.getenv("PAIRS_LOG_DIR", "runs/logs"))
    if log_root.exists():
        shutil.rmtree(log_root, ignore_errors=True)
    now = datetime.now(timezone.utc)
    run_log_dir = log_root / f"pairs_{now:%Y%m%d_%H%M%S}"

    procs: Dict[str, asyncio.subprocess.Process] = {}
    logs: Dict[str, IO[str]] = {}
    log_paths: Dict[str, Path] = {}
    tasks = []
    stop_event = asyncio.Event()

    async def launch_all():
        for name, cmd in services:
            env = base_env.copy()
            port = SERVICE_METRICS_PORT.get(name)
            if port:
                env["METRICS_PORT"] = port
                if name == "pair-watch":
                    env["PAIRWATCH_METRICS_PORT"] = port
            proc, log_path, handle = await spawn_service(name, cmd, env, run_log_dir)
            procs[name] = proc
            logs[name] = handle
            log_paths[name] = log_path
            print(f"[entry] {name:<15} → pid={proc.pid} log={log_path}")

    async def monitor(name: str, proc: asyncio.subprocess.Process):
        rc = await proc.wait()
        print(f"[entry] {name} exited with code {rc}")
        log_path = log_paths.get(name)
        if log_path and log_path.exists():
            try:
                lines = log_path.read_text().splitlines()
                tail = "\n".join(lines[-10:]) if lines else ""
                if tail:
                    print(f"[entry] --- last log lines for {name} ---\n{tail}\n[entry] --- end log ---")
            except Exception as exc:
                print(f"[entry] unable to read log {log_path}: {exc}")
        stop_event.set()

    async def shutdown():
        if not procs:
            return
        print("[entry] shutting down services…")
        for name, proc in procs.items():
            if proc.returncode is None:
                proc.terminate()
        await asyncio.sleep(2)
        for name, proc in procs.items():
            if proc.returncode is None:
                print(f"[entry] force-killing {name}")
                proc.kill()
        for name, proc in procs.items():
            await proc.wait()
        for handle in logs.values():
            try:
                handle.close()
            except Exception:
                pass

    await launch_all()
    loop = asyncio.get_running_loop()

    def _signal_handler(signame: str):
        print(f"[entry] received {signame}; stopping…")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler, sig.name)
        except NotImplementedError:
            pass  # Windows

    for name, proc in procs.items():
        tasks.append(asyncio.create_task(monitor(name, proc)))

    await stop_event.wait()
    await shutdown()
    for t in tasks:
        t.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await t
    print("[entry] all services stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(1)
