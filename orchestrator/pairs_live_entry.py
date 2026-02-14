#!/usr/bin/env python3
"""
orchestrator/pairs_live_entry.py

Launches the live pairs trading stack end-to-end:
  - Zerodha websocket → Kafka ticks
  - 1-second bar builder and multi-timeframe aggregator
  - Pair-watch producer reading configs/pairs_next_day.yaml
  - Pairs executor → risk manager → budget guard
  - Zerodha gateway (orders.allowed → broker) + poller

Mirrors orchestrator/pairs_paper_entry.py but swaps the paper matcher for
the live zerodha gateway and order budget guard.
"""
from __future__ import annotations

import asyncio
import contextlib
import os
import shutil
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, IO, List, Tuple

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
    ("bar-agg-1m", [PYTHON_BIN, "compute/bar_aggregator_1m.py"]),
    ("bar-agg-multi", [PYTHON_BIN, "compute/bar_aggregator_1m_to_multi.py"]),
    ("pair-watch", [PYTHON_BIN, "compute/pair_watch_producer.py"]),
    ("pairs-exec", [PYTHON_BIN, "execution/pairs_executor.py"]),
    ("risk-manager", [PYTHON_BIN, "risk/manager_v2.py"]),
    ("budget-guard", [PYTHON_BIN, "risk/order_budget_guard.py"]),
    ("zerodha-gateway", [PYTHON_BIN, "execution/zerodha_gateway.py"]),
    ("zerodha-poller", [PYTHON_BIN, "execution/zerodha_poller.py"]),
]

SERVICE_METRICS_PORT = {
    "zerodha-ws": "8111",
    "bar-builder-1s": "8112",
    "bar-agg-1m": "8118",
    "bar-agg-multi": "8113",
    "pair-watch": "8114",
    "pairs-exec": "8115",
    "risk-manager": "8116",
    "budget-guard": "8123",
    "zerodha-gateway": "8017",
    "zerodha-poller": "8018",
}


async def spawn_service(
    name: str, cmd: List[str], env: Dict[str, str], log_dir: Path
) -> Tuple[asyncio.subprocess.Process, Path, IO[str]]:
    log_dir.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)
    log_path = log_dir / f"{now:%Y%m%d_%H%M%S}_{name}.log"
    log_file = log_path.open("a", encoding="utf-8")
    log_file.write(f"[live-entry] starting {name}: {' '.join(cmd)}\n")
    log_file.flush()
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=log_file,
        stderr=asyncio.subprocess.STDOUT,
        env=env,
        cwd=str(ROOT_DIR),
    )
    return proc, log_path, log_file


async def main() -> None:
    load_dotenv(ROOT_DIR / ".env", override=False)
    load_dotenv(ROOT_DIR / "infra" / ".env", override=False)

    base_env = os.environ.copy()
    user_leverage = base_env.get("PAIRS_LEVERAGE")
    user_bt_leverage = base_env.get("PAIRS_BT_LEVERAGE")

    venv_bin = VENV_PY.parent
    if venv_bin.exists():
        base_env["PATH"] = f"{venv_bin}{os.pathsep}{base_env.get('PATH','')}"
        base_env.setdefault("VIRTUAL_ENV", str(venv_bin.parent))

    base_env.setdefault("LIVE_TRADING", "1")
    base_env.setdefault("AUTO_SWITCH_TO_LIVE", "0")
    base_env.setdefault("PAIR_HYDRATE_SOURCE", "kite")
    base_env.setdefault("PAIRS_STATE_FILE", "data/runtime/pairs_state.json")
    base_env.setdefault("PAIRS_LEVERAGE", user_leverage or "5")
    base_env.setdefault("PAIRS_BT_LEVERAGE", user_bt_leverage or base_env["PAIRS_LEVERAGE"])
    mode = (base_env.get("PAIRS_MODE") or "").strip().lower()
    if mode in {"swing", "swing_trading"}:
        base_env["PAIRS_MODE"] = "swing"
        base_env.setdefault("PAIRS_NEXT_DAY", "configs/pairs_swing.yaml")
        base_env.setdefault("PAIRWATCH_FLATTEN_HHMM", "none")
        base_env.setdefault("OUT_TFS", base_env.get("OUT_TFS", "3,5,15,60"))
        base_env.setdefault("MAX_HOLD_MIN_DEFAULT", "1440")
        base_env.setdefault("PAIRWATCH_MIN_CANDLES", "200")
        base_env.setdefault("HYDRATE_LOOKBACK_MULT", "5")
        base_env.setdefault("PAIR_ENTRY_RECENT_BARS", "1")
        if not user_leverage:
            base_env["PAIRS_LEVERAGE"] = "3"
        if not user_bt_leverage:
            base_env["PAIRS_BT_LEVERAGE"] = base_env["PAIRS_LEVERAGE"]
    existing_py = base_env.get("PYTHONPATH")
    path_str = str(ROOT_DIR)
    if existing_py:
        if path_str not in existing_py.split(os.pathsep):
            base_env["PYTHONPATH"] = os.pathsep.join([path_str, existing_py])
    else:
        base_env["PYTHONPATH"] = path_str

    services = list(SERVICE_CMDS)

    log_root = Path(os.getenv("PAIRS_LIVE_LOG_DIR", "runs/logs_live"))
    now = datetime.now(timezone.utc)
    run_log_dir = log_root / f"pairs_live_{now:%Y%m%d_%H%M%S}"

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

            if name == "budget-guard":
                env.setdefault("IN_TOPIC", "orders.sized")
                env.setdefault("OUT_TOPIC", "orders.allowed")
            elif name == "zerodha-gateway":
                env.setdefault("IN_TOPIC", "orders.allowed")
                env.setdefault("FILL_TOPIC", "fills")
                env.setdefault("DRY_RUN", "0")
            elif name == "zerodha-poller":
                env.setdefault("FILL_TOPIC", "fills")
                env.setdefault("DRY_RUN", "0")

            proc, log_path, handle = await spawn_service(name, cmd, env, run_log_dir)
            procs[name] = proc
            logs[name] = handle
            log_paths[name] = log_path
            print(f"[live-entry] {name:<16} → pid={proc.pid} log={log_path}")

    async def monitor(name: str, proc: asyncio.subprocess.Process):
        rc = await proc.wait()
        print(f"[live-entry] {name} exited with code {rc}")
        log_path = log_paths.get(name)
        if log_path and log_path.exists():
            try:
                lines = log_path.read_text().splitlines()
                tail = "\n".join(lines[-10:]) if lines else ""
                if tail:
                    print(f"[live-entry] --- last log lines for {name} ---\n{tail}\n[live-entry] --- end log ---")
            except Exception as exc:
                print(f"[live-entry] unable to read log {log_path}: {exc}")
        stop_event.set()

    async def shutdown():
        if not procs:
            return
        print("[live-entry] shutting down services…")
        for name, proc in procs.items():
            if proc.returncode is None:
                proc.terminate()
        await asyncio.sleep(2)
        for name, proc in procs.items():
            if proc.returncode is None:
                print(f"[live-entry] force-killing {name}")
                proc.kill()
        for name, proc in procs.items():
            await proc.wait()
        for handle in logs.values():
            with contextlib.suppress(Exception):
                handle.close()

    await launch_all()
    loop = asyncio.get_running_loop()

    def _signal_handler(signame: str):
        print(f"[live-entry] received {signame}; stopping…")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler, sig.name)
        except NotImplementedError:
            pass

    for name, proc in procs.items():
        tasks.append(asyncio.create_task(monitor(name, proc)))

    await stop_event.wait()
    await shutdown()
    for t in tasks:
        t.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await t
    print("[live-entry] all services stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(1)
