#!/usr/bin/env python3
"""
orchestrator/process_supervisor.py

Generic process supervisor that spawns a stack of services defined in a YAML
configuration, tails logs into timestamped files, and propagates termination
signals to every child process to avoid orphaned or port-holding zombies.

Usage:
    python orchestrator/process_supervisor.py --config configs/stack.yaml
"""
from __future__ import annotations

import argparse
import asyncio
import os
import shlex
import signal
import sys
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, IO, List, Optional

try:
    import yaml
except ImportError as exc:  # pragma: no cover - requires manual install
    raise SystemExit("PyYAML is required. Install with `pip install pyyaml`.") from exc

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency
    load_dotenv = None  # type: ignore[assignment]


ROOT_DIR = Path(__file__).resolve().parents[1]
VENV_PY = ROOT_DIR / ".venv" / "bin" / "python"
PYTHON_BIN = (
    str(VENV_PY)
    if VENV_PY.exists()
    else (sys.executable or shutil.which("python3") or "python3")
)


def _substitute(value: str, tokens: Dict[str, str]) -> str:
    out = value
    for key, replacement in tokens.items():
        out = out.replace(f"{{{{{key}}}}}", replacement)
    return out


def _normalize_cmd(raw_cmd: Any, tokens: Dict[str, str]) -> List[str]:
    if isinstance(raw_cmd, str):
        cmd_str = _substitute(raw_cmd, tokens)
        parts = shlex.split(cmd_str)
        if not parts:
            raise ValueError("Command string must produce at least one argument.")
        return parts
    if isinstance(raw_cmd, list):
        result: List[str] = []
        for part in raw_cmd:
            if not isinstance(part, str):
                raise TypeError(f"Command elements must be strings, got {type(part).__name__}")
            result.append(_substitute(part, tokens))
        if not result:
            raise ValueError("Command list must contain at least one element.")
        return result
    raise TypeError(f"'cmd' must be string or list, got {type(raw_cmd).__name__}")


def _as_env_map(
    obj: Any, *, label: str, tokens: Optional[Dict[str, str]] = None
) -> Dict[str, str]:
    if obj is None:
        return {}
    if not isinstance(obj, dict):
        raise TypeError(f"{label} must be a mapping, got {type(obj).__name__}")
    env: Dict[str, str] = {}
    for key, val in obj.items():
        if not isinstance(key, str):
            raise TypeError(f"{label} keys must be strings (got {type(key).__name__})")
        if val is None:
            env[key] = ""
        elif isinstance(val, str) and tokens:
            env[key] = _substitute(val, tokens)
        else:
            env[key] = str(val)
    return env


def _maybe_override_cmd(name: str, current: List[str]) -> List[str]:
    """Allow swapping a service implementation via environment variables.

    Priority:
      1) SERVICE_CMD_<SERVICE_NAME>  (non-alnum -> _ , uppercased)
      2) Stage-specific aliases for common stages:
         INGEST_CMD, BARS1S_CMD, BARAGG_CMD
    Returns the parsed override (shlex.split) or the original command.
    """

    def _env_key_from_service(service_name: str) -> str:
        normalized = []
        for ch in service_name:
            normalized.append(ch if ch.isalnum() else "_")
        return "SERVICE_CMD_" + "".join(normalized).upper()

    candidates = [_env_key_from_service(name)]
    alias_map = {
        "INGEST_CMD": {"zerodha-ws", "ingestion", "ws"},
        "BARS1S_CMD": {"bar-builder-1s", "bars-1s", "bar_builder_1s"},
        "BARAGG_CMD": {"bar-aggregator", "bar_aggregator", "bars-agg", "agg-multi"},
    }
    for alias, names in alias_map.items():
        if name in names:
            candidates.append(alias)

    for env_key in candidates:
        raw = os.getenv(env_key)
        if raw:
            parts = shlex.split(raw)
            if parts:
                return parts
    return current


@dataclass
class ServiceSpec:
    name: str
    cmd: List[str]
    cwd: Path
    env: Dict[str, str]
    stop_on_exit: bool


@dataclass
class SupervisorConfig:
    services: List[ServiceSpec]
    log_root: Path
    shutdown_grace: float
    tail_lines: int
    base_env: Dict[str, str]


@dataclass
class ServiceState:
    spec: ServiceSpec
    process: asyncio.subprocess.Process
    log_path: Path
    log_handle: IO[str]
    pgid: Optional[int]


class ProcessSupervisor:
    def __init__(self, cfg: SupervisorConfig, base_env: Dict[str, str]) -> None:
        self.cfg = cfg
        self.base_env = base_env
        now = datetime.now(timezone.utc)
        self.run_log_dir = cfg.log_root / f"stack_{now:%Y%m%d_%H%M%S}"
        self.run_log_dir.mkdir(parents=True, exist_ok=True)
        self.stop_event = asyncio.Event()
        self.states: Dict[str, ServiceState] = {}
        self.tasks: List[asyncio.Task[None]] = []

    async def run(self) -> None:
        await self._launch_all()
        loop = asyncio.get_running_loop()

        def _signal_handler(signame: str) -> None:
            print(f"[supervisor] received {signame}; initiating shutdown…", flush=True)
            self.stop_event.set()

        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
            try:
                loop.add_signal_handler(sig, _signal_handler, sig.name)
            except NotImplementedError:  # pragma: no cover (Windows)
                # Fall back to default behaviour; KeyboardInterrupt will still bubble up.
                pass

        await self.stop_event.wait()
        await self._shutdown()
        for task in self.tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        print("[supervisor] all services stopped.", flush=True)

    async def _launch_all(self) -> None:
        print(f"[supervisor] launching services… logs at {self.run_log_dir}", flush=True)
        for spec in self.cfg.services:
            state = await self._spawn_service(spec)
            self.states[spec.name] = state
            self.tasks.append(asyncio.create_task(self._monitor(state)))
            print(f"[supervisor] {spec.name:<20} pid={state.process.pid} log={state.log_path}", flush=True)

    async def _spawn_service(self, spec: ServiceSpec) -> ServiceState:
        env = self.base_env.copy()
        env.update(spec.env)

        log_path = self.run_log_dir / f"{spec.name}.log"
        handle = log_path.open("a", encoding="utf-8")
        handle.write(f"[supervisor] starting {spec.name}: {' '.join(spec.cmd)}\n")
        handle.flush()

        proc = await asyncio.create_subprocess_exec(
            *spec.cmd,
            stdout=handle,
            stderr=asyncio.subprocess.STDOUT,
            env=env,
            cwd=str(spec.cwd),
            start_new_session=True,
        )
        try:
            pgid = os.getpgid(proc.pid)
        except Exception:
            pgid = None

        return ServiceState(spec=spec, process=proc, log_path=log_path, log_handle=handle, pgid=pgid)

    async def _monitor(self, state: ServiceState) -> None:
        rc = await state.process.wait()
        print(f"[supervisor] {state.spec.name} exited with code {rc}", flush=True)
        tail = self._tail_log(state.log_path)
        if tail:
            print(f"[supervisor] --- last {self.cfg.tail_lines} lines from {state.spec.name} ---", flush=True)
            print(tail, flush=True)
            print("[supervisor] --- end log ---", flush=True)
        if state.spec.stop_on_exit or rc != 0:
            self.stop_event.set()

    async def _shutdown(self) -> None:
        if not self.states:
            return
        print("[supervisor] shutting down services…", flush=True)
        self._broadcast(signal.SIGTERM)
        await asyncio.sleep(self.cfg.shutdown_grace)
        kill_signal = getattr(signal, "SIGKILL", signal.SIGTERM)
        self._broadcast(kill_signal)
        for state in self.states.values():
            try:
                await state.process.wait()
            except Exception as exc:  # pragma: no cover - defensive
                print(f"[supervisor] wait failed for {state.spec.name}: {exc}", flush=True)
            try:
                state.log_handle.flush()
                state.log_handle.close()
            except Exception:
                pass

    def _broadcast(self, sig: signal.Signals) -> None:
        for state in self.states.values():
            proc = state.process
            if proc.returncode is not None:
                continue
            sent = False
            if state.pgid is not None and hasattr(os, "killpg"):
                try:
                    os.killpg(state.pgid, sig.value)
                    sent = True
                except ProcessLookupError:
                    continue
                except Exception as exc:
                    print(f"[supervisor] failed to signal group for {state.spec.name}: {exc}", flush=True)
            if not sent:
                try:
                    proc.send_signal(sig)
                except ProcessLookupError:
                    continue
                except Exception as exc:
                    print(f"[supervisor] failed to signal {state.spec.name}: {exc}", flush=True)

    def _tail_log(self, log_path: Path) -> str:
        try:
            data = log_path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            return ""
        lines = data.splitlines()
        tail = lines[-self.cfg.tail_lines :]
        return "\n".join(tail)


def _load_config(path: Path) -> SupervisorConfig:
    try:
        raw = yaml.safe_load(path.read_text()) or {}
    except FileNotFoundError:
        raise SystemExit(f"Config file not found: {path}")
    except yaml.YAMLError as exc:
        raise SystemExit(f"Unable to parse YAML config {path}: {exc}") from exc

    tokens = {"python": PYTHON_BIN, "root": str(ROOT_DIR)}

    log_dir_raw = raw.get("log_dir", "runs/logs/supervisor")
    log_root = Path(_substitute(str(log_dir_raw), tokens)).expanduser()
    if not log_root.is_absolute():
        log_root = ROOT_DIR / log_root
    shutdown_grace = float(raw.get("shutdown_grace_seconds", 5.0))
    tail_lines = int(raw.get("tail_lines", 20))
    default_cwd_raw = raw.get("default_cwd", str(ROOT_DIR))
    default_cwd = Path(_substitute(str(default_cwd_raw), tokens)).expanduser()
    if not default_cwd.is_absolute():
        default_cwd = ROOT_DIR / default_cwd
    config_env = _as_env_map(raw.get("env"), label="config env", tokens=tokens)
    stop_every_exit = bool(raw.get("stop_on_any_exit", True))

    services_raw = raw.get("services")
    if not services_raw or not isinstance(services_raw, list):
        raise SystemExit("Config must define a non-empty `services` list.")

    services: List[ServiceSpec] = []
    seen_names: set[str] = set()
    for idx, item in enumerate(services_raw):
        if not isinstance(item, dict):
            raise SystemExit(f"Service #{idx} must be a mapping, got {type(item).__name__}")
        name = item.get("name")
        if not name or not isinstance(name, str):
            raise SystemExit(f"Service #{idx} is missing a string `name` field.")
        if name in seen_names:
            raise SystemExit(f"Duplicate service name detected: {name}")
        seen_names.add(name)
        cmd_raw = item.get("cmd")
        if cmd_raw is None:
            raise SystemExit(f"Service {name!r} is missing required `cmd` field.")
        cmd = _normalize_cmd(cmd_raw, tokens)
        cmd = _maybe_override_cmd(name, cmd)
        cwd_raw = item.get("cwd")
        if isinstance(cwd_raw, str):
            cwd_path = Path(_substitute(cwd_raw, tokens)).expanduser()
            if not cwd_path.is_absolute():
                cwd_path = ROOT_DIR / cwd_path
        else:
            cwd_path = default_cwd
        env = _as_env_map(item.get("env"), label=f"env for service {name}", tokens=tokens)
        stop_on_exit = bool(item.get("stop_on_exit", stop_every_exit))
        services.append(ServiceSpec(name=name, cmd=cmd, cwd=cwd_path, env=env, stop_on_exit=stop_on_exit))

    return SupervisorConfig(
        services=services,
        log_root=log_root,
        shutdown_grace=shutdown_grace,
        tail_lines=tail_lines,
        base_env=config_env,
    )


def _default_config_path() -> Optional[Path]:
    for rel in ("configs/process_supervisor.yaml", "configs/stack.yaml", "configs/orchestrator.yaml"):
        candidate = ROOT_DIR / rel
        if candidate.exists():
            return candidate
    return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run multiple services and manage their lifecycle.")
    parser.add_argument(
        "-c",
        "--config",
        type=Path,
        default=None,
        help="Path to YAML config (defaults to configs/process_supervisor.yaml if present).",
    )
    return parser.parse_args()


def build_base_env(config_env: Dict[str, str]) -> Dict[str, str]:
    if load_dotenv is not None:
        for env_file in (
            ROOT_DIR / ".env",
            ROOT_DIR / "infra" / ".env.docker",
            ROOT_DIR / "infra" / ".env",
        ):
            if env_file.exists():
                load_dotenv(env_file, override=False)
    base_env = os.environ.copy()
    venv_bin = VENV_PY.parent
    if venv_bin.exists():
        base_env["PATH"] = f"{venv_bin}{os.pathsep}{base_env.get('PATH', '')}"
        base_env.setdefault("VIRTUAL_ENV", str(venv_bin.parent))
    project_path = str(ROOT_DIR)
    existing_py = base_env.get("PYTHONPATH")
    if existing_py:
        if project_path not in existing_py.split(os.pathsep):
            base_env["PYTHONPATH"] = os.pathsep.join([project_path, existing_py])
    else:
        base_env["PYTHONPATH"] = project_path
    base_env.update(config_env)
    return base_env


def main() -> None:
    args = parse_args()
    cfg_path = args.config or _default_config_path()
    if cfg_path is None:
        raise SystemExit(
            "No config provided and no default config found. "
            "Create configs/process_supervisor.yaml or pass --config."
        )
    cfg = _load_config(cfg_path)
    base_env = build_base_env(cfg.base_env)
    supervisor = ProcessSupervisor(cfg, base_env)
    try:
        asyncio.run(supervisor.run())
    except KeyboardInterrupt:
        print("[supervisor] interrupted; shutting down…", flush=True)
        # asyncio.run already handled; nothing else needed.


if __name__ == "__main__":
    main()
