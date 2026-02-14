#!/usr/bin/env python3
"""Manual pipeline load/performance comparison: Python stack vs Go stack.

This script:
1) spins synthetic tick load with varied symbol counts,
2) runs the ingestion+aggregation pipeline in Python and in Go,
3) samples Prometheus-style metrics from service /metrics endpoints,
4) emits a report (markdown + json) with throughput/latency/drop comparisons.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import signal
import socket
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
RUNS_DIR = ROOT / "runs" / "perf_compare"
TARGETS_PATH = ROOT / "infra" / "prometheus" / "targets_apps.json"
BENCH_PORTS = (8111, 8112, 8113, 8114, 8115, 8116, 8117, 8118)
GO_IMAGE_DEFAULT = "trading-stack-go-builder"
GO_NETWORK_DEFAULT = "trading-stack_core"


@dataclass
class StageResult:
    scenario: str
    runtime: str
    symbols: int
    duration_sec: float
    source_ticks_per_sec: float
    source_latency_p95_sec: Optional[float]
    source_dropped: float
    source_queue_peak: float
    builder_ticks_per_sec: float
    builder_bars_per_sec: float
    builder_latency_p95_sec: Optional[float]
    agg_3m_bars_per_sec: float
    agg_latency_p95_sec: Optional[float]
    pair_signals_per_sec: Optional[float] = None
    orders_emitted_per_sec: Optional[float] = None
    risk_approved_per_sec: Optional[float] = None
    fills_per_sec: Optional[float] = None
    executor_signal_lag_p95_sec: Optional[float] = None
    executor_publish_lag_p95_sec: Optional[float] = None
    risk_order_lag_p95_sec: Optional[float] = None
    matcher_order_lag_p95_sec: Optional[float] = None
    end_to_end_p95_sec: Optional[float] = None


@dataclass
class ManagedProcess:
    name: str
    proc: subprocess.Popen
    log_path: Path
    log_handle: any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run manual varied-load benchmark and compare Python vs Go pipeline.")
    parser.add_argument("--loads", default="50,200,500", help="Comma-separated symbol counts per stage.")
    parser.add_argument(
        "--scenario",
        choices=("pipeline", "paper-flow"),
        default=os.getenv("PERF_SCENARIO", "paper-flow"),
        help="Benchmark scenario: pipeline (ingest+agg only) or paper-flow (full paper stack).",
    )
    parser.add_argument("--stage-seconds", type=int, default=180, help="Measurement duration per stage in seconds.")
    parser.add_argument("--warmup-seconds", type=int, default=20, help="Warmup duration before measurements.")
    parser.add_argument("--sample-seconds", type=float, default=5.0, help="Sampling interval for peak gauges.")
    parser.add_argument("--pair-timeframe", type=int, default=1, help="Pair timeframe minutes for paper-flow synthetic config.")
    parser.add_argument("--pair-lookback", type=int, default=4, help="Pair lookback for paper-flow synthetic config.")
    parser.add_argument(
        "--pair-count",
        type=int,
        default=0,
        help="How many synthetic pairs to generate per stage (0 => symbols/2).",
    )
    parser.add_argument(
        "--pair-min-ready",
        type=int,
        default=2,
        help="PAIR_MIN_READY_POINTS for pair-watch in paper-flow mode.",
    )
    parser.add_argument("--sim-base-tps", type=float, default=5.0, help="Baseline ticks/sec per symbol for synthetic source.")
    parser.add_argument("--sim-hot-tps", type=float, default=1000.0, help="Hot-symbol ticks/sec used during burst rotations.")
    parser.add_argument(
        "--sim-hot-symbol-pct",
        type=float,
        default=0.002,
        help="Fraction of symbols treated as hot at a time (0.002 ~= 1 hot symbol for 500).",
    )
    parser.add_argument("--sim-hot-rotate-sec", type=float, default=15.0, help="Seconds before hot-symbol set rotates.")
    parser.add_argument("--sim-step-ms", type=int, default=100, help="Simulation scheduling step in milliseconds.")
    parser.add_argument("--go-produce-workers", type=int, default=16, help="Go ingestion producer worker goroutines.")
    parser.add_argument("--go-produce-queue", type=int, default=20000, help="Per-worker queue depth for Go ingestion.")
    parser.add_argument("--go-max-batch", type=int, default=1024, help="Go ingestion max batch size before flush.")
    parser.add_argument("--go-batch-flush-ms", type=int, default=20, help="Go ingestion flush interval in milliseconds.")
    parser.add_argument("--go-bar1s-workers", type=int, default=8, help="Go 1s bar builder worker goroutines.")
    parser.add_argument("--go-bar1s-queue", type=int, default=8000, help="Per-worker queue depth for Go 1s bar builder.")
    parser.add_argument("--go-bar1s-flush-ms", type=int, default=250, help="Go 1s bar builder flush tick in milliseconds.")
    parser.add_argument("--go-bar1m-workers", type=int, default=8, help="Go 1m bar aggregator worker goroutines.")
    parser.add_argument("--go-bar1m-queue", type=int, default=8000, help="Per-worker queue depth for Go 1m bar aggregator.")
    parser.add_argument("--go-bar1m-flush-ms", type=int, default=500, help="Go 1m bar aggregator flush tick in milliseconds.")
    parser.add_argument("--go-baragg-workers", type=int, default=8, help="Go multi-timeframe aggregator worker goroutines.")
    parser.add_argument("--go-baragg-queue", type=int, default=8000, help="Per-worker queue depth for Go multi-timeframe aggregator.")
    parser.add_argument("--go-baragg-flush-ms", type=int, default=500, help="Go multi-timeframe aggregator flush tick in milliseconds.")
    parser.add_argument("--kafka-broker", default="localhost:9092", help="Kafka bootstrap broker for benchmark services.")
    parser.add_argument("--metrics-host", default="host.docker.internal", help="Prometheus target host override while test runs.")
    parser.add_argument("--run-tag", default=None, help="Optional suffix for report folder.")
    parser.add_argument("--skip-go-build", action="store_true", help="Skip auto-build if Go binaries are missing.")
    parser.add_argument("--go-image", default=GO_IMAGE_DEFAULT, help="Docker image used to run Go benchmark services.")
    parser.add_argument("--go-network", default=GO_NETWORK_DEFAULT, help="Docker network for Go benchmark services.")
    return parser.parse_args()


def resolve_python_bin() -> str:
    venv_python = ROOT / ".venv" / "bin" / "python"
    if venv_python.exists():
        return str(venv_python)
    return sys.executable or "python3"


def run_cmd(cmd: List[str], *, cwd: Path, check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        cwd=str(cwd),
        check=check,
        text=True,
        capture_output=capture,
    )


def ensure_container_running(name: str) -> None:
    out = run_cmd(["docker", "inspect", "-f", "{{.State.Running}}", name], cwd=ROOT, capture=True)
    if out.stdout.strip().lower() != "true":
        raise SystemExit(f"Container `{name}` is not running. Start stack with `make up` first.")


def ensure_go_binaries(skip_build: bool, go_image: str) -> None:
    binaries = [
        ROOT / "go" / "bin" / "ws_bridge",
        ROOT / "go" / "bin" / "bar_builder_1s",
        ROOT / "go" / "bin" / "bar_aggregator_1m",
        ROOT / "go" / "bin" / "bar_aggregator_multi",
    ]
    missing = [p for p in binaries if not p.exists()]
    image_ready = True
    try:
        out = run_cmd(
            ["docker", "image", "inspect", "--format", "{{.Os}}/{{.Architecture}}", go_image],
            cwd=ROOT,
            check=True,
            capture=True,
        )
        image_ready = out.stdout.strip().startswith("linux/")
    except subprocess.CalledProcessError:
        image_ready = False
    if not missing and image_ready:
        return
    if skip_build:
        missing_bits: List[str] = []
        if missing:
            missing_bits.append("binaries")
        if not image_ready:
            missing_bits.append(f"image {go_image}")
        raise SystemExit(f"Missing Go {' and '.join(missing_bits)}. Run `make go-build`.")
    print("[perf] Go artifacts missing; running `make go-build`...")
    run_cmd(["make", "go-build"], cwd=ROOT, check=True)


def create_topics(topic_parts: Mapping[str, int]) -> None:
    for topic, partitions in topic_parts.items():
        cmd = (
            "TOPICS_BIN=${KAFKA_TOPICS_BIN:-/opt/bitnami/kafka/bin/kafka-topics.sh}; "
            "\"$TOPICS_BIN\" --bootstrap-server kafka:29092 --create --if-not-exists "
            f"--topic {topic} --replication-factor 1 --partitions {partitions}"
        )
        run_cmd(["docker", "exec", "-i", "kafka", "bash", "-lc", cmd], cwd=ROOT, check=True)


def write_tokens_csv(path: Path, symbols: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = ["instrument_token,tradingsymbol,subscribe"]
    for idx in range(symbols):
        tok = 100000 + idx
        sym = f"SIM{idx+1:04d}"
        lines.append(f"{tok},{sym},1")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def read_symbols_csv(path: Path) -> List[str]:
    out: List[str] = []
    with path.open("r", encoding="utf-8") as fh:
        header = fh.readline().strip().split(",")
        try:
            sym_idx = [x.strip() for x in header].index("tradingsymbol")
        except ValueError:
            return out
        for line in fh:
            cols = [x.strip() for x in line.strip().split(",")]
            if sym_idx < len(cols) and cols[sym_idx]:
                out.append(cols[sym_idx].upper())
    return out


def write_pairs_yaml(path: Path, symbols: List[str], *, pair_count: int, timeframe: int, lookback: int) -> int:
    usable = len(symbols) // 2
    if usable <= 0:
        raise RuntimeError("Need at least two symbols to generate synthetic pairs config.")
    want = pair_count if pair_count > 0 else usable
    total = min(usable, want)
    if total <= 0:
        raise RuntimeError("Synthetic pair count resolved to zero.")

    lines: List[str] = ["selections:"]
    for idx in range(total):
        leg_a = symbols[idx * 2]
        leg_b = symbols[idx * 2 + 1]
        lines.extend(
            [
                f"- symbol: {leg_a}-{leg_b}",
                "  strategy: PAIRS_MEANREV",
                f"  timeframe: {timeframe}",
                "  params:",
                f"    lookback: {lookback}",
                f"    beta_lookback: {lookback}",
                "    beta_mode: static",
                "    fixed_beta: 1.0",
                "    entry_z: 0.15",
                "    exit_z: 0.05",
                "    stop_z: 2.0",
                "    leverage: 1.0",
                "    notional_per_leg: 10000.0",
                "  bucket: MED",
                "  risk_per_trade: 0.01",
                "  flatten_hhmm: none",
                "  stats:",
                "    avg_hold_min: 5",
                "  notional: 20000.0",
                "  base_notional: 20000.0",
                "  leverage: 1.0",
            ]
        )

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return total


def apply_prometheus_target_override(metrics_host: str) -> str:
    original = TARGETS_PATH.read_text(encoding="utf-8")
    run_cmd(
        [
            resolve_python_bin(),
            "tools/metrics_catalog.py",
            "--registry",
            "configs/metrics_endpoints.json",
            "--host",
            metrics_host,
            "--check",
            "--write-prom-targets",
            "infra/prometheus/targets_apps.json",
        ],
        cwd=ROOT,
        check=True,
    )
    return original


def restore_prometheus_targets(original_content: str) -> None:
    TARGETS_PATH.write_text(original_content, encoding="utf-8")


def fetch_metrics(port: int) -> str:
    with urlopen(f"http://127.0.0.1:{port}/metrics", timeout=3) as resp:
        return resp.read().decode("utf-8", errors="replace")


def _split_labels(raw: str) -> List[str]:
    parts: List[str] = []
    cur: List[str] = []
    in_quotes = False
    escape = False
    for ch in raw:
        if escape:
            cur.append(ch)
            escape = False
            continue
        if ch == "\\":
            cur.append(ch)
            escape = True
            continue
        if ch == '"':
            cur.append(ch)
            in_quotes = not in_quotes
            continue
        if ch == "," and not in_quotes:
            token = "".join(cur).strip()
            if token:
                parts.append(token)
            cur = []
            continue
        cur.append(ch)
    token = "".join(cur).strip()
    if token:
        parts.append(token)
    return parts


def _parse_labels(raw: str) -> Dict[str, str]:
    labels: Dict[str, str] = {}
    for token in _split_labels(raw):
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        key = key.strip()
        value = value.strip()
        if value.startswith('"') and value.endswith('"') and len(value) >= 2:
            value = value[1:-1]
        value = value.replace('\\"', '"').replace("\\\\", "\\")
        labels[key] = value
    return labels


def scrape_component_metrics(component: str, port: int) -> Dict[str, Dict[Tuple[Tuple[str, str], ...], float]]:
    payload = fetch_metrics(port)
    out: Dict[str, Dict[Tuple[Tuple[str, str], ...], float]] = {}
    for line in payload.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        metric_part, value_part = parts[0], parts[1]
        try:
            value = float(value_part)
        except ValueError:
            continue

        if "{" in metric_part and metric_part.endswith("}"):
            name, raw_labels = metric_part.split("{", 1)
            labels = _parse_labels(raw_labels[:-1])
        else:
            name = metric_part
            labels = {}

        if name.endswith("_created"):
            continue
        labels["component"] = component
        key = tuple(sorted((k, str(v)) for k, v in labels.items()))
        out.setdefault(name, {})[key] = value
    return out


def merge_snapshots(
    per_component: Iterable[Dict[str, Dict[Tuple[Tuple[str, str], ...], float]]]
) -> Dict[str, Dict[Tuple[Tuple[str, str], ...], float]]:
    merged: Dict[str, Dict[Tuple[Tuple[str, str], ...], float]] = {}
    for snap in per_component:
        for metric, series in snap.items():
            dst = merged.setdefault(metric, {})
            for k, v in series.items():
                dst[k] = v
    return merged


def scrape_snapshot(ports: Mapping[str, int]) -> Dict[str, Dict[Tuple[Tuple[str, str], ...], float]]:
    snapshots = []
    for component, port in ports.items():
        snapshots.append(scrape_component_metrics(component, port))
    return merge_snapshots(snapshots)


def metric_total(
    snapshot: Mapping[str, Dict[Tuple[Tuple[str, str], ...], float]],
    metric: str,
    labels_filter: Optional[Mapping[str, str]] = None,
) -> float:
    total = 0.0
    for key, value in snapshot.get(metric, {}).items():
        labels = dict(key)
        if labels_filter:
            ok = True
            for fk, fv in labels_filter.items():
                if labels.get(fk) != fv:
                    ok = False
                    break
            if not ok:
                continue
        total += value
    return total


def counter_delta(
    start: Mapping[str, Dict[Tuple[Tuple[str, str], ...], float]],
    end: Mapping[str, Dict[Tuple[Tuple[str, str], ...], float]],
    metric: str,
    labels_filter: Optional[Mapping[str, str]] = None,
) -> float:
    return max(0.0, metric_total(end, metric, labels_filter) - metric_total(start, metric, labels_filter))


def _hist_quantile_from_cumulative(buckets: Mapping[float, float], quantile: float) -> Optional[float]:
    if not buckets:
        return None
    ordered = sorted(buckets.items(), key=lambda kv: kv[0])
    total = ordered[-1][1]
    if total <= 0:
        return None
    rank = total * quantile
    prev_le = 0.0
    prev_count = 0.0
    for le, count in ordered:
        if count >= rank:
            bucket_count = count - prev_count
            if bucket_count <= 0:
                return prev_le if math.isfinite(prev_le) else None
            if math.isinf(le):
                return prev_le if math.isfinite(prev_le) else None
            frac = (rank - prev_count) / bucket_count
            return prev_le + (le - prev_le) * frac
        prev_le = le
        prev_count = count
    return None


def histogram_p95_delta(
    start: Mapping[str, Dict[Tuple[Tuple[str, str], ...], float]],
    end: Mapping[str, Dict[Tuple[Tuple[str, str], ...], float]],
    bucket_metric: str,
    labels_filter: Optional[Mapping[str, str]] = None,
) -> Optional[float]:
    cumulative: Dict[float, float] = {}
    end_series = end.get(bucket_metric, {})
    start_series = start.get(bucket_metric, {})
    for key, end_v in end_series.items():
        labels = dict(key)
        if labels_filter:
            ok = True
            for fk, fv in labels_filter.items():
                if labels.get(fk) != fv:
                    ok = False
                    break
            if not ok:
                continue
        le_raw = labels.get("le")
        if le_raw is None:
            continue
        le = math.inf if le_raw == "+Inf" else float(le_raw)
        start_v = start_series.get(key, 0.0)
        delta = max(0.0, end_v - start_v)
        cumulative[le] = cumulative.get(le, 0.0) + delta
    return _hist_quantile_from_cumulative(cumulative, 0.95)


def _process_failure_details(processes: List[ManagedProcess]) -> List[str]:
    details: List[str] = []
    for mp in processes:
        rc = mp.proc.poll()
        if rc is None:
            continue
        tail = ""
        try:
            raw = mp.log_path.read_text(encoding="utf-8")
            lines = [ln for ln in raw.splitlines() if ln.strip()]
            if lines:
                tail = lines[-1]
        except Exception:
            pass
        if tail:
            details.append(f"{mp.name}(rc={rc}): {tail}")
        else:
            details.append(f"{mp.name}(rc={rc})")
    return details


def wait_for_ports(ports: Mapping[str, int], timeout_sec: int = 45, processes: Optional[List[ManagedProcess]] = None) -> None:
    deadline = time.time() + timeout_sec
    pending = dict(ports)
    while pending and time.time() < deadline:
        if processes:
            failed = _process_failure_details(processes)
            if failed:
                raise RuntimeError("Stage process exited before metrics came up: " + "; ".join(failed))
        for name, port in list(pending.items()):
            try:
                fetch_metrics(port)
                pending.pop(name, None)
            except Exception:
                pass
        if pending:
            time.sleep(1.0)
    if pending:
        raise RuntimeError(f"Metrics endpoints did not come up in time: {pending}")


def _is_port_open(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.5):
            return True
    except OSError:
        return False


def wait_for_ports_free(ports: Iterable[int], timeout_sec: int = 20) -> None:
    deadline = time.time() + timeout_sec
    unique = sorted(set(int(p) for p in ports))
    while time.time() < deadline:
        busy = [p for p in unique if _is_port_open(p)]
        if not busy:
            return
        time.sleep(0.5)
    busy = [p for p in unique if _is_port_open(p)]
    if busy:
        raise RuntimeError(f"Ports are still busy from previous stage: {busy}")


def start_managed_process(name: str, cmd: List[str], env: Mapping[str, str], log_dir: Path) -> ManagedProcess:
    log_path = log_dir / f"{name}.log"
    handle = log_path.open("w", encoding="utf-8")
    proc = subprocess.Popen(
        cmd,
        cwd=str(ROOT),
        env=dict(env),
        stdout=handle,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    return ManagedProcess(name=name, proc=proc, log_path=log_path, log_handle=handle)


def stop_processes(processes: List[ManagedProcess]) -> None:
    for mp in processes:
        if mp.proc.poll() is not None:
            continue
        try:
            os.killpg(mp.proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            continue
    deadline = time.time() + 12
    for mp in processes:
        while mp.proc.poll() is None and time.time() < deadline:
            time.sleep(0.2)
        if mp.proc.poll() is None:
            try:
                os.killpg(mp.proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
    for mp in processes:
        try:
            mp.log_handle.flush()
            mp.log_handle.close()
        except Exception:
            pass


def build_base_env(kafka_broker: str) -> Dict[str, str]:
    env = os.environ.copy()
    env.setdefault("KAFKA_BROKER", kafka_broker)
    env.setdefault("POSTGRES_HOST", "localhost")
    env.setdefault("POSTGRES_PORT", "5432")
    env.setdefault("POSTGRES_DB", "trading")
    env.setdefault("POSTGRES_USER", "trader")
    env.setdefault("POSTGRES_PASSWORD", "trader")
    root_path = str(ROOT)
    existing_py = env.get("PYTHONPATH", "")
    if existing_py:
        parts = existing_py.split(os.pathsep)
        if root_path not in parts:
            env["PYTHONPATH"] = os.pathsep.join([root_path, existing_py])
    else:
        env["PYTHONPATH"] = root_path
    return env


def go_run_cmd(
    *,
    container_name: str,
    image: str,
    network: str,
    metrics_port: int,
    env: Mapping[str, str],
    binary_path: str,
) -> List[str]:
    cmd = [
        "docker",
        "run",
        "--rm",
        "--name",
        container_name,
        "--network",
        network,
        "-p",
        f"{metrics_port}:{metrics_port}",
        "-v",
        f"{ROOT}:/workspace",
        "-w",
        "/workspace",
    ]
    for key, value in env.items():
        cmd.extend(["-e", f"{key}={value}"])
    cmd.extend([image, binary_path])
    return cmd


def start_stack(
    runtime: str,
    *,
    scenario: str,
    run_id: str,
    symbols: int,
    stage_dir: Path,
    topics: Mapping[str, str],
    tokens_csv: Path,
    pairs_yaml: Optional[Path],
    pair_min_ready: int,
    sim_base_tps: float,
    sim_hot_tps: float,
    sim_hot_symbol_pct: float,
    sim_hot_rotate_sec: float,
    sim_step_ms: int,
    go_produce_workers: int,
    go_produce_queue: int,
    go_max_batch: int,
    go_batch_flush_ms: int,
    go_bar1s_workers: int,
    go_bar1s_queue: int,
    go_bar1s_flush_ms: int,
    go_bar1m_workers: int,
    go_bar1m_queue: int,
    go_bar1m_flush_ms: int,
    go_baragg_workers: int,
    go_baragg_queue: int,
    go_baragg_flush_ms: int,
    kafka_broker: str,
    go_image: str,
    go_network: str,
) -> Tuple[List[ManagedProcess], Dict[str, int]]:
    py = resolve_python_bin()
    base = build_base_env(kafka_broker)
    processes: List[ManagedProcess] = []

    if scenario not in {"pipeline", "paper-flow"}:
        raise ValueError(f"Unknown scenario: {scenario}")
    if runtime not in {"python", "go"}:
        raise ValueError(f"Unknown runtime: {runtime}")

    run_slug = run_id.replace(".", "-")

    if runtime == "python":
        src_env = {
            **base,
            "TICKS_TOPIC": topics["ticks"],
            "METRICS_PORT": "8111",
            "ZERODHA_TOKENS_CSV": str(tokens_csv),
            "SIM_SYMBOLS": str(symbols),
            "SIM_STEP_MS": str(max(20, int(sim_step_ms))),
            "SIM_INTERVAL_MS": str(max(20, int(sim_step_ms))),
            "SIM_BASE_TPS": f"{max(0.0, sim_base_tps):.6f}",
            "SIM_HOT_TPS": f"{max(0.0, sim_hot_tps):.6f}",
            "SIM_HOT_SYMBOL_PCT": f"{max(0.0, sim_hot_symbol_pct):.6f}",
            "SIM_HOT_ROTATE_SEC": f"{max(1.0, sim_hot_rotate_sec):.3f}",
        }
        b1_env = {
            **base,
            "IN_TOPIC": topics["ticks"],
            "OUT_TOPIC": topics["bars1s"],
            "KAFKA_GROUP": f"bench-{run_id}-py-b1s",
            "METRICS_PORT": "8112",
        }
        a1_env = {
            **base,
            "IN_TOPIC": topics["bars1s"],
            "OUT_TOPIC": topics["bars1m"],
            "KAFKA_GROUP": f"bench-{run_id}-py-a1m",
            "METRICS_PORT": "8118",
        }
        am_env = {
            **base,
            "IN_TOPIC": topics["bars1m"],
            "OUT_TFS": "3,5,15",
            "OUT_TOPIC_3": topics["bars3m"],
            "OUT_TOPIC_5": topics["bars5m"],
            "OUT_TOPIC_15": topics["bars15m"],
            "KAFKA_GROUP": f"bench-{run_id}-py-amulti",
            "METRICS_PORT": "8113",
            "BAR_AGG_VERIFY": "0",
        }
        processes.extend(
            [
                start_managed_process("py_source", [py, "ingestion/zerodha_ws_sim.py"], src_env, stage_dir),
                start_managed_process("py_builder_1s", [py, "compute/bar_builder_1s.py"], b1_env, stage_dir),
            ]
        )
        if scenario == "pipeline":
            processes.append(start_managed_process("py_agg_1m", [py, "compute/bar_aggregator_1m.py"], a1_env, stage_dir))
        processes.append(start_managed_process("py_agg_multi", [py, "compute/bar_aggregator_1m_to_multi.py"], am_env, stage_dir))
    else:
        tokens_in_container = f"/workspace/{tokens_csv.relative_to(ROOT).as_posix()}"
        src_env = {
            "SIM_TICKS": "true",
            "KAFKA_BROKER": "kafka:29092",
            "ZERODHA_TOKENS_CSV": tokens_in_container,
            "TICKS_TOPIC": topics["ticks"],
            "METRICS_PORT": "8111",
            "KAFKA_GROUP": f"bench-{run_id}-go-src",
            "SIM_BASE_TPS": f"{max(0.0, sim_base_tps):.6f}",
            "SIM_HOT_TPS": f"{max(0.0, sim_hot_tps):.6f}",
            "SIM_HOT_SYMBOL_PCT": f"{max(0.0, sim_hot_symbol_pct):.6f}",
            "SIM_HOT_ROTATE_SEC": str(max(1, int(round(sim_hot_rotate_sec)))),
            "SIM_STEP_MS": str(max(20, int(sim_step_ms))),
            "PRODUCE_WORKERS": str(max(1, int(go_produce_workers))),
            "PRODUCE_QUEUE": str(max(1, int(go_produce_queue))),
            "MAX_BATCH": str(max(1, int(go_max_batch))),
            "BATCH_FLUSH_MS": str(max(1, int(go_batch_flush_ms))),
        }
        b1_env = {
            "KAFKA_BROKER": "kafka:29092",
            "POSTGRES_HOST": "postgres",
            "POSTGRES_PORT": "5432",
            "POSTGRES_DB": "trading",
            "POSTGRES_USER": "trader",
            "POSTGRES_PASSWORD": "trader",
            "IN_TOPIC": topics["ticks"],
            "OUT_TOPIC": topics["bars1s"],
            "KAFKA_GROUP": f"bench-{run_id}-go-b1s",
            "METRICS_PORT": "8112",
            "BAR1S_WORKERS": str(max(1, int(go_bar1s_workers))),
            "BAR1S_QUEUE_SIZE": str(max(1, int(go_bar1s_queue))),
            "BAR1S_FLUSH_TICK_MS": str(max(1, int(go_bar1s_flush_ms))),
        }
        a1_env = {
            "KAFKA_BROKER": "kafka:29092",
            "POSTGRES_HOST": "postgres",
            "POSTGRES_PORT": "5432",
            "POSTGRES_DB": "trading",
            "POSTGRES_USER": "trader",
            "POSTGRES_PASSWORD": "trader",
            "IN_TOPIC": topics["bars1s"],
            "OUT_TOPIC": topics["bars1m"],
            "KAFKA_GROUP": f"bench-{run_id}-go-a1m",
            "METRICS_PORT": "8118",
            "BAR1M_WORKERS": str(max(1, int(go_bar1m_workers))),
            "BAR1M_QUEUE_SIZE": str(max(1, int(go_bar1m_queue))),
            "BAR1M_FLUSH_TICK_MS": str(max(1, int(go_bar1m_flush_ms))),
        }
        am_env = {
            "KAFKA_BROKER": "kafka:29092",
            "POSTGRES_HOST": "postgres",
            "POSTGRES_PORT": "5432",
            "POSTGRES_DB": "trading",
            "POSTGRES_USER": "trader",
            "POSTGRES_PASSWORD": "trader",
            "IN_TOPIC": topics["bars1m"],
            "OUT_TFS": "3,5,15",
            "OUT_TOPIC_PREFIX": topics["bars_prefix"],
            "KAFKA_GROUP": f"bench-{run_id}-go-amulti",
            "METRICS_PORT": "8113",
            "BARAGG_WORKERS": str(max(1, int(go_baragg_workers))),
            "BARAGG_QUEUE_SIZE": str(max(1, int(go_baragg_queue))),
            "BARAGG_FLUSH_TICK_MS": str(max(1, int(go_baragg_flush_ms))),
        }
        processes.extend(
            [
                start_managed_process(
                    "go_source",
                    go_run_cmd(
                        container_name=f"bench-go-source-{run_slug}",
                        image=go_image,
                        network=go_network,
                        metrics_port=8111,
                        env=src_env,
                        binary_path="/out/ws_bridge",
                    ),
                    os.environ.copy(),
                    stage_dir,
                ),
                start_managed_process(
                    "go_builder_1s",
                    go_run_cmd(
                        container_name=f"bench-go-builder-{run_slug}",
                        image=go_image,
                        network=go_network,
                        metrics_port=8112,
                        env=b1_env,
                        binary_path="/out/bar_builder_1s",
                    ),
                    os.environ.copy(),
                    stage_dir,
                ),
                start_managed_process(
                    "go_agg_1m",
                    go_run_cmd(
                        container_name=f"bench-go-agg1m-{run_slug}",
                        image=go_image,
                        network=go_network,
                        metrics_port=8118,
                        env=a1_env,
                        binary_path="/out/bar_aggregator_1m",
                    ),
                    os.environ.copy(),
                    stage_dir,
                ),
                start_managed_process(
                    "go_agg_multi",
                    go_run_cmd(
                        container_name=f"bench-go-agg-{run_slug}",
                        image=go_image,
                        network=go_network,
                        metrics_port=8113,
                        env=am_env,
                        binary_path="/out/bar_aggregator_multi",
                    ),
                    os.environ.copy(),
                    stage_dir,
                ),
            ]
        )

    if scenario == "pipeline":
        ports = {
            "source": 8111,
            "builder": 8112,
            "agg_1m": 8118,
            "agg_multi": 8113,
        }
        return processes, ports

    a1_flow_env = {
        **base,
        "IN_TOPIC": topics["bars1s"],
        "OUT_TOPIC": topics["bars1m"],
        "KAFKA_GROUP": f"bench-{run_id}-flow-a1m",
        "METRICS_PORT": "8118",
    }
    pairwatch_env = {
        **base,
        "METRICS_PORT": "8114",
        "PAIRWATCH_METRICS_PORT": "8114",
        "PAIRWATCH_SOURCE_MODE": "kafka",
        "PAIRWATCH_TOPIC_PREFIX": topics["bars_prefix"],
        "PAIR_HYDRATE_SOURCE": "none",
        "PAIRWATCH_NOTIFY": "0",
        "PAIRWATCH_NOTIFY_SOUND": "0",
        "PAIRWATCH_STATE_EXPORT": "0",
        "PAIRWATCH_FLATTEN_HHMM": "none",
        "PAIR_MIN_READY_POINTS": str(max(1, pair_min_ready)),
        "OUT_TOPIC": topics["pairs_signals"],
    }
    if pairs_yaml:
        pairwatch_env["PAIRS_NEXT_DAY"] = str(pairs_yaml)
    pairs_exec_env = {
        **base,
        "METRICS_PORT": "8115",
        "IN_TOPIC": topics["pairs_signals"],
        "OUT_TOPIC": topics["orders"],
        "GROUP_ID": f"bench-{run_id}-pairs-exec",
        "PAIRS_EXEC_SKIP_VALIDATION": "1",
        "PAIRS_STATE_FILE": str(stage_dir / "pairs_state.json"),
    }
    if pairs_yaml:
        pairs_exec_env["PAIRS_FALLBACK_YAML"] = str(pairs_yaml)
    risk_env = {
        **base,
        "METRICS_PORT": "8116",
        "IN_TOPIC": topics["orders"],
        "OUT_TOPIC": topics["orders_sized"],
        "KAFKA_GROUP": f"bench-{run_id}-risk",
    }
    matcher_env = {
        **base,
        "METRICS_PORT": "8117",
        "IN_TOPIC": topics["orders_sized"],
        "KAFKA_GROUP": f"bench-{run_id}-paper-matcher",
    }

    if runtime == "python":
        processes.append(start_managed_process("flow_agg_1m", [py, "compute/bar_aggregator_1m.py"], a1_flow_env, stage_dir))
    processes.extend(
        [
            start_managed_process("flow_pair_watch", [py, "compute/pair_watch_producer.py"], pairwatch_env, stage_dir),
            start_managed_process("flow_pairs_exec", [py, "execution/pairs_executor.py"], pairs_exec_env, stage_dir),
            start_managed_process("flow_risk_mgr", [py, "risk/manager_v2.py"], risk_env, stage_dir),
            start_managed_process("flow_paper_matcher", [py, "execution/paper_gateway_matcher.py"], matcher_env, stage_dir),
        ]
    )

    ports = {
        "source": 8111,
        "builder": 8112,
        "agg_multi": 8113,
        "pair_watch": 8114,
        "pairs_exec": 8115,
        "risk_mgr": 8116,
        "paper_matcher": 8117,
        "agg_1m": 8118,
    }
    return processes, ports


def metric_config(runtime: str, scenario: str) -> Dict[str, object]:
    if runtime == "python":
        cfg: Dict[str, object] = {
            "source_ticks": "zerodha_ticks_received_total",
            "source_latency_bucket": "zerodha_tick_latency_seconds_bucket",
            "source_drop": "zerodha_ticks_dropped_total",
            "source_queue": "zerodha_queue_depth",
            "builder_ticks": "bars_1s_ticks_total",
            "builder_bars": "bars_1s_published_total",
            "builder_latency_bucket": "bars_1s_publish_latency_seconds_bucket",
            "agg_1m_bars": "bars_1m_published_total",
            "agg_1m_latency_bucket": "bars_1m_publish_latency_seconds_bucket",
            "agg_3m_bars": ("bar_agg_output_3m_total", None),
            "agg_latency_bucket": ("bar_agg_bar_latency_3m_seconds_bucket", None),
        }
    else:
        cfg = {
            "source_ticks": "ingest_ticks_total",
            "source_latency_bucket": "ingest_latency_seconds_bucket",
            "source_drop": "ingest_ticks_dropped_total",
            "source_queue": "ingest_queue_depth",
            "builder_ticks": "bars1s_ticks_total",
            "builder_bars": "bars1s_published_total",
            "builder_latency_bucket": "bars1s_publish_latency_seconds_bucket",
            "agg_1m_bars": "bars_1m_published_total",
            "agg_1m_latency_bucket": "bars_1m_publish_latency_seconds_bucket",
            "agg_3m_bars": ("baragg_output_total", {"tf": "3m"}),
            "agg_latency_bucket": ("baragg_bar_latency_seconds_bucket", {"tf": "3m"}),
        }

    if scenario == "paper-flow":
        cfg.update(
            {
                "pair_signals": "pairwatch_signals_emitted_total",
                "orders_emitted": "pairs_orders_emitted_total",
                "risk_approved": "risk_orders_approved_total",
                "fills": "exec_orders_filled_total",
                "executor_signal_lag": "pairs_executor_signal_lag_seconds_bucket",
                "executor_publish_lag": "pairs_executor_order_publish_lag_seconds_bucket",
                "risk_order_lag": "risk_manager_order_lag_seconds_bucket",
                "matcher_order_lag": "paper_matcher_order_lag_seconds_bucket",
            }
        )
    return cfg


def run_stage(
    runtime: str,
    symbols: int,
    *,
    scenario: str,
    stage_seconds: int,
    warmup_seconds: int,
    sample_seconds: float,
    pair_timeframe: int,
    pair_lookback: int,
    pair_count: int,
    pair_min_ready: int,
    sim_base_tps: float,
    sim_hot_tps: float,
    sim_hot_symbol_pct: float,
    sim_hot_rotate_sec: float,
    sim_step_ms: int,
    go_produce_workers: int,
    go_produce_queue: int,
    go_max_batch: int,
    go_batch_flush_ms: int,
    go_bar1s_workers: int,
    go_bar1s_queue: int,
    go_bar1s_flush_ms: int,
    go_bar1m_workers: int,
    go_bar1m_queue: int,
    go_bar1m_flush_ms: int,
    go_baragg_workers: int,
    go_baragg_queue: int,
    go_baragg_flush_ms: int,
    kafka_broker: str,
    run_root: Path,
    go_image: str,
    go_network: str,
) -> StageResult:
    # Avoid races where previous stage still owns benchmark ports.
    wait_for_ports_free(BENCH_PORTS, timeout_sec=30)

    run_id = f"{runtime}-{symbols}-{int(time.time())}"
    stage_dir = run_root / f"{runtime}_{symbols}"
    stage_dir.mkdir(parents=True, exist_ok=True)
    tokens_csv = stage_dir / "tokens.csv"
    write_tokens_csv(tokens_csv, symbols)
    symbol_list = read_symbols_csv(tokens_csv)

    pairs_yaml: Optional[Path] = None
    pair_total = 0
    if scenario == "paper-flow":
        pairs_yaml = stage_dir / "pairs_bench.yaml"
        pair_total = write_pairs_yaml(
            pairs_yaml,
            symbol_list,
            pair_count=pair_count,
            timeframe=max(1, pair_timeframe),
            lookback=max(2, pair_lookback),
        )

    bars_prefix = f"bench.{run_id}.bars"
    topics = {
        "bars_prefix": bars_prefix,
        "ticks": f"bench.{run_id}.ticks",
        "bars1s": f"{bars_prefix}.1s",
        "bars1m": f"{bars_prefix}.1m",
        "bars3m": f"{bars_prefix}.3m",
        "bars5m": f"{bars_prefix}.5m",
        "bars15m": f"{bars_prefix}.15m",
        "pairs_signals": f"bench.{run_id}.pairs.signals",
        "orders": f"bench.{run_id}.orders",
        "orders_sized": f"bench.{run_id}.orders.sized",
        "fills": f"bench.{run_id}.fills",
    }
    topic_parts = {
        topics["ticks"]: 6,
        topics["bars1s"]: 6,
        topics["bars1m"]: 6,
        topics["bars3m"]: 3,
        topics["bars5m"]: 3,
        topics["bars15m"]: 3,
    }
    if scenario == "paper-flow":
        topic_parts.update(
            {
                topics["pairs_signals"]: 3,
                topics["orders"]: 3,
                topics["orders_sized"]: 3,
                topics["fills"]: 3,
            }
        )
    create_topics(topic_parts)

    if scenario == "paper-flow":
        print(
            f"[perf] starting {runtime} stage for {symbols} symbols "
            f"(pairs={pair_total}, tf={pair_timeframe}m, lookback={pair_lookback})"
        )
    else:
        print(f"[perf] starting {runtime} stage for {symbols} symbols")
    processes, ports = start_stack(
        runtime,
        scenario=scenario,
        run_id=run_id,
        symbols=symbols,
        stage_dir=stage_dir,
        topics=topics,
        tokens_csv=tokens_csv,
        pairs_yaml=pairs_yaml,
        pair_min_ready=pair_min_ready,
        sim_base_tps=sim_base_tps,
        sim_hot_tps=sim_hot_tps,
        sim_hot_symbol_pct=sim_hot_symbol_pct,
        sim_hot_rotate_sec=sim_hot_rotate_sec,
        sim_step_ms=sim_step_ms,
        go_produce_workers=go_produce_workers,
        go_produce_queue=go_produce_queue,
        go_max_batch=go_max_batch,
        go_batch_flush_ms=go_batch_flush_ms,
        go_bar1s_workers=go_bar1s_workers,
        go_bar1s_queue=go_bar1s_queue,
        go_bar1s_flush_ms=go_bar1s_flush_ms,
        go_bar1m_workers=go_bar1m_workers,
        go_bar1m_queue=go_bar1m_queue,
        go_bar1m_flush_ms=go_bar1m_flush_ms,
        go_baragg_workers=go_baragg_workers,
        go_baragg_queue=go_baragg_queue,
        go_baragg_flush_ms=go_baragg_flush_ms,
        kafka_broker=kafka_broker,
        go_image=go_image,
        go_network=go_network,
    )
    try:
        wait_for_ports(ports, processes=processes)
        if warmup_seconds > 0:
            print(f"[perf] warmup {warmup_seconds}s ({runtime}, {symbols} symbols)")
            time.sleep(warmup_seconds)

        cfg = metric_config(runtime, scenario)
        start = scrape_snapshot(ports)
        t0 = time.time()
        queue_peak = metric_total(start, cfg["source_queue"])  # type: ignore[index]

        while time.time() - t0 < stage_seconds:
            time.sleep(max(0.5, sample_seconds))
            snap = scrape_snapshot(ports)
            queue_peak = max(queue_peak, metric_total(snap, cfg["source_queue"]))  # type: ignore[index]

        end = scrape_snapshot(ports)
        duration = max(1e-6, time.time() - t0)

        agg_3m_metric, agg_3m_filter = cfg["agg_3m_bars"]  # type: ignore[index]
        agg_lat_metric, agg_lat_filter = cfg["agg_latency_bucket"]  # type: ignore[index]
        agg_1m_latency_metric = cfg["agg_1m_latency_bucket"]  # type: ignore[index]
        pair_signals_per_sec: Optional[float] = None
        orders_emitted_per_sec: Optional[float] = None
        risk_approved_per_sec: Optional[float] = None
        fills_per_sec: Optional[float] = None
        executor_signal_lag_p95_sec: Optional[float] = None
        executor_publish_lag_p95_sec: Optional[float] = None
        risk_order_lag_p95_sec: Optional[float] = None
        matcher_order_lag_p95_sec: Optional[float] = None
        end_to_end_p95_sec: Optional[float] = None

        if scenario == "paper-flow":
            pair_signals_per_sec = counter_delta(start, end, cfg["pair_signals"]) / duration  # type: ignore[index]
            orders_emitted_per_sec = counter_delta(start, end, cfg["orders_emitted"]) / duration  # type: ignore[index]
            risk_approved_per_sec = counter_delta(start, end, cfg["risk_approved"]) / duration  # type: ignore[index]
            fills_per_sec = counter_delta(start, end, cfg["fills"]) / duration  # type: ignore[index]
            executor_signal_lag_p95_sec = histogram_p95_delta(start, end, cfg["executor_signal_lag"])  # type: ignore[index]
            executor_publish_lag_p95_sec = histogram_p95_delta(start, end, cfg["executor_publish_lag"])  # type: ignore[index]
            risk_order_lag_p95_sec = histogram_p95_delta(start, end, cfg["risk_order_lag"])  # type: ignore[index]
            matcher_order_lag_p95_sec = histogram_p95_delta(start, end, cfg["matcher_order_lag"])  # type: ignore[index]
            end_to_end_p95_sec = matcher_order_lag_p95_sec

        agg_latency_p95 = histogram_p95_delta(start, end, agg_lat_metric, agg_lat_filter)
        if agg_latency_p95 is None:
            agg_latency_p95 = histogram_p95_delta(start, end, agg_1m_latency_metric)

        return StageResult(
            scenario=scenario,
            runtime=runtime,
            symbols=symbols,
            duration_sec=duration,
            source_ticks_per_sec=counter_delta(start, end, cfg["source_ticks"]) / duration,  # type: ignore[index]
            source_latency_p95_sec=histogram_p95_delta(start, end, cfg["source_latency_bucket"]),  # type: ignore[index]
            source_dropped=counter_delta(start, end, cfg["source_drop"]),  # type: ignore[index]
            source_queue_peak=queue_peak,
            builder_ticks_per_sec=counter_delta(start, end, cfg["builder_ticks"]) / duration,  # type: ignore[index]
            builder_bars_per_sec=counter_delta(start, end, cfg["builder_bars"]) / duration,  # type: ignore[index]
            builder_latency_p95_sec=histogram_p95_delta(start, end, cfg["builder_latency_bucket"]),  # type: ignore[index]
            agg_3m_bars_per_sec=counter_delta(start, end, agg_3m_metric, agg_3m_filter) / duration,
            agg_latency_p95_sec=agg_latency_p95,
            pair_signals_per_sec=pair_signals_per_sec,
            orders_emitted_per_sec=orders_emitted_per_sec,
            risk_approved_per_sec=risk_approved_per_sec,
            fills_per_sec=fills_per_sec,
            executor_signal_lag_p95_sec=executor_signal_lag_p95_sec,
            executor_publish_lag_p95_sec=executor_publish_lag_p95_sec,
            risk_order_lag_p95_sec=risk_order_lag_p95_sec,
            matcher_order_lag_p95_sec=matcher_order_lag_p95_sec,
            end_to_end_p95_sec=end_to_end_p95_sec,
        )
    finally:
        stop_processes(processes)
        # Ensure ports are actually released before next stage.
        wait_for_ports_free(BENCH_PORTS, timeout_sec=30)
        print(f"[perf] stopped {runtime} stage for {symbols} symbols")


def _fmt(v: Optional[float], digits: int = 4) -> str:
    if v is None:
        return "-"
    return f"{v:.{digits}f}"


def render_report_md(
    *,
    scenario: str,
    started_at: str,
    loads: List[int],
    stage_seconds: int,
    warmup_seconds: int,
    sample_seconds: float,
    sim_base_tps: float,
    sim_hot_tps: float,
    sim_hot_symbol_pct: float,
    sim_hot_rotate_sec: float,
    sim_step_ms: int,
    go_produce_workers: int,
    go_produce_queue: int,
    go_max_batch: int,
    go_batch_flush_ms: int,
    go_bar1s_workers: int,
    go_bar1s_queue: int,
    go_bar1s_flush_ms: int,
    go_bar1m_workers: int,
    go_bar1m_queue: int,
    go_bar1m_flush_ms: int,
    go_baragg_workers: int,
    go_baragg_queue: int,
    go_baragg_flush_ms: int,
    results: List[StageResult],
) -> str:
    lines: List[str] = []
    if scenario == "paper-flow":
        lines.append("# Paper Flow Load Test Report (Python vs Go core)")
    else:
        lines.append("# Pipeline Load Test Report (Python vs Go)")
    lines.append("")
    lines.append(f"- Scenario: {scenario}")
    lines.append(f"- Started: {started_at}")
    lines.append(f"- Loads (symbols): {loads}")
    lines.append(f"- Stage seconds: {stage_seconds}")
    lines.append(f"- Warmup seconds: {warmup_seconds}")
    lines.append(f"- Sample seconds: {sample_seconds}")
    lines.append(
        f"- Tick profile: base={sim_base_tps:.2f}/sym/s hot={sim_hot_tps:.2f}/sym/s "
        f"hot_pct={sim_hot_symbol_pct:.4f} rotate={sim_hot_rotate_sec:.1f}s step={sim_step_ms}ms"
    )
    lines.append(
        f"- Go ingest workers: workers={go_produce_workers} queue={go_produce_queue} "
        f"max_batch={go_max_batch} flush_ms={go_batch_flush_ms}"
    )
    lines.append(
        f"- Go builder workers: workers={go_bar1s_workers} queue={go_bar1s_queue} "
        f"flush_ms={go_bar1s_flush_ms}"
    )
    lines.append(
        f"- Go 1m agg workers: workers={go_bar1m_workers} queue={go_bar1m_queue} "
        f"flush_ms={go_bar1m_flush_ms}"
    )
    lines.append(
        f"- Go agg workers: workers={go_baragg_workers} queue={go_baragg_queue} "
        f"flush_ms={go_baragg_flush_ms}"
    )
    if scenario == "paper-flow":
        lines.append("- Ports under test: 8111..8118 (source, compute, pair-watch, execution, risk)")
    else:
        lines.append("- Ports under test: source=8111, builder=8112, agg1m=8118, agg_multi=8113")
    lines.append("")
    lines.append("## Per-Stage Results")
    lines.append("")
    lines.append(
        "| Runtime | Symbols | Source ticks/s | Source p95(s) | Dropped | Queue peak | Builder ticks/s | Builder bars/s | Builder p95(s) | 3m bars/s | Agg p95(s) |"
    )
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for r in results:
        lines.append(
            f"| {r.runtime} | {r.symbols} | {r.source_ticks_per_sec:.2f} | {_fmt(r.source_latency_p95_sec)} | "
            f"{r.source_dropped:.0f} | {r.source_queue_peak:.0f} | {r.builder_ticks_per_sec:.2f} | "
            f"{r.builder_bars_per_sec:.2f} | {_fmt(r.builder_latency_p95_sec)} | {r.agg_3m_bars_per_sec:.4f} | {_fmt(r.agg_latency_p95_sec)} |"
        )
    lines.append("")

    if scenario == "paper-flow":
        lines.append("## Full Flow Metrics")
        lines.append("")
        lines.append(
            "| Runtime | Symbols | Signals/s | Orders/s | Risk approved/s | Fills/s | Exec signal p95(s) | Exec publish p95(s) | Risk lag p95(s) | Matcher lag p95(s) | End-to-end p95(s) |"
        )
        lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
        for r in results:
            lines.append(
                f"| {r.runtime} | {r.symbols} | {_fmt(r.pair_signals_per_sec)} | {_fmt(r.orders_emitted_per_sec)} | "
                f"{_fmt(r.risk_approved_per_sec)} | {_fmt(r.fills_per_sec)} | {_fmt(r.executor_signal_lag_p95_sec)} | "
                f"{_fmt(r.executor_publish_lag_p95_sec)} | {_fmt(r.risk_order_lag_p95_sec)} | {_fmt(r.matcher_order_lag_p95_sec)} | {_fmt(r.end_to_end_p95_sec)} |"
            )
        lines.append("")

    lines.append("## Go vs Python Delta")
    lines.append("")
    if scenario == "paper-flow":
        lines.append(
            "| Symbols | Builder ticks/s ratio (Go/Py) | Builder p95 delta (Py-Go sec) | Source drops (Py-Go) | End-to-end p95 delta (Py-Go sec) |"
        )
        lines.append("|---:|---:|---:|---:|---:|")
    else:
        lines.append("| Symbols | Builder ticks/s ratio (Go/Py) | Builder p95 delta (Py-Go sec) | Source drops (Py-Go) |")
        lines.append("|---:|---:|---:|---:|")
    by_key: Dict[int, Dict[str, StageResult]] = {}
    for r in results:
        by_key.setdefault(r.symbols, {})[r.runtime] = r
    for symbols in loads:
        pair = by_key.get(symbols, {})
        py = pair.get("python")
        go = pair.get("go")
        if not py or not go:
            continue
        ratio = go.builder_ticks_per_sec / py.builder_ticks_per_sec if py.builder_ticks_per_sec > 0 else float("nan")
        p95_delta = None
        if py.builder_latency_p95_sec is not None and go.builder_latency_p95_sec is not None:
            p95_delta = py.builder_latency_p95_sec - go.builder_latency_p95_sec
        drop_delta = py.source_dropped - go.source_dropped
        if scenario == "paper-flow":
            e2e_delta: Optional[float] = None
            if py.end_to_end_p95_sec is not None and go.end_to_end_p95_sec is not None:
                e2e_delta = py.end_to_end_p95_sec - go.end_to_end_p95_sec
            lines.append(f"| {symbols} | {_fmt(ratio)} | {_fmt(p95_delta)} | {drop_delta:.0f} | {_fmt(e2e_delta)} |")
        else:
            lines.append(f"| {symbols} | {_fmt(ratio)} | {_fmt(p95_delta)} | {drop_delta:.0f} |")
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- Prometheus targets are temporarily switched to host metrics during test and restored after completion.")
    lines.append("- Keep Grafana open on the control-plane dashboard while test runs to visualize live service health and latency.")
    lines.append("- `Agg p95(s)` uses 3m publish latency when available and falls back to 1m publish latency.")
    lines.append("- Very short runs (<~65s) may not emit enough aggregator bars for stable compute-latency quantiles.")
    if scenario == "paper-flow":
        lines.append("- Full paper-flow keeps pair-watch, pairs-exec, risk-manager, and paper-matcher running concurrently.")
        lines.append("- End-to-end p95 uses `paper_matcher_order_lag_seconds_bucket` (signal ts -> simulated fill).")
        lines.append("- Live-only modules (budget-guard, zerodha-gateway, zerodha-poller) are intentionally skipped in paper-flow.")
    else:
        lines.append("- Python stack path: ws-sim -> bars1s -> bars1m -> multi-agg.")
        lines.append("- Go stack path: ws-bridge(sim) -> bars1s -> bars1m -> multi-agg.")
    return "\n".join(lines) + "\n"


def main() -> int:
    args = parse_args()
    loads = [int(x.strip()) for x in args.loads.split(",") if x.strip()]
    if not loads:
        raise SystemExit("No load stages provided.")
    for n in loads:
        if n <= 0:
            raise SystemExit("Loads must be positive integers.")

    ensure_container_running("kafka")
    ensure_container_running("postgres")
    ensure_go_binaries(skip_build=args.skip_go_build, go_image=args.go_image)

    started_at = datetime.now(timezone.utc).isoformat()
    suffix = args.run_tag or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    run_root = RUNS_DIR / suffix
    run_root.mkdir(parents=True, exist_ok=True)

    print("[perf] overriding Prometheus app target host for manual benchmark...")
    original_targets = apply_prometheus_target_override(args.metrics_host)
    results: List[StageResult] = []
    failed = False
    try:
        print("[perf] benchmark starting. Open Grafana dashboard while this runs: http://localhost:3000")
        for runtime in ("python", "go"):
            for symbols in loads:
                result = run_stage(
                    runtime,
                    symbols,
                    scenario=args.scenario,
                    stage_seconds=args.stage_seconds,
                    warmup_seconds=args.warmup_seconds,
                    sample_seconds=args.sample_seconds,
                    pair_timeframe=args.pair_timeframe,
                    pair_lookback=args.pair_lookback,
                    pair_count=args.pair_count,
                    pair_min_ready=args.pair_min_ready,
                    sim_base_tps=args.sim_base_tps,
                    sim_hot_tps=args.sim_hot_tps,
                    sim_hot_symbol_pct=args.sim_hot_symbol_pct,
                    sim_hot_rotate_sec=args.sim_hot_rotate_sec,
                    sim_step_ms=args.sim_step_ms,
                    go_produce_workers=args.go_produce_workers,
                    go_produce_queue=args.go_produce_queue,
                    go_max_batch=args.go_max_batch,
                    go_batch_flush_ms=args.go_batch_flush_ms,
                    go_bar1s_workers=args.go_bar1s_workers,
                    go_bar1s_queue=args.go_bar1s_queue,
                    go_bar1s_flush_ms=args.go_bar1s_flush_ms,
                    go_bar1m_workers=args.go_bar1m_workers,
                    go_bar1m_queue=args.go_bar1m_queue,
                    go_bar1m_flush_ms=args.go_bar1m_flush_ms,
                    go_baragg_workers=args.go_baragg_workers,
                    go_baragg_queue=args.go_baragg_queue,
                    go_baragg_flush_ms=args.go_baragg_flush_ms,
                    kafka_broker=args.kafka_broker,
                    run_root=run_root,
                    go_image=args.go_image,
                    go_network=args.go_network,
                )
                results.append(result)
    except Exception as exc:
        failed = True
        print(f"[perf] ERROR: {exc}", file=sys.stderr)
    finally:
        print("[perf] restoring Prometheus targets...")
        restore_prometheus_targets(original_targets)

    report_md = render_report_md(
        scenario=args.scenario,
        started_at=started_at,
        loads=loads,
        stage_seconds=args.stage_seconds,
        warmup_seconds=args.warmup_seconds,
        sample_seconds=args.sample_seconds,
        sim_base_tps=args.sim_base_tps,
        sim_hot_tps=args.sim_hot_tps,
        sim_hot_symbol_pct=args.sim_hot_symbol_pct,
        sim_hot_rotate_sec=args.sim_hot_rotate_sec,
        sim_step_ms=args.sim_step_ms,
        go_produce_workers=args.go_produce_workers,
        go_produce_queue=args.go_produce_queue,
        go_max_batch=args.go_max_batch,
        go_batch_flush_ms=args.go_batch_flush_ms,
        go_bar1s_workers=args.go_bar1s_workers,
        go_bar1s_queue=args.go_bar1s_queue,
        go_bar1s_flush_ms=args.go_bar1s_flush_ms,
        go_bar1m_workers=args.go_bar1m_workers,
        go_bar1m_queue=args.go_bar1m_queue,
        go_bar1m_flush_ms=args.go_bar1m_flush_ms,
        go_baragg_workers=args.go_baragg_workers,
        go_baragg_queue=args.go_baragg_queue,
        go_baragg_flush_ms=args.go_baragg_flush_ms,
        results=results,
    )
    (run_root / "report.md").write_text(report_md, encoding="utf-8")
    (run_root / "report.json").write_text(json.dumps([asdict(r) for r in results], indent=2) + "\n", encoding="utf-8")

    print(f"[perf] report written: {run_root / 'report.md'}")
    print(f"[perf] raw json written: {run_root / 'report.json'}")
    print(f"[perf] logs directory: {run_root}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
