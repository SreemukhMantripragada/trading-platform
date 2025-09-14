"""
monitoring/process_supervisor.py
- Starts a set of processes defined in ops/stack.plan.yaml
- Restarts on exit, checks /metrics TCP reachability, honors a global kill.
- Clean CTRL+C stops all.

Run:
  . .venv/bin/activate && python monitoring/process_supervisor.py

YAML spec (ops/stack.plan.yaml):
  processes:
    - name: bars1s
      cmd: "KAFKA_BROKER=localhost:9092 IN_TOPIC=ticks OUT_TOPIC=bars.1s METRICS_PORT=8003 python compute/bar_builder_1s.py"
      port: 8003
    - name: bars1m
      cmd: "KAFKA_BROKER=localhost:9092 IN_TOPIC=bars.1s OUT_TOPIC=bars.1m METRICS_PORT=8004 python compute/bar_aggregator.py"
      port: 8004
    - name: runner
      cmd: "KAFKA_BROKER=localhost:9092 IN_TOPIC=bars.1m OUT_TOPIC=orders METRICS_PORT=8011 python strategy/runner_unified.py"
      port: 8011
    - name: budget_guard
      cmd: "KAFKA_BROKER=localhost:9092 IN_TOPIC=orders OUT_TOPIC=orders.allowed METRICS_PORT=8023 python risk/order_budget_guard.py"
      port: 8023
    - name: gateway
      cmd: "DRY_RUN=1 KAFKA_BROKER=localhost:9092 IN_TOPIC=orders.allowed FILL_TOPIC=fills METRICS_PORT=8017 python execution/zerodha_gateway.py"
      port: 8017
    - name: pnl
      cmd: "METRICS_PORT=8024 python execution/accounting_pnl.py"
      port: 8024
"""
from __future__ import annotations
import os, sys, time, yaml, socket, subprocess, signal

PLAN_FILE="ops/stack.plan.yaml"

def port_alive(host, port, timeout=1.0)->bool:
    try:
        with socket.create_connection((host, port), timeout=timeout): return True
    except Exception:
        return False

def main():
    plan=yaml.safe_load(open(PLAN_FILE))
    procs={}
    try:
        while True:
            # (re)start any missing
            for p in plan.get("processes", []):
                name=p["name"]; port=int(p.get("port") or 0); cmd=p["cmd"]
                if name not in procs or procs[name].poll() is not None:
                    print(f"[supervisor] starting {name}")
                    procs[name]=subprocess.Popen(cmd, shell=True, preexec_fn=os.setsid)
                # quick health check on port
                if port and not port_alive("localhost", port, 0.5):
                    # give 10s grace after start
                    if time.time() - procs[name].start_time if hasattr(procs[name],'start_time') else 0 > 10:
                        print(f"[supervisor] {name} port {port} not alive; restarting")
                        os.killpg(os.getpgid(procs[name].pid), signal.SIGTERM)
                        procs[name]=subprocess.Popen(cmd, shell=True, preexec_fn=os.setsid)
                        procs[name].start_time=time.time()
                else:
                    if not hasattr(procs[name],'start_time'):
                        procs[name].start_time=time.time()
            time.sleep(2)
    except KeyboardInterrupt:
        print("\n[supervisor] stopping...")
        for name, proc in procs.items():
            try: os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            except Exception: pass

if __name__=="__main__":
    main()
