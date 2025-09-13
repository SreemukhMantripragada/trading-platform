"""
orchestrator/eod_pipeline.py
Nightly EOD pipeline:
  1) Recon dry-run (summary)
  2) Recon apply (auto-heal)
  3) Backtest grid (configs/grid.yaml)
  4) Export Top-5 => configs/next_day.yaml

Run:
  DATE=2025-09-10 python orchestrator/eod_pipeline.py
"""
from __future__ import annotations
import os, subprocess, sys, datetime, pathlib

DATE=os.getenv("DATE")  # if None -> today IST inside callee scripts
ROOT=pathlib.Path(__file__).resolve().parents[1]
LOG_DIR=ROOT / "data" / "archive" / "eod"
LOG_DIR.mkdir(parents=True, exist_ok=True)
stamp=datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
LOG_FILE=LOG_DIR / f"eod_{stamp}.log"

def run(cmd: list[str], env: dict|None=None, title: str=""):
    line=f"[EOD] {title or 'step'}: {' '.join(cmd)}"
    print(line)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")
        p=subprocess.Popen(cmd, cwd=ROOT, env=env, stdout=f, stderr=subprocess.STDOUT)
        rc=p.wait()
    if rc!=0:
        print(f"✖ {title} failed (see {LOG_FILE})"); sys.exit(rc)
    print(f"✔ {title} ok")

def main():
    env=os.environ.copy()
    if DATE: env["DATE"]=DATE

    run([sys.executable, "monitoring/recon_autoheal.py"], env, "recon-dry")
    env_apply=env.copy(); env_apply["APPLY"]="1"
    run([sys.executable, "monitoring/recon_autoheal.py"], env_apply, "recon-apply")

    run([sys.executable, "backtest/grid_runner.py"], env, "grid")
    run([sys.executable, "backtest/top5_export.py"], env, "top5-export")

    print(f"[EOD] done; log => {LOG_FILE}")

if __name__=="__main__":
    main()
