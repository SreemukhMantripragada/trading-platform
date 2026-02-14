"""
This script performs health checks and status reporting for the trading platform's infrastructure services.

Modules:
    - os: Provides a way of using operating system dependent functionality.
    - subprocess: Allows spawning new processes, connecting to their input/output/error pipes, and obtaining their return codes.
    - sys: Provides access to some variables used or maintained by the interpreter.
    - pathlib.Path: Object-oriented filesystem paths.

Global Variables:
    - INFRA: Path object pointing to the 'infra' directory, two levels above the current file.
    - ENV: Dictionary containing environment variables loaded from 'infra/.env.docker' (fallback 'infra/.env').

Functions:
    - http_ok(url): Checks if the given URL returns an HTTP status code in the 2xx or 3xx range.
        Args:
            url (str): The URL to check.
        Returns:
            bool: True if the HTTP status code is 2xx or 3xx, False otherwise.

    - main(): Main function that:
        - Checks the status of Docker Compose services.
        - Inspects the status of specific containers (zookeeper, kafka, postgres, prometheus, grafana, kafka-ui).
        - Lists Kafka topics.
        - Checks PostgreSQL readiness.
        - Checks the health of Prometheus and Grafana HTTP endpoints.
        - Prints the status of each service.

Usage:
    Run this script directly to perform infrastructure health checks and print the results.
"""
import os, subprocess, sys
from pathlib import Path

INFRA = Path(__file__).resolve().parents[1] / "infra"
ENV_PATH = INFRA / ".env.docker"
if not ENV_PATH.exists():
    fallback = INFRA / ".env"
    if fallback.exists():
        ENV_PATH = fallback
    else:
        raise SystemExit("Missing infra/.env.docker. Run `make docker-config-sync`.")
COMPOSE_ENV_FILE = ENV_PATH.name
ENV = {}
for ln in ENV_PATH.read_text(encoding="utf-8").splitlines():
    ln=ln.strip()
    if not ln or ln.startswith("#"): continue
    k,v=ln.split("=",1); ENV[k]=v

def http_ok(url):
    try:
        out = subprocess.check_output(["curl","-fsS","-o","/dev/null","-w","%{http_code}", url], timeout=5).decode().strip()
        return out and out[0] in ("2","3")
    except Exception:
        return False

def main():
    subprocess.check_call(["docker","compose","--env-file",COMPOSE_ENV_FILE,"ps"], cwd=str(INFRA))
    for name in ["zookeeper","kafka","postgres","prometheus","grafana","kafka-ui","app-supervisor"]:
        try:
            st = subprocess.check_output(["docker","inspect","-f","{{.State.Status}}", name]).decode().strip()
        except subprocess.CalledProcessError:
            st = "missing"
        print(f"{name}: {st}")
    subprocess.check_call(["docker","exec","-i","kafka","bash","-lc","/opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list"])
    subprocess.check_call(["docker","exec","-i","postgres","pg_isready","-U", os.getenv("POSTGRES_USER","trader")])
    prom = ENV.get("PROMETHEUS_PORT","9090"); graf = ENV.get("GRAFANA_PORT","3000")
    print("Prometheus:", "up" if http_ok(f"http://localhost:{prom}/-/healthy") else "down")
    print("Grafana   :", "up" if http_ok(f"http://localhost:{graf}/login") else "down")
    print("doctor done")

if __name__ == "__main__":
    main()
