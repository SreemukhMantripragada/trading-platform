import subprocess, time, sys, os, signal

PROCS=[
 ["python","compute/bar_builder_1s.py"],
 ["python","compute/bar_aggregator_1m.py"],
 ["python","strategy/runner_cfg.py"],
 ["python","risk/manager.py"],
 ["python","execution/paper_gateway.py"],
]

ENV=dict(os.environ); ENV.update({
 "KAFKA_BROKER":"localhost:9092",
 "POSTGRES_HOST":"localhost","POSTGRES_PORT":"5432","POSTGRES_DB":"trading","POSTGRES_USER":"trader","POSTGRES_PASSWORD":"trader",
})

ps=[]
try:
  for cmd in PROCS:
    ps.append(subprocess.Popen(cmd, env=ENV))
  print("running... Ctrl+C to stop")
  while all(p.poll() is None for p in ps):
    time.sleep(1)
finally:
  for p in ps:
    if p.poll() is None:
      p.send_signal(signal.SIGINT)
  for p in ps: p.wait(timeout=5)

