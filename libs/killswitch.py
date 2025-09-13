import os, time, json, pathlib
PATH=os.getenv("KILL_FILE","/tmp/trading.kill")
class KillSwitch:
    def is_tripped(self):
        p=pathlib.Path(PATH)
        if not p.exists(): return False, ""
        try:
            d=json.loads(p.read_text()); return True, d.get("reason","manual")
        except Exception:
            return True, "killfile"
