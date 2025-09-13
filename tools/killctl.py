import sys, json, os
PATH=os.getenv("KILL_FILE","/tmp/trading.kill")
if len(sys.argv)<2: print("usage: killctl ON reason... | OFF"); raise SystemExit(2)
cmd=sys.argv[1].upper()
if cmd=="ON":
    reason=" ".join(sys.argv[2:]) or "manual"
    open(PATH,"w").write(json.dumps({"reason":reason}))
    print("KILL ON:", reason)
elif cmd=="OFF":
    try: os.remove(PATH); print("KILL OFF")
    except FileNotFoundError: print("KILL already OFF")
else: print("unknown"); raise SystemExit(2)
