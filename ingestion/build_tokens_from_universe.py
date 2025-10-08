"""
This script builds a list of instrument tokens from a specified universe of stocks for use with the Zerodha Kite Connect API.

Workflow:
1. Loads API credentials and file paths from environment variables.
2. Loads the Zerodha access token from a JSON file, ensuring the API key matches.
3. Reads a universe CSV file containing tradingsymbols and exchanges.
4. Fetches the full list of NSE instruments from Kite Connect and saves a copy for auditing.
5. For each symbol in the universe, attempts to resolve the corresponding instrument token:
    - Prefers cash equity instruments (instrument_type == 'EQ', no expiry, lot_size == 1).
    - Falls back to the first matching row if no perfect match is found.
6. Writes the resolved tokens to a CSV file for downstream use.
7. Warns about any symbols that could not be resolved.

Environment Variables:
- KITE_API_KEY: Zerodha Kite Connect API key.
- ZERODHA_TOKEN_FILE: Path to the access token JSON file.
- UNIVERSE_CSV: Path to the input universe CSV file.
- TOKENS_OUT: Path to the output tokens CSV file.
- INSTR_OUTDIR: Directory to save the instruments dump.

Usage:
     python build_tokens_from_universe.py

Dependencies:
- kiteconnect
- python-dotenv
"""
import os, csv, json, sys
from datetime import datetime, timezone
from dotenv import load_dotenv
from kiteconnect import KiteConnect

load_dotenv(".env"); load_dotenv("infra/.env")

API_KEY    = os.getenv("KITE_API_KEY")
TOKEN_FILE = os.getenv("ZERODHA_TOKEN_FILE", "ingestion/auth/token.json")
UNIVERSE_CSV = os.getenv("UNIVERSE_CSV", "configs/universe_largecap.csv")
TOKENS_OUT   = os.getenv("TOKENS_OUT", "configs/tokens.csv")
INSTR_OUTDIR = os.getenv("INSTR_OUTDIR", "data/instruments")

os.makedirs(os.path.dirname(TOKENS_OUT), exist_ok=True)
os.makedirs(INSTR_OUTDIR, exist_ok=True)

def load_access_token():
    if not API_KEY: raise SystemExit("KITE_API_KEY missing")
    with open(TOKEN_FILE) as f:
        t = json.load(f)
    if t.get("api_key") != API_KEY:
        raise SystemExit("Token file API key mismatch. Re-run ingestion/zerodha_login.py")
    return t["access_token"]

def load_universe(path):
    out=[]
    with open(path, newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            sym = (row.get("tradingsymbol") or "").strip()
            ex  = (row.get("exchange") or "NSE").strip().upper()
            if sym: out.append((sym, ex))
    if not out: raise SystemExit(f"No rows in {path}")
    return out

def pick_equity(rows):
    """
    From instrument dump rows for a tradingsymbol/exchange, pick the cash equity:
    - instrument_type == 'EQ' (spot)
    - no expiry
    - lot_size (if present) == 1
    """
    best=None
    for r in rows:
        it = r.get("instrument_type") or r.get("segment") or ""
        if str(r.get("instrument_type","")).upper() != "EQ": continue
        if r.get("expiry"): continue
        if "lot_size" in r and int(r["lot_size"]) != 1: continue
        best = r; break
    # fallback: first row
    return best or (rows[0] if rows else None)

def main():
    access = load_access_token()
    kite = KiteConnect(api_key=API_KEY)
    kite.set_access_token(access)

    # Fetch NSE instruments and cache to disk for audit
    instr = kite.instruments("NSE")
    ts = datetime.now(timezone.utc).strftime("%Y%m%d")
    inst_path = os.path.join(INSTR_OUTDIR, f"instruments_NSE_{ts}.csv")
    with open(inst_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=instr[0].keys())
        w.writeheader(); w.writerows(instr)
    print(f"[tokens] saved instruments dump → {inst_path} ({len(instr)} rows)")

    # index by tradingsymbol
    by_sym = {}
    for r in instr:
        sym = r.get("tradingsymbol")
        if not sym: continue
        by_sym.setdefault(sym.upper(), []).append(r)

    # Read your desired universe and resolve tokens
    universe = load_universe(UNIVERSE_CSV)
    rows_out=[]
    missing=[]
    for sym, ex in universe:
        cand = [r for r in by_sym.get(sym.upper(), []) if (r.get("exchange") or "").upper()==ex]
        pick = pick_equity(cand)
        if not pick:
            missing.append((sym, ex))
            continue
        rows_out.append({
            "instrument_token": int(pick["instrument_token"]),
            "tradingsymbol": sym,
            "exchange": ex,
            "subscribe": 1
        })

    if missing:
        print("[tokens] WARNING: could not resolve tokens for:", missing, file=sys.stderr)

    with open(TOKENS_OUT, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["instrument_token","tradingsymbol","exchange","subscribe"])
        w.writeheader(); w.writerows(rows_out)
    print(f"[tokens] wrote {len(rows_out)} tokens → {TOKENS_OUT}")

if __name__ == "__main__":
    main()
