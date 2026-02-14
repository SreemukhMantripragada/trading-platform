"""Zerodha account adapter with graceful offline fallback."""

import os
import json
from dotenv import load_dotenv

try:
    from kiteconnect import KiteConnect
except Exception:  # pragma: no cover - optional dependency
    KiteConnect = None  # type: ignore[assignment]

load_dotenv(".env"); load_dotenv("infra/.env")

class Account:
    def __init__(self):
        self.api_key=os.getenv("KITE_API_KEY")
        self.token_path=os.getenv("ZERODHA_TOKEN_FILE","ingestion/auth/token.json")
        self.k = None
        if not self.api_key or KiteConnect is None:
            return
        try:
            self.k = KiteConnect(api_key=self.api_key)
            tok = json.load(open(self.token_path))["access_token"]
            self.k.set_access_token(tok)
        except Exception:
            self.k = None

    async def tradable_equity(self, conf):
        if self.k is None:
            raise RuntimeError("zerodha account client unavailable")
        try:
            m=self.k.margins("equity")
            cash=float(m.get("net",0))
        except Exception:
            cash=0.0
        leverage=float(conf.get("leverage",1))
        reserve=float(conf.get("reserve_pct",0))/100.0
        gross = cash * leverage
        return {"cash":cash, "gross":gross, "tradable": gross * (1.0 - reserve), "equity": cash}
