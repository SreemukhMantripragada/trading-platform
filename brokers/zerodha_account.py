# brokers/zerodha_account.py
import os, json
from dotenv import load_dotenv
from kiteconnect import KiteConnect, exceptions as kz_ex

load_dotenv(".env"); load_dotenv("infra/.env")

class Account:
    def __init__(self):
        self.api_key=os.getenv("KITE_API_KEY")
        self.token_path=os.getenv("ZERODHA_TOKEN_FILE","ingestion/auth/token.json")
        self.k=KiteConnect(api_key=self.api_key)
        tok=json.load(open(self.token_path))["access_token"]
        self.k.set_access_token(tok)

    async def tradable_equity(self, conf):
        try:
            m=self.k.margins("equity")
            cash=float(m.get("net",0))
        except kz_ex.TokenException:
            cash=0.0
        leverage=float(conf.get("leverage",1))
        reserve=float(conf.get("reserve_pct",0))/100.0
        gross = cash * leverage
        return {"cash":cash, "gross":gross, "tradable": gross * (1.0 - reserve), "equity": cash}
