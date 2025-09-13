# execution/costs.py
import yaml

class CostModel:
    def __init__(self, path="configs/costs.yaml"):
        self.c = yaml.safe_load(open(path))

    def legs_fees(self, side:str, qty:int, price:float):
        notional = qty * price
        b = self.c
        brokerage = b.get("brokerage_fixed", 0.0)
        stt = notional * b.get("stt_pct", 0.0)
        exch = notional * b.get("exchange_txn_pct", 0.0)
        sebi = notional * b.get("sebi_fee_pct", 0.0)
        stamp = notional * b.get("stamp_duty_pct", 0.0)
        gst = (brokerage + exch) * b.get("gst_pct", 0.0)
        total = brokerage + stt + exch + sebi + stamp + gst
        return {"brokerage":brokerage,"stt":stt,"exchange":exch,"sebi":sebi,"stamp":stamp,"gst":gst,"total":total}
