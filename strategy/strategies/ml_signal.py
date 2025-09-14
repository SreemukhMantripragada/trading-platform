"""
strategy/strategies/ml_signal.py
Example ML strategy:
- Pull latest N features per symbol (you inject from your feature store).
- Score with registry model.
- Emit BUY/SELL when score crosses thresholds; cool-down to avoid churn.

Config (strategies.yaml):
  - name: ML_SCORE
    kind: ml
    model_name: rsi_clf
    model_file: rsi.pkl
    model_kind: sklearn
    buy_threshold: 0.6
    sell_threshold: 0.4
    cooldown_sec: 60
"""
from __future__ import annotations
import time
from typing import Dict, Any, List
from ml.model_registry import ModelRegistry

class MLSignal:
    def __init__(self, cfg: Dict[str, Any]):
        self.name = cfg.get("name","ML_SCORE")
        self.buy_th = float(cfg.get("buy_threshold",0.6))
        self.sell_th= float(cfg.get("sell_threshold",0.4))
        self.cool   = int(cfg.get("cooldown_sec",60))
        self.model_name = cfg.get("model_name","rsi_clf")
        self.model_file = cfg.get("model_file","rsi.pkl")
        self.model_kind = cfg.get("model_kind","sklearn")
        self.last_ts: Dict[str,int] = {}
        self.MR = ModelRegistry().ensure(self.model_name, self.model_file, self.model_kind)

    def _ok(self, sym:str)->bool:
        now=int(time.time())
        lt=self.last_ts.get(sym,0)
        if now-lt < self.cool: return False
        self.last_ts[sym]=now; return True

    def decide(self, sym:str, feature_vector: List[float]) -> Dict[str, Any] | None:
        """
        feature_vector: latest features for 'sym' (your pipeline supplies this).
        Returns order intent dict or None.
        """
        score = float(self.MR.predict(self.model_name, [feature_vector])[0])
        if score >= self.buy_th and self._ok(sym):
            return {"symbol":sym, "action":"BUY", "score":score, "why":"ml>=buy_threshold"}
        if score <= self.sell_th and self._ok(sym):
            return {"symbol":sym, "action":"SELL", "score":score, "why":"ml<=sell_threshold"}
        return None
