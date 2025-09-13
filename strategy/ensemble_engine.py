# strategy/ensemble_engine.py
from typing import List, Dict, Any, Tuple
from libs.signal import Signal, NONE

class EnsembleEngine:
    def __init__(self, mode:str="majority", k:int=1, weights:Dict[str,float]=None, min_score:float=0.0):
        self.mode=mode; self.k=k; self.weights=weights or {}; self.min_score=min_score

    def combine(self, signals: List[Tuple[str, Signal]]) -> Signal:
        votes={"BUY":0.0,"SELL":0.0,"EXIT":0.0}
        max_tag={}
        for name, sig in signals:
            if sig.side in ("NONE",): continue
            w=self.weights.get(name,1.0)
            s=sig.score if sig.score is not None else 1.0
            votes[sig.side]+= w*s
            max_tag[name]=sig.to_dict().get("tags",{})
        if self.mode == "majority":
            side = max(votes.items(), key=lambda kv: kv[1])[0]
            if votes[side] <= 0 or votes[side] < self.min_score: return NONE
            return Signal(side=side, score=votes[side], tags={"votes":votes,"details":max_tag})
        if self.mode == "k_of_n":
            # count non-NONE with score > min_score
            positives = sum(1 for _,sig in signals if sig.side!="NONE" and (sig.score or 0)>=self.min_score)
            if positives >= self.k:
                side = max(votes.items(), key=lambda kv: kv[1])[0]
                return Signal(side=side, score=votes[side], tags={"votes":votes})
            return NONE
        if self.mode == "score_avg":
            total = sum(v for v in votes.values())
            if total < self.min_score: return NONE
            side = max(votes.items(), key=lambda kv: kv[1])[0]
            return Signal(side=side, score=votes[side]/max(1.0,total), tags={"votes":votes})
        return NONE
