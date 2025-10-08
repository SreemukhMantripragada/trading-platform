from strategy.base import BaseStrategy, Signal, Candle

class MLSignal(BaseStrategy):
    def __init__(self, model_name:str="baseline"):
        self.model_name=model_name
        self.model=None
        try:
            from ml.model_registry import load_model
            self.model = load_model(model_name)
        except Exception:
            self.model=None

        # quick features
        self.prev=None; self.ema=None; self.a=2/21

    def on_bar(self, c:Candle)->Signal:
        # Simple hand-crafted features; extend as needed
        ret = 0.0 if self.prev is None else (c.c - self.prev)/self.prev
        self.prev = c.c
        self.ema = c.c if self.ema is None else (self.a*c.c + (1-self.a)*self.ema)
        mom = 0.0 if self.ema is None else (c.c - self.ema)

        if not self.model:
            # Fallback heuristic to keep behavior deterministic
            if mom > 0: return Signal("BUY", reason="ml_fallback_up")
            if mom < 0: return Signal("SELL", reason="ml_fallback_down")
            return Signal("HOLD")

        try:
            x = [ret, mom]
            y = self.model.predict([x])[0]
            if y > 0: return Signal("BUY", reason="ml_up")
            if y < 0: return Signal("SELL", reason="ml_down")
            return Signal("HOLD")
        except Exception:
            return Signal("HOLD")
