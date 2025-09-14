"""
ml/model_registry.py
Simple registry for sklearn/ONNX models.

Usage:
  MR = ModelRegistry(models_dir="models")
  MR.load("rsi_clf", "models/rsi.pkl", kind="sklearn")
  y = MR.predict("rsi_clf", X)   # X = 2D list/ndarray
"""
from __future__ import annotations
import os, pickle, numpy as np
from typing import Dict, Any
try:
    import onnxruntime as ort
except Exception:
    ort = None

class ModelRegistry:
    def __init__(self, models_dir: str = "models"):
        self.models: Dict[str, Any] = {}
        self.models_dir=models_dir

    def load(self, name:str, path:str, kind:str="sklearn"):
        if kind=="sklearn":
            with open(path,"rb") as f:
                self.models[name]=("sklearn", pickle.load(f))
        elif kind=="onnx":
            if ort is None: raise RuntimeError("onnxruntime not installed")
            self.models[name]=("onnx", ort.InferenceSession(path, providers=["CPUExecutionProvider"]))
        else:
            raise ValueError("unknown kind")

    def predict(self, name:str, X):
        kind, obj = self.models[name]
        X = np.asarray(X, dtype=float)
        if kind=="sklearn":
            return obj.predict_proba(X)[:,1] if hasattr(obj,"predict_proba") else obj.predict(X)
        else:
            # assume first input
            input_name = obj.get_inputs()[0].name
            out = obj.run(None, {input_name: X.astype("float32")})
            return out[0].squeeze()

    def ensure(self, name:str, default_filename:str, kind:str="sklearn"):
        if name not in self.models:
            path=os.path.join(self.models_dir, default_filename)
            if not os.path.exists(path): raise FileNotFoundError(path)
            self.load(name, path, kind)
        return self
