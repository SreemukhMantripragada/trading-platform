import json, jsonschema, os
_CACHE={}
def _load(p):
    if p in _CACHE: return _CACHE[p]
    with open(p) as f: _CACHE[p]=json.load(f); return _CACHE[p]
def validate(obj, schema_path):
    schema=_load(schema_path); jsonschema.validate(obj, schema); return True
