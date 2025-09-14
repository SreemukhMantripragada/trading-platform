import os, json, time
from kiteconnect import KiteConnect

# Simple helper to persist access_token (you can replace with something more robust later)
TOKEN_FILE = os.path.join(os.path.dirname(__file__), "token.json")

def save_token(api_key, access_token):
    with open(TOKEN_FILE, "w") as f:
        json.dump({"api_key": api_key, "access_token": access_token, "ts": time.time()}, f)

def load_token():
    if not os.path.exists(TOKEN_FILE):
        return None
    with open(TOKEN_FILE) as f:
        return json.load(f)

def login_flow(api_key, api_secret, request_token):
    """Complete the login: exchange request_token for access_token once, then save it."""
    kite = KiteConnect(api_key=api_key)
    data = kite.generate_session(request_token, api_secret=api_secret)
    save_token(api_key, data["access_token"])
    return data["access_token"]
