import json
import os
from pathlib import Path
from typing import Dict, Any

import pandas as pd
import streamlit as st

PAIRWATCH_STATE = Path(os.getenv("PAIRWATCH_STATE_PATH", "runs/pairwatch_state.json"))


@st.cache_data(ttl=5.0)
def load_state() -> pd.DataFrame:
    if not PAIRWATCH_STATE.exists():
        return pd.DataFrame()
    try:
        content: Dict[str, Any] = json.loads(PAIRWATCH_STATE.read_text())
    except Exception:
        return pd.DataFrame()
    rows = []
    for row in content.get("pairs") or []:
        pair_id = row.get("pair_id")
        symbol = row.get("symbol") or pair_id
        tf = row.get("tf")
        last_ts = row.get("last_ts")
        last_z = row.get("last_z")
        if last_ts:
            try:
                last_dt = pd.to_datetime(int(last_ts), unit="s", utc=True).tz_convert("Asia/Kolkata")
            except Exception:
                last_dt = None
        else:
            last_dt = None
        rows.append(
            {
                "Pair": symbol,
                "TF": f"{tf}m" if tf else "",
                "Position": row.get("position"),
                "Last Z": round(float(last_z), 4) if isinstance(last_z, (int, float)) else None,
                "Last Update": last_dt,
            }
        )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    if df["Last Update"].notna().any():
        now = pd.Timestamp.now(tz="Asia/Kolkata")
        df["Minutes since"] = (
            (now - df["Last Update"]).dt.total_seconds() / 60.0
        ).round(2)
    return df


def main() -> None:
    st.set_page_config(page_title="Pairs Live Monitor", layout="wide")
    st.title("Pairs Liveness Monitor")
    st.caption(f"Source: {PAIRWATCH_STATE}")

    max_minutes = st.sidebar.slider("Highlight stale pairs (> minutes)", 1.0, 30.0, 5.0, 0.5)
    df = load_state()
    if df.empty:
        st.warning("No pair_watch state found. Is pair_watch_producer running?")
        return

    df_display = df.copy()
    st.metric("Tracked pairs", len(df_display))

    st.dataframe(df_display, use_container_width=True, height=min(600, 40 * len(df_display) + 50))

    stale = df_display[(df_display["Minutes since"].notna()) & (df_display["Minutes since"] > max_minutes)]
    if not stale.empty:
        st.error(f"{len(stale)} pair(s) stale beyond {max_minutes} minutes")
        st.table(stale[["Pair", "TF", "Minutes since"]])
    else:
        st.success("All pairs are within the freshness threshold.")


if __name__ == "__main__":
    main()
