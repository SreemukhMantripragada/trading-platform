import asyncio
import os
import time
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import asyncpg
import pandas as pd
import streamlit as st
import plotly.graph_objects as go

try:
    from orchestrator import pairs_watch_and_trade as watch_trade  # type: ignore

    ORDER_NOTIONAL = watch_trade.NOTIONAL_PER_LEG
    KITE_PRODUCT = getattr(watch_trade, "KITE_PRODUCT", "MIS")
    KITE_EXCHANGE = getattr(watch_trade, "KITE_EXCHANGE", "NSE")
    LIVE_ENABLED = getattr(watch_trade, "_kite", None) is not None
except Exception:
    ORDER_NOTIONAL = max(1.0, float(os.getenv("WATCH_TRADE_NOTIONAL", "200000")))
    KITE_PRODUCT = os.getenv("WATCH_TRADE_PRODUCT", os.getenv("ZERODHA_PRODUCT", "MIS"))
    KITE_EXCHANGE = os.getenv("WATCH_TRADE_EXCHANGE", "NSE")
    LIVE_ENABLED = False

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "trading")
PG_USER = os.getenv("POSTGRES_USER", "trader")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "trader")

SOURCE_TAG = "pairs_watch_and_trade"
STATE_PATH = Path(os.getenv("WATCH_TRADE_STATE", "runs/pairs_watch_live.json"))
PAIRWATCH_STATE = Path(os.getenv("PAIRWATCH_STATE_PATH", "runs/pairwatch_state.json"))


async def _fetch(sql: str, *params: Any) -> List[asyncpg.Record]:
    conn = await asyncpg.connect(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DB,
        user=PG_USER,
        password=PG_PASS,
    )
    try:
        return await conn.fetch(sql, *params)
    finally:
        await conn.close()


def _query(sql: str, *params: Any) -> pd.DataFrame:
    try:
        rows = asyncio.run(_fetch(sql, *params))
    except Exception as exc:
        st.error(f"DB query failed: {exc}")
        return pd.DataFrame()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame([dict(r) for r in rows])


@st.cache_data(ttl=5.0)
def live_pairs_state() -> Tuple[pd.DataFrame, Dict[str, Any]]:
    if not STATE_PATH.exists():
        return pd.DataFrame(), {"max_open": None, "open_count": 0}
    try:
        data = json.loads(STATE_PATH.read_text())
    except Exception:
        return pd.DataFrame(), {"max_open": None, "open_count": 0}
    rows = data.get("open_pairs") or []
    df = pd.DataFrame(rows) if rows else pd.DataFrame()
    if not df.empty and "last_update" in df.columns:
        df["Last Update (IST)"] = (
            pd.to_datetime(df["last_update"], unit="s", utc=True)
            .dt.tz_convert("Asia/Kolkata")
        )
        df.drop(columns=["last_update"], inplace=True, errors="ignore")
    meta = {
        "max_open": data.get("max_open"),
        "open_count": data.get("open_count", len(df)),
    }
    return df, meta


@st.cache_data(ttl=8.0)
def pairwatch_near_thresholds(buffer: float) -> pd.DataFrame:
    if not PAIRWATCH_STATE.exists():
        return pd.DataFrame()
    try:
        content = json.loads(PAIRWATCH_STATE.read_text())
    except Exception:
        return pd.DataFrame()
    rows: List[Dict[str, Any]] = []
    for row in content.get("pairs") or []:
        thresholds = row.get("thresholds") or {}
        try:
            last_z = float(row.get("last_z"))
            entry_z = float(thresholds.get("entry_z")) if thresholds.get("entry_z") is not None else None
            exit_z = float(thresholds.get("exit_z")) if thresholds.get("exit_z") is not None else None
            stop_z = float(thresholds.get("stop_z")) if thresholds.get("stop_z") is not None else None
        except (TypeError, ValueError):
            continue
        pair_symbol = row.get("symbol") or row.get("pair_id")
        timeframe = row.get("tf")
        position = str(row.get("position") or "").upper()
        if entry_z is not None:
            entry_gap = min(abs(last_z - entry_z), abs(last_z + entry_z))
            if entry_gap <= buffer:
                rows.append(
                    {
                        "Pair": pair_symbol,
                        "TF": f"{timeframe}m" if timeframe else "",
                        "Position": position or "FLAT",
                        "Alert": "ENTRY",
                        "Gap": round(entry_gap, 4),
                        "Last Z": round(last_z, 4),
                        "Entry Z": entry_z,
                        "Exit Z": exit_z,
                        "Stop Z": stop_z,
                    }
                )
        if position != "FLAT" and exit_z is not None:
            exit_gap = abs(abs(last_z) - exit_z)
            if exit_gap <= buffer:
                rows.append(
                    {
                        "Pair": pair_symbol,
                        "TF": f"{timeframe}m" if timeframe else "",
                        "Position": position or "FLAT",
                        "Alert": "EXIT",
                        "Gap": round(exit_gap, 4),
                        "Last Z": round(last_z, 4),
                        "Entry Z": entry_z,
                        "Exit Z": exit_z,
                        "Stop Z": stop_z,
                    }
                )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    return df.sort_values(by=["Alert", "Gap", "Pair"])


@st.cache_data(ttl=5.0)
def pairwatch_latest_z() -> pd.DataFrame:
    if not PAIRWATCH_STATE.exists():
        return pd.DataFrame()
    try:
        content = json.loads(PAIRWATCH_STATE.read_text()) or {}
    except Exception:
        return pd.DataFrame()
    rows = []
    for row in content.get("pairs") or []:
        try:
            last_z = float(row.get("last_z")) if row.get("last_z") is not None else None
        except (TypeError, ValueError):
            last_z = None
        last_ts = row.get("last_ts")
        if last_ts:
            try:
                last_dt = pd.to_datetime(int(last_ts), unit="s", utc=True).tz_convert("Asia/Kolkata")
            except Exception:
                last_dt = None
        else:
            last_dt = None
        rows.append(
            {
                "pair_id": row.get("pair_id"),
                "pair_symbol": row.get("symbol"),
                "tf": f"{row.get('tf')}m" if row.get("tf") else "",
                "position": row.get("position"),
                "last_z": round(last_z, 4) if last_z is not None else None,
                "last_update": last_dt,
            }
        )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if "last_update" in df.columns and df["last_update"].notna().any():
        df["Minutes since last"] = (
            (pd.Timestamp.now(tz="Asia/Kolkata") - df["last_update"]).dt.total_seconds() / 60.0
        ).round(2)
    return df.rename(
        columns={
            "tf": "TF",
            "position": "Position",
            "last_z": "Last Z",
            "last_update": "Last Update (IST)",
        }
    )


@st.cache_data(ttl=8.0)
def pairwatch_z_history(limit_pairs: int = 10, points: int = 120) -> List[Dict[str, Any]]:
    if not PAIRWATCH_STATE.exists():
        return []
    try:
        content = json.loads(PAIRWATCH_STATE.read_text())
    except Exception:
        return []
    rows = content.get("pairs") or []
    selected: List[Dict[str, Any]] = []
    for row in rows[:limit_pairs]:
        z_hist = row.get("z_history") or []
        if not z_hist:
            continue
        z_hist = z_hist[-points:]
        df = pd.DataFrame(z_hist)
        if df.empty:
            continue
        df["datetime"] = pd.to_datetime(df["ts"], unit="s", utc=True).dt.tz_convert("Asia/Kolkata")
        selected.append(
            {
                "title": row.get("symbol") or row.get("pair_id"),
                "timeframe": row.get("tf"),
                "data": df,
                "entry_z": (row.get("thresholds") or {}).get("entry_z"),
                "exit_z": (row.get("thresholds") or {}).get("exit_z"),
            }
        )
    return selected


@st.cache_data(ttl=8.0)
def recent_orders(limit: int) -> pd.DataFrame:
    sql = """
    SELECT
      client_order_id,
      ts AT TIME ZONE 'Asia/Kolkata' AS ts_ist,
      symbol,
      side,
      qty,
      status,
      strategy,
      risk_bucket,
      COALESCE(extra->>'pair_id','') AS pair_id,
      COALESCE(extra->>'leg','') AS leg,
      COALESCE(extra->>'broker_order_id','') AS broker_order_id,
      COALESCE(extra->>'broker_error','') AS broker_error
    FROM orders
    WHERE extra->>'source' = $2
      AND ts >= now() - interval '24 hours'
    ORDER BY ts DESC
    LIMIT $1
    """
    df = _query(sql, limit, SOURCE_TAG)
    if "ts_ist" in df.columns:
        df.rename(columns={"ts_ist": "Timestamp (IST)"}, inplace=True)
    return df


@st.cache_data(ttl=8.0)
def pair_order_summary() -> pd.DataFrame:
    sql = """
    SELECT
      COALESCE(extra->>'pair_id','') AS pair_id,
      COUNT(*) FILTER (WHERE status='NEW') AS new_count,
      COUNT(*) FILTER (WHERE status='ACK') AS ack_count,
      COUNT(*) FILTER (WHERE status='FILLED') AS filled_count,
      COUNT(*) FILTER (WHERE status='PARTIAL') AS partial_count,
      COUNT(*) FILTER (WHERE status='CANCELED') AS canceled_count,
      COUNT(*) FILTER (WHERE (extra->>'broker_error') IS NOT NULL AND extra->>'broker_error' <> '') AS error_count,
      MAX(ts) AT TIME ZONE 'Asia/Kolkata' AS last_order_ist
    FROM orders
    WHERE extra->>'source' = $1
    GROUP BY pair_id
    ORDER BY last_order_ist DESC NULLS LAST
    """
    df = _query(sql, SOURCE_TAG)
    if not df.empty:
        df.rename(
            columns={
                "last_order_ist": "Last Order (IST)",
                "error_count": "Broker Errors",
            },
            inplace=True,
        )
    return df


@st.cache_data(ttl=10.0)
def pair_positions() -> pd.DataFrame:
    sql = """
    WITH legs AS (
      SELECT
        o.extra->>'pair_id' AS pair_id,
        o.extra->>'leg' AS leg,
        SUM(
          CASE WHEN o.side = 'BUY' THEN f.qty ELSE -f.qty END
        )::int AS net_qty,
        MAX(f.ts) AT TIME ZONE 'Asia/Kolkata' AS last_fill_ist
      FROM orders o
      JOIN fills f ON f.order_id = o.order_id
      WHERE o.extra->>'source' = $1
      GROUP BY 1,2
    )
    SELECT
      pair_id,
      COALESCE(MAX(CASE WHEN leg='A' THEN net_qty END), 0) AS net_qty_a,
      COALESCE(MAX(CASE WHEN leg='B' THEN net_qty END), 0) AS net_qty_b,
      MAX(last_fill_ist) AS last_fill_ist
    FROM legs
    GROUP BY pair_id
    ORDER BY pair_id
    """
    df = _query(sql, SOURCE_TAG)
    if not df.empty:
        df.rename(
            columns={
                "net_qty_a": "Net Qty A",
                "net_qty_b": "Net Qty B",
                "last_fill_ist": "Last Fill (IST)",
            },
            inplace=True,
        )
    return df


@st.cache_data(ttl=8.0)
def recent_signals(limit: int) -> pd.DataFrame:
    sql = """
    SELECT
      ts AT TIME ZONE 'Asia/Kolkata' AS ts_ist,
      sym_a,
      sym_b,
      side,
      zscore,
      reason
    FROM pairs_signals
    ORDER BY ts DESC
    LIMIT $1
    """
    df = _query(sql, limit)
    if "ts_ist" in df.columns:
        df.rename(columns={"ts_ist": "Timestamp (IST)"}, inplace=True)
    return df


def auto_refresh():
    st.sidebar.header("Refresh")
    enable = st.sidebar.checkbox("Auto refresh", value=True)
    interval = st.sidebar.slider("Interval (sec)", 5, 60, 10, 1)
    if not enable:
        st.sidebar.caption("Manual refresh only")
        return
    now = time.time()
    last = st.session_state.get("_watch_trade_last_refresh", 0.0)
    if now - last >= interval:
        st.session_state["_watch_trade_last_refresh"] = now
        _safe_rerun()
    else:
        remaining = max(0, int(interval - (now - last)))
        st.sidebar.caption(f"Next refresh in ~{remaining}s")


def _safe_rerun() -> None:
    rerun = getattr(st, "rerun", None)
    if callable(rerun):
        rerun()
        return
    exp_rerun = getattr(st, "experimental_rerun", None)
    if callable(exp_rerun):
        exp_rerun()


def main() -> None:
    st.set_page_config(page_title="pairs_watch_and_trade Status", layout="wide")
    st.title("pairs_watch_and_trade Dashboard")
    st.caption("Live snapshot derived from orders/fills written by pairs_watch_and_trade.")

    auto_refresh()

    st.sidebar.header("Display")
    order_limit = st.sidebar.slider("Recent orders", 10, 200, 50, 10)
    signal_limit = st.sidebar.slider("Recent signals", 10, 200, 50, 10)
    z_plot_limit = st.sidebar.slider(
        "Z-score charts to display", 2, 40, 12, 1, key="zscore_plot_limit"
    )
    z_history_points = st.sidebar.slider(
        "History points per chart", 50, 500, 200, 10, key="zscore_history_points"
    )

    live_text = "enabled" if LIVE_ENABLED else "disabled"
    st.markdown(
        f"""
        **Config**  
        • Per-leg notional: `{ORDER_NOTIONAL:,.0f}`  
        • Broker product: `{KITE_PRODUCT}` @ `{KITE_EXCHANGE}`  
        • Live placement: `{live_text}`
        """
    )

    live_df, live_meta = live_pairs_state()
    open_count = int(live_meta.get("open_count") or 0)
    max_open = live_meta.get("max_open")
    max_label = f" / {max_open}" if max_open else ""
    st.subheader("Live Pair State")
    st.caption(f"Pairs reported by pairs_watch_and_trade / pair_watch (max {max_open or 'n/a'})")
    st.metric("Open entries", f"{open_count}{max_label}")

    z_snapshot = pairwatch_latest_z()
    if z_snapshot.empty:
        st.info("pair_watch snapshot not found or empty.")
    else:
        st.dataframe(z_snapshot, use_container_width=True, height=260)

    if live_df.empty:
        st.info("No active trades recorded by watch_and_trade.")
    else:
        display_df = live_df.copy()
        cols_order = [
            "pair_id",
            "pair_symbol",
            "a_symbol",
            "sideA",
            "qtyA",
            "b_symbol",
            "sideB",
            "qtyB",
            "bucket",
            "reason",
            "action",
            "Last Update (IST)",
        ]
        existing = [c for c in cols_order if c in display_df.columns]
        display_df = display_df[existing + [c for c in display_df.columns if c not in existing]]
        st.dataframe(display_df, use_container_width=True, height=260)
    st.divider()

    st.subheader("Pairs Near Thresholds")
    buffer_default = float(os.getenv("PAIRWATCH_ALERT_BUFFER", "0.25"))
    threshold_buffer = st.sidebar.slider(
        "Z-score proximity",
        0.05,
        1.0,
        buffer_default,
        0.05,
        key="zscore_proximity_slider",
    )
    alert_df = pairwatch_near_thresholds(threshold_buffer)
    if alert_df.empty:
        st.info("No pairs near entry/exit thresholds right now.")
    else:
        st.dataframe(alert_df, use_container_width=True, height=260)
    st.subheader("Latest Z-score Plots")
    z_plots = pairwatch_z_history(limit_pairs=z_plot_limit, points=z_history_points)
    if not z_plots:
        st.info("No z-score history available. Ensure pair_watch_producer exports z_history in the state file.")
    else:
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for entry in z_plots:
            tf_key = f"{int(entry.get('timeframe') or 0)}m"
            grouped.setdefault(tf_key, []).append(entry)
        for tf in sorted(grouped.keys()):
            st.write(f"**Timeframe {tf}**")
            cols_plots = st.columns(2)
            for idx, entry in enumerate(grouped[tf]):
                df = entry["data"]
                fig = go.Figure()
                fig.add_trace(go.Scatter(x=df["datetime"], y=df["z"], mode="lines", name="z"))
                entry_z = entry.get("entry_z")
                exit_z = entry.get("exit_z")
                if entry_z is not None:
                    try:
                        ez = float(entry_z)
                        fig.add_hline(y=ez, line=dict(color="orange", dash="dash"))
                        fig.add_hline(y=-ez, line=dict(color="orange", dash="dash"))
                    except (TypeError, ValueError):
                        pass
                if exit_z is not None:
                    try:
                        exz = float(exit_z)
                        fig.add_hline(y=exz, line=dict(color="green", dash="dot"))
                        fig.add_hline(y=-exz, line=dict(color="green", dash="dot"))
                    except (TypeError, ValueError):
                        pass
                title_tf = entry["title"]
                if entry.get("timeframe"):
                    title_tf = f"{title_tf} ({int(entry['timeframe'])}m)"
                fig.update_layout(title=title_tf, height=280, margin=dict(l=10, r=10, t=40, b=10))
                fig.update_yaxes(range=[-3.5, 3.5])
                cols_plots[idx % 2].plotly_chart(fig, use_container_width=True)
    st.divider()

    cols = st.columns(2)

    with cols[0]:
        st.subheader("Order Summary by Pair")
        summary_df = pair_order_summary()
        if summary_df.empty:
            st.info("No orders from pairs_watch_and_trade yet.")
        else:
            st.dataframe(summary_df, use_container_width=True, height=320)

    with cols[1]:
        st.subheader("Net Filled Position")
        pos_df = pair_positions()
        if pos_df.empty:
            st.info("No fills recorded for pairs_watch_and_trade orders.")
        else:
            st.dataframe(pos_df, use_container_width=True, height=320)

    st.subheader("Recent Orders")
    orders_df = recent_orders(order_limit)
    if orders_df.empty:
        st.info("No recent orders.")
    else:
        st.dataframe(orders_df, use_container_width=True, height=360)

    st.subheader("Recent Signals")
    sig_df = recent_signals(signal_limit)
    if sig_df.empty:
        st.info("No signal rows found.")
    else:
        st.dataframe(sig_df, use_container_width=True, height=360)


if __name__ == "__main__":
    main()
