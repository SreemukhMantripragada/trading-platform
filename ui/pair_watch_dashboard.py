import json
import math
import os
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import altair as alt
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - Python <3.9 fallback
    ZoneInfo = None  # type: ignore

_FALSEY = {"0", "false", "no", "off"}

STATE_PATH = Path(os.getenv("PAIRWATCH_STATE_PATH", "runs/pairwatch_state.json")).expanduser()
DEFAULT_REFRESH_SEC = int(os.getenv("PAIRWATCH_DASHBOARD_REFRESH_SEC", "60"))

_IST_OFFSET = timedelta(hours=5, minutes=30)
if ZoneInfo:
    try:
        IST = ZoneInfo("Asia/Kolkata")
    except Exception:  # pragma: no cover - fallback if tz data missing
        IST = timezone(_IST_OFFSET)
else:
    IST = timezone(_IST_OFFSET)

MARKET_OPEN = time(hour=9, minute=15)
MARKET_CLOSE = time(hour=15, minute=30)


def now_ist() -> datetime:
    return datetime.now(tz=IST)


def ts_to_ist(ts: int) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(IST)


def trading_seconds_between(start: datetime, end: datetime) -> int:
    if start >= end:
        return 0
    total = 0
    start_date = start.date()
    end_date = end.date()
    current_date = start_date
    while current_date <= end_date:
        if current_date.weekday() < 5:
            day_open = datetime.combine(current_date, MARKET_OPEN, tzinfo=IST)
            day_close = datetime.combine(current_date, MARKET_CLOSE, tzinfo=IST)
            day_start = day_open
            day_end = day_close
            if current_date == start_date:
                day_start = max(start, day_open)
            if current_date == end_date:
                day_end = min(end, day_close)
            if day_start < day_end:
                total += int((day_end - day_start).total_seconds())
        current_date += timedelta(days=1)
    return total


def trading_seconds_since(ts: Optional[int], *, now: Optional[datetime] = None) -> Optional[int]:
    if ts is None:
        return None
    start = ts_to_ist(ts)
    end = now or now_ist()
    if start >= end:
        return 0
    return trading_seconds_between(start, end)


def load_state(path: Path) -> Optional[Dict[str, Any]]:
    try:
        raw = path.read_text()
    except FileNotFoundError:
        return None
    except OSError as exc:
        st.error(f"Failed to read {path}: {exc}")
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def format_float(value: Optional[float], precision: int = 2) -> str:
    if value is None:
        return "--"
    if not isinstance(value, (int, float)) or not math.isfinite(value):
        return "--"
    return f"{value:.{precision}f}"


def format_time(ts: Optional[int]) -> str:
    if not ts:
        return "--"
    dt = ts_to_ist(ts)
    return dt.strftime("%Y-%m-%d %H:%M:%S IST")


def _format_timespan(seconds: int) -> str:
    if seconds < 60:
        return f"{seconds}s"
    minutes = seconds // 60
    if minutes < 90:
        return f"{minutes}m"
    hours = minutes / 60.0
    if hours < 48:
        return f"{hours:.1f}h"
    days = hours / 24.0
    return f"{days:.1f}d"


def format_age(ts: Optional[int]) -> str:
    seconds = trading_seconds_since(ts)
    if seconds is None:
        return "--"
    return _format_timespan(seconds)


def format_elapsed(ts: Optional[int]) -> str:
    if ts is None:
        return "--"
    delta = now_ist() - ts_to_ist(ts)
    seconds = max(0, int(delta.total_seconds()))
    return _format_timespan(seconds)


def format_duration(seconds: Optional[int]) -> str:
    if not seconds:
        return "--"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m"
    hours = minutes / 60.0
    return f"{hours:.1f}h"


def format_price(info: Optional[Dict[str, Any]]) -> str:
    if not info:
        return "--"
    px = info.get("px")
    if px is None or not isinstance(px, (int, float)) or not math.isfinite(px):
        return "--"
    return f"{px:.2f}"


def describe_pending(pending: Optional[Dict[str, Any]]) -> str:
    if not pending:
        return "--"
    direction = pending.get("direction")
    bars = pending.get("bars_since")
    if direction == -1:
        label = "Short setup"
    elif direction == 1:
        label = "Long setup"
    else:
        label = "Pending"
    if isinstance(bars, int):
        return f"{label} ({bars} bars)"
    return label


def build_threshold_df(thresholds: Dict[str, Optional[float]]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    def add(label: str, value: Optional[float], style: str = "threshold") -> None:
        if value is None:
            return
        if not isinstance(value, (int, float)) or not math.isfinite(value):
            return
        rows.append(
            {
                "id": f"{label}-{len(rows)}",
                "label": label,
                "value": float(value),
                "style": style,
            }
        )

    entry = thresholds.get("entry_z")
    exit_level = thresholds.get("exit_z")
    stop = thresholds.get("stop_z")

    add("Stop +", stop)
    add("Stop -", -stop if stop is not None else None)
    add("Entry +", entry)
    add("Entry -", -entry if entry is not None else None)
    add("Exit +", exit_level)
    add("Exit -", -exit_level if exit_level is not None else None)
    add("Zero", 0.0, style="zero")

    return pd.DataFrame(rows)


def render_chart(pair: Dict[str, Any]) -> None:
    history = pair.get("z_history") or []
    df = pd.DataFrame(history)
    if df.empty or "z" not in df:
        st.info("Waiting for z-score history…")
        return
    df = df.dropna(subset=["z"])
    if df.empty:
        st.info("Waiting for z-score history…")
        return
    df["ts"] = df["ts"].astype(int)
    ist_series = df["ts"].map(lambda v: ts_to_ist(int(v)))
    def _in_session(dt: datetime) -> bool:
        if dt.weekday() >= 5:
            return False
        local_time = dt.time()
        return MARKET_OPEN <= local_time <= MARKET_CLOSE

    mask = ist_series.map(_in_session)
    if not mask.any():
        st.info("No in-market z-score data yet (waiting for trading hours).")
        return
    df = df.loc[mask].copy()
    df["timestamp"] = pd.DatetimeIndex(ist_series[mask].to_list())
    df = df.sort_values("timestamp")

    base = (
        alt.Chart(df)
        .mark_line(color="#1f77b4")
        .encode(
            x=alt.X("timestamp:T", title="Time"),
            y=alt.Y("z:Q", title="Z-score"),
            tooltip=[
                alt.Tooltip("timestamp:T", title="Time"),
                alt.Tooltip("z:Q", title="Z", format=".2f"),
            ],
        )
    )

    last = df.tail(1)
    point = (
        alt.Chart(last)
        .mark_circle(size=70, color="#e377c2")
        .encode(x="timestamp:T", y="z:Q")
    )

    thresholds = pair.get("thresholds") or {}
    threshold_df = build_threshold_df(thresholds)
    if not threshold_df.empty:
        color_scale = alt.Scale(
            domain=[
                "Stop +",
                "Stop -",
                "Entry +",
                "Entry -",
                "Exit +",
                "Exit -",
                "Zero",
            ],
            range=[
                "#d62728",
                "#d62728",
                "#ff7f0e",
                "#ff7f0e",
                "#2ca02c",
                "#2ca02c",
                "#7f7f7f",
            ],
        )
        dash_scale = alt.Scale(domain=["threshold", "zero"], range=[[4, 4], [2, 2]])
        threshold_chart = (
            alt.Chart(threshold_df)
            .mark_rule()
            .encode(
                y="value:Q",
                color=alt.Color("label:N", scale=color_scale, title="Thresholds"),
                strokeDash=alt.StrokeDash("style:N", scale=dash_scale),
                detail="id:N",
                opacity=alt.condition("datum.style == 'zero'", alt.value(0.35), alt.value(0.6)),
            )
        )
        chart = alt.layer(threshold_chart, base, point).resolve_scale(color="independent")
    else:
        chart = alt.layer(base, point)

    st.altair_chart(chart.properties(height=260).interactive(), use_container_width=True)


def render_pair(pair: Dict[str, Any]) -> None:
    header = f"{pair.get('symbol', '--')} · {pair.get('tf', '--')}m"
    st.markdown(f"### {header}")

    top_cols = st.columns([1.2, 1, 1, 0.8])
    top_cols[0].metric("Z-score", format_float(pair.get("last_z")))
    top_cols[1].metric("Position", pair.get("position", "--"))
    top_cols[2].metric("Beta", format_float(pair.get("last_beta")))
    top_cols[3].metric("Bars", str(pair.get("points", 0)))

    mid_cols = st.columns([1, 1, 1.2])
    mid_cols[0].markdown(f"**Ready:** {'Yes' if pair.get('ready') else 'No'}")
    mid_cols[1].markdown(f"**Pending:** {describe_pending(pair.get('pending'))}")
    mid_cols[2].markdown(f"**Risk bucket:** {pair.get('risk_bucket', '--')} · RPT {format_float(pair.get('risk_per_trade'), 3)}")

    time_cols = st.columns([1.2, 1.2, 0.8])
    time_cols[0].markdown(
        f"**Last bar:** {format_time(pair.get('last_ts'))} "
        f"({format_age(pair.get('last_ts'))} ago)"
    )
    entry_reason = pair.get("entry_reason")
    entry_extra = f" – {entry_reason}" if entry_reason else ""
    time_cols[1].markdown(
        f"**Entry time:** {format_time(pair.get('entry_ts'))}{entry_extra}"
    )
    time_cols[2].markdown(f"**Max hold:** {format_duration(pair.get('max_hold_sec'))}")

    prices = pair.get("latest_prices") or {}
    price_cols = st.columns([1, 1])
    legs = [pair.get("leg_a"), pair.get("leg_b")]
    for idx, leg in enumerate(legs):
        info = prices.get(leg) if leg else None
        label = f"{leg or '--'} price"
        value = format_price(info)
        age = format_age(info.get("ts") if info else None)
        delta = f"age {age}" if age != "--" else None
        price_cols[idx].metric(label, value, delta=delta)

    render_chart(pair)


def main() -> None:
    st.set_page_config(page_title="Pair Watch Dashboard", layout="wide")
    st.title("Pair Watch Dashboard")

    st.caption(f"Snapshot source: `{STATE_PATH}`")

    refresh_sec = st.sidebar.slider(
        "Refresh interval (seconds)", min_value=15, max_value=300, value=DEFAULT_REFRESH_SEC, step=5
    )
    auto_default = os.getenv("PAIRWATCH_DASHBOARD_AUTO", "1").lower() not in _FALSEY
    auto_enabled = st.sidebar.checkbox("Auto-refresh", value=auto_default)
    if auto_enabled:
        components.html(
            f"""
            <script>
            setTimeout(function() {{ window.location.reload(); }}, {refresh_sec * 1000});
            </script>
            """,
            height=0,
            width=0,
        )

    state = load_state(STATE_PATH)
    if not state:
        st.info("Waiting for `pair_watch_producer` to publish snapshots…")
        return

    generated_at = state.get("generated_at")
    st.subheader("Snapshot")
    st.write(
        f"Last update: **{format_time(generated_at)}** "
        f"({format_elapsed(generated_at)} ago)"
    )

    pairs = state.get("pairs") or []
    if not pairs:
        st.warning("No pair data available yet.")
        return

    pairs = sorted(pairs, key=lambda p: (p.get("tf", 0), p.get("symbol", "")))
    label_to_pair = {
        f"{p.get('symbol', '--')} · {p.get('tf', '--')}m": p for p in pairs
    }
    default_labels = list(label_to_pair.keys())
    selected_labels = st.sidebar.multiselect(
        "Visible pairs", default_labels, default=default_labels
    )
    if not selected_labels:
        st.warning("Select at least one pair to display.")
        return

    visible_pairs = [label_to_pair.get(label) for label in selected_labels if label_to_pair.get(label)]
    total = len(visible_pairs)
    if not visible_pairs:
        st.warning("Selected pairs missing from snapshot.")
        return

    for idx in range(0, total, 2):
        cols = st.columns(2)
        row_pairs = visible_pairs[idx : idx + 2]
        for col_idx, pair in enumerate(row_pairs):
            with cols[col_idx]:
                render_pair(pair)
        if idx + 2 < total:
            st.markdown("---")


if __name__ == "__main__":
    main()
