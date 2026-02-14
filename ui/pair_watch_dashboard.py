import json
import math
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

STATE_PATH = Path(os.getenv("PAIRWATCH_STATE_PATH", "runs/pairwatch_state.json"))
DEFAULT_BUFFER = float(os.getenv("PAIRWATCH_ALERT_BUFFER", "0.2"))
Z_CHART_RANGE = (-3.25, 3.25)


@dataclass
class PairSnapshot:
    pair_id: str
    symbol: str
    timeframe: int
    position: str
    last_z: Optional[float]
    entry_z: Optional[float]
    exit_z: Optional[float]
    stop_z: Optional[float]
    last_ts: Optional[int]
    entry_ts: Optional[int]


def _safe_float(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    try:
        fval = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(fval):
        return None
    return fval


def load_snapshots(path: Path = STATE_PATH) -> List[PairSnapshot]:
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError:
        return []
    if not isinstance(data, dict):
        return []

    pairs = data.get("pairs") or []
    out: List[PairSnapshot] = []
    for row in pairs:
        if not isinstance(row, dict):
            continue
        thresholds = row.get("thresholds") or {}
        out.append(
            PairSnapshot(
                pair_id=str(row.get("pair_id") or ""),
                symbol=str(row.get("symbol") or row.get("pair_id") or ""),
                timeframe=int(row.get("tf") or 0),
                position=str(row.get("position") or "FLAT").upper(),
                last_z=_safe_float(row.get("last_z")),
                entry_z=_safe_float(thresholds.get("entry_z")),
                exit_z=_safe_float(thresholds.get("exit_z")),
                stop_z=_safe_float(thresholds.get("stop_z")),
                last_ts=int(row.get("last_ts") or 0) if row.get("last_ts") else None,
                entry_ts=int(row.get("entry_ts") or 0) if row.get("entry_ts") else None,
            )
        )
    return out


def compute_alerts(
    snapshots: List[PairSnapshot],
    buffer: float,
    show_entry: bool,
    show_exit: bool,
    show_stop: bool,
    overrides_by_key: Dict[Tuple[str, int], Dict[str, float]],
    overrides_by_id: Dict[str, Dict[str, float]],
) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []
    for snap in snapshots:
        override = overrides_by_id.get(snap.pair_id) or overrides_by_key.get((snap.symbol, snap.timeframe))
        entry_z = override.get("entry_z") if override and override.get("entry_z") is not None else snap.entry_z
        exit_z = override.get("exit_z") if override and override.get("exit_z") is not None else snap.exit_z
        stop_z = override.get("stop_z") if override and override.get("stop_z") is not None else snap.stop_z

        if snap.last_z is None or entry_z is None or exit_z is None:
            continue
        abs_z = abs(snap.last_z)
        entry_gap = min(abs(snap.last_z - entry_z), abs(snap.last_z + entry_z))
        exit_gap = abs(abs_z - exit_z)
        stop_gap = abs(abs_z - (stop_z or math.inf)) if stop_z is not None else math.inf

        if snap.position == "FLAT" and show_entry and entry_gap <= buffer:
            rows.append(_format_row(snap, "ENTRY", entry_gap, entry_z, exit_z, stop_z))

        if snap.position != "FLAT":
            if show_exit and exit_gap <= buffer:
                rows.append(_format_row(snap, "EXIT", exit_gap, entry_z, exit_z, stop_z))
            if show_stop and stop_z is not None and stop_gap <= buffer:
                rows.append(_format_row(snap, "STOP", stop_gap, entry_z, exit_z, stop_z))

    if not rows:
        return pd.DataFrame(
            columns=[
                "Pair",
                "TF",
                "Position",
                "Alert",
                "Gap",
                "Last Z",
                "Entry Z",
                "Exit Z",
                "Stop Z",
                "Last Update",
            ]
        )
    df = pd.DataFrame(rows)
    return df.sort_values(by=["Alert", "Gap", "Pair"])


def _format_row(
    snap: PairSnapshot,
    alert: str,
    gap: float,
    entry_z: Optional[float],
    exit_z: Optional[float],
    stop_z: Optional[float],
) -> Dict[str, object]:
    last_update = (
        datetime.fromtimestamp(snap.last_ts, tz=timezone.utc).isoformat(sep=" ", timespec="seconds")
        if snap.last_ts
        else ""
    )
    return {
        "Pair": snap.symbol,
        "TF": f"{snap.timeframe}m" if snap.timeframe else "",
        "Position": snap.position,
        "Alert": alert,
        "Gap": round(gap, 4),
        "Last Z": round(snap.last_z or 0.0, 4),
        "Entry Z": round(entry_z or 0.0, 4) if entry_z is not None else None,
        "Exit Z": round(exit_z or 0.0, 4) if exit_z is not None else None,
        "Stop Z": round(stop_z or 0.0, 4) if stop_z is not None else None,
        "Last Update": last_update,
    }


def main() -> None:
    st.set_page_config(page_title="Pair Watch Dashboard", layout="wide")
    st.title("Pairs Near Thresholds")
    st.caption(f"Snapshot source: {STATE_PATH}")

    snapshots = load_snapshots()
    if not snapshots:
        st.warning("Pair watch snapshot not found or empty. Ensure pair_watch_producer is running.")
        return

    st.sidebar.write("### Filters")
    buffer_default = min(max(DEFAULT_BUFFER, 0.05), 1.0)
    buffer = st.sidebar.slider("Z-distance buffer", 0.05, 1.0, buffer_default, 0.05)
    show_entry = st.sidebar.checkbox("Show entries", value=True)
    show_exit = st.sidebar.checkbox("Show exits", value=True)
    show_stop = st.sidebar.checkbox("Show stops", value=True)

    auto_refresh = st.sidebar.checkbox("Auto-refresh", value=True)
    if auto_refresh:
        interval_sec = int(
            st.sidebar.number_input("Interval (sec)", min_value=5, max_value=60, value=10, step=1)
        )
        now = time.time()
        last_refresh = st.session_state.get("_pairwatch_last_refresh_ts")
        if not isinstance(last_refresh, (int, float)):
            st.session_state["_pairwatch_last_refresh_ts"] = now
        else:
            elapsed = now - float(last_refresh)
            if elapsed >= interval_sec:
                st.session_state["_pairwatch_last_refresh_ts"] = now
                st.experimental_rerun()
            else:
                remaining = max(0, int(interval_sec - elapsed))
                st.sidebar.caption(f"Auto refresh in ~{remaining}s")

    overrides_by_key, overrides_by_id = _load_next_day_thresholds()

    alerts_df = compute_alerts(
        snapshots,
        buffer,
        show_entry,
        show_exit,
        show_stop,
        overrides_by_key,
        overrides_by_id,
    )
    state_map = _load_state_map()
    if alerts_df.empty:
        st.success("No pairs close to entry/exit/stop thresholds.")
    else:
        st.dataframe(alerts_df, use_container_width=True, hide_index=True)
        _render_inline_charts(alerts_df, snapshots, state_map)
    _render_state_zscore_grid(snapshots, state_map, overrides_by_key, overrides_by_id)
    _render_in_trade_table(snapshots, state_map)
    _render_zscore_blotter(snapshots)
    _render_strategy_insights(snapshots, state_map)

    st.write("\n### Full Pair State")
    state_df = build_state_dataframe(snapshots, overrides_by_key, overrides_by_id)
    st.dataframe(state_df, use_container_width=True, hide_index=True)

    st.sidebar.write("### Utilities")
    if st.sidebar.button("Refresh now"):
        st.experimental_rerun()


def _render_inline_charts(
    alerts_df: pd.DataFrame,
    snapshots: List[PairSnapshot],
    state_map: Dict[str, dict],
) -> None:
    snapshots_map = {snap.pair_id: snap for snap in snapshots}

    st.write("\n### Z-Score Mini Charts")
    columns = st.columns(3)
    idx = 0
    for _, row in alerts_df.iterrows():
        pair = row["Pair"]
        pair_id = _resolve_pair_id(pair, snapshots_map)
        if not pair_id:
            continue
        state = state_map.get(pair_id)
        if not state:
            continue
        history = state.get("z_history") or []
        points = [(pt.get("ts"), pt.get("z")) for pt in history if isinstance(pt, dict)]
        points = [(ts, z) for ts, z in points if ts is not None and z is not None]
        if len(points) < 3:
            continue
        df_hist = pd.DataFrame(points, columns=["ts", "z"])
        df_hist["ts"] = pd.to_datetime(df_hist["ts"], unit="s", utc=True)
        col = columns[idx % len(columns)]
        with col:
            st.write(f"**{pair} ({row['Alert']})**")
            st.line_chart(df_hist.set_index("ts"))
        idx += 1


def _render_state_zscore_grid(
    snapshots: List[PairSnapshot],
    state_map: Dict[str, dict],
    overrides_by_key: Dict[Tuple[str, int], Dict[str, float]],
    overrides_by_id: Dict[str, Dict[str, float]],
) -> None:
    if not snapshots or not state_map:
        return

    chart_payload: List[Tuple[PairSnapshot, pd.DataFrame, Dict[str, Optional[float]]]] = []

    for snap in snapshots:
        if snap.last_z is None or abs(snap.last_z) <= 1.0:
            continue
        state = state_map.get(snap.pair_id)
        if not state:
            continue
        history = state.get("z_history") or []
        points = [(pt.get("ts"), pt.get("z")) for pt in history if isinstance(pt, dict)]
        points = [(ts, z) for ts, z in points if ts is not None and z is not None]
        if len(points) < 2:
            continue
        df_hist = pd.DataFrame(points, columns=["ts", "z"]).drop_duplicates(subset=["ts"]).sort_values("ts")
        if df_hist.empty:
            continue
        df_hist["ts"] = pd.to_datetime(df_hist["ts"], unit="s", utc=True)
        override = overrides_by_id.get(snap.pair_id) or overrides_by_key.get((snap.symbol, snap.timeframe))
        entry_z = override.get("entry_z") if override and override.get("entry_z") is not None else snap.entry_z
        exit_z = override.get("exit_z") if override and override.get("exit_z") is not None else snap.exit_z
        stop_z = override.get("stop_z") if override and override.get("stop_z") is not None else snap.stop_z

        chart_payload.append(
            (
                snap,
                df_hist,
                {"entry": entry_z, "exit": exit_z, "stop": stop_z},
            )
        )

    if not chart_payload:
        return

    y_min, y_max = Z_CHART_RANGE

    st.write("\n### Pair Z-Score History")
    columns = st.columns(2)
    for idx, (snap, df_hist, thresholds) in enumerate(chart_payload):
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=df_hist["ts"],
                y=df_hist["z"],
                mode="lines",
                name="Z-score",
                line=dict(color="#1f77b4", width=2),
            )
        )

        x_bounds = [df_hist["ts"].min(), df_hist["ts"].max()]
        if x_bounds[0] == x_bounds[1]:
            x_bounds[1] = x_bounds[0] + pd.Timedelta(seconds=1)

        zone_styles = [
            (Z_CHART_RANGE[0], -3.0, "rgba(214, 39, 40, 0.12)"),
            (-3.0, -2.0, "rgba(31, 119, 180, 0.12)"),
            (-1.0, 1.0, "rgba(44, 160, 44, 0.12)"),
            (2.0, 3.0, "rgba(31, 119, 180, 0.12)"),
            (3.0, Z_CHART_RANGE[1], "rgba(214, 39, 40, 0.12)"),
        ]
        for y0, y1, color in zone_styles:
            fig.add_shape(
                type="rect",
                xref="x",
                yref="y",
                x0=x_bounds[0],
                x1=x_bounds[1],
                y0=y0,
                y1=y1,
                fillcolor=color,
                layer="below",
                line=dict(width=0),
            )

        threshold_styles = {
            "entry": ("Entry Z", "#ff7f0e"),
            "exit": ("Exit Z", "#2ca02c"),
            "stop": ("Stop Z", "#d62728"),
        }
        for key, (label, color) in threshold_styles.items():
            value = thresholds.get(key)
            if value is None:
                continue
            fig.add_trace(
                go.Scatter(
                    x=x_bounds,
                    y=[value, value],
                    mode="lines",
                    name=label,
                    line=dict(color=color, width=1, dash="dash"),
                    showlegend=True,
                )
            )

        last_point = df_hist.iloc[-1]
        fig.add_trace(
            go.Scatter(
                x=[last_point["ts"]],
                y=[last_point["z"]],
                mode="markers",
                name="Latest",
                marker=dict(color="#1f77b4", size=8, line=dict(color="white", width=1)),
                showlegend=False,
            )
        )

        fig.update_layout(
            title=f"{snap.symbol} · {snap.timeframe}m · {snap.position}",
            height=260,
            margin=dict(l=40, r=20, t=50, b=40),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
            yaxis=dict(range=[y_min, y_max], title="Z-score"),
            xaxis=dict(title="Time (UTC)", showgrid=False),
        )
        fig.update_xaxes(tickformat="%m-%d %H:%M")

        col = columns[idx % len(columns)]
        with col:
            st.plotly_chart(fig, use_container_width=True)


def _render_in_trade_table(
    snapshots: List[PairSnapshot],
    state_map: Dict[str, dict],
) -> None:
    if not snapshots or not state_map:
        return

    rows: List[Dict[str, object]] = []
    now = datetime.now(timezone.utc)
    for snap in snapshots:
        state = state_map.get(snap.pair_id)
        if not state:
            continue
        position = (state.get("position") or "").upper()
        if position == "FLAT":
            continue
        entry_ts = state.get("entry_ts")
        entry_dt = (
            datetime.fromtimestamp(int(entry_ts), tz=timezone.utc)
            if entry_ts
            else None
        )
        held_sec = (now - entry_dt).total_seconds() if entry_dt else None
        thresholds = state.get("thresholds") or {}
        entry_z = thresholds.get("entry_z")
        exit_z = thresholds.get("exit_z")
        stop_z = thresholds.get("stop_z")
        last_z = state.get("last_z")

        rows.append(
            {
                "Pair": snap.symbol,
                "TF": f"{snap.timeframe}m" if snap.timeframe else "",
                "Position": position,
                "Entry Z": round(entry_z, 3) if entry_z is not None else None,
                "Last Z": round(last_z, 3) if last_z is not None else None,
                "Exit Z": exit_z,
                "Stop Z": stop_z,
                "Held": _format_duration(held_sec),
                "Reason": state.get("entry_reason") or "",
            }
        )

    if not rows:
        return
    df = pd.DataFrame(rows).sort_values(by=["TF", "Pair"]).reset_index(drop=True)
    st.write("\n### Active Trades")
    st.dataframe(df, use_container_width=True, hide_index=True)


def _render_strategy_insights(
    snapshots: List[PairSnapshot],
    state_map: Dict[str, dict],
) -> None:
    if not snapshots or not state_map:
        return
    snap_by_id = {snap.pair_id: snap for snap in snapshots}
    rows: List[Dict[str, object]] = []
    now_utc = datetime.now(timezone.utc)

    for pair_id, state in state_map.items():
        snap = snap_by_id.get(pair_id)
        symbol = state.get("symbol") or (snap.symbol if snap else pair_id)
        tf_val = state.get("tf") or (snap.timeframe if snap else None)
        tf_label = f"{tf_val}m" if tf_val else ""
        thresholds = state.get("thresholds") or {}
        entry_z = thresholds.get("entry_z")
        exit_z = thresholds.get("exit_z")
        stop_z = thresholds.get("stop_z")
        last_z = state.get("last_z")
        position = (state.get("position") or "").upper()
        pending = state.get("pending")
        entry_ts = state.get("entry_ts")
        entry_reason = state.get("entry_reason")
        status = "FLAT"
        detail = ""

        if position and position != "FLAT":
            status = f"HOLD {position}"
            if entry_ts:
                try:
                    entry_dt = datetime.fromtimestamp(int(entry_ts), tz=timezone.utc)
                    held = now_utc - entry_dt
                    detail = f"Since {entry_dt.strftime('%H:%M')} UTC ({_format_duration(held.total_seconds())})"
                except Exception:
                    detail = ""
            if entry_reason:
                detail = f"{entry_reason} {detail}".strip()
        elif pending:
            direction = int(pending.get("direction") or 0)
            label = "Await short re-entry" if direction == -1 else "Await long re-entry"
            age_sec = pending.get("age_sec")
            if age_sec is None and pending.get("start_ts"):
                try:
                    pending_start = datetime.fromtimestamp(int(pending["start_ts"]), tz=timezone.utc)
                    age_sec = max(0, (now_utc - pending_start).total_seconds())
                except Exception:
                    age_sec = None
            age_label = _format_duration(age_sec) if age_sec is not None else ""
            detail = f"{label} {age_label}".strip()
        else:
            if last_z is not None and entry_z is not None:
                distance = abs(abs(last_z) - abs(entry_z))
                if abs(last_z) > abs(entry_z):
                    status = "OUTSIDE ENTRY"
                    detail = f"|z| beyond entry by {distance:.2f}"
                else:
                    status = "NEUTRAL"
                    detail = f"|z| {distance:.2f} from entry"

        rows.append(
            {
                "Pair": symbol,
                "TF": tf_label,
                "Status": status,
                "Detail": detail,
                "Last Z": round(last_z, 3) if last_z is not None else None,
                "Entry Z": entry_z,
                "Exit Z": exit_z,
                "Stop Z": stop_z,
            }
        )

    if not rows:
        return
    df = pd.DataFrame(rows)
    st.write("\n### Strategy Insights")
    st.dataframe(
        df.sort_values(by=["TF", "Pair"]).reset_index(drop=True),
        use_container_width=True,
        hide_index=True,
    )


def _format_duration(seconds: Optional[float]) -> str:
    if seconds is None:
        return ""
    seconds = max(0, float(seconds))
    if seconds < 60:
        return f"{int(seconds)}s"
    minutes, sec = divmod(int(seconds), 60)
    if minutes < 60:
        return f"{minutes}m {sec}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes}m"


def _render_zscore_blotter(snapshots: List[PairSnapshot]) -> None:
    if not snapshots:
        return
    rows = []
    for snap in snapshots:
        if snap.last_z is None or not snap.timeframe:
            continue
        rows.append(
            {
                "pair": snap.symbol,
                "tf": f"{snap.timeframe}m",
                "tf_int": snap.timeframe,
                "z": snap.last_z,
            }
        )
    if not rows:
        return
    df = pd.DataFrame(rows)
    tfs = sorted(df["tf_int"].unique())
    st.write("\n### Latest Z-Score Scatter")
    cols = st.columns(len(tfs))
    for idx, tf in enumerate(tfs):
        label = f"{tf}m"
        sub = df[df["tf_int"] == tf].copy()
        if sub.empty:
            continue
        fig = px.scatter(
            sub,
            x="pair",
            y="z",
            hover_name="pair",
            title=f"{label} pairs (n={len(sub)})",
        )
        fig.update_layout(
            xaxis=dict(title="Pair", tickangle=45),
            yaxis=dict(title="Z-score"),
            margin=dict(l=40, r=20, t=60, b=120),
            height=400,
        )
        with cols[idx]:
            st.plotly_chart(fig, use_container_width=True)


def _resolve_pair_id(symbol: str, snaps_map: Dict[str, PairSnapshot]) -> Optional[str]:
    for snap in snaps_map.values():
        if snap.symbol == symbol:
            return snap.pair_id
    return None


def _load_state_map() -> Dict[str, dict]:
    if not STATE_PATH.exists():
        return {}
    try:
        data = json.loads(STATE_PATH.read_text())
    except json.JSONDecodeError:
        return {}
    if not isinstance(data, dict):
        return {}
    out: Dict[str, dict] = {}
    for row in data.get("pairs") or []:
        if isinstance(row, dict):
            pid = str(row.get("pair_id") or "")
            if pid:
                out[pid] = row
    return out


def build_state_dataframe(
    snapshots: List[PairSnapshot],
    overrides_by_key: Dict[Tuple[str, int], Dict[str, float]],
    overrides_by_id: Dict[str, Dict[str, float]],
) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []
    for snap in snapshots:
        last_update = (
            datetime.fromtimestamp(snap.last_ts, tz=timezone.utc).isoformat(sep=" ", timespec="seconds")
            if snap.last_ts
            else ""
        )
        entry_time = (
            datetime.fromtimestamp(snap.entry_ts, tz=timezone.utc).isoformat(sep=" ", timespec="seconds")
            if snap.entry_ts
            else ""
        )
        override = overrides_by_id.get(snap.pair_id) or overrides_by_key.get((snap.symbol, snap.timeframe))
        entry_z = override.get("entry_z") if override and override.get("entry_z") is not None else snap.entry_z
        exit_z = override.get("exit_z") if override and override.get("exit_z") is not None else snap.exit_z
        stop_z = override.get("stop_z") if override and override.get("stop_z") is not None else snap.stop_z

        rows.append(
            {
                "Pair": snap.symbol,
                "TF": f"{snap.timeframe}m" if snap.timeframe else "",
                "Position": snap.position,
                "Last Z": round(snap.last_z or 0.0, 4) if snap.last_z is not None else None,
                "Entry Z": round(entry_z or 0.0, 4) if entry_z is not None else None,
                "Exit Z": round(exit_z or 0.0, 4) if exit_z is not None else None,
                "Stop Z": round(stop_z or 0.0, 4) if stop_z is not None else None,
                "Last Update": last_update,
                "Entry Time": entry_time,
            }
        )
    return pd.DataFrame(rows)


def _load_next_day_thresholds() -> Tuple[
    Dict[Tuple[str, int], Dict[str, float]], Dict[str, Dict[str, float]]
]:
    yaml_path = Path(os.getenv("NEXT_DAY_PATH", "configs/pairs_next_day.yaml"))
    if not yaml_path.exists():
        return {}, {}
    try:
        import yaml

        doc = yaml.safe_load(yaml_path.read_text())
    except Exception:
        return {}, {}
    overrides_by_key: Dict[Tuple[str, int], Dict[str, float]] = {}
    overrides_by_id: Dict[str, Dict[str, float]] = {}
    for row in (doc or {}).get("selections", []) or []:
        symbol = str(row.get("symbol") or "")
        tf = int(row.get("timeframe") or 0)
        params = row.get("params") or {}
        if not symbol or tf <= 0:
            continue
        entry = _safe_float(params.get("entry_z"))
        exit_ = _safe_float(params.get("exit_z"))
        stop = _safe_float(params.get("stop_z"))

        overrides_map = {"entry_z": entry, "exit_z": exit_, "stop_z": stop}

        overrides_by_key[(symbol, tf)] = overrides_map
        pair_id = f"{symbol.replace('-', '_')}_{tf}m"
        overrides_by_id[pair_id] = overrides_map
    return overrides_by_key, overrides_by_id


if __name__ == "__main__":
    main()
