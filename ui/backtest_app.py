import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import streamlit as st

STATE_PATH = Path(os.getenv("PAIRS_STATE_FILE", "data/runtime/pairs_state.json"))


@dataclass
class PairPosition:
    pair_id: str
    pair_symbol: str
    position: str
    side_a: str
    side_b: str
    qty_a: int
    qty_b: int
    bucket: str
    beta: float | None
    opened_at: datetime | None

    @property
    def hold_minutes(self) -> float | None:
        if not self.opened_at:
            return None
        return max((datetime.now(timezone.utc) - self.opened_at).total_seconds() / 60.0, 0.0)


def load_positions() -> list[PairPosition]:
    if not STATE_PATH.exists():
        return []
    try:
        data = json.loads(STATE_PATH.read_text())
    except json.JSONDecodeError:
        return []
    if not isinstance(data, dict):
        return []

    positions: list[PairPosition] = []
    for pair_id, payload in sorted(data.items()):
        if not isinstance(payload, dict):
            continue
        position = str(payload.get("position") or "").upper()
        if not position:
            continue

        direction = (
            position.replace("ENTER_", "")
            .replace("_", " ")
            .title()
            if position not in {"EXIT", ""} else position.title()
        )

        opened_ts = payload.get("ts")
        opened_at: datetime | None = None
        if isinstance(opened_ts, (int, float)):
            opened_at = datetime.fromtimestamp(float(opened_ts), tz=timezone.utc)

        pair_symbol = str(payload.get("pair_symbol") or pair_id)
        side_a = str(payload.get("sideA") or "BUY")
        side_b = str(payload.get("sideB") or "SELL")
        qty_a = int(payload.get("qtyA") or 0)
        qty_b = int(payload.get("qtyB") or 0)
        bucket = str(payload.get("bucket") or "MED").upper()
        beta_val = payload.get("beta")
        beta = float(beta_val) if isinstance(beta_val, (int, float)) else None

        positions.append(
            PairPosition(
                pair_id=pair_id,
                pair_symbol=pair_symbol,
                position=direction,
                side_a=side_a,
                side_b=side_b,
                qty_a=qty_a,
                qty_b=qty_b,
                bucket=bucket,
                beta=beta,
                opened_at=opened_at,
            )
        )
    return positions


def build_dataframe(rows: list[PairPosition]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(
            columns=[
                "Pair",
                "Direction",
                "Side A",
                "Qty A",
                "Side B",
                "Qty B",
                "Bucket",
                "Beta",
                "Opened (UTC)",
                "Hold (min)",
            ]
        )
    return pd.DataFrame(
        [
            {
                "Pair": row.pair_symbol,
                "Direction": row.position,
                "Side A": row.side_a,
                "Qty A": row.qty_a,
                "Side B": row.side_b,
                "Qty B": row.qty_b,
                "Bucket": row.bucket,
                "Beta": round(row.beta, 3) if row.beta is not None else None,
                "Opened (UTC)": row.opened_at.isoformat(sep=" ", timespec="seconds") if row.opened_at else "",
                "Hold (min)": round(row.hold_minutes, 1) if row.hold_minutes is not None else None,
            }
            for row in rows
        ]
    )


def main() -> None:
    st.set_page_config(page_title="Live Pairs Monitor", layout="wide")
    st.title("Live Pairs In Trade")
    st.caption(f"Source: {STATE_PATH}")

    positions = load_positions()
    total_open = len(positions)

    summary_col, bucket_col = st.columns([2, 1])
    with summary_col:
        st.metric(label="Open Pairs", value=total_open)
        if positions:
            avg_hold = sum(p.hold_minutes or 0.0 for p in positions) / total_open
            st.metric(label="Avg Hold (min)", value=f"{avg_hold:.1f}")

    with bucket_col:
        bucket_counts: dict[str, int] = {}
        for p in positions:
            bucket_counts[p.bucket] = bucket_counts.get(p.bucket, 0) + 1
        st.write("## Buckets")
        if bucket_counts:
            bucket_df = pd.DataFrame(
                [{"Bucket": b, "Open": bucket_counts[b]} for b in sorted(bucket_counts)]
            )
            st.dataframe(bucket_df, use_container_width=True, hide_index=True)
        else:
            st.info("No open positions across buckets.")

    table_df = build_dataframe(positions)
    if table_df.empty:
        st.success("No pairs are currently in trade.")
    else:
        st.dataframe(table_df, use_container_width=True, hide_index=True)

    st.sidebar.write("### Troubleshooting")
    if st.sidebar.button("Refresh now"):
        st.experimental_rerun()

    if not STATE_PATH.exists():
        st.sidebar.error(f"State file not found: {STATE_PATH}")
    else:
        st.sidebar.write(
            "State updates every time the executor enters/exits a pair. "
            "If this view is stale, confirm the executor has write access."
        )


if __name__ == "__main__":
    main()
