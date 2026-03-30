from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st

from src.metrics import (
    compute_case_metrics,
    compute_summary_metrics,
    compute_transition_metrics,
    compute_variant_metrics,
)
from src.recommendations import generate_recommendations


st.set_page_config(page_title="ProcessPulse", layout="wide")


@st.cache_data
def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


def render_summary(summary: dict[str, float]) -> None:
    c1, c2, c3 = st.columns(3)
    c4, c5, c6 = st.columns(3)

    c1.metric("Tickets", int(summary["num_tickets"]))
    c2.metric("Avg Cycle Time (hrs)", f"{summary['avg_cycle_time_hours']:.1f}")
    c3.metric("Median Cycle Time (hrs)", f"{summary['median_cycle_time_hours']:.1f}")
    c4.metric("Reopen Rate", f"{summary['reopen_rate_pct']:.1f}%")
    c5.metric("Escalation Rate", f"{summary['escalation_rate_pct']:.1f}%")
    c6.metric("Avg Team Handoffs", f"{summary['avg_handoffs']:.1f}")


def render_priority_chart(case_metrics: pd.DataFrame) -> None:
    priority_stats = (
        case_metrics.groupby("priority")["cycle_time_hours"]
        .mean()
        .reindex(["Low", "Medium", "High"])
    )

    fig, ax = plt.subplots(figsize=(7, 4))
    ax.bar(priority_stats.index, priority_stats.values)
    ax.set_title("Average Cycle Time by Priority")
    ax.set_ylabel("Hours")
    st.pyplot(fig)


def render_transition_chart(transitions: pd.DataFrame) -> None:
    top = transitions.head(10).sort_values("avg_duration_hours", ascending=True)

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.barh(top["transition"], top["avg_duration_hours"])
    ax.set_title("Top Slowest Transitions")
    ax.set_xlabel("Average Duration (Hours)")
    st.pyplot(fig)


def render_variant_chart(variants: pd.DataFrame) -> None:
    top = variants.head(8).sort_values("count", ascending=True)

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(top["variant"], top["count"])
    ax.set_title("Most Common Process Variants")
    ax.set_xlabel("Number of Tickets")
    st.pyplot(fig)


def main() -> None:
    st.title("ProcessPulse")
    st.subheader("Process Mining for IT Support Ticket Workflows")

    data_path = "data/processed/ticket_event_log_clean.csv"
    if not Path(data_path).exists():
        st.error("Cleaned event log not found. Run data generation and preprocessing first.")
        return

    df = load_data(data_path)

    st.sidebar.header("Filters")
    priorities = st.sidebar.multiselect(
        "Priority",
        options=sorted(df["priority"].dropna().unique().tolist()),
        default=sorted(df["priority"].dropna().unique().tolist()),
    )
    teams = st.sidebar.multiselect(
        "Team",
        options=sorted(df["team"].dropna().unique().tolist()),
        default=sorted(df["team"].dropna().unique().tolist()),
    )

    filtered = df[df["priority"].isin(priorities) & df["team"].isin(teams)].copy()

    if filtered.empty:
        st.warning("No data after applying filters.")
        return

    summary = compute_summary_metrics(filtered)
    case_metrics = compute_case_metrics(filtered)
    transitions = compute_transition_metrics(filtered)
    variants = compute_variant_metrics(filtered)
    recommendations = generate_recommendations(filtered)

    st.header("Overview")
    render_summary(summary)

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Cycle Time by Priority")
        render_priority_chart(case_metrics)
    with col2:
        st.subheader("Slowest Transitions")
        render_transition_chart(transitions)

    st.header("Variant Analysis")
    render_variant_chart(variants)
    st.dataframe(variants.head(10), use_container_width=True)

    st.header("Transition Metrics")
    st.dataframe(transitions.head(15), use_container_width=True)

    st.header("Per-Ticket Metrics")
    st.dataframe(case_metrics.head(20), use_container_width=True)

    st.header("Recommendations")
    for rec in recommendations:
        st.write(f"- {rec}")


if __name__ == "__main__":
    main()
