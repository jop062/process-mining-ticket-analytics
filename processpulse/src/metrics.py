from __future__ import annotations

import pandas as pd


def compute_case_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute per-ticket metrics.
    """
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    grouped = df.groupby("ticket_id")

    case_metrics = grouped.agg(
        start_time=("timestamp", "min"),
        end_time=("timestamp", "max"),
        num_events=("activity", "count"),
        priority=("priority", "first"),
    ).reset_index()

    case_metrics["cycle_time_hours"] = (
        (case_metrics["end_time"] - case_metrics["start_time"]).dt.total_seconds() / 3600.0
    )

    reopen_counts = (
        df.assign(is_reopened=df["activity"].eq("Reopened").astype(int))
        .groupby("ticket_id")["is_reopened"]
        .sum()
        .reset_index()
    )
    reopen_counts = reopen_counts.rename(columns={"is_reopened": "reopen_count"})

    escalation_counts = (
        df.assign(is_escalated=df["activity"].eq("Escalated").astype(int))
        .groupby("ticket_id")["is_escalated"]
        .sum()
        .reset_index()
    )
    escalation_counts = escalation_counts.rename(columns={"is_escalated": "escalation_count"})

    handoffs = (
        df.groupby("ticket_id")["team"]
        .nunique()
        .reset_index()
        .rename(columns={"team": "unique_teams"})
    )

    case_metrics = case_metrics.merge(reopen_counts, on="ticket_id", how="left")
    case_metrics = case_metrics.merge(escalation_counts, on="ticket_id", how="left")
    case_metrics = case_metrics.merge(handoffs, on="ticket_id", how="left")

    case_metrics["reopen_count"] = case_metrics["reopen_count"].fillna(0).astype(int)
    case_metrics["escalation_count"] = case_metrics["escalation_count"].fillna(0).astype(int)
    case_metrics["has_reopen"] = case_metrics["reopen_count"] > 0
    case_metrics["has_escalation"] = case_metrics["escalation_count"] > 0

    return case_metrics


def compute_transition_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute time spent between steps.
    """
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values(["ticket_id", "timestamp"]).reset_index(drop=True)

    df["next_activity"] = df.groupby("ticket_id")["activity"].shift(-1)
    df["next_timestamp"] = df.groupby("ticket_id")["timestamp"].shift(-1)

    transitions = df.dropna(subset=["next_activity", "next_timestamp"]).copy()
    transitions["transition"] = transitions["activity"] + " → " + transitions["next_activity"]
    transitions["duration_hours"] = (
        (transitions["next_timestamp"] - transitions["timestamp"]).dt.total_seconds() / 3600.0
    )

    summary = (
        transitions.groupby("transition")
        .agg(
            count=("transition", "count"),
            avg_duration_hours=("duration_hours", "mean"),
            median_duration_hours=("duration_hours", "median"),
            max_duration_hours=("duration_hours", "max"),
        )
        .reset_index()
        .sort_values("avg_duration_hours", ascending=False)
    )

    return summary


def compute_variant_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute process variants as sequences of activities.
    """
    variants = (
        df.sort_values(["ticket_id", "timestamp"])
        .groupby("ticket_id")["activity"]
        .apply(lambda x: " > ".join(x.tolist()))
        .reset_index(name="variant")
    )

    variant_counts = (
        variants.groupby("variant")
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )

    total = variant_counts["count"].sum()
    variant_counts["pct_of_cases"] = variant_counts["count"] / total * 100.0
    return variant_counts


def compute_summary_metrics(df: pd.DataFrame) -> dict[str, float]:
    case_metrics = compute_case_metrics(df)

    return {
        "num_tickets": float(case_metrics["ticket_id"].nunique()),
        "avg_cycle_time_hours": float(case_metrics["cycle_time_hours"].mean()),
        "median_cycle_time_hours": float(case_metrics["cycle_time_hours"].median()),
        "reopen_rate_pct": float(case_metrics["has_reopen"].mean() * 100.0),
        "escalation_rate_pct": float(case_metrics["has_escalation"].mean() * 100.0),
        "avg_handoffs": float(case_metrics["unique_teams"].mean()),
    }
