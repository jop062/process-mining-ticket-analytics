from __future__ import annotations

import pandas as pd

from src.metrics import compute_case_metrics, compute_transition_metrics


def generate_recommendations(df: pd.DataFrame) -> list[str]:
    recommendations: list[str] = []

    case_metrics = compute_case_metrics(df)
    transition_metrics = compute_transition_metrics(df)

    if not case_metrics.empty:
        avg_cycle = case_metrics["cycle_time_hours"].mean()
        reopen_rate = case_metrics["has_reopen"].mean() * 100
        escalation_rate = case_metrics["has_escalation"].mean() * 100
        avg_handoffs = case_metrics["unique_teams"].mean()

        if reopen_rate > 10:
            recommendations.append(
                f"Reopen rate is {reopen_rate:.1f}%, suggesting resolution quality or handoff clarity can be improved."
            )

        if escalation_rate > 20:
            recommendations.append(
                f"Escalation rate is {escalation_rate:.1f}%, indicating possible gaps in first-line triage or routing."
            )

        if avg_handoffs > 2.5:
            recommendations.append(
                f"Tickets involve an average of {avg_handoffs:.1f} teams, which may increase delays from handoffs."
            )

        recommendations.append(
            f"Average end-to-end cycle time is {avg_cycle:.1f} hours. Compare variants to target the slowest paths first."
        )

    if not transition_metrics.empty:
        slowest = transition_metrics.iloc[0]
        recommendations.append(
            f"The slowest transition is '{slowest['transition']}' with an average duration of "
            f"{slowest['avg_duration_hours']:.1f} hours."
        )

    if not recommendations:
        recommendations.append("No major bottlenecks detected in the current event log.")

    return recommendations
