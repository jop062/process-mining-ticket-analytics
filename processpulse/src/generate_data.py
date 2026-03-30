from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd


@dataclass
class TicketConfig:
    num_tickets: int = 300
    seed: int = 42


TEAMS = ["Help Desk", "Network", "Security", "Systems", "Cloud Ops"]
PRIORITIES = ["Low", "Medium", "High"]
ASSIGNEES = ["Alex", "Jordan", "Taylor", "Morgan", "Riley", "Casey", "Jamie"]


def choose_path() -> list[str]:
    """
    Choose a realistic workflow path for a support ticket.
    """
    r = random.random()

    if r < 0.55:
        return [
            "Ticket Created",
            "Assigned",
            "In Progress",
            "Resolved",
            "Closed",
        ]
    if r < 0.75:
        return [
            "Ticket Created",
            "Assigned",
            "Escalated",
            "In Progress",
            "Resolved",
            "Closed",
        ]
    if r < 0.90:
        return [
            "Ticket Created",
            "Assigned",
            "In Progress",
            "Resolved",
            "Reopened",
            "In Progress",
            "Resolved",
            "Closed",
        ]

    return [
        "Ticket Created",
        "Assigned",
        "Waiting on User",
        "In Progress",
        "Escalated",
        "In Progress",
        "Resolved",
        "Closed",
    ]


def delay_for_activity(activity: str, priority: str) -> int:
    """
    Return a delay in minutes for each activity.
    """
    priority_multiplier = {"Low": 1.4, "Medium": 1.0, "High": 0.7}[priority]

    base_ranges = {
        "Ticket Created": (1, 10),
        "Assigned": (5, 120),
        "Waiting on User": (120, 1440),
        "In Progress": (30, 360),
        "Escalated": (20, 180),
        "Resolved": (15, 120),
        "Reopened": (10, 240),
        "Closed": (5, 60),
    }

    low, high = base_ranges.get(activity, (10, 60))
    sampled = random.randint(low, high)
    return max(1, int(sampled * priority_multiplier))


def assign_team(activity: str) -> str:
    if activity in {"Ticket Created", "Assigned", "Closed", "Waiting on User"}:
        return "Help Desk"
    if activity == "Escalated":
        return random.choice(["Network", "Security", "Systems", "Cloud Ops"])
    return random.choice(TEAMS)


def generate_ticket_log(config: TicketConfig) -> pd.DataFrame:
    random.seed(config.seed)

    rows: list[dict[str, object]] = []
    base_time = datetime(2026, 1, 1, 8, 0, 0)

    for ticket_num in range(1, config.num_tickets + 1):
        ticket_id = f"T{ticket_num:04d}"
        priority = random.choices(PRIORITIES, weights=[0.25, 0.50, 0.25], k=1)[0]
        current_time = base_time + timedelta(minutes=random.randint(0, 30000))
        path = choose_path()

        for activity in path:
            current_time += timedelta(minutes=delay_for_activity(activity, priority))
            rows.append(
                {
                    "ticket_id": ticket_id,
                    "activity": activity,
                    "timestamp": current_time,
                    "team": assign_team(activity),
                    "priority": priority,
                    "assignee": random.choice(ASSIGNEES),
                }
            )

    df = pd.DataFrame(rows).sort_values(["ticket_id", "timestamp"]).reset_index(drop=True)
    return df


def main() -> None:
    output_dir = Path("data/raw")
    output_dir.mkdir(parents=True, exist_ok=True)

    df = generate_ticket_log(TicketConfig())
    output_path = output_dir / "ticket_event_log.csv"
    df.to_csv(output_path, index=False)
    print(f"Saved raw event log to {output_path}")


if __name__ == "__main__":
    main()
