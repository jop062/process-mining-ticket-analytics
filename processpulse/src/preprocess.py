from __future__ import annotations

from pathlib import Path

import pandas as pd


REQUIRED_COLUMNS = ["ticket_id", "activity", "timestamp"]


def validate_columns(df: pd.DataFrame) -> None:
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def preprocess_event_log(input_path: str | Path, output_path: str | Path) -> pd.DataFrame:
    df = pd.read_csv(input_path)
    validate_columns(df)

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["ticket_id", "activity", "timestamp"])

    df["ticket_id"] = df["ticket_id"].astype(str).str.strip()
    df["activity"] = df["activity"].astype(str).str.strip()

    df = df.sort_values(["ticket_id", "timestamp"]).drop_duplicates().reset_index(drop=True)

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)

    return df


def main() -> None:
    input_path = "data/raw/ticket_event_log.csv"
    output_path = "data/processed/ticket_event_log_clean.csv"
    df = preprocess_event_log(input_path, output_path)
    print(f"Saved cleaned event log with {len(df)} rows to {output_path}")


if __name__ == "__main__":
    main()
