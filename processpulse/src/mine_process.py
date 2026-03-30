from __future__ import annotations

from pathlib import Path

import pandas as pd
from pm4py.algo.discovery.inductive import algorithm as inductive_miner
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.util import dataframe_utils
from pm4py.visualization.petri_net import visualizer as pn_visualizer


def load_pm4py_log(input_path: str | Path):
    df = pd.read_csv(input_path)
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    df = df.rename(
        columns={
            "ticket_id": "case:concept:name",
            "activity": "concept:name",
            "timestamp": "time:timestamp",
        }
    )

    df = dataframe_utils.convert_timestamp_columns_in_df(df)
    event_log = log_converter.apply(df)
    return event_log


def discover_and_save_process_model(input_path: str | Path, output_image: str | Path) -> None:
    log = load_pm4py_log(input_path)
    net, initial_marking, final_marking = inductive_miner.apply(log)

    gviz = pn_visualizer.apply(net, initial_marking, final_marking)
    output_image = Path(output_image)
    output_image.parent.mkdir(parents=True, exist_ok=True)
    pn_visualizer.save(gviz, str(output_image))


def main() -> None:
    input_path = "data/processed/ticket_event_log_clean.csv"
    output_path = "outputs/figures/process_model.png"
    discover_and_save_process_model(input_path, output_path)
    print(f"Saved process model to {output_path}")


if __name__ == "__main__":
    main()
