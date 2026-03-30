# ProcessPulse

ProcessPulse is a process mining project that analyzes IT support ticket workflows using event logs. It reconstructs the real process, identifies bottlenecks, measures delays, and generates operational recommendations.

## Features

- Synthetic event log generation for support tickets
- Event log cleaning and preprocessing
- Process mining with pm4py
- Variant analysis
- Bottleneck detection
- Streamlit dashboard with metrics and recommendations

## Tech Stack

- Python
- pandas
- pm4py
- Streamlit
- matplotlib

## Project Workflow

1. Generate synthetic ticket event log
2. Clean and preprocess data
3. Discover process structure
4. Compute cycle time, reopen rate, escalation rate, and handoffs
5. Visualize workflows and bottlenecks in Streamlit

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
