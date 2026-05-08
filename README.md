# COVID Analytics App

A robust PySpark-based data analytics pipeline for processing, standardizing, and analyzing global COVID-19 datasets. The project demonstrates advanced data engineering practices by separating the data processing layer, the orchestration layer, and the reporting layer into a clean workflow.

## Features Implemented

### Data Engineering & Standardization
- **PySpark Pipelines:** Built scalable data pipelines using Apache Spark to ingest and process the raw COVID-19 datasets stored in the `data` directory.
- **Data Cleansing:** Standardized text and categorical data, handled missing values, and generated consistent summary tables for downstream use.
- **Automated Outputs:** Exported finalized datasets into the `pipeline_output` directory for the dashboard and other consumers.

### Advanced Analytics
- **Growth Metrics:** Calculated epidemiological metrics, including infection rates, death growth measures, recovery rates, and severity breakdowns across different regions and timeframes.
- **Aggregations:** Performed country-wise, region-wise, and time-series aggregations to generate structured reporting tables.
- **Scheduling:** Implemented a lightweight Python-based orchestrator that repeatedly triggers the PySpark backend on a fixed interval.

### Visualizations & Reporting
- **Streamlit Dashboard:** Built a readable dashboard for reviewing the processed results through KPIs, charts, and summary panels.
- **Manual Execution Panel:** Added a dashboard control for triggering the PySpark job directly and viewing execution logs.
- **Output Review:** Loaded processed CSV files from `pipeline_output` and rendered them without re-running Spark inside the browser session.

## Tech Stack
- Python 3
- Apache Spark / PySpark (Distributed Data Processing)
- Streamlit
- pandas
- matplotlib
- schedule

## How To Run

1. Ensure you have Python installed and activate the project virtual environment.
2. Install dependencies from `Week12_PySpark_Requirements.txt` if needed.
3. The raw datasets are already included in the `data` directory.
4. Run `python orchestrator.py` to start the scheduled backend execution.
5. Run `streamlit run app.py` to open the dashboard and review the generated reports.
6. Use the `⚙️ Admin: Orchestration` tab if you want to trigger the backend manually.
7. Processed datasets will be generated in the `pipeline_output` folder and reused by the dashboard.

## Validation

1. Run the PySpark job from the orchestrator or the dashboard.
2. Confirm that the expected CSV files are written to `pipeline_output`.
3. Refresh the dashboard and verify that the charts and KPI values update from the new output.
4. Keep the orchestrator running if you want scheduled refreshes during development.

