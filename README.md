# COVID Analytics App

A robust PySpark-based data analytics pipeline for processing, standardizing, and analyzing global COVID-19 datasets. The project demonstrates advanced data engineering practices by separating the data processing layer, the orchestration layer, and the reporting layer into a clean, cloud-ready workflow.

## Features Implemented

### Data Engineering & Standardization
- **PySpark Pipelines:** Built scalable data pipelines using Apache Spark to ingest and process the raw COVID-19 datasets stored in the `data` directory.
- **Data Cleansing:** Standardized text and categorical data, handled missing values, and generated consistent summary tables for downstream use.
- **Automated Outputs:** Exported finalized datasets (CSV and Parquet) into the `pipeline_output` directory for the dashboard and other consumers.

### Advanced Analytics
- **Growth Metrics:** Calculated epidemiological metrics, including infection rates, death growth measures, recovery rates, and severity breakdowns across different regions and timeframes.
- **Aggregations:** Performed country-wise, region-wise, and time-series aggregations to generate structured reporting tables.
- **Enterprise Orchestration:** Implemented Apache Airflow via Docker to schedule and monitor the pipeline, alongside a custom pure-Python fallback orchestrator (`orchestrator.py`) designed to bypass restrictive corporate endpoint firewalls.

### Visualizations & Reporting
- **Streamlit Dashboard:** Built a readable dashboard for reviewing the processed results through KPIs, charts, and summary panels.
- **Manual Execution Panel:** Added a dashboard control for triggering the PySpark job directly from the UI and viewing live execution logs.
- **Output Review:** Loaded processed files from `pipeline_output` and rendered them seamlessly without re-running Spark inside the browser session.

## Tech Stack
- Python 3
- Apache Spark / PySpark (Distributed Data Processing)
- Apache Airflow (Pipeline Orchestration)
- Docker & Docker Compose (Containerization)
- Streamlit (Frontend Dashboard)
- pandas & pyarrow (Data Manipulation & File Export)
- matplotlib (Data Visualization)

## How To Run

### Option A: Production Environment (Docker + Airflow)
*Recommended for cloud deployments (e.g., AWS EC2) or local machines without corporate restrictions.*
1. Ensure Docker is installed and running on your machine.
2. Open a terminal in the project root and start the cluster: `docker-compose up -d --build`
3. Access the Airflow UI at `http://localhost:8080` (Credentials: `admin` / `admin`).
4. Toggle the `covid_pipeline` DAG to "Unpaused" and trigger it to process the data.
5. Access the live Streamlit dashboard at `http://localhost:8501`.

### Option B: Local Environment (Pure Python Fallback)
*Recommended for corporate laptops with locked-down WSL/Docker access.*
1. Ensure you have Python installed and activate your project virtual environment.
2. Install the necessary dependencies: `pip install -r requirements.txt`
3. Ensure the raw datasets are present in the `data` directory.
4. Run `python orchestrator.py` to start the background scheduled execution.
5. Open a new terminal and run `streamlit run app.py` to open the dashboard.
6. Use the `⚙️ Admin: Orchestration` tab in the dashboard if you want to trigger the PySpark backend manually.

## Validation

1. Trigger the PySpark job via Airflow, the local orchestrator, or the Streamlit Admin dashboard.
2. Confirm that the expected CSV and Parquet files are successfully written to the `pipeline_output` folder.
3. Refresh the dashboard and verify that the charts and KPI values update from the newly generated output.
4. Keep the orchestrator running if you want scheduled refreshes during development.