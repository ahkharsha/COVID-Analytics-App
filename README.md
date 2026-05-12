# Serverless Real-Time COVID-19 Telemetry Pipeline 🦠

This project simulates a highly scalable, real-time edge computing pipeline that ingests, processes, and visualizes live COVID-19 telemetry data from multiple concurrent sources (e.g., separate hospitals) using AWS and Docker.

## 🏗️ Architecture

This is an event-driven, decoupled streaming architecture:
1. **Edge Generation (Python/EC2):** Python scripts act as edge devices, reading data and pushing JSON payloads to AWS Kinesis.
2. **Ingestion (AWS Kinesis):** Acts as a shock-absorbing queue to securely handle high-throughput concurrent data streams.
3. **ETL Processing (AWS Lambda):** Event-triggered serverless functions decode Base64 payloads, enrich the data with UTC processing timestamps, and manage file writing.
4. **Data Lake (Amazon S3):** Secure, low-cost object storage for the processed JSON records.
5. **Visualization (Streamlit/Docker):** A containerized Python frontend that continuously polls S3 and aggregates the data in-memory using Pandas to render a live, auto-refreshing dashboard.

## 🛠️ Technology Stack
* **Cloud Infrastructure:** AWS Kinesis, AWS Lambda, Amazon S3, AWS IAM
* **Compute:** Ubuntu EC2, Docker Compose
* **Data Engineering:** Python (Boto3, Pandas)
* **Frontend:** Streamlit

## 📂 Repository Structure
* `producer.py` - The data generation script simulating edge devices.
* `lambda_function.py` - The serverless AWS ETL processor.
* `app.py` - The Streamlit frontend visualization code.
* `docker-compose.yml` - Infrastructure management for the UI container.
* `requirements.txt` - Python dependencies for the Docker container.
