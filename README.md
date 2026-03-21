# Stream Lakehouse (Kafka -> Spark -> dbt -> Airflow -> FastAPI -> Power BI)

This repository is a project that builds a streaming data pipeline and a lakehouse using only local Docker.
- **Streaming**: Kafka -> Spark Structured Streaming
- **Storage**: Parquet/Delta on MinIO (S3-compatible)
- **Modeling**: dbt (staging/silver/gold, tests, docs)
- **Orchestration**: Apache Airflow
- **Serving**: FastAPI (Metrics/Time Series API) + Power BI Desktop (Local)
- **Observability**: Prometheus + Grafana
- **Quality/Lineage**: Great Expectations + OpenLineage/Marquez

> Goal: To implement an end-to-end data product locally in a reproducible form.
