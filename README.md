# Real-Time Big Data Streaming Lakehouse on Azure

### Azure Databricks | Apache Spark | Event Hubs | Azure Data Factory | ADLS Gen2 | Delta Lake | Lakeflow

This project implements an end-to-end **big data streaming and batch data engineering architecture on Microsoft Azure**.

A Python/FastAPI ride-booking application generates simulated ride events and sends them in real time to **Azure Event Hubs**. At the same time, reference datasets are ingested through **Azure Data Factory** using an HTTP-based ingestion pipeline and stored in **Azure Data Lake Storage Gen2**.

Azure Databricks processes both data streams using **Apache Spark, PySpark, Spark Structured Streaming, Delta Lake and Lakeflow Declarative Pipelines**.

The final output is an analytics-ready **Gold Layer Star Schema** containing fact and dimension tables with **SCD Type 1 and SCD Type 2** processing.

---

# Architecture

```mermaid
flowchart LR

    A[FastAPI Ride Booking UI] --> B[Python Event Producer]
    B --> C[Azure Event Hubs]

    C --> D[Azure Databricks]
    D --> E[Apache Spark Structured Streaming]
    E --> F[Bronze rides_raw]

    G[GitHub Reference Data] --> H[Azure Data Factory]
    H --> I[HTTP Copy Activity]
    I --> J[ADLS Gen2]
    J --> K[Bronze Reference Tables]

    F --> L[stg_rides]
    K --> M[Silver OBT]
    L --> M

    M --> N[Gold Layer]

    N --> O[dim_passenger]
    N --> P[dim_driver]
    N --> Q[dim_vehicle]
    N --> R[dim_payment]
    N --> S[dim_booking]
    N --> T[dim_location]
    N --> U[fact_rides]
