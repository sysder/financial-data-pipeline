# Financial Data Pipeline (Modern Data Stack PoC)

## Project Status: In Active Development (Nov 2025 - Present)
> **Note:** This repository creates a production-grade financial data platform. I am currently building the core infrastructure and ingestion pipelines.

## Objective
To demonstrate a **modern, scalable, and testable** data pipeline architecture suitable for financial data analysis and algorithmic trading preparation.
This project focuses on **"Asset-centric" orchestration** and **"Python-native" engineering practices**, moving away from traditional task-based workflows.

## Tech Stack & Architecture Strategy

### 1. Orchestration: [Dagster](https://dagster.io/)
* **Role:** Unified control plane for data assets.
* **Why Chosen:**
    * Leverages **Software-Defined Assets (SDAs)** for superior observability and lineage tracking compared to Airflow.
    * **Python-native** approach allows for easier unit testing (`pytest`) and local development.
    * Seamless integration with dbt assets.

### 2. Transformation: [dbt Core](https://www.getdbt.com/)
* **Role:** SQL-based data transformation and modeling.
* **Why Chosen:**
    * To implement modular data modeling (Staging -> Intermediate -> Marts).
    * For built-in documentation and data quality tests.

### 3. Compute & Storage: [DuckDB](https://duckdb.org/)
* **Role:** In-process OLAP database for high-performance analytics.
* **Architectural Decision (Why not Spark?):**
    * While **Apache Spark** is the standard for petabyte-scale distributed processing, I strategically selected **DuckDB** for this project.
    * **Reason:** To prioritize **local development velocity** and vectorized execution efficiency on moderate-sized financial datasets, avoiding the complexity and overhead of managing JVM clusters (avoiding over-engineering).

### 4. Data Processing: [Polars](https://pola.rs/)
* **Role:** Lightning-fast DataFrame manipulation in Python assets.
* **Why Chosen:**
    * To replace legacy Pandas for better memory management and speed (Rust-based).

### 5. Data Source
* **APIs:** `yfinance`, `Alpha Vantage` (Planned)

## Roadmap & Milestones

- [ ] **Phase 1: Foundation & Ingestion**
    - [ ] Setup Dagster project structure with Poetry.
    - [ ] Implement Python Assets to fetch OHLCV data from APIs.
    - [ ] Store raw data into DuckDB/Parquet.

- [ ] **Phase 2: Transformation (dbt)**
    - [ ] Define dbt models (Staging, Marts).
    - [ ] Calculate technical indicators (SMA, RSI) using SQL/dbt.
    - [ ] Integrate dbt assets into the Dagster graph.

- [ ] **Phase 3: Quality Assurance & MLOps**
    - [ ] Implement Unit Tests using `pytest`.
    - [ ] Add Dagster Asset Checks for data quality.
    - [ ] (Future) Integrate a simple ML prediction model asset.

---
*Author: [Yuki Umezawa / sysder]*
