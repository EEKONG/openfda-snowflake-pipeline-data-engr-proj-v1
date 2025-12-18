## OpenFDA Drug Shortages — Analytics-Ready Data Pipeline & Case Study
# Overview

Drug shortages are a persistent challenge across the U.S. healthcare system, impacting patient care, hospital operations, and supply-chain planning. This project transforms raw openFDA drug shortage data into reliable, decision-ready insights by combining an automated ETL pipeline with analytical modeling and visualization.
The solution is designed to support both engineering reliability and business storytelling, enabling stakeholders to understand why shortages occur, where they concentrate clinically, and which shortages persist over time.

# Key Business Insights
  - A small number of poorly defined root causes account for a disproportionate share of shortage records.
  - Certain root causes generate high operational workload, requiring frequent updates and administrative follow-ups per drug.
  - Drug shortages are concentrated in clinically critical therapeutic areas such as cardiovascular, endocrine, and neurological treatments.
  - Timeline analysis reveals that several drugs remain in active shortage for multiple years, indicating chronic supply-chain failures rather than temporary disruptions.
# Why This Project Matters
  - This project demonstrates how modern data engineering practices can convert messy regulatory data into trusted analytical assets.
  - By combining automated ingestion, tested transformations, and business-focused visualization, the pipeline supports better prioritization, risk assessment, and operational decision-making in healthcare supply chains.


## Case Study: From Raw API Data to Business Insight
# The Problem

The openFDA drug shortages dataset is:
- Semi-structured and nested
- Updated frequently with incremental changes
- Difficult to analyze directly due to inconsistent fields and unstructured root-cause notes

Without transformation, it is hard to answer critical questions such as:
- Which root causes drive the majority of shortages?
- Which shortages consume the most operational effort?
- Which drugs remain in shortage for years rather than weeks?


## The Solution
This project implements an end-to-end pipeline that:
- Ingests raw FDA data from an external API
- Cleans and standardizes inconsistent fields
- Models analytical metrics such as duration, operational burden, and clinical concentration
- Delivers trusted tables for dashboards and downstream analysis

## How the ETL Pipeline Runs
# 1. Ingestion (Python + openFDA API)
   - A Python ingestion job paginates through the openFDA drug shortages endpoint
   - Nested JSON payloads are flattened into tabular format
   - Each run writes to a timestamped raw table in Snowflake
   - A rolling master view is refreshed to preserve historical snapshots while exposing the latest data
# Why this matters:
Historical tracking allows analysts to study long-running shortages and repeated updates rather than only the most recent state.

# 2. Orchestration (Airflow)
  -  An Airflow DAG orchestrates the workflow on a scheduled cadence
  -  Tasks are chained to ensure:
      - API ingestion completes successfully
      - dbt models run only after fresh data is available
      - dbt tests validate transformations before analytics are consumed
# Why this matters:
Automation ensures the dataset stays current and eliminates manual intervention, making the pipeline production-ready.

# 3. Transformation & Modeling (dbt + Snowflake)
  - Staging models clean and normalize source fields (e.g., dosage forms, availability status)
  - Business logic models classify shortage root causes from unstructured FDA notes
  - Mart models compute decision-ready metrics:
      -  Shortage duration (active vs resolved)
      -  Entries per drug (operational workload)
      -  Root-cause impact rankings
      -  Therapeutic category aggregations
# Why this matters:
Separating raw, staging, and mart layers ensures transparency, testability, and trust in analytical outputs.

# 4. Analytics & Storytelling (Tableau)
  The curated tables power an interactive dashboard that answers:
    - WHY shortages happen (root causes)
    - WHERE they cluster clinically (therapeutic areas)
    - HOW long they persist (timeline analysis)
    - WHICH cases require disproportionate operational effort
  The dashboard is designed for decision-makers, not just analysts.

# Tech Stack & Skills Demonstrated
Data Engineering
- Python (API ingestion, JSON flattening)
- Airflow (DAG orchestration, scheduling)
- Snowflake (raw, staging, analytics schemas)
- dbt (transformations, tests, analytics modeling)
- Docker & docker-compose (local reproducibility)

Data Analytics
- Analytical modeling and KPI design
- Root-cause classification and ranking
- Duration and lifecycle analysis
- Business storytelling with Tableau dashboards









