FROM apache/airflow:2.10.2

# Allow installing system packages
USER root
ENV PIP_BREAK_SYSTEM_PACKAGES=1

# Switch to airflow user BEFORE running pip (important)
USER airflow

RUN pip install --no-cache-dir \
    dbt-core \
    dbt-snowflake \
    snowflake-connector-python \
    pandas \
    python-dotenv \
    requests
