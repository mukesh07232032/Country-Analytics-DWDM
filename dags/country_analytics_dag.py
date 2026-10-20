"""
Apache Airflow DAG: Country Analytics Pipeline
Orchestrates scheduled extraction of country demographics from REST Countries v5,
transforms data into the relational analytics schema, and idempotently loads
records into PostgreSQL.
"""

from datetime import datetime, timedelta, timezone
import logging
from pathlib import Path
import sys

# Ensure project root is available on sys.path for local runs and containerized Airflow
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from airflow import DAG
from airflow.operators.python import PythonOperator

task_logger = logging.getLogger("airflow.task")


def run_country_analytics_etl(**context):
    """
    Executes the Country Analytics ETL pipeline within an Airflow task.
    Imports etl lazily inside the task callable to ensure fast DAG parsing
    and to prevent premature database connectivity checks during DAG collection.
    """
    import etl

    task_logger.info("Starting Country Analytics ETL execution...")

    # Reads REST_COUNTRIES_API_KEY and PostgreSQL connection parameters
    # from the environment variables configured for the Airflow container.
    result = etl.run_etl()

    task_logger.info(
        "ETL task completed successfully. Ingestion summary: "
        "countries_loaded=%(countries_loaded)s, "
        "languages_loaded=%(languages_loaded)s, "
        "currencies_loaded=%(currencies_loaded)s",
        result,
    )
    return result


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="country_analytics_pipeline",
    default_args=default_args,
    description=(
        "Extracts country demographics and geography from REST Countries v5, "
        "transforms records into relational tables, and loads data into PostgreSQL."
    ),
    schedule="@daily",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "country_analytics", "rest_countries", "postgres"],
) as dag:

    etl_task = PythonOperator(
        task_id="extract_transform_load_countries",
        python_callable=run_country_analytics_etl,
    )
