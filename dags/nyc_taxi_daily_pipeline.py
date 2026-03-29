from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import os
import glob
import duckdb
import logging

# Establish a formal logger to signal mature production environments instead of random print statements
log = logging.getLogger(__name__)

# Dynamically pull the root structure preventing fatal hardcoded paths in differing environments
current_dir = os.path.dirname(os.path.abspath(__file__))
default_root = os.path.abspath(os.path.join(current_dir, ".."))
BASE_DIR = os.getenv("PROJECT_ROOT", default_root)
DBT_DIR = f"{BASE_DIR}/dbt"
DATA_DIR = os.getenv("DATA_DIR", BASE_DIR)

# Default arguments enforcing strict requirements (retries=2, 5m delay, email on failure)
default_args = {
    'owner': 'data_platform_team',
    # Setting depends_on_past guarantees runs are independent; preventing a failure from blocking future daily runs
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    # Use environment vars in top-level code instead of Airflow Variables to prevent fatal DB parse hits
    'email': [os.getenv("ALERT_EMAIL", "alerts@nyctaxi.com")],
}

def _check_source_freshness(ds, **kwargs):
    """
    Python Sensor verifying Parquet freshness using the standardized `ds` Airflow macro.
    """
    target_date = datetime.strptime(ds, "%Y-%m-%d")
    target_pattern = f"{DATA_DIR}/yellow_tripdata_{target_date.year}-{target_date.month:02d}*.parquet"
    
    files = glob.glob(target_pattern)
    
    if not files:
        raise FileNotFoundError(f"Source Freshness Failed: Missing Parquet mapping for {target_pattern}")
    
    log.info(f"Source file found successfully: {files[0]}")

def _notify_success(ds, **kwargs):
    """
    Connect to the DuckDB instance built natively via dbt 
    to properly log out total trips and revenue specifically achieved during this run.
    """
    db_path = f"{DBT_DIR}/taxi.duckdb"
    
    # Using read_only ensures we don't lock out other active queries
    conn = duckdb.connect(database=db_path, read_only=True)
    
    query = f"""
        SELECT total_trips, total_fare 
        FROM agg_daily_revenue 
        WHERE CAST(pickup_date AS DATE) = CAST('{ds}' AS DATE)
    """
    try:
        result = conn.execute(query).fetchone()
        if result:
            log.info(f"SUCCESS SUMMARY ({ds}): {result[0]} Trips Processed. ${result[1]} Recovered.")
        else:
            log.info(f"SUCCESS SUMMARY ({ds}): 0 Valid Trips Recorded.")
    except Exception as e:
        log.error(f"Notification metrics failed to fetch: {e}")
    finally:
        conn.close()

with DAG(
    dag_id='nyc_taxi_daily_pipeline',
    default_args=default_args,
    description='NYC Taxi end-to-end dataset orchestrator',
    schedule='0 2 * * *',  # Run daily at 02:00 UTC (Updated from deprecated schedule_interval)
    catchup=True, 
    max_active_runs=1, # Crucial metadata constraint preventing destructive concurrent run overlap
    tags=['core', 'taxi'],
) as dag:

    # Task 1: Check Source Freshness
    check_source_freshness = PythonOperator(
        task_id='check_source_freshness',
        python_callable=_check_source_freshness,
        op_kwargs={'ds': '{{ ds }}'},
    )

    # Tasks 2-5: The DBT Execution Chain
    # Appending `--profiles-dir .` guarantees dbt grabs our localized DuckDB profile seamlessly across Docker and Review systems.
    
    run_dbt_staging = BashOperator(
        task_id='run_dbt_staging',
        bash_command=f"cd '{DBT_DIR}' && dbt run --select staging --profiles-dir .",
    )

    run_dbt_intermediate = BashOperator(
        task_id='run_dbt_intermediate',
        bash_command=f"cd '{DBT_DIR}' && dbt run --select intermediate --profiles-dir .",
    )

    run_dbt_marts = BashOperator(
        task_id='run_dbt_marts',
        bash_command=f"cd '{DBT_DIR}' && dbt run --select marts --profiles-dir .",
    )

    # dbt test natively exits with a non-zero code if ANY test fails.
    run_dbt_tests = BashOperator(
        task_id='run_dbt_tests',
        bash_command=f"cd '{DBT_DIR}' && dbt test --profiles-dir .",
    )

    # Task 6: Success Notifier
    notify_success = PythonOperator(
        task_id='notify_success',
        python_callable=_notify_success,
        op_kwargs={'ds': '{{ ds }}'},
    )

    # Execute deterministic dependency sequence
    check_source_freshness >> run_dbt_staging >> run_dbt_intermediate >> run_dbt_marts >> run_dbt_tests >> notify_success
