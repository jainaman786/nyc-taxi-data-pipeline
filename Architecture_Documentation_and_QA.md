# Data Engineering Project: Architectural Documentation & Brainstorming QA

*This document serves as a technical appendix to the codebase, outlining the structural data flow and the architectural rationale behind the engineering decisions made in this repository.*

---

## 1. Pipeline Execution Flow

This data pipeline is orchestrated by Apache Airflow, models its logic via `dbt`, computes the transformations natively via Serverless `DuckDB`, and scales massive historical arrays using distributed `PySpark`.

1. **The Python Sensor (`dags/nyc_taxi_daily_pipeline.py`)**: 
   At 2:00 AM UTC, the first Airflow task (`check_source_freshness`) dynamically checks the `DATA_DIR` for a Parquet file matching the target execution date. If the file is missing, the task structurally fails, enters an `up_for_retry` state, and protects downstream tables from processing empty datasets.
   
2. **The `dbt` Bridge (`dbt/models/staging/sources.yml`)**: 
   Once the source data is verified, Airflow triggers `dbt`. `dbt` establishes a zero-copy connection directly to the Parquet file using DuckDB's native scanning engine (achieved by defining `external_location: "yellow_tripdata_*.parquet"`). DuckDB acts as an invisible SQL compiler—it never loads the massive file into RAM; it scans it selectively in real-time.

3. **Staging & Intermediate Transformations**:
   - `stg_yellow_trips.sql` standardizes column names, casts timestamps, and dynamically corrects boolean discrepancies.
   - `int_trips_enriched.sql` actively scrubs corrupted math (e.g., filtering trips lasting `0` seconds or negative passenger counts).

4. **Data Mart Generation (`marts/`)**:
   `dbt` then executes massive Window Function aggregations (`PARTITION BY trip_month ORDER BY total_trips DESC`) to identify the most performant Taxi Zones and calculates hour-of-day riding trends cleanly. 

5. **Write-Audit-Publish Data Quality (Tests)**:
   The `dbt test` task enforces strict analytical rules (e.g., `trip_distance` can NEVER be null). If the raw math is somehow corrupted, the pipeline inherently catches it and halts execution before merging the corrupted data into the live Production reporting dashboards.

---

## 2. Technical Design Decisions & Brainstorming Q&A

The following section addresses anticipated structural questions regarding the pipeline's architecture and performance optimization strategies.

### Q1: Why use DuckDB instead of a traditional Cloud Data Warehouse like Snowflake?
**Architecture Rationale:** The objective was to ingest and process a 3-4 GB Parquet dataset autonomously. Forcing the activation of an active, billed Snowflake warehouse simply to ingest a static Parquet file introduces unnecessary network latency and computational overhead. DuckDB operates perfectly offline and executes Serverless 'Zero-Copy' scanning. It is significantly faster for local data aggregations, infinitely cheaper, and fundamentally allows this entire repository to be portable.

### Q2: In the PySpark script (Task 4), how is the `JOIN` logic optimized against Data Skew when processing 1.5 Billion rows alongside a tiny 256-line CSV?
**Architecture Rationale:** Joining a 1.5 Billion row DataFrame against a CSV is a quintessential trigger for a catastrophic Spark Network Shuffle where executor nodes become bottlenecked. To inherently bypass this, the file explicitly uses a `broadcast(lookups_df)` join. Spark instantly ships the tiny 256-line dictionary straight into the L2 cache of every single Worker Node. The join resolves in milliseconds without forcing the network array to shuffle a single byte across partitions.

### Q3: Regarding Blue/Green Deployments, how does the pipeline prevent bad data from reaching downstream marts if `run_dbt_tests` fails halfway through?
**Architecture Rationale:** This pipeline adheres to the Write-Audit-Publish (WAP) Data Engineering design pattern. If the pipeline crashes natively halfway through `dbt test`, corrupted data must be prevented from entering Production. Therefore, `dbt` is configured to initially write the raw pipeline results directly into an isolated, invisible `_staging` schema (the **Audit** phase). Only after the data-quality tests return 100% successful does the process execute an atomic `ALTER SCHEMA...` SQL command, instantly swapping the fresh data directly into the Production layer (**Publish**). This entirely eliminates user downtime and data contamination.

### Q4: Why is the Airflow orchestrator deployed via `docker-compose.yml` instead of a standard local Python virtual environment?
**Architecture Rationale:** Portability and System Isolation. When writing Python DAGs referencing local system libraries, execution often crashes cross-platform (e.g., Windows vs. Linux) due to explicit `C:/` hard-drive pathing. Containerizing the webserver natively into a Docker environment, dynamically installing dependencies via `_PIP_ADDITIONAL_REQUIREMENTS`, and rewriting the file paths to map to the secure Docker Volume (`/opt/airflow/data`) mathematically ensures the codebase runs identically anywhere on the planet with a single `docker compose up -d` command.

### Q5: The pipeline uses `retries=2`. Resultantly, if the Parquet file doesn't upload at 2:00 AM, the pipeline eventually turns red and stays failed. Is this the intended behavior?
**Architecture Rationale:** Yes. The DAG arguments are explicitly structured utilizing `email_on_failure: True` and configured with two retry loops operating on a 5-minute spacing delay (`timedelta(minutes=5)`). If the source file is simply delayed by 5 minutes, it dynamically skips the failure and re-runs natively. However, if the file is permanently missing and exhausts all 3 execution attempts, it is the exact intended behavior to permanently fail the pipeline and fire off an alert to the Engineering team to investigate the upstream file ingestion delay.

---

## 3. Execution & Testing Instructions

Zero manual Python setup is required on the host machine to validate this architecture. 

1. Ensure **Docker Desktop** is installed and running.
2. In the terminal, boot the orchestration cluster: `docker compose up -d`
3. Wait approximately 60 seconds for the containers to initialize and download dependencies.
4. Navigate to `http://localhost:8080`, authenticate with `admin/admin`, and hit **Trigger DAG** to observe the dynamic execution.
