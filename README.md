# NYC TLC Taxi Platform Analytics

![DuckDB](https://img.shields.io/badge/DuckDB-v1.10.1-yellow?style=flat-square&logo=duckdb)
![dbt](https://img.shields.io/badge/dbt-v1.11.7-FF694B?style=flat-square&logo=dbt)
![Apache Airflow](https://img.shields.io/badge/Airflow-2.x-017CEE?style=flat-square&logo=apacheairflow)
![PySpark](https://img.shields.io/badge/PySpark-Distributed-E25A1C?style=flat-square&logo=apachespark)
![Snowflake](https://img.shields.io/badge/Snowflake-Dialect-29B5E8?style=flat-square&logo=snowflake)

A production-inspired data engineering pipeline for the NYC Yellow Taxi dataset (2023). Ingests, cleans, models, tests, and orchestrates ~38M rows locally via DuckDB + dbt, with a distributed PySpark script for the full 1.5B-row historical dataset.

---

## 📂 Repository Structure

```
.
├── dbt/                        # dbt project
│   ├── models/
│   │   ├── staging/            # stg_yellow_trips, stg_taxi_zones
│   │   ├── intermediate/       # int_trips_enriched (filtered + joined)
│   │   └── marts/              # fct_trips, dim_zones, agg_daily_revenue, agg_zone_performance
│   ├── tests/                  # Custom singular + generic tests
│   ├── seeds/                  # taxi_zone_lookup.csv
│   ├── dbt_project.yml
│   ├── profiles.yml            # DuckDB target config
│   └── profiles.yml.example
├── dags/
│   └── nyc_taxi_daily_pipeline.py   # Airflow DAG
├── queries/
│   ├── q1_top_zones_by_revenue.sql
│   ├── q2_hour_of_day_pattern.sql
│   └── q3_consecutive_gap_analysis.sql
├── spark/
│   └── process_historical.py        # PySpark bonus script
├── docker-compose.yml
├── requirements.txt
├── .gitignore
└── README.md
```

---

## 🚀 Setup & Execution

This project uses a **zero-copy approach**: raw `.parquet` files are queried directly off disk via DuckDB — no `INSERT` sweeps, no dedicated database server.

### Prerequisites

- Docker + Docker Compose (for Option 1)
- Python 3.9+ with `pip` (for Option 2)
- `yellow_tripdata_2023-01.parquet` (or multiple months) placed in the project root
- `taxi_zone_lookup.csv` placed in `dbt/seeds/`

---

### ▶️ Option 1: Full Orchestrated Pipeline via Docker (Recommended)

Spins up Airflow (Webserver + Scheduler + Postgres metadata DB) with dbt and DuckDB pre-configured inside the container.

```bash
# 1. Start the stack
docker compose up -d

# 2. Open Airflow UI
#    URL:      http://localhost:8080
#    Username: admin
#    Password: admin

# 3. Trigger the DAG: nyc_taxi_daily_pipeline
```

Airflow will run the full pipeline in sequence:
1. `check_source_freshness` — validates the Parquet file exists for the target date
2. `run_dbt_staging` — cleans and casts raw data
3. `run_dbt_intermediate` — enriches with zone names, filters invalid records
4. `run_dbt_marts` — builds fact table, dimensions, and aggregations
5. `run_dbt_tests` — runs all built-in + custom dbt tests; fails the DAG if any test fails
6. `notify_success` — logs trip count and revenue for the day

---

### ▶️ Option 2: Run dbt Locally (Without Airflow)

Use this to iterate on models or run tests independently.

**Step 1 — Create and activate a virtual environment**

```bash
# Create venv
python -m venv .venv

# Activate (macOS/Linux)
source .venv/bin/activate

# Activate (Windows)
.venv\Scripts\activate
```

**Step 2 — Install dependencies**

```bash
pip install -r requirements.txt
# or install dbt-duckdb directly:
pip install dbt-duckdb
```

**Step 3 — Navigate to the dbt project**

```bash
cd dbt
```

**Step 4 — Load seed data**

```bash
dbt seed
```

**Step 5 — Run models**

```bash
# Run all models
dbt run

# Or run layer by layer
dbt run --select staging
dbt run --select intermediate
dbt run --select marts
```

**Step 6 — Run tests**

```bash
dbt test
```

**Step 7 — Run a specific model**

```bash
dbt run --select stg_yellow_trips
dbt run --select fct_trips
dbt run --select agg_daily_revenue
```

---

### ▶️ Option 3: Run SQL Queries Directly

The `queries/` folder contains three analytical SQL scripts written in Snowflake dialect (also compatible with DuckDB for local testing).

```bash
# With DuckDB CLI
duckdb dbt/taxi.duckdb < queries/q1_top_zones_by_revenue.sql
duckdb dbt/taxi.duckdb < queries/q2_hour_of_day_pattern.sql
duckdb dbt/taxi.duckdb < queries/q3_consecutive_gap_analysis.sql
```

---

## 🏛️ Architecture Overview

### DuckDB + dbt over Snowflake

The primary trade-off was using **DuckDB + dbt-duckdb** instead of a Snowflake trial account. DuckDB can query `.parquet` files directly via `external_location` in `sources.yml` — no ingestion step needed. For a 3–4 GB local dataset, this eliminates setup friction while preserving the full dbt model layer (staging → intermediate → marts) that a production Snowflake workflow would use.

For a real production deployment, swapping the dbt profile target from DuckDB to Snowflake requires changing only `profiles.yml`.

### Containerized Airflow

Rather than requiring reviewers to install Airflow natively (which is particularly fragile on Windows without WSL2), the `docker-compose.yml` spins up a self-contained `LocalExecutor` stack. The `dbt/` and `dags/` directories are bind-mounted into the container, so model edits are reflected immediately without rebuilding.

---

## 📊 Data Model Overview

```
Raw Parquet Files
      │
      ▼
[Staging Layer]
  stg_yellow_trips     — renamed columns, cast types, trip_duration_minutes
  stg_taxi_zones       — zone lookup from seed CSV
      │
      ▼
[Intermediate Layer]
  int_trips_enriched   — joins pickup/dropoff zone names, filters invalid records
      │
      ▼
[Mart Layer]
  fct_trips            — final fact table of valid, enriched trips
  dim_zones            — zone dimension
  agg_daily_revenue    — daily KPIs: trips, fare, tips, tip rate %
  agg_zone_performance — zone-level ranking + high-volume flag
```

### Data Quality Tests

- `not_null` and `unique` on all primary keys
- Custom generic test: `test_duration_range` — asserts `trip_duration_minutes` is within a configurable `min_value` / `max_value`
- Custom singular test: `assert_total_amount_geq_fare` — asserts no trip has `total_amount < fare_amount`

---

## 🧾 SQL Queries (Task 3)

All queries are in `queries/` — Snowflake dialect, tested locally against DuckDB.

| File | Description |
|---|---|
| `q1_top_zones_by_revenue.sql` | Top 10 pickup zones by revenue per month using `DENSE_RANK()` |
| `q2_hour_of_day_pattern.sql` | Hourly demand: trips, avg fare, avg tip %, 3-hour rolling average |
| `q3_consecutive_gap_analysis.sql` | Max gap between consecutive trips per zone per day using `LEAD()` |

---

## 🧠 Brainstormer Responses

### Task 1: Zone Revenue Ranking — Monthly vs. Annual

Ranking zones by revenue across the entire year flattens seasonality — a zone that dominates January but is quiet in July gets buried under a flat annual rank. Partitioning by month (`RANK() OVER (PARTITION BY trip_month ORDER BY total_revenue DESC)`) surfaces which zones perform well *within each time period*, which is more useful for operational planning (e.g., fleet allocation, surge pricing). This is implemented in `agg_zone_performance.sql`.

### Task 2: Preventing Bad Data from Reaching Marts (Write-Audit-Publish)

If `run_dbt_tests` fails mid-pipeline, no data should be visible to downstream consumers. The approach implemented is the **Write-Audit-Publish (WAP)** pattern:

1. **Write** — `dbt run` writes all mart models to an isolated schema: `taxi_marts_staging`
2. **Audit** — `dbt test` runs against `taxi_marts_staging` exclusively
3. **Publish** — Only if all tests pass does Airflow execute an atomic schema swap:
   ```sql
   ALTER SCHEMA taxi_marts_staging RENAME TO taxi_marts_production;
   ```

Downstream BI tools (Looker, Metabase) point only at `_production`. A mid-flight test failure leaves `_production` untouched at its last known-good state.

### Task 3: Query 3 Performance on 38M Rows

`LEAD()`-based gap analysis is expensive because it requires a full sort by `(PULocationID, tpep_dropoff_datetime)` across the dataset. Mitigation strategies:

- **Clustering key**: Cluster the source table on `(PULocationID, DATE(tpep_pickup_datetime))` so micro-partitions are pruned before the window sort
- **Materialized View**: Pre-compute and cache the gap results; refreshed nightly — ad-hoc queries hit the cache, not the warehouse compute
- **Result cache**: Snowflake's 24-hour result cache means the same query re-runs in milliseconds if the underlying data hasn't changed
- **Avoid Search Optimization Service** here — SOS targets random point lookups, not sequential window computations

---

## ⚖️ Key Trade-offs

| Decision | Benefit | Limitation |
|---|---|---|
| DuckDB over Snowflake | Zero setup, instant Parquet queries | Not representative of cloud warehouse scale behavior |
| Dockerized Airflow | Fully reproducible, no host OS dependencies | Slight startup overhead (~30s) |
| Direct Parquet querying | No ingestion step, zero-copy reads | Limited indexing vs. warehouse tables |
| Single-month Parquet for local testing | Fast iteration | Full 12-month pipeline requires all files in `DATA_DIR` |

---

## 📸 Proof of Execution

<img width="940" height="241" alt="image" src="https://github.com/user-attachments/assets/e025b498-eb9e-459a-9e7b-fdeb4be7c4cf" />
<img width="940" height="438" alt="image" src="https://github.com/user-attachments/assets/c3137af3-74e2-4dae-a86e-3b5f0e7ea559" />
<img width="940" height="433" alt="image" src="https://github.com/user-attachments/assets/fe1c7bc9-31ec-46f6-b52e-ab2af240b290" />

---

## ⚙️ AI-Assisted Development

This project was built with assistance from Antigravity and other AI coding tools to accelerate delivery across all four tasks. AI was used to generate initial scaffolding, suggest optimization strategies, and debug environment issues (Airflow 3 deprecations, DuckDB profile config, Docker networking).

All AI-generated outputs were reviewed, validated against the assignment requirements, and iteratively refined — particularly around the custom dbt tests, window function logic, and the WAP pattern implementation.

AI meaningfully compressed the development timeline; the architectural decisions, trade-off reasoning, and Brainstormer answers reflect deliberate engineering judgment rather than generated boilerplate.
