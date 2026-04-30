# Airflow Docker — Project Overview

## What This Project Is

A self-contained, Dockerised **Apache Airflow 3.1.5** deployment running on **CeleryExecutor**. It hosts a collection of data-pipeline DAGs built for the **Farmlytics** agricultural-analytics platform, alongside several demo/learning DAGs. All infrastructure (Airflow, its metadata DB, and the task broker) runs in Docker Compose; the Farmlytics production database (`farmlytics`) lives in a separate container called `kifaru-db` that is connected at runtime.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   Docker Compose Stack                   │
│                                                         │
│  ┌──────────────┐   ┌──────────────┐  ┌─────────────┐  │
│  │  PostgreSQL  │   │    Redis      │  │  Airflow    │  │
│  │     :5432    │   │   :6379       │  │  API Server │  │
│  │  (metadata)  │   │  (broker)    │  │   :8080     │  │
│  └──────────────┘   └──────────────┘  └─────────────┘  │
│                                                         │
│  ┌──────────────┐   ┌──────────────┐  ┌─────────────┐  │
│  │  Scheduler   │   │ DAG Processor│  │  Triggerer  │  │
│  │  :8974(hc)   │   │              │  │             │  │
│  └──────────────┘   └──────────────┘  └─────────────┘  │
│                                                         │
│  ┌──────────────┐   ┌──────────────┐                   │
│  │Celery Worker │   │  Flower UI   │                   │
│  │              │   │   :5555      │                   │
│  └──────────────┘   └──────────────┘                   │
└─────────────────────────────────────────────────────────┘
              │
              │ airflow_docker_default network
              ▼
       ┌────────────┐
       │  kifaru-db │  (external PostgreSQL — farmlytics DB)
       └────────────┘
```

### Executor: CeleryExecutor

Tasks are dispatched by the Scheduler → Redis broker → Celery Worker. This allows horizontal scaling of workers.

### Auth Manager: FabAuthManager

Apache Airflow 3's pluggable auth system is configured to use Flask-AppBuilder (FAB) for user/role management.

---

## Infrastructure Services

| Service | Image | Port | Role |
|---|---|---|---|
| `postgres` | `postgres:16` | 5432 (internal) | Airflow metadata database |
| `redis` | `redis:7.2-bookworm` | 6379 (internal) | Celery message broker |
| `airflow-apiserver` | `apache/airflow:3.1.5` | **8080** | REST API + Web UI |
| `airflow-scheduler` | `apache/airflow:3.1.5` | 8974 (health) | DAG scheduling |
| `airflow-dag-processor` | `apache/airflow:3.1.5` | — | Parses & processes DAG files |
| `airflow-worker` | `apache/airflow:3.1.5` | — | Executes tasks via Celery |
| `airflow-triggerer` | `apache/airflow:3.1.5` | — | Handles async/deferred tasks |
| `airflow-init` | `apache/airflow:3.1.5` | — | One-shot: DB migration + admin user creation |
| `airflow-cli` | `apache/airflow:3.1.5` | — | Debug shell (profile: `debug`) |
| `flower` | `apache/airflow:3.1.5` | **5555** | Celery monitoring UI (profile: `flower`) |

### Key Environment Variables

| Variable | Default | Description |
|---|---|---|
| `AIRFLOW_IMAGE_NAME` | `apache/airflow:3.1.5` | Docker image |
| `AIRFLOW_UID` | `50000` | UID for volume file ownership |
| `AIRFLOW_PROJ_DIR` | `.` | Host path for volume mounts |
| `ENV_FILE_PATH` | `.env` | Path to env file |
| `_AIRFLOW_WWW_USER_USERNAME` | `airflow` | Web UI admin username |
| `_AIRFLOW_WWW_USER_PASSWORD` | `airflow` | Web UI admin password |
| `_PIP_ADDITIONAL_REQUIREMENTS` | `apache-airflow-providers-postgres` | Extra pip packages installed at startup |

---

## Volume Mounts

Every Airflow container mounts these host directories:

| Host Path | Container Path | Purpose |
|---|---|---|
| `./dags` | `/opt/airflow/dags` | DAG Python files |
| `./logs` | `/opt/airflow/logs` | Task and scheduler logs |
| `./config` | `/opt/airflow/config` | `airflow.cfg` custom config |
| `./plugins` | `/opt/airflow/plugins` | Reusable Python packages/plugins |
| `./data` | `/opt/airflow/data` | Shared data files (CSVs, input/output) |

PostgreSQL data is persisted in the named Docker volume `postgres-db-volume`.

---

## Database Connections

### 1. Airflow Metadata DB (internal)
- **URI:** `postgresql+psycopg2://airflow:airflow@postgres/airflow`
- Used by: all Airflow services
- Celery result backend: `db+postgresql://airflow:airflow@postgres/airflow`
- Broker: `redis://:@redis:6379/0`

### 2. Farmlytics Production DB (`farmlytics_postgres` Airflow connection)
- **Host:** `kifaru-db` (external container connected to the Airflow network)
- **Port:** 5432
- **Database:** `farmlytics`
- **User:** `postgres`
- Added manually via `connection.sh` from inside the scheduler container

---

## DAGs

### 1. `simple_demo_pipeline` (`simple_demo.py`)
**Purpose:** Learning/demo pipeline.
**Schedule:** Every minute (`* * * * *`)
**Flow:**
```
get_number() → double_number() → report_result()
```
Generates a random integer (1–100), doubles it, and prints whether the result is "High" (>100) or "Low".

---

### 2. `masinde_dag` (`sampe.py`)
**Purpose:** Basic BashOperator demo.
**Schedule:** `@daily`
**Flow:**
```
echo_masinde (BashOperator: "echo masinde charles")
```

---

### 3. `brian_dag` (`sample_two.py`)
**Purpose:** Basic BashOperator demo (second variant).
**Schedule:** `@daily`
**Flow:**
```
echo_masinde (BashOperator: "echo brian masinde")
```

---

### 4. `name_age_sentence_dag_v2` (`name_age_sentence_dag.py`)
**Purpose:** TaskFlow API learning example — demonstrates XCom-based data passing.
**Schedule:** `@daily`, `catchup=True`
**Flow:**
```
get_name() ──┐
             ├──► build_sentence()
get_age()  ──┘
```
Returns the string `"My name is John and I am 30 years old."`.

---

### 5. `fake_store_etl_db_dag` (`fake_store_etl.py`)
**Purpose:** End-to-end ETL from a public REST API to PostgreSQL.
**Schedule:** `@daily`, `catchup=False`
**Tags:** `fake_store`, `etl`, `db`
**External API:** `https://fakestoreapi.com/products`
**Target table:** `fake_store_products` in the `farmlytics` database
**Connection:** `farmlytics_postgres`

**Flow:**
```
extract_products() → save_products_csv() → load_to_db()
```

| Task | Action |
|---|---|
| `extract_products` | HTTP GET to FakeStore API, returns list of product dicts |
| `save_products_csv` | Writes products to `/opt/airflow/data/fake_store/fake_store_products.csv` |
| `load_to_db` | Creates table if missing (with PK on `id`), then does a bulk PostgreSQL upsert (`ON CONFLICT DO UPDATE`) |

**Output CSV schema:** `id, image, price, title, rating, category, description`

The `rating` column is stored as a string (the raw JSON dict from the API, e.g. `{'rate': 3.9, 'count': 120}`).

---

### 6. `kamis_etl_dag_v3` (`kamis_etl_dag.py`) — Primary Production DAG
**Purpose:** Daily scrape of Kenya agricultural market prices from the [KAMIS portal](https://kamis.kilimo.go.ke) and load into the Farmlytics star schema.
**Schedule:** `@daily`, `catchup=True`
**Tags:** `kamis`, `etl`, `farmlytics`
**Owner:** `masinde`
**Retries:** 5 (10-second delay)
**Connection:** `farmlytics_postgres`

**Flow:**
```
extract() → load_to_db() → archive()
```

| Task | Action |
|---|---|
| `extract` | Scrapes KAMIS market pages in parallel (5 workers, 5 pages × 3000 rows/page) and appends to `/opt/airflow/data/input/kamis_all_data.csv` |
| `load_to_db` | Loads CSV → staging table → cleans data → populates dimensions → inserts fact records |
| `archive` | Moves the input CSV to `/opt/airflow/data/output/kamis_<timestamp>.csv` |

---

## Plugins — `farmlytics_etl` Package

Located in `plugins/farmlytics_etl/`, this package is auto-discovered by Airflow and importable from all DAGs.

```
plugins/farmlytics_etl/
├── config/
│   └── settings.py          — Paths, KAMIS URL, DB config, date range helpers
├── extract/
│   └── kamis_scraper.py     — Scrapes KAMIS HTML tables via requests + pandas
├── load/
│   └── csv_loader.py        — Loads CSV into kamis_raw_data staging table
├── db/
│   ├── engine.py            — SQLAlchemy engine factory (reads from settings)
│   ├── staging.py           — Cleans placeholder "-" values in staging
│   ├── dimensions.py        — Inserts new rows into dimension tables
│   └── facts.py             — Inserts into product_market_prices fact table
└── utils/
    └── logger.py            — Sets up file + console logging to /opt/airflow/logs/custom/app.log
```

### Settings (`config/settings.py`)

| Setting | Value |
|---|---|
| `INPUT_DIR` | `/opt/airflow/data/input` |
| `OUTPUT_DIR` | `/opt/airflow/data/output` |
| `CSV_FILENAME` | `kamis_all_data.csv` |
| `BASE_URL` | `https://kamis.kilimo.go.ke/site/market` |
| `MAX_WORKERS` | `5` (parallel scrape threads) |
| `PAGES_TO_CHECK` | `5` (pages scraped per run) |
| `DATE_START` | 7 days ago (computed at runtime) |
| `DATE_END` | Today (computed at runtime) |

### Farmlytics Star Schema

The KAMIS ETL populates this database schema in the `farmlytics` PostgreSQL database:

**Staging:**
- `kamis_raw_data` — Raw scraped data (replaced on every run): `Market`, `Commodity`, `Classification`, `Grade`, `Sex`, `Wholesale`, `Retail`, `Supply Volume`, `County`, `Date`

**Dimension Tables (INSERT-only, new rows only):**
- `products` — Unique commodity names
- `product_classes` — Unique classification values
- `product_grades` — Unique grade values
- `product_sexes` — Unique sex/gender classifications
- `markets` — Unique market names (linked to `counties`)
- `counties` — Referenced by `markets` (assumed pre-populated)

**Fact Table:**
- `product_market_prices` — One row per observation, with unique constraint `pmp_unique_observation` (deduplication via `ON CONFLICT DO NOTHING`)
  - `product_id`, `product_class_id`, `product_grade_id`, `product_sex_id`
  - `wholesale_price`, `retail_price`, `unit` (parsed from strings like `40.00/Kg`)
  - `market_id`, `date`, `source_id`
  - `created_at`, `updated_at`

---

## Data Directory

```
data/
├── fake_store/
│   └── fake_store_products.csv   — Latest FakeStore API product cache
├── input/
│   └── kamis_all_data.csv        — Active KAMIS scrape (overwritten each run)
└── output/
    └── kamis_<YYYYMMDD_HHmmss>.csv  — Archived KAMIS runs (13 files as of Jan 19 2026)
```

---

## Configuration

### `config/airflow.cfg`
Custom Airflow configuration file, mounted into every container at `/opt/airflow/config/airflow.cfg` and activated via `AIRFLOW_CONFIG=/opt/airflow/config/airflow.cfg`.

### `.env`
Required file at the project root. At minimum must contain:
```
AIRFLOW_UID=<your-linux-uid>   # run: id -u
```

---

## External Dependencies

| Dependency | Type | Purpose |
|---|---|---|
| `apache-airflow-providers-postgres` | Pip | `PostgresHook` for DAG-to-DB connectivity |
| `pandas` | Pip (built-in with Airflow image) | Data transformation |
| `requests` | Pip (built-in) | HTTP calls to KAMIS and FakeStore API |
| `sqlalchemy` | Pip (built-in) | ORM / SQL execution |
| `psycopg2` | Pip (built-in) | PostgreSQL driver |
| `kifaru-db` | External Docker container | Farmlytics production PostgreSQL database |

---

## Web Interfaces

| Interface | URL | Credentials |
|---|---|---|
| Airflow Web UI | http://localhost:8080 | `airflow` / `airflow` |
| Celery Flower | http://localhost:5555 | — (start with `--profile flower`) |

---

## DAG History (from logs)

The logs directory reveals the full history of DAG IDs that have been run on this instance:

- `asset1_producer`
- `brian_dag`
- `echo_masinde_dag`
- `fake_store_etl_dag` (earlier version)
- `fake_store_etl_db_dag` (current)
- `kamis_etl_dag_v1`, `kamis_etl_dag_v2`, `kamis_etl_dag_v3` (current)
- `masinde_dag`
- `name_age_sentence_dag`, `name_age_sentence_dag_v2` (current)
- `simple_demo_pipeline`
