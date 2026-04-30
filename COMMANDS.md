# Commands Reference

All commands are run from the project root (`airflow_docker/`) unless stated otherwise.

---

## Setup

### 1. Create the required `.env` file (Linux only — must be done once)
```bash
echo "AIRFLOW_UID=$(id -u)" > .env
```

### 2. Initialise the database and create the admin user
```bash
docker compose up airflow-init
```
Waits for PostgreSQL and Redis to be healthy, runs DB migrations, then creates the default admin user (`airflow` / `airflow`). The container exits when done.

---

## Starting the Stack

### Start all services (detached / background)
```bash
docker compose up -d
```

### Start all services and stream logs in the foreground
```bash
docker compose up
```

### Start with Celery Flower monitoring UI (port 5555)
```bash
docker compose --profile flower up -d
```

### Rebuild the image then start (if you add a custom Dockerfile)
```bash
docker compose build && docker compose up -d
```

---

## Stopping the Stack

### Stop all services (keep volumes)
```bash
docker compose down
```

### Stop all services and remove volumes (wipes Airflow metadata DB — destructive)
```bash
docker compose down --volumes
```

### Stop all services, remove volumes and images
```bash
docker compose down --volumes --rmi all
```

---

## Status & Health

### Check container status and health
```bash
docker compose ps
```

### Watch live container statuses
```bash
watch docker compose ps
```

### Check Airflow API version (confirms UI is up)
```bash
curl http://localhost:8080/api/v2/version
```

---

## Logs

### Stream logs from all services
```bash
docker compose logs -f
```

### Stream logs from a specific service
```bash
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-worker
docker compose logs -f airflow-apiserver
docker compose logs -f airflow-dag-processor
docker compose logs -f airflow-triggerer
```

### Tail the last N lines from a service
```bash
docker compose logs --tail=100 airflow-scheduler
```

---

## Airflow CLI (inside the scheduler container)

### Open a shell in the scheduler container
```bash
docker exec -it airflow_docker-airflow-scheduler-1 bash
```

### Open a shell using the debug CLI profile
```bash
docker compose --profile debug run airflow-cli bash
```

### List all DAGs
```bash
docker exec airflow_docker-airflow-scheduler-1 airflow dags list
```

### Trigger a DAG manually
```bash
docker exec airflow_docker-airflow-scheduler-1 airflow dags trigger kamis_etl_dag_v3
docker exec airflow_docker-airflow-scheduler-1 airflow dags trigger fake_store_etl_db_dag
docker exec airflow_docker-airflow-scheduler-1 airflow dags trigger simple_demo_pipeline
```

### Pause / unpause a DAG
```bash
docker exec airflow_docker-airflow-scheduler-1 airflow dags pause   kamis_etl_dag_v3
docker exec airflow_docker-airflow-scheduler-1 airflow dags unpause kamis_etl_dag_v3
```

### Backfill a DAG for a date range
```bash
docker exec airflow_docker-airflow-scheduler-1 airflow dags backfill \
    -s 2026-01-01 -e 2026-01-19 kamis_etl_dag_v3
```

### List DAG runs
```bash
docker exec airflow_docker-airflow-scheduler-1 airflow dags list-runs -d kamis_etl_dag_v3
```

### List tasks in a DAG
```bash
docker exec airflow_docker-airflow-scheduler-1 airflow tasks list kamis_etl_dag_v3
```

### Test a single task without recording a run
```bash
docker exec airflow_docker-airflow-scheduler-1 airflow tasks test kamis_etl_dag_v3 extract 2026-01-19
```

### Check scheduler health
```bash
docker exec airflow_docker-airflow-scheduler-1 \
    airflow jobs check --job-type SchedulerJob
```

---

## Connections

### Add the Farmlytics PostgreSQL connection (run inside the scheduler container)
```bash
# Step 1: connect the external Farmlytics DB container to the Airflow network
docker network connect airflow_docker_default kifaru-db

# Step 2: shell into the scheduler
docker exec -it airflow_docker-airflow-scheduler-1 bash

# Step 3: add the connection (run inside the container)
airflow connections add "farmlytics_postgres" \
    --conn-type "postgres" \
    --conn-host "kifaru-db" \
    --conn-port "5432" \
    --conn-login "postgres" \
    --conn-password "hsgahwsb23@" \
    --conn-schema "farmlytics"
```

Or run it as a one-liner from the host:
```bash
docker exec airflow_docker-airflow-scheduler-1 airflow connections add "farmlytics_postgres" \
    --conn-type "postgres" \
    --conn-host "kifaru-db" \
    --conn-port "5432" \
    --conn-login "postgres" \
    --conn-password "hsgahwsb23@" \
    --conn-schema "farmlytics"
```

### List all configured connections
```bash
docker exec airflow_docker-airflow-scheduler-1 airflow connections list
```

### Delete a connection
```bash
docker exec airflow_docker-airflow-scheduler-1 airflow connections delete farmlytics_postgres
```

---

## Users

### List users
```bash
docker exec airflow_docker-airflow-scheduler-1 airflow users list
```

### Create a new admin user
```bash
docker exec airflow_docker-airflow-scheduler-1 airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password yourpassword
```

### Reset a user password
```bash
docker exec airflow_docker-airflow-scheduler-1 airflow users reset-password \
    --username airflow \
    --use-random-password
```

---

## Scaling

### Scale the number of Celery workers
```bash
docker compose up -d --scale airflow-worker=3
```

---

## Accessing the Database Directly

### Connect to the Airflow metadata PostgreSQL DB
```bash
docker exec -it airflow_docker-postgres-1 psql -U airflow -d airflow
```

### Connect to the Farmlytics DB (from inside any Airflow container)
```bash
docker exec -it airflow_docker-airflow-scheduler-1 bash
# then inside:
# psql -h kifaru-db -U postgres -d farmlytics
```

---

## Web UIs

| Interface | URL | Default Login |
|---|---|---|
| Airflow Web UI | http://localhost:8080 | `airflow` / `airflow` |
| Celery Flower | http://localhost:5555 | — |

---

## Updating Airflow Version

1. Edit `docker-compose.yaml` — change the image tag (e.g. `apache/airflow:3.1.5` → next version)
2. Pull the new image:
   ```bash
   docker compose pull
   ```
3. Re-run init to migrate the DB:
   ```bash
   docker compose up airflow-init
   ```
4. Restart all services:
   ```bash
   docker compose up -d
   ```

---

## Installing Additional Python Packages

For quick testing only — packages are reinstalled on every container start:
```bash
# In docker-compose.yaml, under x-airflow-common environment:
_PIP_ADDITIONAL_REQUIREMENTS: "apache-airflow-providers-postgres your-package-here"
```

For production use, build a custom image (uncomment `build: .` in `docker-compose.yaml` and create a `Dockerfile`).

---

## Common Troubleshooting

### Services show "unhealthy" — check what's wrong
```bash
docker inspect airflow_docker-airflow-worker-1 | grep -A 10 '"Health"'
docker compose logs airflow-worker --tail=50
```

### DAG file has a syntax error — check parser output
```bash
docker exec airflow_docker-airflow-dag-processor-1 \
    airflow dags list-import-errors
```

### Airflow DB is out of sync after image upgrade
```bash
docker exec airflow_docker-airflow-scheduler-1 airflow db migrate
```

### Reset the entire Airflow database (destructive — loses all run history)
```bash
docker compose down --volumes
docker compose up airflow-init
docker compose up -d
```
