# Farmlytics Airflow — Hosting & CI/CD Strategy

## Overview

This document describes the plan for hosting the Farmlytics Airflow ETL platform in a production environment, including infrastructure choices, environment promotion flow, CI/CD pipeline, secrets management, and monitoring.

**Stack summary:**
- Apache Airflow 3.1.5 with CeleryExecutor
- Services: api-server, scheduler, dag-processor, worker, triggerer, PostgreSQL 16, Redis 7.2
- Custom plugin: `farmlytics_etl` (KAMIS scraper → staging → dimension/fact tables)
- Target data store: `kifaru-db` (remote PostgreSQL — `farmlytics` database)

---

## 1. Infrastructure

### Recommended Host: Cloud VM (Docker Compose)

For the current scale, a single VM running Docker Compose is the most practical choice. Managed Kubernetes (EKS, GKE) can be adopted later when worker auto-scaling is needed.

| Provider | Recommended Size | Notes |
|---|---|---|
| AWS | `t3.xlarge` (4 vCPU, 16 GB) | Use an EBS-backed volume for logs/data |
| GCP | `e2-standard-4` (4 vCPU, 16 GB) | Attach a persistent disk |
| DigitalOcean | `s-4vcpu-8gb` or larger | Simplest setup, good for early prod |
| Self-hosted VPS | 4 vCPU, 8 GB RAM minimum | Ensure outbound HTTPS to `kamis.kilimo.go.ke` |

> Airflow requires at minimum 4 GB RAM and 2 CPUs. The ETL also runs concurrent scraping threads (`MAX_WORKERS=5`), so lean toward 8–16 GB RAM.

### Persistent Storage

| Path | What to persist | How |
|---|---|---|
| `./logs` | Airflow task logs | Named Docker volume or mounted host path |
| `./data` | Input/output CSVs | Named Docker volume or cloud object storage |
| `postgres-db-volume` | Airflow metadata DB | Named Docker volume, backed up daily |

---

## 2. Environments

```
dev (local Docker Compose)
   └── staging (VM, mirrors prod config, uses test DB)
         └── production (VM, live farmlytics DB on kifaru-db)
```

- **dev** — local machine, `.env` with dev credentials, `catchup=False` override recommended.
- **staging** — identical Docker Compose setup, points to a separate staging database. Used to validate DAG changes before promotion.
- **production** — the live environment. DAG changes only land here via a passing CI/CD pipeline.

---

## 3. Repository & Branch Strategy

```
main        ← production deploys on merge
staging     ← staging deploys on merge
feature/*   ← CI lint/test only, no deploy
```

All changes go through a pull request. Direct pushes to `main` are blocked.

---

## 4. CI/CD Pipeline (GitHub Actions)

### Pipeline overview

```
Push / PR
  └── [CI] lint-and-test
         ├── Lint DAGs (ruff / flake8)
         ├── Import-check all DAGs
         └── Run unit tests (pytest)

Merge to staging
  └── [CD] deploy-staging
         └── SSH → pull → docker compose up -d

Merge to main
  └── [CD] deploy-production
         └── SSH → pull → docker compose up -d
```

### Workflow file

Create `.github/workflows/ci-cd.yml`:

```yaml
name: CI/CD

on:
  push:
    branches: [main, staging]
  pull_request:
    branches: [main, staging]

env:
  PYTHON_VERSION: "3.12"

jobs:
  # ── CI: runs on every push and PR ──────────────────────────────────────
  lint-and-test:
    name: Lint & Test
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: ${{ env.PYTHON_VERSION }}

      - name: Install dependencies
        run: |
          pip install apache-airflow==3.1.5 \
            apache-airflow-providers-postgres \
            ruff pytest

      - name: Lint with ruff
        run: ruff check dags/ plugins/

      - name: DAG import check
        run: |
          export AIRFLOW_HOME=$(mktemp -d)
          export AIRFLOW__CORE__LOAD_EXAMPLES=false
          airflow db migrate
          python -c "
          import os, sys
          from airflow.models import DagBag
          bag = DagBag(dag_folder='dags', include_examples=False)
          if bag.import_errors:
              print('DAG import errors:', bag.import_errors)
              sys.exit(1)
          print(f'OK: {len(bag.dags)} DAGs loaded')
          "

      - name: Run unit tests
        run: pytest tests/ -v --tb=short
        if: hashFiles('tests/**/*.py') != ''

  # ── CD: deploy to staging ───────────────────────────────────────────────
  deploy-staging:
    name: Deploy to Staging
    needs: lint-and-test
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/staging' && github.event_name == 'push'

    steps:
      - uses: actions/checkout@v4

      - name: Deploy via SSH
        uses: appleboy/ssh-action@v1.0.3
        with:
          host: ${{ secrets.STAGING_HOST }}
          username: ${{ secrets.STAGING_USER }}
          key: ${{ secrets.STAGING_SSH_KEY }}
          script: |
            cd /opt/farmlytics-airflow
            git pull origin staging
            docker compose pull
            docker compose up -d --remove-orphans
            docker compose exec airflow-apiserver airflow db migrate

  # ── CD: deploy to production ────────────────────────────────────────────
  deploy-production:
    name: Deploy to Production
    needs: lint-and-test
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main' && github.event_name == 'push'

    environment: production          # requires manual approval in GitHub

    steps:
      - uses: actions/checkout@v4

      - name: Deploy via SSH
        uses: appleboy/ssh-action@v1.0.3
        with:
          host: ${{ secrets.PROD_HOST }}
          username: ${{ secrets.PROD_USER }}
          key: ${{ secrets.PROD_SSH_KEY }}
          script: |
            cd /opt/farmlytics-airflow
            git pull origin main
            docker compose pull
            docker compose up -d --remove-orphans
            docker compose exec airflow-apiserver airflow db migrate
```

---

## 5. Secrets Management

### GitHub Secrets (CI/CD)

Store these in **Settings → Secrets and variables → Actions**:

| Secret | Description |
|---|---|
| `STAGING_HOST` | IP or hostname of staging VM |
| `STAGING_USER` | SSH user on staging VM |
| `STAGING_SSH_KEY` | Private key for staging SSH access |
| `PROD_HOST` | IP or hostname of production VM |
| `PROD_USER` | SSH user on production VM |
| `PROD_SSH_KEY` | Private key for production SSH access |

### On the Server — `.env` file

The `.env` file is **never committed to git** (add it to `.gitignore`). On each server, create it manually or via a secrets manager (AWS Secrets Manager / HashiCorp Vault):

```dotenv
AIRFLOW_UID=50000
AIRFLOW_IMAGE_NAME=apache/airflow:3.1.5

_AIRFLOW_WWW_USER_USERNAME=<admin-username>
_AIRFLOW_WWW_USER_PASSWORD=<strong-password>

AIRFLOW__CORE__FERNET_KEY=<generated-fernet-key>

# Farmlytics database connection (used by Airflow connection: farmlytics_postgres)
AIRFLOW_CONN_FARMLYTICS_POSTGRES=postgresql://postgres:<password>@kifaru-db:5432/farmlytics
```

Generate a Fernet key with:
```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

---

## 6. Server Setup (First-time)

```bash
# On the production/staging VM
sudo apt update && sudo apt install -y docker.io docker-compose-plugin git

sudo usermod -aG docker $USER
newgrp docker

git clone <your-repo-url> /opt/farmlytics-airflow
cd /opt/farmlytics-airflow

# Place the .env file (do not copy from dev)
nano .env

# Set correct UID
echo "AIRFLOW_UID=$(id -u)" >> .env

# Initialize and start
docker compose up airflow-init
docker compose up -d
```

---

## 7. Networking & Security

- **Firewall rules**: only ports `22` (SSH, restricted to your IP) and `8080` (Airflow UI) should be open. The UI should sit behind an HTTPS reverse proxy (Nginx + Let's Encrypt / Caddy).
- **Airflow UI**: configure `AIRFLOW__WEBSERVER__SECRET_KEY` and enforce strong passwords.
- **kifaru-db access**: Airflow workers connect to the remote DB via the connection string in `.env`. Ensure the DB allows inbound connections only from the Airflow host IP.
- **Redis**: not exposed to host (`expose:` not `ports:`). Remains internal to the Docker network.

---

## 8. Monitoring & Alerting

### Health checks (already in docker-compose.yaml)

All services have health checks defined. Monitor them with:
```bash
docker compose ps          # see health status
docker compose logs -f     # tail all logs
```

### Email / Slack alerts from Airflow

Add to `docker-compose.yaml` environment or `airflow.cfg`:
```
AIRFLOW__EMAIL__EMAIL_BACKEND: airflow.utils.email.send_email_smtp
AIRFLOW__SMTP__SMTP_HOST: smtp.gmail.com
AIRFLOW__SMTP__SMTP_USER: <your-email>
AIRFLOW__SMTP__SMTP_PASSWORD: <app-password>
```

Set `email_on_failure: True` in `default_args` of the DAG.

### Uptime monitoring

Use a free tier of [UptimeRobot](https://uptimerobot.com) or [Better Uptime](https://betterstack.com/uptime) to ping `http://<host>:8080/api/v2/version` every 5 minutes.

---

## 9. Backup Strategy

| What | How | Frequency |
|---|---|---|
| Airflow metadata DB (`postgres-db-volume`) | `pg_dump` + upload to S3/GCS | Daily |
| Archived CSVs (`./data/output`) | Sync to S3/GCS with `rclone` or `aws s3 sync` | Daily |
| `.env` file | Store encrypted in a password manager or secrets vault | On change |

---

## 10. Rollback Plan

If a production deploy fails:

```bash
# On the production VM
cd /opt/farmlytics-airflow
git log --oneline -5          # find the last good commit
git checkout <commit-hash>
docker compose up -d --remove-orphans
```

For database migrations, Airflow's `db migrate` is forward-only. Keep a daily `pg_dump` to restore if a migration breaks the metadata DB.
