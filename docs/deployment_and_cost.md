# Deployment, DevOps & Cost (brief section 15)

## What is deployed
One Docker image (FastAPI + React dashboard) on **Render** (free web service) talking to a managed **Neon** Postgres (free). Everything else (Kafka, Airflow, pgAdmin) runs locally through Docker Compose for development and demo.

```
GitHub push -> GitHub Actions CI (full pipeline + 20 API tests + docker build)
            -> CD: image pushed to GHCR, Render redeploys -> https://<your-app>.onrender.com
```

## Deployment steps (summary - click-by-click version is in FINISH_GUIDE.md)
1. Create a free Neon project; copy the connection details.
2. Load the warehouse in Neon from your laptop: set `PGHOST/PGUSER/PGPASSWORD/PGDATABASE` and `PGSSLMODE=require`, run `bash scripts/run_all.sh full`.
3. Push the repo to GitHub (CI runs automatically).
4. On Render: *New -> Blueprint*, pick the repo (`render.yaml` is picked up), set `DATABASE_URL` (Neon string), `ADMIN_PASSWORD`, `ANALYST_PASSWORD`, `VIEWER_PASSWORD`.
5. Open the Render URL, sign in, verify `/health` and `/docs`.

## Docker / repeatable environments
* `Dockerfile` - API + dashboard, non-root user, health check.
* `docker-compose.yml` - postgres (with healthcheck), api, kafka, kafka-ui, pgadmin, and Airflow behind the `airflow` profile.
* `airflow/Dockerfile` - Airflow with the project's dependencies isolated in a separate virtualenv.

## CI/CD
* **CI** (`.github/workflows/ci.yml`): Postgres service container -> install -> compile check -> `run_all.sh full` (clean, load, dbt run + 13 tests, ML, alerts) -> pytest (20 API tests) -> `docker build`.
* **CD** (`.github/workflows/deploy.yml`): only after CI succeeds on `main`: build and push image to GHCR, optional Render deploy hook.
* **Secrets:** environment variables / GitHub & Render secret stores; nothing sensitive committed.

## Monitoring & logs
* `GET /health` (DB connectivity) used as the platform health check; Docker `HEALTHCHECK`.
* API request log (`api/logs/api.log` locally, stdout in the cloud -> visible in Render's Logs tab).
* `GET /api/data-quality`, dashboard *Data Quality* page, Airflow UI, Kafka UI, `pipeline_run_log`.

## Estimated monthly cost
| Setup | Components | Estimate |
|---|---|---|
| **This project (free tier)** | Render free web service, Neon free Postgres (0.5 GB is plenty for ~30 k rows), GitHub Actions free minutes, local Docker for Kafka/Airflow | **USD 0** (limits: Render free instances sleep after ~15 min idle and take ~30-60 s to wake; Neon free has storage/compute caps) |
| Small always-on production | Managed Postgres (~USD 15-30), container hosting for API (~USD 7-25), small VM for Airflow (~USD 10-20), managed Kafka basic tier (~USD 0-50 depending on provider), monitoring/logging (~USD 0-15) | **roughly USD 40-140 / month** |

_Production figures are rough planning estimates - confirm with the provider's pricing calculator before quoting them as facts._

## Cost-reduction ideas (interview Q)
Right-size and stop dev environments off-hours; use Neon/serverless Postgres that scales to zero; batch small Kafka messages (`linger_ms`, compression) and keep short topic retention; partition and prune warehouse tables by date so queries scan less; pre-aggregate in dbt marts instead of scanning facts for every dashboard call; cache hot API endpoints; use spot/preemptible workers for batch jobs.
