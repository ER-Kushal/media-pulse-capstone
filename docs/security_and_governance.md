# Security & Governance (brief section 16)

| Requirement | How it is met |
|---|---|
| Authentication | JWT bearer tokens from `POST /api/auth/login` (2 h expiry, HS256). Every `/api/*` route except login requires a valid token; `/health` is public for load balancers only. |
| Role-based authorization | Roles `admin`, `analyst`, `viewer`. Acknowledging an alert requires `admin` or `analyst` (403 for `viewer`, verified by tests). The UI mirrors this (button disabled + notice). |
| No credentials in Git | All secrets come from environment variables (`PG*`, `DATABASE_URL`, `JWT_SECRET`, `*_PASSWORD`, `SLACK_WEBHOOK_URL`). `.env` is git-ignored; `.env.example` holds placeholders. The only literals are documented **local-dev defaults** and the throw-away CI database password. |
| Least privilege | API container runs as a non-root user; the API only needs SELECT on marts/facts plus UPDATE on `alerts` (recommended: a dedicated DB role with exactly those grants in production); CI database is disposable. |
| Data masking / synthetic data | The entire dataset is synthetic (generated), so no real personal data exists. User identifiers are opaque IDs (`U00010`); no names, emails or payment data are stored. |
| Audit trail for automated actions | Every alert has an append-only JSON `audit_log` (created, notification sent/failed, acknowledged-by-whom-when). Airflow task outcomes go to `pipeline_run_log`. API logs every request with status, latency and client IP. |
| Input validation | Pydantic/FastAPI validation on every parameter: ID regex `^[A-Za-z0-9_-]{1,30}$`, bounded `limit/offset/minutes`, enumerated filters (`tier`, `status`, `severity`), max-length note. All SQL uses bound parameters (no string-built queries with user input) -> no SQL injection. |
| API rate / abuse | In-memory sliding-window limiter: 300 requests/min per IP overall, 10 login attempts/min per IP (brute-force protection); failed logins are logged. For production put a gateway/Redis limiter in front. |

## Known limitations (state these honestly)
* Demo users are defined in code with env-overridable passwords - production would use an identity provider (OIDC) and a users table.
* The rate limiter is per-process memory (fine for one container).
* Tokens are held in `sessionStorage`; production should prefer httpOnly cookies + CSRF protection.
