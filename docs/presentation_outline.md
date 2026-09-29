# Final presentation outline (10-12 minutes, 11 slides)

1. **Title** - MediaPulse: media, streaming & audience intelligence. Your name, ID, mentor, date.
2. **Business problem** - what audiences watch now, where engagement drops, what drives subscriptions and revenue (brief section 2).
3. **Solution overview** - one-line pitch + architecture diagram (`docs/architecture.md`).
4. **Data & engineering** - 6 sources, Bronze -> Silver (list the defects found and fixed), star schema (ERD `docs/erd.md`).
5. **Streaming** - Kafka producer -> topics -> consumer -> warehouse; retry, duplicate protection, dead-letter, monitoring. Screenshot of Kafka UI.
6. **Analytics layer** - dbt: 7 marts covering all 8 KPIs, 13 tests; one example KPI story (what changed / what drives it / what action).
7. **Intelligence** - churn, anomaly detection, forecast; show metrics and the honest limitation (AUC 0.53).
8. **Automation** - 4 rules, Slack notification, alert with audit log; acknowledge live.
9. **Application** - API (12 endpoints, JWT, RBAC, OpenAPI) + dashboard screenshots (5 pages).
10. **DevOps & cloud** - Docker, CI/CD, Airflow DAG + run log, deployed URL, cost ($0 free tier; production estimate).
11. **Results, lessons & next steps** - what worked, bugs the quality checks caught, what you'd do at 10x scale.

**Live demo order (3-4 min):** start consumer + producer -> Kafka UI -> pgAdmin row -> `run_all.sh refresh` -> dashboard -> alert acknowledge -> audit log. Keep a recorded backup video in case Wi-Fi fails.
