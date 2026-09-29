# Interview preparation - answers grounded in *this* project

**30-second pitch (from the brief):** "I built MediaPulse, an end-to-end media, streaming & audience intelligence platform. It ingests live events, stores governed data in a warehouse, creates business analytics, applies intelligence to predict/detect risk, automatically triggers actions, exposes APIs, and runs as a cloud application."

**Numbers to remember:** 6 sources / ~34 k raw rows -> 6 cleaned tables; star schema with 6 dimensions + 4 facts; 16 dbt models, 13 dbt tests, 20 API tests, 9-task Airflow DAG; 12 required API endpoints; full rebuild in ~20 seconds.

**How do you process millions of events per minute?**
My demo runs ~3 events/s, but the design scales: Kafka topics are keyed by event id and can be split into many partitions; run several consumers in one consumer group (one per partition); batch inserts (accumulate N messages, one multi-row insert, commit offset once) instead of one insert per message; move heavy transforms to Spark structured streaming and land data in a partitioned columnar warehouse (BigQuery/Snowflake) rather than row-by-row into Postgres; keep the consumer idempotent so retries are safe.

**How do you sessionize viewing?**
Order a user's events by timestamp and start a new session when the gap to the previous event exceeds a threshold (typically 30 min) or the content changes: `SUM(CASE WHEN gap > 30 min THEN 1 ELSE 0 END) OVER (PARTITION BY user ORDER BY ts)` gives a session number. In this project `session_count` in `mart_engagement_daily` counts viewing events per user-day - a simplification I'd replace with the gap-based method on real play/pause/heartbeat events.

**How do you calculate completion rate?**
Per view: `min(watch_seconds / (duration_min x 60), 1)` (capped, since replays/seeks can exceed the runtime), then averaged - see `mart_completion_rate`. Rows with unknown watch seconds or duration are excluded (not imputed) so the KPI isn't biased. Alternative definition: share of views with a `complete` event (`completions` in `mart_content_performance`).

**How would you optimise streaming cost?**
Batch and compress producer messages, short topic retention, right-sized partitions, compact topics where only the latest value matters, consumer autoscaling on lag, land raw events in cheap object storage and only aggregate what dashboards need, avoid over-provisioned always-on clusters (serverless tiers, scale to zero).

**How do you avoid recommendation bias?**
Popularity feedback loops favour already-popular titles. Normalise by exposure (completion rate not raw views), cap exposure share, reserve an exploration slice, monitor share by genre/language/new-release, and evaluate with offline counterfactual metrics plus A/B tests. My signals table (`mart_content_performance` + surge detection) deliberately includes completion, not just views.

**Why did you choose this warehouse schema?**
A star schema: facts at event grain (`fact_view`, `fact_ad`, `fact_subscription`) joined to conformed dimensions (user, content, device, geography, campaign, date). One join per dimension keeps BI queries simple and fast; surrogate keys decouple the warehouse from source IDs; business keys are UNIQUE for idempotent loads; FKs enforce integrity; audit columns give lineage. Postgres stands in for BigQuery/Snowflake at zero cost.

**How do you prevent duplicate processing?**
Three layers: (1) Silver cleaning drops duplicate business keys; (2) the warehouse enforces `UNIQUE(event_id)` and every load uses `INSERT ... ON CONFLICT DO NOTHING`; (3) the Kafka consumer commits offsets only after a successful DB write (at-least-once) so a crash re-reads at worst - and the idempotent insert turns that into effectively-once. The alert engine de-duplicates by (type, entity, reason). I tested re-running everything: 0 new rows, 0 new alerts.

**How do you handle late or out-of-order events?**
Key on event time, not arrival time (`event_timestamp`), so a late event lands in the right day. Because loads are idempotent upserts, late data can simply be inserted and the dbt marts recomputed (mine rebuild fully; at scale use incremental models with a look-back window, e.g. re-process the last 2-3 days). In streaming engines add watermarks + allowed lateness. My anomaly detector needs a settled day, so it should run after the look-back window closes.

**How would you scale it 10x?**
Partition Kafka and add consumers; batch writes; partition fact tables by date and cluster by user/content; move to a columnar cloud warehouse; make dbt models incremental; add Redis caching for hot API endpoints and run several API replicas behind a load balancer; put a gateway rate limiter in front; move ML training to scheduled jobs writing to a feature/score table.

**How would you reduce cloud cost?**
See `deployment_and_cost.md`: serverless/scale-to-zero DB and app tier, off-hours shutdown of dev, date-partition pruning, pre-aggregated marts, caching, batching/compression in Kafka, spot instances for batch jobs.

**How do you test the pipeline end to end?**
CI creates an empty Postgres and runs the whole chain (`run_all.sh full`): cleaning -> load -> dbt run + tests -> ML -> alerts, then 20 API tests, then a Docker build. I also verified idempotency (re-run adds nothing), Airflow execution (`airflow dags test`), and rendered every dashboard page against the live API in a headless test. Data-quality tests are part of the pipeline, not an afterthought - they even caught a real bug (null impression on clicked ads).

**What would you monitor in production?**
Freshness (newest event age), volume (rows per hour vs baseline), Kafka consumer lag and dead-letter count, pipeline task success/failure/duration, dbt test results, API latency/error rate (p95, 5xx), DB connections/CPU/storage, alert volume (a spike means a broken rule), model drift (score distribution, AUC when labels arrive), and cloud spend.

## Questions you should be ready for about weaknesses (answer honestly)
* *"Your churn model is at chance."* Yes - AUC 0.53. The dataset is synthetic and status is unrelated to behaviour; the point is the working pipeline and honest evaluation. With real data I'd improve features, validate out-of-time and choose a precision-constrained threshold.
* *"CPM is an estimate."* The source data has no revenue; I assumed $2 CPM, labelled it everywhere, and it is one config value.
* *"Why Postgres, not Snowflake?"* Cost - the modelling, dbt and SQL are identical; only the adapter changes.
* *"Is it really real-time?"* Sub-second ingestion into the warehouse; the dashboard refreshes every 15 s; dbt marts refresh on the DAG schedule (a production version would use incremental models run every few minutes).
