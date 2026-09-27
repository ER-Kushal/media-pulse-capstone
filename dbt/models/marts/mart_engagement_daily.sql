-- Daily per-user engagement aggregate.
-- This is the dbt-built equivalent of the fact_engagement table sketched
-- in the warehouse schema: it's a DERIVED aggregate (not raw source data),
-- so it's built here in the transformation layer rather than by the
-- Python loader. Segmentation analysis (section 8) and the churn-risk /
-- engagement-drop automation rules (section 10) both read from this.

select
    fv.user_key,
    d.date_key,
    d.full_date,
    sum(coalesce(fv.watch_seconds, 0))                             as total_watch_seconds,
    count(*)                                                        as session_count,
    avg(
        case when c.duration_min > 0
             then least(fv.watch_seconds / (c.duration_min * 60.0), 1.0)
        end
    )                                                                as completion_rate
from {{ ref('stg_fact_view') }} fv
join {{ ref('stg_dim_date') }} d    on fv.date_key = d.date_key
join {{ ref('stg_dim_content') }} c on fv.content_key = c.content_key
group by 1, 2, 3
