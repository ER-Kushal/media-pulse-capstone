-- KPI: Watch time
-- Question: How is watch time changing, what drives it, and what action
-- should the business take?
-- Grain: one row per day x genre x device.

with daily as (
    select
        d.full_date,
        c.genre,
        dv.device_name,
        sum(fv.watch_seconds)  as total_watch_seconds,
        count(*)               as view_count,
        avg(fv.watch_seconds)  as avg_watch_seconds
    from {{ ref('stg_fact_view') }} fv
    join {{ ref('stg_dim_date') }} d     on fv.date_key = d.date_key
    join {{ ref('stg_dim_content') }} c  on fv.content_key = c.content_key
    join {{ ref('stg_dim_device') }} dv  on fv.device_key = dv.device_key
    where fv.watch_seconds is not null
    group by 1, 2, 3
)

select
    full_date,
    genre,
    device_name,
    total_watch_seconds,
    view_count,
    avg_watch_seconds,
    total_watch_seconds
        - lag(total_watch_seconds) over (partition by genre, device_name order by full_date)
        as change_vs_prev_day
from daily
