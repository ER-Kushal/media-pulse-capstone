-- KPI: Completion rate
-- Question: How is completion rate changing, what drives it, and what
-- action should the business take?
-- Definition: watch_seconds / (content duration in seconds), capped at 1.0.
-- Grain: one row per day x genre.

with joined as (
    select
        d.full_date,
        c.genre,
        fv.watch_seconds,
        c.duration_min
    from {{ ref('stg_fact_view') }} fv
    join {{ ref('stg_dim_date') }} d    on fv.date_key = d.date_key
    join {{ ref('stg_dim_content') }} c on fv.content_key = c.content_key
    where fv.watch_seconds is not null
      and c.duration_min is not null
      and c.duration_min > 0
),

rated as (
    select
        *,
        least(watch_seconds / (duration_min * 60.0), 1.0) as completion_ratio
    from joined
)

select
    full_date,
    genre,
    avg(completion_ratio) as avg_completion_rate,
    count(*)              as sample_size,
    avg(completion_ratio)
        - lag(avg(completion_ratio)) over (partition by genre order by full_date)
        as change_vs_prev_day
from rated
group by 1, 2
