-- KPI: Content performance
-- Question: How is content performance changing, what drives it, and
-- what action should the business take?

select
    c.content_id,
    c.genre,
    c.language,
    count(distinct fv.event_id)  as total_views,
    sum(fv.watch_seconds)        as total_watch_seconds,
    avg(fv.watch_seconds)        as avg_watch_seconds,
    count(distinct fv.user_key)  as unique_viewers,
    count(*) filter (where fv.event_type = 'complete') as completions
from {{ ref('stg_fact_view') }} fv
join {{ ref('stg_dim_content') }} c on fv.content_key = c.content_key
group by 1, 2, 3
order by total_watch_seconds desc nulls last
