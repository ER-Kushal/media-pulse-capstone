-- KPI: Retention
-- Question: How is retention changing, what drives it, and what action
-- should the business take?
-- Definition: day-over-day retention = users active yesterday who are
-- also active today, divided by yesterday's active users.

with daily_users as (
    select distinct
        d.full_date,
        fv.user_key
    from {{ ref('stg_fact_view') }} fv
    join {{ ref('stg_dim_date') }} d on fv.date_key = d.date_key
),

dau as (
    select full_date, count(distinct user_key) as active_users
    from daily_users
    group by 1
),

retained as (
    select
        today.full_date,
        count(distinct today.user_key) as retained_users
    from daily_users today
    inner join daily_users yesterday
        on today.user_key = yesterday.user_key
        and today.full_date = (yesterday.full_date + interval '1 day')::date
    group by 1
)

select
    dau.full_date,
    dau.active_users,
    coalesce(retained.retained_users, 0) as retained_from_prev_day,
    round(
        coalesce(retained.retained_users, 0)::numeric
        / nullif(lag(dau.active_users) over (order by dau.full_date), 0),
        4
    ) as day_over_day_retention_rate
from dau
left join retained on dau.full_date = retained.full_date
order by dau.full_date
