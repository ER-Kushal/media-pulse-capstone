-- KPI: Subscriber growth
-- Question: How is subscriber growth changing, what drives it, and what
-- action should the business take?

with daily_subs as (
    select
        d.full_date,
        count(*)                                              as new_subscriptions,
        count(*) filter (where fs.status = 'active')          as new_active_subscriptions,
        count(*) filter (where fs.status = 'cancelled')       as new_cancelled_subscriptions
    from {{ ref('stg_fact_subscription') }} fs
    join {{ ref('stg_dim_date') }} d on fs.start_date_key = d.date_key
    group by 1
)

select
    full_date,
    new_subscriptions,
    new_active_subscriptions,
    new_cancelled_subscriptions,
    sum(new_subscriptions) over (order by full_date) as cumulative_subscriptions,
    new_subscriptions
        - lag(new_subscriptions) over (order by full_date)
        as change_vs_prev_day
from daily_subs
order by full_date
