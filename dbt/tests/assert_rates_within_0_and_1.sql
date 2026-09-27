-- Singular test: fails (returns rows) if any rate/ratio column falls
-- outside the valid 0-1 range. dbt treats any row returned here as a
-- failure, which is why this SELECT looks for "bad" rows, not good ones.

select full_date, avg_completion_rate from {{ ref('mart_completion_rate') }}
where avg_completion_rate < 0 or avg_completion_rate > 1

union all

select full_date, ad_fill_rate from {{ ref('mart_ad_performance') }}
where ad_fill_rate < 0 or ad_fill_rate > 1

union all

select full_date, ctr from {{ ref('mart_ad_performance') }}
where ctr < 0 or ctr > 1
