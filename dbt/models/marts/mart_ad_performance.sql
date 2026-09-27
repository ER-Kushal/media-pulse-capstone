-- KPIs: Ad fill, CTR, CPM
-- Questions: How is ad fill/CTR/CPM changing, what drives it, and what
-- action should the business take?
--
-- ASSUMPTION: the raw dataset has no revenue/cost field, so CPM here is
-- an ESTIMATE using an assumed $2.00 per 1000 impressions. Replace
-- assumed_cpm_usd below with real rate-card data if/when it's available -
-- document this assumption in your interview pitch and data dictionary.

{% set assumed_cpm_usd = 2.00 %}

with daily as (
    select
        d.full_date,
        camp.campaign_id,
        count(*)          as ad_opportunities,
        sum(fa.impression) as impressions,
        sum(fa.click)       as clicks
    from {{ ref('stg_fact_ad') }} fa
    join {{ ref('stg_dim_date') }} d       on fa.date_key = d.date_key
    join {{ ref('stg_dim_campaign') }} camp on fa.campaign_key = camp.campaign_key
    group by 1, 2
)

select
    full_date,
    campaign_id,
    ad_opportunities,
    impressions,
    clicks,
    round(impressions::numeric / nullif(ad_opportunities, 0), 4) as ad_fill_rate,
    round(clicks::numeric / nullif(impressions, 0), 4)           as ctr,
    round((impressions / 1000.0) * {{ assumed_cpm_usd }}, 2)     as estimated_revenue_usd,
    {{ assumed_cpm_usd }}                                        as assumed_cpm_usd
from daily
order by full_date, campaign_id
