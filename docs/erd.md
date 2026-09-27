# MediaPulse Warehouse - ERD (Star Schema)

```mermaid
erDiagram
    dim_user ||--o{ fact_view : "views"
    dim_content ||--o{ fact_view : "watched"
    dim_device ||--o{ fact_view : "on device"
    dim_geography ||--o{ fact_view : "from region"
    dim_date ||--o{ fact_view : "on date"

    dim_user ||--o{ fact_ad : "served to"
    dim_content ||--o{ fact_ad : "shown during"
    dim_campaign ||--o{ fact_ad : "part of"
    dim_geography ||--o{ fact_ad : "from region"
    dim_date ||--o{ fact_ad : "on date"

    dim_user ||--o{ fact_subscription : "holds"
    dim_date ||--o{ fact_subscription : "started on"

    dim_user ||--o{ fact_engagement : "aggregated for"
    dim_date ||--o{ fact_engagement : "on date"

    dim_user {
        int user_key PK
        string user_id UK
        string subscription_type
        string region
        string device
    }
    dim_content {
        int content_key PK
        string content_id UK
        string genre
        string language
        date release_date
        numeric duration_min
    }
    dim_device {
        int device_key PK
        string device_name UK
    }
    dim_geography {
        int geography_key PK
        string region UK
    }
    dim_campaign {
        int campaign_key PK
        string campaign_id UK
    }
    dim_date {
        int date_key PK
        date full_date UK
        int year
        int quarter
        int month
        int day_of_week
        bool is_weekend
    }
    fact_view {
        bigint view_key PK
        string event_id UK
        int user_key FK
        int content_key FK
        int device_key FK
        int geography_key FK
        int date_key FK
        timestamp event_timestamp
        string event_type
        numeric watch_seconds
    }
    fact_ad {
        bigint ad_key PK
        string ad_event_id UK
        int user_key FK
        int content_key FK
        int campaign_key FK
        int geography_key FK
        int date_key FK
        timestamp event_timestamp
        smallint impression
        smallint click
    }
    fact_subscription {
        bigint subscription_key PK
        string subscription_id UK
        int user_key FK
        int start_date_key FK
        string plan
        string status
    }
    fact_engagement {
        bigint engagement_key PK
        int user_key FK
        int date_key FK
        numeric total_watch_seconds
        int session_count
        numeric completion_rate
    }
```

## Design rationale

- **Star schema** (not snowflake): dimensions are kept flat/denormalized for
  fast BI queries, matching section 7's requirement.
- **Surrogate keys** (`xxx_key`, auto-incrementing integers) are the primary
  keys everywhere, with the original source ID (`user_id`, `content_id`, ...)
  kept as a `UNIQUE` business key. This is standard warehouse practice: it
  decouples the warehouse from source-system ID formats and is faster to
  join on than variable-length strings.
- **dim_device / dim_geography** are modeled as their own dimensions (not
  just columns on `dim_user`) so `fact_view`/`fact_ad` can be sliced by
  device or region directly. The raw data only captures device/region per
  *user* (not per individual event), so a view's device/geography reflects
  that user's profile at load time — noted as a limitation in the ML/model
  documentation.
- **fact_engagement** is intentionally left for dbt to populate: it's a
  derived daily aggregate (total watch time, session count, completion
  rate per user per day), not raw source data, so it belongs in the
  transformation layer, not the load script.
- **Referential integrity** is enforced with `FOREIGN KEY` constraints on
  every fact table — a row literally cannot be inserted if it references a
  user/content/etc. that doesn't exist in the warehouse.
