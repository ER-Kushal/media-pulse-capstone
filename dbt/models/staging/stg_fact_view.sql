select * from {{ source('mediapulse_warehouse', 'fact_view') }}
