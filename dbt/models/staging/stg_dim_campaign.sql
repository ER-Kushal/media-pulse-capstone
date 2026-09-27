select * from {{ source('mediapulse_warehouse', 'dim_campaign') }}
