select * from {{ source('mediapulse_warehouse', 'dim_device') }}
