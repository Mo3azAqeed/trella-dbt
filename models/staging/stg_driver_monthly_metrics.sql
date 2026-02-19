{{ config(enabled=false) }}
select *
from {{ source('raw_csv', 'driver_monthly_metrics') }}
