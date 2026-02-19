select *
from {{ source('raw_csv', 'truck_utilization_metrics') }}
