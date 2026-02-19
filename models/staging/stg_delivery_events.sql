select *
from {{ source('raw_csv', 'delivery_events') }}
