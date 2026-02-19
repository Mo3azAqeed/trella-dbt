select *
from {{ source('raw_csv', 'trips') }}
