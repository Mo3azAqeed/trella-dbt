select *
from {{ source('raw_csv', 'customers') }}
