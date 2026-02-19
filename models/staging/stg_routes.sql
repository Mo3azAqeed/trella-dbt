select *
from {{ source('raw_csv', 'routes') }}
