select *
from {{ source('raw_csv', 'trucks') }}
