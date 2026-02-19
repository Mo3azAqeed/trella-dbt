select *
from {{ source('raw_csv', 'drivers') }}
