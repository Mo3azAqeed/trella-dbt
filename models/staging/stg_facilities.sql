select *
from {{ source('raw_csv', 'facilities') }}
