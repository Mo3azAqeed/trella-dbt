select *
from {{ source('raw_csv', 'trailers') }}
