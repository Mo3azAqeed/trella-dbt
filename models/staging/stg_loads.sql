select *
from {{ source('raw_csv', 'loads') }}
