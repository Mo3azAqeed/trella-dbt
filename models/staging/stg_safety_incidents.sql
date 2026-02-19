select *
from {{ source('raw_csv', 'safety_incidents') }}
