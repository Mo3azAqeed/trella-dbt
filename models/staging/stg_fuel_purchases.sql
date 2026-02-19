select *
from {{ source('raw_csv', 'fuel_purchases') }}
