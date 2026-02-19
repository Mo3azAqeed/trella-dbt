select *
from {{ source('raw_csv', 'maintenance_records') }}
