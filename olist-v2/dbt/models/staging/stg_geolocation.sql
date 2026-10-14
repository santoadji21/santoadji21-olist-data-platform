select * from {{ source('raw', 'geolocation') }}
