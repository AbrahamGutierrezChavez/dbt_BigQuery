with source as (

    select * from {{ source('abguch22', 'ip_location') }}

),

renamed as (

    select
        cast (`end` as integer) as end_ip_range, 
        cast (rowid as integer) as row_id,
        cast (`start` as integer) as start_ip_range,
        replace(countrycode,"-","") as country_code,
        countryname,
        _airbyte_ab_id as airbyte_ab_id,
        FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S",_airbyte_emitted_at) as airbyte_emitted_at,
        FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", _airbyte_normalized_at) as airbyte_normalized_at,
        _airbyte_ip_location_hashid
        

    from source

)

select * from renamed