with source as (

    select * from {{ source('abguch22', 'ip_location') }}

),

renamed as (

    select
        end,
        rowid,
        start,
        countrycode,
        countryname,
        _airbyte_ab_id,
        _airbyte_emitted_at,
        _airbyte_normalized_at,
        _airbyte_ip_location_hashid

    from source

)

select * from renamed