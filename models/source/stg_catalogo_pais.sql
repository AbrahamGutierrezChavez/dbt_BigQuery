with source as (

    select * from {{ source('abguch22', 'catalogo_pais') }}

),

renamed as (

    select
        nfid,
        rowid,
        countrycode,
        _airbyte_ab_id,
        _airbyte_emitted_at,
        _airbyte_normalized_at,
        _airbyte_catalogo_pais_hashid

    from source

)

select * from renamed