with source as (

    select * from {{ source('abguch22', 'catalogo_pais') }}

),

renamed as (

    select
        cast (nfid as integer) as nf_id,
        cast (rowid as integer) as row_id,
        countrycode as country_code,
        _airbyte_ab_id as airbyte_ab_id,
        FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", _airbyte_emitted_at) as airbyte_emitted_at,
        FORMAT_TIMESTAMP("%A", _airbyte_emitted_at) as dia,
        FORMAT_TIMESTAMP("%a", _airbyte_emitted_at) as dia_es, -- FALTA A ESPAÑOL
        FORMAT_TIMESTAMP("%D", _airbyte_emitted_at) as mmddyyyy,
        FORMAT_TIMESTAMP("%D", CURRENT_TIMESTAMP()) as mes_actual,
        FORMAT_TIMESTAMP("%D", TIMESTAMP_ADD(CURRENT_TIMESTAMP(), interval 30 day)) as mes_actual_mas_1,
        FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", _airbyte_normalized_at) as airbyte_normalized_at,
        _airbyte_catalogo_pais_hashid as airbyte_catalogo_pais_hashid,

    from source

)

select * from renamed