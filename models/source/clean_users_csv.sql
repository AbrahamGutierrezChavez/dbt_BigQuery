with source as (

    select * from {{ source('abguch22', 'users_csv') }}

),

renamed as (

    select
        pais,
        `plan`,
        cast (rowid as integer) as row_id,
        correo,
        nombre,
        FORMAT_DATE("%b %Y", (PARSE_DATE("%b %Y", antiguedad))) as antiguedad,
        cast (costo_plan as integer) as costo_plan ,
        usuario_id,
        _airbyte_ab_id as airbyte_ab_id,
        FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", _airbyte_emitted_at) as airbyte_emitted_at,
        FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", _airbyte_normalized_at) as airbyte_normalized_at,
        _airbyte_users_csv_hashid as airbyte_users_csv_hashid

    from source

)

select * from renamed