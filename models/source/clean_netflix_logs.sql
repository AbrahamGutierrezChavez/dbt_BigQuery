with source as (

    select * from {{ source('abguch22', 'netflix_logs') }}

),

renamed as (

    select
        `ip`,
        --PARSE_TIMESTAMP("%x", replace(fecha," ","T") as fecha,
        --2022-01-29 10:57:53.291548
        -- split(fecha," ")[OFFSET(0)] as fecha,
        --PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S.%E<6>S" , fecha) as fecha,
        fecha,
        cast (rowid as integer) as row_id,
        show_name,
        ubicacion,
        cast (netflix_id as integer) as netflix_id,
        usuario_id,
        dispositivo,
        cast (tiempo_visto as integer) as tiempo_visto,
        _airbyte_ab_id as airbyte_ab_id,
        FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", _airbyte_emitted_at) as airbyte_emitted_at,
        FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", _airbyte_normalized_at) as airbyte_normalized_at,
        _airbyte_netflix_logs_hashid as airbyte_netflix_logs_hashid

    from source

)

select * from renamed