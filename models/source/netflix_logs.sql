with source as (

    select * from {{ source('abguch22', 'netflix_logs') }}

),

renamed as (

    select
        ip,
        fecha,
        cast (rowid as integer),
        show_name,
        ubicacion,
        cast (netflix_id as integer) as netflix_id,
        usuario_id, 
        dispositivo,
        tiempo_visto,
        _airbyte_ab_id as airbyte_ab_id,,
        _airbyte_emitted_at as airbyte_emitted_at,
        _airbyte_normalized_at as airbyte_normalized_at,
        _airbyte_netflix_logs_hashid as airbyte_netflix_logs_hashid

    from source

)

select * from renamed