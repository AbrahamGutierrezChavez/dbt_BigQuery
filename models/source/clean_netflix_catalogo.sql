with source as (

    select * from {{ source('abguch22', 'netflix_catalogo') }}

),

renamed as (

    select
        cast (nfid as integer) as nf_id,
        cast (`year` as integer) as `year`,
        cast (rowid as integer) as row_id,
        title,
        lower(vtype) as content_type,
        imdbid as IMBd_id,
        poster,
        top250 as top_250,
        cast (runtime as integer) as run_time,
        synopsis,
        cast( top250tv as integer) as top_250_tv,
        cast (avgrating as integer) as avg_rating,
        titledate as title_date,
        imdbrating as imdb_rating,
        countrycode as country_code,
        _airbyte_ab_id as airbyte_ab_id,
        FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", _airbyte_emitted_at) as airbyte_emitted_at,
        FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", _airbyte_normalized_at) as airbyte_normalized_at,
        _airbyte_netflix_catalogo_hashid as airbyte_netflix_catalogo_hashid

    from source

)

select * from renamed