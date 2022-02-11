with source as (

    select * from {{ source('abguch22', 'netflix_catalogo') }}

),

renamed as (

    select
        nfid,
        year,
        rowid,
        title,
        vtype,
        imdbid,
        poster,
        top250,
        runtime,
        synopsis,
        top250tv,
        avgrating,
        titledate,
        imdbrating,
        countrycode,
        _airbyte_ab_id,
        _airbyte_emitted_at,
        _airbyte_normalized_at,
        _airbyte_netflix_catalogo_hashid

    from source

)