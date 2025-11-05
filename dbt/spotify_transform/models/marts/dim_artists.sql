with 
    unnested_artists as (
        select * from {{ ref('int_track_artists_unnested') }}
    ),

    cleaned_artists as (
        select
            artist_id
            , trim(lower(artist_name)) as artist_name_clean
        from unnested_artists
        where artist_id is not null
    ),

    unique_artists as (
        select distinct
            artist_id
            , artist_name_clean as artist_name
        from cleaned_artists
        group by 1, 2
    ),

    final as (
        select
            {{ dbt_utils.generate_surrogate_key(['artist_id']) }} as dim_artist_pk
            , artist_id
            , min(artist_name) as artist_name
        from unique_artists
        group by artist_id
    )

select * from final
