/*
PURPOSE: This table connects tracks to artists. It resolves the
many-to-many relationship between them.
GRAIN: One row per unique track-artist combination.
*/

with 
    unnested_data as (
        select distinct
            track_id,
            artist_id
        from {{ ref('int_track_artists_unnested') }}
    ),

    dim_tracks as (
        select
            dim_track_pk,
            track_id
        from {{ ref('dim_tracks') }}
    ),

    dim_artists as (
        select
            dim_artist_pk,
            artist_id
        from {{ ref('dim_artists') }}
    ),

    final as (
        select
            dim_tracks.dim_track_pk,
            dim_artists.dim_artist_pk
        from unnested_data
        inner join dim_tracks
            on unnested_data.track_id = dim_tracks.track_id
        inner join dim_artists
            on unnested_data.artist_id = dim_artists.artist_id
        group by 1, 2
    )

select * from final
