/*
PURPOSE: This is the central fact table for the Spotify model.
It contains all the numeric, measurable audio features for each track.
GRAIN: One row per unique track.
*/

with 
    stg_tracks as (
        select distinct
            track_id,
            album_id,
            duration_ms,
            danceability,
            energy,
            musical_key,
            loudness,
            mode,
            speechiness,
            instrumentalness,
            liveness,
            valence,
            tempo
        from {{ ref('stg_spotify__tracks') }}
    ),

    dim_tracks as (
        select
            dim_track_pk,
            track_id
        from {{ ref('dim_tracks') }}

    ),

    dim_albums as (
        select
            dim_album_pk,
            album_id
        from {{ ref('dim_albums') }}
    ),

    final as (
        select
            dim_tracks.dim_track_pk,
            dim_albums.dim_album_pk,
            stg_tracks.duration_ms,
            stg_tracks.danceability,
            stg_tracks.energy,
            stg_tracks.musical_key,
            stg_tracks.loudness,
            stg_tracks.mode,
            stg_tracks.speechiness,
            stg_tracks.instrumentalness,
            stg_tracks.liveness,
            stg_tracks.valence,
            stg_tracks.tempo
        from stg_tracks
        left join dim_tracks
            on stg_tracks.track_id = dim_tracks.track_id
        left join dim_albums
            on stg_tracks.album_id = dim_albums.album_id
    )

select * from final