WITH
    raw_data AS (
        SELECT * 
        FROM public.spotify
    ),

    renamed AS (
        SELECT
            id AS track_id
            , name AS track_name
            , album AS album_name
            , album_id
            , artists AS artists_name
            , artist_ids
            , track_number
            , disc_number
            , explicit AS explicit_content
            , danceability
            , energy
            , key AS musical_key
            , loudness
            , mode
            , speechiness
            , instrumentalness
            , liveness
            , valence
            , tempo
            , duration_ms
            , time_signature
            , year AS release_year
            , release_date
        FROM raw_data
    )

SELECT *
FROM raw_data
