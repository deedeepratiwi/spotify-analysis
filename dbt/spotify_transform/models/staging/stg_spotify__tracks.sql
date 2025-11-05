with
    raw_data as (
        select * 
        from {{ source('spotify', 'spotify') }}
    ),

    renamed as (
        select
            id as track_id
            , name as track_name
            , album as album_name
            , album_id
            , artists as artists_name
            , artist_ids
            , track_number
            , disc_number
            , explicit as explicit_content
            , danceability
            , energy
            , key as musical_key
            , loudness
            , mode
            , speechiness
            , instrumentalness
            , liveness
            , valence
            , tempo
            , duration_ms
            , time_signature
            , year as release_year
            , release_date
        from raw_data
    )

select * from renamed
