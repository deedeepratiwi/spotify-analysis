with 
    tracks as (
        select * from {{ ref('stg_spotify__tracks') }}
    ),

    final as (
        select
            {{ dbt_utils.generate_surrogate_key(['track_id']) }} AS dim_track_pk
            , track_id
            , track_name
            , track_number
            , disc_number
            , explicit_content
            , duration_ms
            , time_signature
            , release_year
            , release_date
        from tracks
    )

select * from final
