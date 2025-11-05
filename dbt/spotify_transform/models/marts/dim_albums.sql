with 
    albums as (
        select * from {{ ref('stg_spotify__tracks') }}
    ),

    final as (
        select distinct
            {{ dbt_utils.generate_surrogate_key(['album_id']) }} AS dim_album_pk
            , album_id
            , album_name
        from albums
        group by album_id, album_name
    )

select * from final
