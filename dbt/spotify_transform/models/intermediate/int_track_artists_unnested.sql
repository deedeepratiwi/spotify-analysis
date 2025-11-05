with
    base as (
        select
            track_id,
            string_to_array(artist_ids, ',') as artist_id_array,
            string_to_array(artists_name, ',') as artist_name_array
        from {{ ref('stg_spotify__tracks') }}
    ),

    unnested as (
        select
            b.track_id,
            trim(lower(replace(replace(replace(aid.value, '[', ''), ']', ''), '''', ''))) as artist_id,
            trim(lower(replace(replace(replace(aname.value, '[', ''), ']', ''), '''', ''))) as artist_name
        from base b
        join unnest(b.artist_id_array) with ordinality as aid(value, idx)
        on true
        join unnest(b.artist_name_array) with ordinality as aname(value, idx)
        on aid.idx = aname.idx
    )

select distinct *
from unnested
where artist_id is not null 
    and artist_name <> ''