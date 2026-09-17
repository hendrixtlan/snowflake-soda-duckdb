-- A song must always carry the same artist and genre. LEFT JOIN so that an
-- orphan song_id is a failure here too, not a row that quietly disappears.
select e.song_id
from {{ ref('stg_listening_events') }} e
left join {{ ref('stg_songs') }} s on e.song_id = s.song_id
where s.song_id is null
   or e.artist_id <> s.artist_id
   or e.genre <> s.genre
