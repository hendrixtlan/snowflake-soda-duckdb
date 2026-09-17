select
  song_id,
  song_name,
  artist_id,
  genre,
  duration_ms
from {{ source('raw','songs') }}
