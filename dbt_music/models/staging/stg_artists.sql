select
  artist_id,
  artist_name,
  genre
from {{ source('raw','artists') }}
