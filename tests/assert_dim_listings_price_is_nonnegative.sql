select
    listing_id,
    price
from {{ ref('dim_listings_cleansed') }}
where price < 0
