select
    listing_id,
    minimum_nights
from {{ ref('dim_listings_cleansed') }}
where minimum_nights <= 0
