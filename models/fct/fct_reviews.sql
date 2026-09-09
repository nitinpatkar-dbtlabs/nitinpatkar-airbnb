{{
  config(
    materialized = 'incremental',
    unique_key = 'review_id',
    incremental_strategy = 'merge',
    on_schema_change = 'fail'
  )
}}

with source_reviews as (

    select
        listing_id,
        review_date,
        reviewer_name,
        review_text,
        review_sentiment
    from {{ ref('src_reviews') }}
    where review_text is not null

    {% if is_incremental() %}
        and review_date >= dateadd(
            day,
            -3,
            (select coalesce(max(review_date), '1900-01-01'::date) from {{ this }})
        )
    {% endif %}

),

listings as (

    select
        listing_id,
        host_id
    from {{ ref('dim_listings_cleansed') }}

),

reviews_enriched as (

    select
        md5(
            concat_ws(
                '||',
                coalesce(to_varchar(reviews.listing_id), ''),
                coalesce(to_varchar(reviews.review_date), ''),
                coalesce(reviews.reviewer_name, ''),
                coalesce(reviews.review_text, '')
            )
        ) as review_id,
        reviews.listing_id,
        listings.host_id,
        reviews.review_date,
        date_trunc('month', reviews.review_date)::date as review_month,
        reviews.reviewer_name,
        reviews.review_text,
        reviews.review_sentiment,
        case reviews.review_sentiment
            when 'positive' then 1
            when 'neutral' then 0
            when 'negative' then -1
        end as sentiment_score,
        length(reviews.review_text) as review_character_count,
        iff(
            trim(reviews.review_text) = '',
            0,
            array_size(strtok_to_array(trim(reviews.review_text), ' '))
        ) as review_word_count,
        reviews.review_text ilike '%automated posting%' as is_automated_posting,
        (
            reviews.review_text ilike '%host canceled this reservation%'
            or reviews.review_text ilike '%host cancelled this reservation%'
        ) as is_host_cancellation,
        1 as review_count
    from source_reviews as reviews
    left join listings
        on reviews.listing_id = listings.listing_id

)

select *
from reviews_enriched
