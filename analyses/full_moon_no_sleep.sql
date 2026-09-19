WITH fullmoon_reviews AS (
    SELECT * FROM {{ ref('mart_fullmoon_reviews')}}
)

SELECT 
    is_full_moon,
    review_sentiment,
    COUNT(*) AS reviews
FROM fullmoon_reviews
GROUP BY is_full_moon, review_sentiment
ORDER BY is_full_moon, review_sentiment