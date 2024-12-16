WITH  mentions_periode AS (
    SELECT
date_post AS date,
canal_social,
SUM(volume_mentions) AS total_mentions
FROM {{ref('stg_posts_data') }}
GROUP BY canal_social, date
ORDER BY date DESC
)
SELECT * 
FROM mentions_periode