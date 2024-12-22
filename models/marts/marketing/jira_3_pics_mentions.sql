WITH mentions_periode AS (
    SELECT
        date_post AS date,
        canal_social,
        SUM(volume_mentions) AS total_mentions,
        EXTRACT(YEAR FROM date_post) AS annee,   
        EXTRACT(MONTH FROM date_post) AS mois     
    FROM {{ ref('stg_posts_data') }}
    GROUP BY canal_social, date_post
    ORDER BY date_post DESC
)

SELECT * 
FROM mentions_periode
