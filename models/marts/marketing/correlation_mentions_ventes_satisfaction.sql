WITH posts AS (
    -- Sélectionner les informations des posts sociaux
    SELECT
        date_post,
        canal_social,
        SUM(volume_mentions) AS total_mentions,
        sentiment_global
    FROM {{ ref('stg_posts_data') }}
    GROUP BY
        date_post,
        canal_social,
        sentiment_global
),
ventes AS (
    -- Sélectionner les informations des commandes et des ventes
    SELECT
        c.date_commande AS date_post,  -- Nous utilisons ici la date de la commande comme date de post
        COUNT(DISTINCT c.id_commande) AS total_commandes,
        SUM(dc.`quantité`) AS total_ventes
    FROM {{ ref('stg_commandes_data') }} c
    JOIN {{ ref('stg_details_commandes_data') }} dc
        ON c.id_commande = dc.id_commande
    GROUP BY c.date_commande
)
SELECT
    p.date_post,
    p.canal_social,
    p.total_mentions,
    v.total_commandes,
    v.total_ventes,
    p.sentiment_global
FROM posts p
LEFT JOIN ventes v
    ON p.date_post = v.date_post  
ORDER BY p.date_post
