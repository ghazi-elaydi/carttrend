WITH raw_data AS (
    SELECT  
        COUNT(id_commande) AS total_commande, 
        SUM(CASE 
                WHEN `id_promotion_appliquée` != '' THEN 1 
                ELSE 0 -- Sinon
            END) AS avec_promotion,
        COUNT(id_commande) - SUM(CASE 
                WHEN `id_promotion_appliquée` != '' THEN 1 
                ELSE 0 -- Sinon
            END) AS sans_promotion 
    FROM {{ ref('stg_commandes_data') }} 
)
SELECT *
FROM raw_data


