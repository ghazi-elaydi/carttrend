WITH raw_data AS(
SELECT  COUNT(id_commande) AS total_commande,date_commande,
    CASE 
         WHEN `id_promotion_appliquée` != '' THEN 'Avec promotion'
         ELSE 'Sans promotion'
    END AS type_promotion,
FROM {{ ref('stg_commandes_data') }}
GROUP BY  date_commande,
    CASE 
        WHEN `id_promotion_appliquée` != '' THEN 'Avec promotion'
         ELSE 'Sans promotion'
     END

ORDER BY total_commande DESC
)
SELECT *
FROM raw_data