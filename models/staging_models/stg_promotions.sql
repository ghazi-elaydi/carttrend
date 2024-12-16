WITH raw_data AS (
    -- Nettoyage des données de la table Promotions
    SELECT 
        id_promotion,
        id_produit,
        type_promotion,
        valeur_promotion, 
        -- Nettoyage et conversion de la valeur de la promotion
        SAFE_CAST(REPLACE(REPLACE(valeur_promotion, ',', '.'), '%', '') AS FLOAT64) AS valeur_promotion_float,
        `date_début`,
        date_fin,
        responsable_promotion
    FROM {{ source('data_carttrend', 'Carttrend_Promotions') }}
)
-- Sélection des données nettoyées, sans erreur de LIMIT
SELECT
    id_promotion,
    id_produit,
    type_promotion,
    valeur_promotion,
    valeur_promotion_float,
    `date_début`,
    date_fin,
    responsable_promotion
FROM raw_data