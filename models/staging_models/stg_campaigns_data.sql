WITH raw_data AS (
    SELECT *
    FROM {{ source('data_carttrend', 'Carttrend_Campaigns') }}
    
)
-- Nettoyage et transformation des données
SELECT *
FROM raw_data

