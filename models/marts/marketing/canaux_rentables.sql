WITH raw_data AS (
SELECT 
    canal,
    SUM(budget) AS total_budget,
    SUM(conversions) AS total_conversions,
    -- Calcul du Coût par Conversion (CPA)
    CASE 
        WHEN SUM(conversions) > 0 THEN SUM(budget) / SUM(conversions)
        ELSE 0
    END AS cpa
FROM {{ ref('stg_campaigns_data') }} 
GROUP BY canal
HAVING SUM(conversions) > 0  -- Exclure les canaux sans conversions
ORDER BY 
    total_conversions DESC, cpa ASC -- Priorise les canaux générant le plus de conversions
              -- Ensuite, trie par coût par conversion croissant
)
SELECT * 
FROM raw_data