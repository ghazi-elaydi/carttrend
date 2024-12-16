WITH raw_data AS(
SELECT 
    canal,
    SUM(budget) AS total_budget,
    SUM(clics) AS total_clics,
    SUM(conversions) AS total_conversions,
    -- Coût par clic (CPC)
    CASE 
        WHEN SUM(clics) > 0 THEN SUM(budget) / SUM(clics)
        ELSE 0
    END AS cpc,
    -- Coût par conversion (CPA)
    CASE 
        WHEN SUM(conversions) > 0 THEN SUM(budget) / SUM(conversions)
        ELSE 0
    END AS cpa
FROM {{ ref('stg_campaigns_data') }} 
GROUP BY 
    canal
ORDER BY 
    total_budget DESC
)
SELECT * 
FROM raw_data