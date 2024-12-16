WITH clics_canal AS (
  SELECT 
  canal,
  SUM(clics) AS total_clics,
FROM 
  {{ ref('stg_campaigns_data') }}
GROUP BY canal 
ORDER BY total_clics DESC
)
SELECT * 
FROM clics_canal






