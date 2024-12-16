  -- Calcul du volume de mentions par sentiment
WITH mentions_sentiments AS (
  SELECT canal_social,sentiment_global,
    SUM(volume_mentions) AS total_mentions  -- Compte le total des mentions
    
  FROM 
    {{ ref('stg_posts_data') }}
  GROUP BY 
    sentiment_global,canal_social
)
SELECT *
FROM mentions_sentiments


