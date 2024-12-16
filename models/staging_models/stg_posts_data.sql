
WITH mentions_data AS (
    SELECT
      id_post,date_post,canal_social,volume_mentions,sentiment_global,contenu_post
    FROM {{ source('data_carttrend', 'Carttrend_Posts') }}
)
SELECT
id_post,date_post,canal_social,volume_mentions,sentiment_global,contenu_post
FROM mentions_data