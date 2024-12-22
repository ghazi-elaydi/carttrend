WITH Favoris_Explodes AS (
    -- Découper la colonne favoris en lignes et nettoyer les valeurs
    SELECT 
        TRIM(fav) AS id_produit
    FROM `data_carttrend.Carttrend_Clients`, 
         UNNEST(SPLIT(favoris, ',')) AS fav
    WHERE favoris IS NOT NULL AND favoris != ''
),

Produits_Ajustes AS (
    -- Retirer les zéros non significatifs dans les ID
    SELECT 
        ID,
        Produit,
        `Catégorie`,
        CONCAT('P', CAST(CAST(SUBSTRING(ID, 2) AS INT64) AS STRING)) AS id_produit_nettoye
    FROM `data_carttrend.Carttrend_Produits`
)

SELECT 
    pa.Produit AS nom_produit,
    fe.id_produit,
    `Catégorie`,
    COUNT(*) AS nombre_de_fois
FROM Favoris_Explodes fe
JOIN Produits_Ajustes pa
    ON fe.id_produit = pa.id_produit_nettoye
GROUP BY pa.Produit, fe.id_produit, `Catégorie`
ORDER BY nombre_de_fois DESC