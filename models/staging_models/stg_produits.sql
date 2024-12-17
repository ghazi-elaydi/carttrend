 WITH base2 AS(   
     SELECT
        `ID`,
        `Catégorie`,
        `Marque`,
        `Produit`,
        `Prix`,
        `Souscatégorie`,
        `Variation`
    FROM {{source('data_carttrend', 'Carttrend_Produits')}}

-- Calcule les produits les plus populaires ou moins / pas achetés
-- Ventes pas catégorie
-- Produits achetés ensemble
 )
 SELECT
      `ID`,
        `Catégorie`,
        `Marque`,
        `Produit`,
        `Prix`,
        `Souscatégorie`,
        `Variation`
    FROM base2 