-- Identifier les categories les plus génératrices de revenus et les catégories en déclin
WITH ventes_par_categorie AS (
    SELECT 
        p.`Catégorie`,  
        SUM(d.`quantité` * p.Prix) AS total_ventes  
    FROM 
        {{ ref('stg_details_commandes_data') }} d
    JOIN 
        {{ ref('stg_produits') }} p 
        ON d.id_produit = p.ID
    GROUP BY 
        p.`Catégorie`
)
SELECT 
    `Catégorie`,
    total_ventes
FROM 
    ventes_par_categorie
ORDER BY 
    total_ventes DESC
