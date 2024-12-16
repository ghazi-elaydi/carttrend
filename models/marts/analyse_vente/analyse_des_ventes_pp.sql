WITH ventes_par_produit AS (
    SELECT 
        p.ID AS id_produit,
        p.Produit,
        SUM(d.`quantité`) AS total_quantite_vendue
    FROM 
        {{ ref('stg_details_commandes_data') }} d
    JOIN 
        {{ ref('stg_produits') }} p ON d.id_produit = p.ID
    GROUP BY 
        p.ID, p.Produit
)
-- Liste des produits populaires et ceux qui trainent 
SELECT 
    id_produit, 
    Produit,
    total_quantite_vendue
FROM 
    ventes_par_produit
ORDER BY 
    total_quantite_vendue DESC