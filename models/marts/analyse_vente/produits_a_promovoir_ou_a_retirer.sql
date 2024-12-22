--Produits à promouvoir ou à retirer du catalogue
WITH produits_analyses AS (
    SELECT 
        p.ID AS produit_id,
        p.Produit AS produit_nom,
        SUM(dc.`quantité` * p.Prix) AS revenus_generes,  -- Revenus générés par produit
        COUNT(DISTINCT c.id_commande) AS nb_commandes,  -- Nombre de commandes
       
    FROM {{ref('stg_details_commandes_data')}} dc
    JOIN {{ref('stg_commandes_data')}} c ON dc.id_commande = c.id_commande
    JOIN {{ref('stg_produits')}} p ON dc.id_produit = p.ID
    GROUP BY p.ID, p.Produit
    ORDER BY revenus_generes,nb_commandes ASC
)
SELECT *
FROM produits_analyses



