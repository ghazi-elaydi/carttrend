--id_promotion_appliquée id_commande faut les chercher de la table commande
--id_produit faut le chercher de la table détails commande 
WITH produits_revenus AS (
    SELECT 
        p.ID AS produit_id,
        p.Produit AS produit_nom,
        SUM(dc.`quantité` * p.Prix) AS revenus_generes,  -- Calcul des revenus générés par produit
        COUNT(DISTINCT c.id_commande) AS nb_commandes,  -- Nombre total de commandes pour chaque produit
        COUNT(c.`id_promotion_appliquée`) AS nb_achats_avec_promo  -- Nombre d'achats avec promotion
    FROM {{ref('stg_details_commandes_data')}} dc
    JOIN {{ref('stg_commandes_data')}} c ON dc.id_commande = c.id_commande
    JOIN {{ref('stg_produits')}} p ON dc.id_produit = p.ID
    
    WHERE c.`id_promotion_appliquée` !=""  
    GROUP BY p.ID, p.Produit
)
SELECT 
    produit_id,
    produit_nom,
    revenus_generes,
    nb_commandes,
    nb_achats_avec_promo,
    -- Calcul du pourcentage de commandes avec promotion   
FROM produits_revenus
ORDER BY revenus_generes DESC, nb_achats_avec_promo DESC