-- Analyser les produits fréquemment achetés ensemble
WITH produits_achetes_ensemble AS (
    SELECT 
    id_produit,
    SUM(`quantité`) AS nb_achats_ensemble
    FROM {{ref('stg_details_commandes_data')}}
    GROUP BY id_produit
)
-- Liste des produits achetés ensemble
SELECT *
FROM produits_achetes_ensemble
ORDER BY nb_achats_ensemble DESC