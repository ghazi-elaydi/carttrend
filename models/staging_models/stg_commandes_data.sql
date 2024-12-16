
WITH commandes_data AS (
    -- Sélection des commandes livrées et leurs détails associés
    SELECT 
        id_commande,
         `id_client`,
        `id_entrepôt_départ`,
        date_commande,
        statut_commande,
        `id_promotion_appliquée`,
        mode_de_paiement,
        `numéro_tracking`,
        `date_livraison_estimée`,
    FROM 
        {{ source('data_carttrend', 'Carttrend_Commandes') }}
)
SELECT * 
FROM commandes_data   