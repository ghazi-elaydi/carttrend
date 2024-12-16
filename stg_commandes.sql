SELECT 
    id_commande,
    id_client,
    id_entrepot_depart,
    date_commande,
    statut_commande,
    id_promotion_appliquee,
    mode_de_paiement,
    numero_tracking,
    date_livraison_estimee
FROM 
    {{ source('data_carttrend', 'Carttrend_commandes') }} 
WHERE 
    date_commande IS NOT NULL;


