-- corrélation des retards et des ventes
WITH commandes_data AS (
    SELECT
        c.`id_commande`,
        c.`id_client`,
        c.`id_entrepôt_départ`,
        c.`date_commande`,
        c.`statut_commande`,
        c.`date_livraison_estimée`,
        -- Calcul du retard : si la commande est livrée après la date estimée
        CASE 
            WHEN c.`statut_commande` = 'Livrée' 
                 AND c.`date_livraison_estimée` < CURRENT_DATE THEN 1  -- Retard si la livraison est après la date estimée
            ELSE 0  -- Pas de retard
        END AS `retard`,
        -- Identification des erreurs de livraison (commandes annulées)
        CASE
            WHEN c.`statut_commande` = 'Annulée' THEN 1  -- Erreur si la commande est annulée
            ELSE 0
        END AS `erreur_livraison`
    FROM 
        {{ ref('stg_commandes_data') }} c
    WHERE
        c.`statut_commande` IN ('Livrée', 'En transit', 'Validée')  -- Seules les commandes validées ou livrées sont concernées
)
SELECT *
FROM commandes_data