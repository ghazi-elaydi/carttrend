-- corrélation des retards et des ventes
WITH commandes_data AS (
    SELECT
        AVG(s.note_client) AS note_moyenne_client,
        -- Calcul du retard : si la commande est livrée après la date estimée
        CASE 
            WHEN c.`statut_commande` = 'Livrée' 
                 AND  DATE_DIFF (c.`date_livraison_estimée`,c.date_commande,DAY) > 5  THEN 'En retard'  -- Retard si la livraison est après la date estimée
            ELSE 'Ponctuel'  -- Pas de retard
        END AS `retard`
        -- Identification des erreurs de livraison (commandes annulées)
    FROM 
        {{ ref('stg_commandes_data') }} c
    JOIN {{ ref('stg_satisfaction_data') }} s ON s.id_commande = c.id_commande
    WHERE
        c.`statut_commande` IN ('Livrée')  -- Seules les commandes validées ou livrées sont concernées
    GROUP BY retard
    ORDER BY note_moyenne_client DESC
)

SELECT *
FROM commandes_data