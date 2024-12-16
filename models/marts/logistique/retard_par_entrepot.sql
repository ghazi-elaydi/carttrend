-- CEUX qui causent des retards (avec information sur les entrepôts)
WITH retards_par_entrepot AS (
    SELECT 
        c.`id_entrepôt_départ`, 
        e.localisation,
        AVG(DATE_DIFF(c.`date_livraison_estimée`, c.date_commande,DAY)) AS retard_moyen
    FROM {{ ref('stg_commandes_data') }} c
    JOIN {{ ref('stg_entrepots_data') }} e 
        ON c.`id_entrepôt_départ` = e.`id_entrepôt`
    WHERE c.statut_commande = 'Livrée'
    GROUP BY c.`id_entrepôt_départ`, e.localisation
    ORDER BY retard_moyen DESC
    
)
SELECT *
FROM retards_par_entrepot