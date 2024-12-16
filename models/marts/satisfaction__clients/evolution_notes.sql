-- 2. Evolution des notes au fil du temps (par mois et par produit)
 with evolution_notes AS (
    SELECT 
        EXTRACT(YEAR FROM c.`date_commande`) AS `annee`,  -- Utilisation de la date_commande de Carttrend_Commandes
        EXTRACT(MONTH FROM c.`date_commande`) AS `mois`,  -- Utilisation de la date_commande de Carttrend_Commandes
        dc.`id_produit`,  -- Utiliser id_produit de Carttrend_Details_Commandes
        p.`Produit` AS `produit_nom`,
        AVG(n.`note_client`) AS `note_moyenne`
    FROM 
        {{ ref('stg_satisfaction_data') }} n
    JOIN
        {{ ref('stg_details_commandes_data') }} dc 
        ON n.`id_commande` = dc.`id_commande`  -- Joindre Carttrend_Satisfaction à Carttrend_Details_Commandes via id_commande
    JOIN 
        {{ ref('stg_produits') }} p 
        ON dc.`id_produit` = p.`ID`  -- Joindre Carttrend_Details_Commandes à Carttrend_Produits via id_produit
    JOIN 
        {{ ref('stg_commandes_data') }} c
        ON n.`id_commande` = c.`id_commande`  -- Joindre Carttrend_Satisfaction à Carttrend_Commandes via id_commande
    GROUP BY 
        `annee`, `mois`, dc.`id_produit`, p.`Produit`
    ORDER BY 
        `annee` DESC, `mois` DESC
)
SELECT * 
FROM evolution_notes