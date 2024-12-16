-- 1. Analyse des notes produits : Moyenne des notes des produits
WITH notes_par_produit AS (
    SELECT 
        p.`ID` AS `id_produit`,  -- Utiliser `ID` pour la table Carttrend_Produits
        p.`Produit` AS `produit_nom`,
        AVG(s.`note_client`) AS `note_moyenne`,  -- Moyenne des notes des clients
        COUNT(s.`id_satisfaction`) AS `nb_avis`  -- Nombre d'avis sur ce produit
    FROM 
        {{ ref('stg_details_commandes_data') }} dc  -- Alias pour Carttrend_Details_Commandes
    JOIN
        {{ ref('stg_satisfaction_data') }} s  -- Alias pour Carttrend_Satisfaction
        ON dc.`id_commande` = s.`id_commande`  -- Jointure avec Carttrend_Satisfaction sur id_commande
    JOIN 
        {{ ref('stg_produits') }} p  -- Alias pour Carttrend_Produits
        ON dc.`id_produit` = p.`ID`  -- Jointure avec Carttrend_Produits
    GROUP BY 
        p.`ID`, p.`Produit`
    ORDER BY note_moyenne DESC
)
SELECT * 
FROM notes_par_produit