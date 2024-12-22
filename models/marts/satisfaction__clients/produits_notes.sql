WITH notes_par_produit AS (
    SELECT 
        p.`ID` AS `id_produit`,  -- Identifiant du produit
        p.`Produit` AS `produit_nom`,  -- Nom du produit
        p.`Catégorie` AS `categorie`,  -- Catégorie du produit
        AVG(s.`note_client`) AS `note_moyenne`,  -- Moyenne des notes des clients
        COUNT(s.`id_satisfaction`) AS `nb_avis`  -- Nombre d'avis pour ce produit
    FROM 
        {{ ref('stg_details_commandes_data') }} dc  -- Alias pour Carttrend_Details_Commandes
    JOIN
        {{ ref('stg_satisfaction_data') }} s  -- Alias pour Carttrend_Satisfaction
        ON dc.`id_commande` = s.`id_commande`  -- Jointure avec Carttrend_Satisfaction sur id_commande
    JOIN 
        {{ ref('stg_produits') }} p  -- Alias pour Carttrend_Produits
        ON dc.`id_produit` = p.`ID`  -- Jointure avec Carttrend_Produits
    GROUP BY 
        p.`ID`, p.`Produit`, p.`Catégorie`
),

notes_par_categorie AS (
    SELECT 
        p.`Catégorie` AS `categorie`,  -- Catégorie du produit
        AVG(s.`note_client`) AS `note_moyenne_categorie`,  -- Moyenne des notes pour la catégorie
        COUNT(s.`id_satisfaction`) AS `nb_avis_categorie`  -- Nombre total d'avis dans la catégorie
    FROM 
        {{ ref('stg_details_commandes_data') }} dc  -- Alias pour Carttrend_Details_Commandes
    JOIN
        {{ ref('stg_satisfaction_data') }} s  -- Alias pour Carttrend_Satisfaction
        ON dc.`id_commande` = s.`id_commande`  -- Jointure avec Carttrend_Satisfaction sur id_commande
    JOIN 
        {{ ref('stg_produits') }} p  -- Alias pour Carttrend_Produits
        ON dc.`id_produit` = p.`ID`  -- Jointure avec Carttrend_Produits
    GROUP BY 
        p.`Catégorie`
)

-- Sélection finale avec produit, catégorie, moyenne des notes et nombre d'avis
SELECT 
    np.`id_produit`,
    np.`produit_nom`,
    np.`categorie`,
    np.`note_moyenne`,
    np.`nb_avis`,
    npc.`note_moyenne_categorie`,
    npc.`nb_avis_categorie`
FROM 
    notes_par_produit np
JOIN 
    notes_par_categorie npc
    ON np.`categorie` = npc.`categorie`
ORDER BY 
    np.`note_moyenne` DESC  -- Trier par note moyenne du produit (meilleur produit en premier)
