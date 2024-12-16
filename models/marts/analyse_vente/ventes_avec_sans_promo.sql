WITH ventes_promotion AS (
    SELECT 
        SUM(d.`quantité` * p.Prix) AS total_ventes,
        COUNT(DISTINCT c.id_commande) AS nb_commandes
    FROM 
        {{ ref('stg_commandes_data') }} c
    JOIN 
        {{ ref('stg_details_commandes_data') }} d ON c.id_commande = d.id_commande
    JOIN 
        {{ ref('stg_produits') }} p ON d.id_produit = p.ID
    WHERE 
        c.`id_promotion_appliquée` IS NOT NULL
),
ventes_sans_promotion AS (
    SELECT 
        SUM(d.`quantité` * p.Prix) AS total_ventes,
        COUNT(DISTINCT c.id_commande) AS nb_commandes
    FROM 
        {{ ref('stg_commandes_data') }} c
    JOIN 
        {{ ref('stg_details_commandes_data') }} d ON c.id_commande = d.id_commande
    JOIN 
        {{ ref('stg_produits') }} p ON d.id_produit = p.ID
    WHERE 
        c.`id_promotion_appliquée` =""
)
SELECT 
    'Avec Promotion' AS type_promotion, 
    total_ventes, 
    nb_commandes 
FROM 
    ventes_promotion
UNION ALL
SELECT 
    'Sans Promotion' AS type_promotion, 
    total_ventes, 
    nb_commandes 
FROM 
    ventes_sans_promotion

-- ------ Calcul des produits populaires, produits favoris par groupe d'âge, et des ventes par catégorie.
-- Calcule les produits les plus populaires ou moins / pas achetés
-- Ventes par catégorie
-- Produits achetés ensemble
-- Calcule les ventes par mois, semaine et jour