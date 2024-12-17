-- Analyse de la popularité des produits par tranche d'âge
WITH produits_par_age AS (
    SELECT 
        CASE 
            WHEN cl.`âge` BETWEEN 18 AND 24 THEN '18-24'
            WHEN cl.`âge` BETWEEN 25 AND 34 THEN '25-34'
            WHEN cl.`âge` BETWEEN 35 AND 44 THEN '35-44'
            WHEN cl.`âge` BETWEEN 45 AND 54 THEN '45-54'
            WHEN cl.`âge` BETWEEN 55 AND 64 THEN '55-64'
            ELSE '65+' 
        END AS groupe_age,  
        d.id_produit,   -- Utilisation de id_produit directement dans la table des détails de commande
        p.Produit,
        p.`Catégorie`,
        p.Marque,
        d.`quantité`,
        p.Prix,
        (d.`quantité` * p.Prix) AS total_ventes,
        COUNT(*) AS nb_fois_ajoute
    FROM 
        {{ ref('stg_satisfaction_data') }} s
    JOIN 
        {{ ref('stg_commandes_data') }} c 
        ON s.id_commande = c.id_commande  -- Jointure avec la table Commandes
    JOIN 
        {{ ref('stg_clients_data') }} cl 
        ON c.id_client = cl.id_client  -- Jointure avec la table Clients pour accéder à l'âge
    JOIN 
        {{ ref('stg_details_commandes_data') }} d 
        ON d.id_commande = c.id_commande  -- Jointure avec la table des détails de commandes pour accéder aux produits
    JOIN
        {{ ref('stg_produits') }} p 
        ON d.id_produit = p.ID
    WHERE cl.`âge` IS NOT NULL  -- Vérifier que l'âge n'est pas null dans Carttrend_Clients
    GROUP BY 
        groupe_age, d.id_produit, p.Produit, p.`Catégorie`, p.Marque, d.`quantité`, p.Prix
),

ventes_totales AS (
    SELECT 
        groupe_age, 
        id_produit, 
        Produit, 
        `Catégorie`,
        Marque,
        SUM(`quantité`) AS total_quantite,  -- Total des quantités vendues par produit
        SUM(total_ventes) AS total_ventes  -- Total des ventes pour chaque produit
    FROM 
        produits_par_age
    GROUP BY 
        groupe_age, id_produit, Produit, `Catégorie`, Marque
)
-- Liste des produits favoris par groupe d'âge
--SELECT 
--    p.Produit, 
--    pa.groupe_age, 
--    pa.nb_fois_ajoute 
--FROM 
--   produits_par_age pa
--JOIN 
--    {{ ref('stg_produits') }} p ON pa.id_produit = p.ID
--ORDER BY 
--    pa.nb_fois_ajoute DESC

SELECT 
    groupe_age, 
    Produit, 
    `Catégorie`, 
    Marque, 
    total_quantite, 
    total_ventes
FROM 
    ventes_totales
ORDER BY 
    total_ventes DESC
