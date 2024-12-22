with produits_par_age as (SELECT 
        CASE 
            WHEN cl.`âge` BETWEEN 18 AND 24 THEN '18-24'
            WHEN cl.`âge` BETWEEN 25 AND 34 THEN '25-34'
            WHEN cl.`âge` BETWEEN 35 AND 44 THEN '35-44'
            WHEN cl.`âge` BETWEEN 45 AND 54 THEN '45-54'
            WHEN cl.`âge` BETWEEN 55 AND 64 THEN '55-64'
            ELSE '65+' 
        END AS groupe_age,  
        d.id_produit,   -- Utilisation de id_produit directement dans la table des détails de commande
        SUM(d.`quantité`) AS total_quantite,   -- Calcul de la quantité totale par produit
        SUM(d.`quantité` * p.`Prix`) AS total_ventes -- Calcul du total des ventes par produit (quantité * prix)
    FROM {{ ref('stg_commandes_data') }} c 
    JOIN 
        {{ ref('stg_clients_data') }} cl 
        ON c.id_client = cl.id_client  -- Jointure avec la table Clients pour accéder à l'âge
    JOIN 
        {{ ref('stg_details_commandes_data') }} d 
        ON d.id_commande = c.id_commande  -- Jointure avec la table des détails de commandes pour accéder aux produits
    JOIN 
        {{ ref('stg_produits') }} p 
        ON d.id_produit = p.ID  -- Jointure avec la table Produits pour obtenir le prix et la catégorie
    WHERE cl.`âge` IS NOT NULL  -- Vérifier que l'âge n'est pas null
    GROUP BY 
        groupe_age, d.id_produit
)
-- Liste des produits avec le total des ventes, la quantité et la catégorie par groupe d'âge
SELECT 
    pa.groupe_age, 
    p.Produit, 
    p.`Catégorie`, 
    pa.total_quantite, 
    pa.total_ventes
FROM 
    produits_par_age pa
JOIN 
    {{ ref('stg_produits') }} p 
    ON pa.id_produit = p.ID
ORDER BY 
    pa.total_ventes DESC
