WITH ventes_par_categorie AS (
    SELECT 
        p.`Catégorie`,  
        SUM(d.`quantité` * p.Prix) AS total_ventes,
        CONCAT(EXTRACT(YEAR FROM c.date_commande), '-', LPAD(CAST(EXTRACT(MONTH FROM c.date_commande) AS STRING), 2, '0')) AS annee_mois  -- Combinaison de l'année et du mois dans une colonne
    FROM 
        {{ ref('stg_details_commandes_data') }} d
    JOIN {{ ref('stg_commandes_data') }} c
        ON d.id_commande = c.id_commande
    JOIN {{ ref('stg_produits') }} p 
        ON d.id_produit = p.ID
    GROUP BY 
        p.`Catégorie`, annee_mois
    ORDER BY 
        total_ventes DESC
)
SELECT *
FROM 
    ventes_par_categorie
