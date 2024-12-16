WITH ventes_par_temps AS (
    SELECT
        EXTRACT(MONTH FROM CAST(c.date_commande AS DATE)) AS mois,
        EXTRACT(DAY FROM CAST(c.date_commande AS DATE)) AS jour,
        SUM(d.`quantité` * p.Prix) AS total_ventes,  
        SUM(d.`quantité`) AS total_quantite_vendue  
    FROM 
        {{ ref('stg_commandes_data') }} c
    JOIN 
        {{ ref('stg_details_commandes_data') }} d ON c.id_commande = d.id_commande
    JOIN 
        {{ ref('stg_produits') }} p ON d.id_produit = p.ID
    WHERE 
        c.statut_commande = 'Livrée'  
    GROUP BY 
        mois, jour
)
-- Liste des ventes par mois et jour
SELECT 
    mois, jour,
    total_ventes, 
    total_quantite_vendue
FROM 
    ventes_par_temps
ORDER BY 
    total_ventes DESC
