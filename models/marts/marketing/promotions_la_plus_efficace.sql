WITH promotion_analysis AS (
    SELECT
        `id_promotion_appliquée`,
        EXTRACT(YEAR FROM pr.`date_début`) AS annee_promotion,  -- Extraction de l'année
        EXTRACT(MONTH FROM pr.`date_début`) AS mois_promotion,   -- Extraction du mois
        COUNT(c.id_commande) AS total_commandes,
        SUM(dc.`quantité` * p.Prix) AS total_ventes,
        AVG(dc.`quantité` * p.Prix) AS moyenne_ventes
    FROM {{ ref('stg_commandes_data') }} c
    JOIN {{ ref('stg_details_commandes_data') }} dc
    ON c.id_commande = dc.id_commande
    JOIN {{ ref('stg_produits') }} p
    ON dc.id_produit = p.ID
    JOIN {{ ref('stg_promotions') }} pr
    ON p.ID = pr.id_produit
    WHERE `id_promotion_appliquée` IS NOT NULL
    GROUP BY `id_promotion_appliquée`, EXTRACT(YEAR FROM pr.`date_début`), EXTRACT(MONTH FROM pr.`date_début`)
)
SELECT
    `id_promotion_appliquée`,
    CONCAT(annee_promotion, '-', LPAD(CAST(mois_promotion AS STRING), 2, '0')) AS mois_annee_promotion, -- Conversion explicite du mois en chaîne
    total_commandes,
    total_ventes,
    moyenne_ventes
FROM promotion_analysis
ORDER BY total_ventes DESC
