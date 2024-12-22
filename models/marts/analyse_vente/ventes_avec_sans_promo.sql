WITH commandes_details AS (
    SELECT
        c.id_commande,
        c.date_commande,
        d.id_produit,
        p.Produit AS nom_produit,
        d.`quantité`,
        p.Prix,
        c.`id_promotion_appliquée`
    FROM 
        {{ ref('stg_commandes_data') }} AS c
    JOIN 
        {{ ref('stg_details_commandes_data') }} AS d 
        ON c.id_commande = d.id_commande
    JOIN 
        {{ ref('stg_produits') }} AS p
        ON d.id_produit = p.ID
),

`commandes_aggregées` AS (
    SELECT
        id_commande,
        date_commande,
        SUM(`quantité` * Prix) AS total_commande,
        CASE
            WHEN `id_promotion_appliquée` !='' THEN 'Avec Promotion'
            ELSE 'Sans Promotion'
        END AS type_promotion,
        nom_produit
    FROM
        commandes_details
    GROUP BY
        id_commande, date_commande, nom_produit, `id_promotion_appliquée`
)

SELECT
    id_commande,
    date_commande,
    nom_produit,
    total_commande,
    type_promotion
FROM 
    `commandes_aggregées`
ORDER BY
    date_commande DESC
