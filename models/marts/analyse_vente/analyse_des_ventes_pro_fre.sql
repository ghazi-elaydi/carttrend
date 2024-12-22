WITH Paires_Produits AS (
    SELECT 
        dc1.id_produit AS produit_1,
        dc2.id_produit AS produit_2,
        COUNT(*) AS nombre_occurrences
    FROM {{ref('stg_commandes_data')}} c1
    JOIN {{ref('stg_commandes_data')}} c2
        ON c1.id_client = c2.id_client
        AND c1.date_commande = c2.date_commande
    JOIN {{ref('stg_details_commandes_data')}} dc1
        ON dc1.id_commande = c1.id_commande
    JOIN {{ref('stg_details_commandes_data')}} dc2
        ON dc1.id_produit < dc2.id_produit  -- Évite les doublons symétriques
        AND dc2.id_commande = c2.id_commande
    GROUP BY dc1.id_produit, dc2.id_produit
)
SELECT 
    produit_1, 
    produit_2, 
    CONCAT(produit_1, '-', produit_2) AS combinaison_produits, -- Colonne avec la combinaison des produits
    p1.Produit AS produit_1_nom, -- Nom du produit 1
    p2.Produit AS produit_2_nom, -- Nom du produit 2
    nombre_occurrences
FROM Paires_Produits
JOIN {{ref('stg_produits')}} p1 ON p1.ID = produit_1 -- Jointure pour le nom du produit 1
JOIN {{ref('stg_produits')}} p2 ON p2.ID = produit_2 -- Jointure pour le nom du produit 2
ORDER BY nombre_occurrences DESC
