WITH raw_data AS(
SELECT
    id_commande,
    id_produit,
    `quantité`,
    `emballage_spécial`
FROM {{ source('data_carttrend', 'Carttrend_Details_Commandes') }}
)
SELECT
id_commande,
    id_produit,
    `quantité`,
    `emballage_spécial`
FROM raw_data