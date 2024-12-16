WITH entrepot_taille AS (
    SELECT 
        e.`id_entrepôt`, 
        e.localisation, 
        e.`capacité_max`, 
        e.`volume_stocké`,
        e.taux_remplissage
    FROM {{ ref ('stg_entrepots_data') }} e
    ORDER BY e.`volume_stocké` DESC
    LIMIT 100  -- Ajout de LIMIT ici
)
SELECT *
FROM entrepot_taille