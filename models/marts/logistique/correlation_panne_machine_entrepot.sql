WITH raw_data AS (
    SELECT 
         e.`id_entrepôt`,
        SUM(c.`volume_traité`) AS total_commandes_traites,
        COUNT(CASE WHEN c.`état_machine` = 'En maintenance' THEN 1 END) AS nb_machines_en_maintenance,
        COUNT(CASE WHEN c.`état_machine` = 'En panne' THEN 1 END) AS nb_machines_en_panne,
        COUNT(CASE WHEN c.`état_machine` = 'En service' THEN 1 END) AS nb_machines_en_service
      
    FROM 
        {{ ref('stg_machines_data') }} c
    JOIN 
        {{ ref('stg_entrepots_data') }} e
    ON 
        c.`id_entrepôt` = e.`id_entrepôt`
    GROUP BY  
        e.`id_entrepôt`
    ORDER BY 
        total_commandes_traites DESC
)
SELECT *
FROM raw_data
