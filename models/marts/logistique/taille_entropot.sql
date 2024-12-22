WITH volume_traite AS (
    SELECT 
        `id_entrepôt`,
        COUNT(`volume_traité`) AS `total_volume_traite`
    FROM {{ ref('stg_machines_data') }} vt
    GROUP BY `id_entrepôt`
    ORDER BY total_volume_traite DESC
)

SELECT * 
FROM volume_traite