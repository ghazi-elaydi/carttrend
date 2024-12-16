--MOYENNE VENTES    
 WITH vente_satisfaction AS (
    SELECT 
    c.date_commande AS date, 
    AVG(s.note_client) AS total_notes 
    FROM {{ ref('stg_commandes_data') }} c 
    INNER JOIN {{ ref('stg_satisfaction_data') }} s
    ON c.id_commande = s.id_commande 
    GROUP BY c.date_commande
 )
SELECT *
FROM vente_satisfaction

