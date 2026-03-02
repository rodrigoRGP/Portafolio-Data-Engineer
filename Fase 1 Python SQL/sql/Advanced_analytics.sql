-- REPORTE: El "Top 3" de servicios más costosos por Región
WITH resumen_gastos AS (
    SELECT 
        region,
        servicio,
        SUM(monto) as gasto_total,
        COUNT(*) as transacciones
    FROM raw_aws_billing.facturacion
    GROUP BY region, servicio
),
ranking_servicios AS (
    SELECT 
        region,
        servicio,
        gasto_total,
        transacciones,
        RANK() OVER (PARTITION BY region ORDER BY gasto_total DESC) as posicion_en_region
    FROM resumen_gastos
)
SELECT * FROM ranking_servicios 
WHERE posicion_en_region <= 3
ORDER BY region, posicion_en_region;