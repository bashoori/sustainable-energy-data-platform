-- Month-over-month energy trend analysis

WITH monthly_energy AS (
    SELECT
        region,
        DATE_TRUNC('month', metric_date) AS month,
        SUM(metric_value) AS energy_mwh
    FROM fact_sustainability_metric
    WHERE metric_name = 'energy_mwh'
    GROUP BY region, DATE_TRUNC('month', metric_date)
)
SELECT
    region,
    month,
    energy_mwh,
    energy_mwh - LAG(energy_mwh) OVER (
        PARTITION BY region ORDER BY month
    ) AS month_over_month_change
FROM monthly_energy
ORDER BY region, month;
