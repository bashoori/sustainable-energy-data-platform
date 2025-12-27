-- Sustainability KPIs for product and analytics consumption

-- Total energy consumption by region and month
SELECT
    region,
    DATE_TRUNC('month', metric_date) AS month,
    SUM(metric_value) AS total_energy_mwh
FROM fact_sustainability_metric
WHERE metric_name = 'energy_mwh'
GROUP BY region, DATE_TRUNC('month', metric_date)
ORDER BY region, month;


-- Average emissions per region
SELECT
    region,
    AVG(metric_value) AS avg_emissions_tco2
FROM fact_sustainability_metric
WHERE metric_name = 'emissions_tco2'
GROUP BY region
ORDER BY avg_emissions_tco2 DESC;
