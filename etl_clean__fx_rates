CREATE OR REPLACE TABLE `ecommerce_db.fx_rates_clean` AS
SELECT
  PARSE_DATE('%Y-%m', month) AS month_start_date,
  SAFE_CAST(REPLACE(CAST(eur_pln_avg AS STRING), ',', '.') AS NUMERIC) AS eur_pln_avg,
  source,
  notes
FROM `ecommerce_db.fx_rates_raw`
WHERE month IS NOT NULL;
