CREATE OR REPLACE TABLE `ecommerce_db.fx_rates` AS
WITH expanded AS (
  SELECT
    d AS date,
    c.eur_pln_avg AS eur_pln
  FROM `ecommerce_db.fx_rates_clean` c,
  UNNEST(
    GENERATE_DATE_ARRAY(
      c.month_start_date,
      DATE_SUB(DATE_ADD(c.month_start_date, INTERVAL 1 MONTH), INTERVAL 1 DAY)
    )
  ) AS d
)
SELECT
  date,
  eur_pln
FROM expanded
WHERE eur_pln IS NOT NULL;
