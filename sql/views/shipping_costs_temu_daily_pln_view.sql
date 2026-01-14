CREATE OR REPLACE VIEW `twinpol-ecommerce.ecommerce_db.shipping_costs_temu_daily_pln_view` AS
WITH base AS (
  SELECT
    DATE(date_time) AS date,
    currency,
    (CAST(shipping AS FLOAT64) - CAST(shipping_tax AS FLOAT64)) AS shipping_net_raw_int
  FROM `twinpol-ecommerce.ecommerce_db.shipping_costs_temu_raw_bq`
  WHERE currency = 'EUR'
),
daily_eur AS (
  SELECT
    date,
    SUM(shipping_net_raw_int) / 100.0 AS shipping_cost_eur_signed
  FROM base
  GROUP BY 1
),
fx AS (
  SELECT
    month_start_date,
    CAST(eur_pln_avg AS FLOAT64) AS eur_pln
  FROM `twinpol-ecommerce.ecommerce_db.fx_rates_clean`
)
SELECT
  d.date,
  ABS(d.shipping_cost_eur_signed) AS shipping_cost_eur,
  ABS(d.shipping_cost_eur_signed) * fx.eur_pln AS shipping_cost_pln
FROM daily_eur d
LEFT JOIN fx
  ON DATE_TRUNC(d.date, MONTH) = fx.month_start_date;
