CREATE OR REPLACE VIEW `ecommerce_db.shipping_costs_temu_daily_pln_view` AS
WITH shipping_by_day AS (
  SELECT
    s.order_date AS date,
    SUM(c.shipping_cost_eur) AS shipping_cost_eur
  FROM `ecommerce_db.shipping_costs_temu_clean` c
  JOIN (
    SELECT DISTINCT order_id, order_date
    FROM `ecommerce_db.sales_temu_view`
  ) s
  ON c.order_id = s.order_id
  GROUP BY 1
)
SELECT
  sh.date,
  sh.shipping_cost_eur,
  fx.eur_pln,
  sh.shipping_cost_eur * fx.eur_pln AS shipping_cost_pln
FROM shipping_by_day sh
LEFT JOIN `ecommerce_db.fx_rates` fx
  ON sh.date = fx.date;
