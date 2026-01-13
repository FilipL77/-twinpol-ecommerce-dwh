CREATE OR REPLACE VIEW `ecommerce_db.daily_temu_finance_view` AS
WITH sales AS (
  SELECT
    order_date AS data,
    SUM(revenue_pln) AS revenue_pln,
    SUM(total_cost_pln) AS cogs_pln,
    COUNT(DISTINCT order_id) AS orders,
    SUM(quantity) AS products_sold
  FROM `ecommerce_db.sales_temu_profit_view`
  GROUP BY 1
),

ads AS (
  SELECT
    data,
    SUM(wydatek) AS ads_cost_pln
  FROM `ecommerce_db.ads_temu_clean`
  GROUP BY 1
),

shipping AS (
  SELECT
    data,
    SUM(shipping_cost_pln) AS shipping_cost_pln
  FROM `ecommerce_db.shipping_costs_temu_daily_pln_view`
  GROUP BY 1
)

SELECT
  s.data,
  s.revenue_pln,
  s.cogs_pln,
  IFNULL(a.ads_cost_pln, 0) AS ads_cost_pln,
  IFNULL(sh.shipping_cost_pln, 0) AS shipping_cost_pln,
  s.orders,
  s.products_sold,

  -- PROFIT FINAL (po COGS, ads, shipping)
  (
    s.revenue_pln
    - s.cogs_pln
    - IFNULL(a.ads_cost_pln, 0)
    - IFNULL(sh.shipping_cost_pln, 0)
  ) AS profit_pln_final,

  -- MARGIN FINAL
  (
    (
      s.revenue_pln
      - s.cogs_pln
      - IFNULL(a.ads_cost_pln, 0)
      - IFNULL(sh.shipping_cost_pln, 0)
    )
    / NULLIF(s.revenue_pln, 0)
  ) AS margin_final,

  -- ROAS / TACOS
  (s.revenue_pln / NULLIF(a.ads_cost_pln, 0)) AS roas,
  (a.ads_cost_pln / NULLIF(s.revenue_pln, 0)) AS tacos

FROM sales s
LEFT JOIN ads a USING (data)
LEFT JOIN shipping sh USING (data);
