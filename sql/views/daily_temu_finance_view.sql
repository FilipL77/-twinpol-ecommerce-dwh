CREATE OR REPLACE VIEW `twinpol-ecommerce.ecommerce_db.daily_temu_finance_view` AS
WITH
sales_daily AS (
  SELECT
    order_date AS date,
    SUM(revenue_pln) AS revenue_pln,
    SUM(CAST(total_cost_pln AS FLOAT64)) AS cogs_pln,
    COUNT(DISTINCT order_id) AS orders,
    SUM(quantity) AS products_sold,

    -- profit z tego view = revenue_pln - total_cost_pln (bez ads i bez shipping label)
    SUM(profit_pln) AS profit_pln_net
  FROM `twinpol-ecommerce.ecommerce_db.sales_temu_profit_view`
  GROUP BY 1
),
ads_daily_eur AS (
  SELECT
    data AS date,
    SUM(wydatek_eur) AS ads_cost_eur
  FROM `twinpol-ecommerce.ecommerce_db.ads_temu_clean`
  GROUP BY 1
),
fx_monthly AS (
  SELECT
    month_start_date,
    CAST(eur_pln_avg AS FLOAT64) AS eur_pln_avg
  FROM `twinpol-ecommerce.ecommerce_db.fx_rates_clean`
),
shipping_financial_daily AS (
  SELECT
    date,
    -- 1 rekord dziennie w view, więc MAX/ANY_VALUE OK
    MAX(shipping_cost_pln) AS shipping_cost_pln
  FROM `twinpol-ecommerce.ecommerce_db.shipping_costs_temu_daily_pln_view`
  GROUP BY 1
),
shipping_labels_daily AS (
  SELECT
    date,
    MAX(shipments) AS shipments,
    MAX(label_cost_pln) AS shipping_labels_cost_pln
  FROM `twinpol-ecommerce.ecommerce_db.shipping_costs_temu_label_daily_pln_view`
  GROUP BY 1
)

SELECT
  s.date,
  s.revenue_pln,
  s.cogs_pln,

  IFNULL(a.ads_cost_eur, 0) AS ads_cost_eur,
  (IFNULL(a.ads_cost_eur, 0) * fx.eur_pln_avg) AS ads_cost_pln,

  -- FINANCIAL shipping (net settlement from Temu)
  IFNULL(sf.shipping_cost_pln, 0) AS shipping_cost_pln,

  -- OPERATIONAL shipping labels (real label purchases)
  IFNULL(sl.shipments, 0) AS shipments,
  IFNULL(sl.shipping_labels_cost_pln, 0) AS shipping_labels_cost_pln,

  s.orders,
  s.products_sold,

  -- Final profit (P&L): (revenue - cogs) - ads - financial shipping
  (s.profit_pln_net
    - (IFNULL(a.ads_cost_eur, 0) * fx.eur_pln_avg)
    - IFNULL(sf.shipping_cost_pln, 0)
  ) AS profit_pln_final,

  SAFE_DIVIDE(
    (s.profit_pln_net
      - (IFNULL(a.ads_cost_eur, 0) * fx.eur_pln_avg)
      - IFNULL(sf.shipping_cost_pln, 0)
    ),
    NULLIF(s.revenue_pln, 0)
  ) AS margin_final

FROM sales_daily s
LEFT JOIN fx_monthly fx
  ON DATE_TRUNC(s.date, MONTH) = fx.month_start_date
LEFT JOIN ads_daily_eur a
  ON s.date = a.date
LEFT JOIN shipping_financial_daily sf
  ON s.date = sf.date
LEFT JOIN shipping_labels_daily sl
  ON s.date = sl.date;
