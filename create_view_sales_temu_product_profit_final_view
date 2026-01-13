CREATE OR REPLACE VIEW `ecommerce_db.sales_temu_product_profit_final_view` AS
WITH product_daily AS (
  -- dzienne metryki per produkt (bez shipping)
  SELECT
    order_date AS date,
    sku,
    product_name,
    SUM(quantity) AS products_sold,
    SUM(revenue_pln) AS revenue_pln,

    -- u Ciebie koszt produktu jest w profit view jako total_cost_pln
    SUM(total_cost_pln) AS cogs_pln
  FROM `ecommerce_db.sales_temu_profit_view`
  GROUP BY 1,2,3
),

daily_totals AS (
  SELECT
    date,
    SUM(revenue_pln) AS daily_revenue_pln
  FROM product_daily
  GROUP BY 1
),

shipping_daily AS (
  SELECT
    date,
    SUM(shipping_cost_pln) AS shipping_cost_pln
  FROM `ecommerce_db.shipping_costs_temu_daily_pln_view`
  GROUP BY 1
)

SELECT
  p.date,
  p.sku,
  p.product_name,
  p.products_sold,
  p.revenue_pln,
  p.cogs_pln,

  -- shipping alokowany proporcjonalnie do revenue w danym dniu
  CASE
    WHEN IFNULL(t.daily_revenue_pln, 0) = 0 THEN 0
    ELSE IFNULL(s.shipping_cost_pln, 0) * (p.revenue_pln / t.daily_revenue_pln)
  END AS shipping_cost_allocated_pln,

  -- profit po odjęciu cogs i alokowanego shipping
  (p.revenue_pln - p.cogs_pln
    - CASE
        WHEN IFNULL(t.daily_revenue_pln, 0) = 0 THEN 0
        ELSE IFNULL(s.shipping_cost_pln, 0) * (p.revenue_pln / t.daily_revenue_pln)
      END
  ) AS profit_pln_final,

  -- margin po odjęciu cogs i alokowanego shipping
  CASE
    WHEN IFNULL(p.revenue_pln, 0) = 0 THEN NULL
    ELSE (
      (p.revenue_pln - p.cogs_pln
        - CASE
            WHEN IFNULL(t.daily_revenue_pln, 0) = 0 THEN 0
            ELSE IFNULL(s.shipping_cost_pln, 0) * (p.revenue_pln / t.daily_revenue_pln)
          END
      ) / p.revenue_pln
    )
  END AS margin_final

FROM product_daily p
LEFT JOIN daily_totals t USING (date)
LEFT JOIN shipping_daily s USING (date);
