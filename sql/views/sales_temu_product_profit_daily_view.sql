CREATE OR REPLACE VIEW `ecommerce_db.sales_temu_product_profit_daily_view` AS
SELECT
  date,
  sku,
  product_name,
  SUM(products_sold)               AS products_sold,
  SUM(revenue_pln)                 AS revenue_pln,
  SUM(cogs_pln)                    AS cogs_pln,
  SUM(shipping_cost_allocated_pln) AS shipping_cost_pln,
  SUM(profit_pln_final)            AS profit_pln_final
FROM `ecommerce_db.sales_temu_product_profit_final_view`
GROUP BY 1,2,3;
