SELECT
  s.sku,
  s.product_name,
  COUNT(DISTINCT s.order_id) AS orders_count,
  SUM(s.quantity)            AS total_quantity,
  SUM(s.total_value)         AS total_revenue_pln,
  p.cogs_pln
FROM `ecommerce_db.sales_temu_view` s
LEFT JOIN `ecommerce_db.products` p
  ON s.sku = p.sku_internal
WHERE
  p.sku_internal IS NULL
  OR p.cogs_pln IS NULL
  OR p.cogs_pln = 0
GROUP BY
  s.sku,
  s.product_name,
  p.cogs_pln
ORDER BY
  total_revenue_pln DESC;
