SELECT
  t.order_date,
  t.order_id,
  t.sku,
  t.product_name,
  t.quantity,
  
  -- 1. Przychód w EUR (oryginał)
  t.total_value AS revenue_eur,
  
  -- 2. Przychód przeliczony na PLN (Kurs sztywny 4.30 - zmień jeśli trzeba)
  t.total_value * 4.30 AS revenue_pln,
  
  -- 3. Koszt zakupu (z tabeli products)
  -- IFNULL(..., 0) oznacza: jak nie znajdziesz produktu, wpisz koszt 0 (żeby nie było błędu)
  IFNULL(p.cogs_pln, 0) * t.quantity AS total_cost_pln,
  
  -- 4. Zysk (Profit) w PLN
  (t.total_value * 4.30) - (IFNULL(p.cogs_pln, 0) * t.quantity) AS profit_pln,
  
  -- 5. Marża procentowa (Profit / Revenue)
  SAFE_DIVIDE(
    (t.total_value * 4.30) - (IFNULL(p.cogs_pln, 0) * t.quantity),
    (t.total_value * 4.30)
  ) AS margin_percent

FROM `twinpol-ecommerce.ecommerce_db.sales_temu_view` t
LEFT JOIN `twinpol-ecommerce.ecommerce_db.products` p
  ON t.sku = p.sku_internal
