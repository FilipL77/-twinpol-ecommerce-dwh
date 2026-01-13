SELECT *
FROM `ecommerce_db.shipping_costs_temu_daily_pln_view`
WHERE shipping_cost_eur IS NOT NULL
  AND (eur_pln IS NULL OR shipping_cost_pln IS NULL);
