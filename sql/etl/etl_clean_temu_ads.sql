SELECT
  COUNT(*) AS row_count,
  MIN(data) AS min_date,
  MAX(data) AS max_date,
  ROUND(SUM(wydatek_eur), 2) AS spend_eur
FROM `twinpol-ecommerce.ecommerce_db.ads_temu_clean`;
