CREATE OR REPLACE VIEW `twinpol-ecommerce.ecommerce_db.sales_temu_view` AS
WITH base AS (
  SELECT
    order_id,
    sku_raw AS sku,
    product_name_raw AS product_name,
    TRIM(CAST(order_status_raw AS STRING)) AS order_status,
    SAFE_CAST(quantity_raw AS INT64) AS quantity,

    -- AH: unit net item price (EUR)
    SAFE_CAST(
      REPLACE(REPLACE(REPLACE(REPLACE(CAST(item_price_eur_raw AS STRING), '€', ''), ' ', ''), '.', ''), ',', '.')
      AS FLOAT64
    ) AS item_price_eur,

    -- AR: customer-paid shipping (EUR), per row (NOT per unit)
    SAFE_CAST(
      REPLACE(REPLACE(REPLACE(REPLACE(CAST(customer_shipping_eur_raw AS STRING), '€', ''), ' ', ''), '.', ''), ',', '.')
      AS FLOAT64
    ) AS customer_shipping_eur,

    -- Example: "13 gru. 2025, 07:32 CET(UTC+1)" -> take "13 gru. 2025"
    TRIM(SPLIT(CAST(purchase_datetime_raw AS STRING), ',')[OFFSET(0)]) AS date_part_raw
  FROM `twinpol-ecommerce.ecommerce_db.sales_temu_raw`
  WHERE TRIM(CAST(order_status_raw AS STRING)) IN ('Wysłane', 'Doręczona')
),

dated AS (
  SELECT
    *,
    SAFE.PARSE_DATE(
      '%d %m %Y',
      REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        date_part_raw,
        'sty.', '01'), 'lut.', '02'), 'mar.', '03'), 'kwi.', '04'), 'maj', '05'), 'cze.', '06'),
        'lip.', '07'), 'sie.', '08'), 'wrz.', '09'), 'paź.', '10'), 'lis.', '11'), 'gru.', '12')
    ) AS order_date
  FROM base
)

SELECT
  order_id,
  sku,
  product_name,
  order_status,
  quantity,
  item_price_eur,
  customer_shipping_eur,

  -- convenience
  (IFNULL(item_price_eur, 0) + IFNULL(customer_shipping_eur, 0)) AS price,

  -- correct line revenue:
  (IFNULL(item_price_eur, 0) * IFNULL(quantity, 0)) + IFNULL(customer_shipping_eur, 0) AS total_value,

  order_date
FROM dated
WHERE order_date IS NOT NULL;
