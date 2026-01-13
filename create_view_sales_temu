CREATE OR REPLACE VIEW `twinpol-ecommerce.ecommerce_db.sales_temu_view` AS
WITH raw_data AS (
  SELECT
    order_id,

    CAST(REPLACE(REPLACE(REPLACE(item_price_eur_raw, '€', ''), ' ', ''), ',', '.') AS FLOAT64) AS item_price_eur,
    CAST(REPLACE(REPLACE(REPLACE(customer_shipping_eur_raw, '€', ''), ' ', ''), ',', '.') AS FLOAT64) AS customer_shipping_eur,

    SAFE_CAST(quantity_raw AS INT64) AS quantity,
    product_name_raw AS product_name,
    sku_raw AS sku,
    order_status_raw AS order_status,

    TRIM(SPLIT(CAST(purchase_datetime_raw AS STRING), ',')[OFFSET(0)]) AS date_part_raw
  FROM `twinpol-ecommerce.ecommerce_db.sales_temu_raw`
)

SELECT
  order_id,
  sku,
  product_name,
  order_status,
  quantity,

  (IFNULL(item_price_eur, 0) + IFNULL(customer_shipping_eur, 0)) AS price,
  (IFNULL(item_price_eur, 0) + IFNULL(customer_shipping_eur, 0)) * IFNULL(quantity, 0) AS total_value,

  PARSE_DATE('%d %m %Y',
    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
      date_part_raw,
      'sty.', '01'), 'lut.', '02'), 'mar.', '03'), 'kwi.', '04'), 'maj', '05'), 'cze.', '06'),
      'lip.', '07'), 'sie.', '08'), 'wrz.', '09'), 'paź.', '10'), 'lis.', '11'), 'gru.', '12')
  ) AS order_date
FROM raw_data;
