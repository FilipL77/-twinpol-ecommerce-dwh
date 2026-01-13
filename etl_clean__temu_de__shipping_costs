CREATE OR REPLACE TABLE `ecommerce_db.shipping_costs_temu_clean` AS
WITH base AS (
  SELECT
    DATE(date_time) AS transaction_date,
    transaction_type,
    related_id,
    order_id,
    currency,

    -- parse numeric robustly
    SAFE_CAST(
      REPLACE(REGEXP_REPLACE(CAST(total AS STRING), r'[^0-9,.\-]', ''), ',', '.')
      AS NUMERIC
    ) AS raw_total_num
  FROM `ecommerce_db.shipping_costs_temu_raw`
),
normalized AS (
  SELECT
    *,
    -- if decimal separator was lost, values become ~100x too large (e.g. 461 instead of 4.61)
    CASE
      WHEN ABS(raw_total_num) >= 100 AND ABS(raw_total_num) < 100000
        THEN raw_total_num / 100
      ELSE raw_total_num
    END AS total_eur
  FROM base
),
filtered AS (
  SELECT *
  FROM normalized
  WHERE transaction_type IN (
    'Shipping label purchase',
    'Shipping label purchase adjustment',
    'Shipping label for return purchase'
  )
  AND order_id IS NOT NULL
  AND total_eur IS NOT NULL
)
SELECT
  order_id,
  MIN(transaction_date) AS transaction_date,
  SUM(-1 * total_eur) AS shipping_cost_eur,  -- positive expense
  ARRAY_AGG(DISTINCT transaction_type IGNORE NULLS) AS transaction_types,
  ARRAY_AGG(DISTINCT related_id IGNORE NULLS) AS related_ids,
  'temu' AS channel,
  'de' AS country,
  'eur' AS currency
FROM filtered
GROUP BY order_id;
