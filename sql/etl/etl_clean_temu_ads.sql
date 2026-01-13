CREATE OR REPLACE TABLE `ecommerce_db.ads_temu_clean` AS
WITH mapped AS (
  SELECT
    -- Date: "2025-12-03"
    SAFE.PARSE_DATE('%Y-%m-%d', string_field_0) AS data,

    -- Spend EUR: "24,61€" / "3.400,77€"
    SAFE_CAST(
      REPLACE(
        REPLACE(
          REPLACE(
            REPLACE(string_field_1, '€', ''),
          ' ', ''),
        '.', ''),         -- remove thousand dots
      ',', '.')           -- decimal comma -> dot
      AS FLOAT64
    ) AS wydatek_eur,

    -- Base sales EUR: "110,02€" / "16.280,13€"
    SAFE_CAST(
      REPLACE(
        REPLACE(
          REPLACE(
            REPLACE(string_field_2, '€', ''),
          ' ', ''),
        '.', ''),
      ',', '.')
      AS FLOAT64
    ) AS sprzedaz_bazowa_eur,

    -- ROAS: "4.47" (dot decimal)
    SAFE_CAST(REPLACE(string_field_3, ',', '.') AS FLOAT64) AS roas,

    -- ACOS: "22.36%" -> 0.2236
    SAFE_CAST(
      REPLACE(REPLACE(string_field_4, '%', ''), ',', '.')
      AS FLOAT64
    ) / 100.0 AS acos,

    -- CPA EUR: "2,24€"
    SAFE_CAST(
      REPLACE(
        REPLACE(
          REPLACE(
            REPLACE(string_field_5, '€', ''),
          ' ', ''),
        '.', ''),
      ',', '.')
      AS FLOAT64
    ) AS cpa_eur,

    -- Orders: "1,873" (thousand comma) OR "11"
    SAFE_CAST(REPLACE(REPLACE(string_field_6, ',', ''), ' ', '') AS INT64) AS zamowienia,

    -- Products: "1,981" OR "13"
    SAFE_CAST(REPLACE(REPLACE(string_field_7, ',', ''), ' ', '') AS INT64) AS produkty,

    -- Impressions: "1,969,389" OR "16,582"
    SAFE_CAST(REPLACE(REPLACE(string_field_8, ',', ''), ' ', '') AS INT64) AS wyswietlenia,

    -- Clicks: "42,405" OR "363"
    SAFE_CAST(REPLACE(REPLACE(string_field_9, ',', ''), ' ', '') AS INT64) AS klikniecia,

    -- CTR: "2.18%" -> 0.0218
    SAFE_CAST(
      REPLACE(REPLACE(string_field_10, '%', ''), ',', '.')
      AS FLOAT64
    ) / 100.0 AS ctr,

    -- CVR: "3.03%" -> 0.0303
    SAFE_CAST(
      REPLACE(REPLACE(string_field_11, '%', ''), ',', '.')
      AS FLOAT64
    ) / 100.0 AS cvr,

    -- Add to cart: "7,633" OR "56"
    SAFE_CAST(REPLACE(REPLACE(string_field_12, ',', ''), ' ', '') AS INT64) AS dodania_do_koszyka

  FROM `ecommerce_db.ads_temu_raw`
  WHERE TRUE
    -- usuń wiersz nagłówka
    AND string_field_0 IS NOT NULL
    AND string_field_0 != 'Data'
    -- usuń wiersz sumaryczny
    AND NOT STARTS_WITH(string_field_0, 'Łącznie')
),
fx AS (
  SELECT
    date AS data,
    eur_pln
  FROM `ecommerce_db.fx_rates`
)

SELECT
  m.data,

  -- zachowujemy jawnie EUR i PLN
  m.wydatek_eur,
  ROUND(m.wydatek_eur * f.eur_pln, 2) AS wydatek_pln,

  m.sprzedaz_bazowa_eur,
  ROUND(m.sprzedaz_bazowa_eur * f.eur_pln, 2) AS sprzedaz_bazowa_pln,

  m.roas,
  m.acos,
  m.cpa_eur,
  ROUND(m.cpa_eur * f.eur_pln, 2) AS cpa_pln,

  m.zamowienia,
  m.produkty,
  m.wyswietlenia,
  m.klikniecia,
  m.ctr,
  m.cvr,
  m.dodania_do_koszyka

FROM mapped m
LEFT JOIN fx f
  ON m.data = f.data
WHERE m.data IS NOT NULL;
