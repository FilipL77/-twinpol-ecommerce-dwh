CREATE OR REPLACE TABLE `ecommerce_db.ads_temu_clean` AS
SELECT
  -- Kolumna 1 (Indeks 0): Data
  SAFE_CAST(string_field_0 AS DATE) AS data,

  -- Kolumna 2 (Indeks 1): Wydatek
  CAST(REPLACE(REPLACE(REPLACE(string_field_1, '€', ''), '.', ''), ',', '.') AS FLOAT64) AS wydatek,

  -- Kolumna 3 (Indeks 2): Sprzedaż
  CAST(REPLACE(REPLACE(REPLACE(string_field_2, '€', ''), '.', ''), ',', '.') AS FLOAT64) AS sprzedaz_bazowa,

  -- Kolumna 4 (Indeks 3): ROAS
  SAFE_CAST(string_field_3 AS FLOAT64) AS roas,

  -- Kolumna 5 (Indeks 4): ACOS
  CAST(REPLACE(string_field_4, '%', '') AS FLOAT64) AS acos,

  -- Kolumna 6 (Indeks 5): CPA (Koszt za transakcję)
  CAST(REPLACE(REPLACE(REPLACE(string_field_5, '€', ''), '.', ''), ',', '.') AS FLOAT64) AS cpa,

  -- Kolumna 7 (Indeks 6): Zamówienia
  CAST(REPLACE(string_field_6, ',', '') AS INT64) AS zamowienia,

  -- Kolumna 8 (Indeks 7): Produkty
  CAST(REPLACE(string_field_7, ',', '') AS INT64) AS produkty,

  -- Kolumna 9 (Indeks 8): Wyświetlenia
  CAST(REPLACE(string_field_8, ',', '') AS INT64) AS wyswietlenia,

  -- Kolumna 10 (Indeks 9): Kliknięcia
  CAST(REPLACE(string_field_9, ',', '') AS INT64) AS klikniecia,

  -- Kolumna 11 (Indeks 10): CTR
  CAST(REPLACE(string_field_10, '%', '') AS FLOAT64) AS ctr,

  -- Kolumna 12 (Indeks 11): CVR
  CAST(REPLACE(string_field_11, '%', '') AS FLOAT64) AS cvr,

  -- Kolumna 13 (Indeks 12): Dodania do koszyka
  CAST(REPLACE(string_field_12, ',', '') AS INT64) AS dodania_do_koszyka

FROM `ecommerce_db.ads_temu_raw`
WHERE 
  -- Pomijamy wiersz nagłówkowy ("Data") oraz wiersz podsumowania ("Łącznie...")
  string_field_0 NOT IN ('Data') 
  AND string_field_0 NOT LIKE 'Łącznie%'
  AND string_field_0 IS NOT NULL;
