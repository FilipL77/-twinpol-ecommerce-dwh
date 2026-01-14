# twinpol-ecommerce-dwh

Repozytorium do utrzymania logiki DWH (BigQuery + Looker Studio) dla kanałów e-commerce.
Na start obsługujemy kanał: **TEMU_DE**.

## Architektura (high-level)

**Źródła (Google Sheets)**
- `IMPORT_TEMU_RAW` (sales) – arkusz z eksportem sprzedaży Temu
- `ads_temu_raw` (ads) – arkusz z raportem reklam Temu (dzienne metryki)
- `import_temu_shipping_costs` (shipping labels) – arkusz z raportem kosztów etykiet / shipping kosztów

**BigQuery dataset**
- `twinpol-ecommerce.ecommerce_db`

**Warstwy danych (BQ)**
1) RAW (tabele docelowe ingestu z arkuszy)
   - `sales_temu_raw`
   - `ads_temu_raw`
   - `shipping_costs_temu_raw` *(uwaga: w projekcie historycznie bywa EXTERNAL – docelowo chcemy mieć zwykłą TABLE, żeby dało się MERGE i dopisywanie)*

2) CLEAN / ETL
   - `ads_temu_clean`
   - `shipping_costs_temu_clean`
   - `fx_rates_clean` (z `fx_rates_raw` -> `fx_rates_clean`)
   - `products` (Master Data: SKU + COGS)

3) VIEWS (do Lookera)
   - `sales_temu_view` – standaryzacja sprzedaży (SKU, daty, revenue eur)
   - `sales_temu_profit_view` – revenue_pln, cogs, profit_pln (po COGS), margin
   - `shipping_costs_temu_daily_pln_view` – dzienne koszty shipping label (PLN)
   - `sales_temu_product_profit_final_view` – final per SKU/dzień (revenue, cogs, shipping alloc, profit)
   - `sales_temu_product_profit_daily_view` – tabela TOP produktów (agregacje)
   - `daily_temu_finance_view` – agregacja dzienna (revenue, cogs, ads, shipping, profit_final, margin)

4) QA (kontrole jakości)
   - `qa_check__temu_de__cogs_completeness`
   - `qa_check__fx_rates__coverage_for_shipping_costs`
   - + ad-hoc sanity checks (opcjonalnie)

## Struktura repo

- `sql/views/` – widoki raportowe (używane w Looker Studio)
- `sql/etl/` – zapytania ETL/clean (tworzenie/odświeżanie tabel clean)
- `sql/qa/` – zapytania QA / sanity check
- `apps_script/temu_de_ingest/` – Google Apps Script do automatycznego ingestu z Sheets do BigQuery

## Ważne definicje (TEMU_DE)

### Revenue (definicja obowiązkowa)
Revenue na poziomie pozycji zamówienia = to co zapłacił klient:
- **AH** = cena netto za sztukę produktu (customer-paid, net)
- **AR** = kwota jaką klient zapłacił za wysyłkę

`unit_revenue_eur = AH + AR`
`line_revenue_eur = unit_revenue_eur * quantity`

W BigQuery w `sales_temu_view` pole `price` = `unit_revenue_eur`.
Pole `total_value` = `line_revenue_eur`.

### Profit
- `profit_pln` w `sales_temu_profit_view` = `revenue_pln - cogs_pln` (bez ads i bez shipping labels)
- `profit_pln_final` w `daily_temu_finance_view` = `(revenue - cogs) - ads_cost_pln - shipping_cost_pln`
- `margin_final` = `profit_pln_final / revenue_pln`

### FX (EUR->PLN)
- Kursy są liczone miesięcznie, trzymane w `fx_rates_clean`:
  - `month_start_date` (DATE)
  - `eur_pln_avg` (NUMERIC/FLOAT)

## Jak odtworzyć projekt (kolejność)

### 1) ETL / Clean
Uruchom kolejno zapytania z `sql/etl/`:
1. `etl_clean_fx_rates.sql`
2. `etl_clean_temu_ads.sql`
3. `etl_clean_temu_de_shipping_costs.sql`

### 2) Views (Looker)
Uruchom/odśwież widoki z `sql/views/` (kolejność sugerowana):
1. `sales_temu_view.sql`
2. `sales_temu_profit_view.sql`
3. `shipping_costs_temu_daily_pln_view.sql`
4. `sales_temu_product_profit_final_view.sql`
5. `sales_temu_product_profit_daily_view.sql`
6. `daily_temu_finance_view.sql`

### 3) QA
Uruchom zapytania z `sql/qa/` i sprawdź wyniki:
- brakujące COGS / brak SKU w `products`
- brak kursów FX dla miesięcy, w których jest shipping/ads

## Apps Script – automatyczny ingest

### Cel
Co godzinę (lub ręcznie) pobiera dane z 3 Google Sheets i dopisuje tylko nowe rekordy do tabel RAW w BigQuery.

### Konfiguracja
W `apps_script/temu_de_ingest/code.gs` ustaw:
- `PROJECT_ID = "twinpol-ecommerce"`
- `DATASET_ID = "ecommerce_db"`

**Sheet IDs**
- sales: `1Y1P2HxNUM4H9Jg5YC6SwZFwCgBc1Iw0b5d2vRpNT4s0`
- ads: `1i2eJntg15U6PEoEryJ_rq94I-cckQP0HVIA2be1fXsc`
- shipping: `1NuMPV5_Me4IcsXxoZiixsgWnaU3L_eZa7In_pfbBnts`

**Docelowe tabele**
- sales -> `sales_temu_raw`
- ads -> `ads_temu_raw`
- shipping -> `shipping_costs_temu_raw` (docelowo TABLE, nie EXTERNAL)

### Triggery
W Apps Script: Triggers → Add Trigger:
- function: `run_hourly_ingest__temu_de_sales` (czasowy, hourly)
- function: `run_hourly_ingest__temu_de_ads` (czasowy, hourly)
- function: `run_hourly_ingest__temu_de_shipping_costs` (czasowy, hourly)

### Uwaga dot. uprawnień
Skrypt wymaga uprawnień do:
- Google Sheets (odczyt)
- BigQuery (load + query/merge)

## Master Data (products, fx)
Master Data jest utrzymywana osobno (Sheets -> BQ) i zasila:
- `products` (SKU + COGS + nazwy)
- `fx_rates_clean` (miesięczne kursy EUR/PLN)

W kolejnych krokach automatyzujemy także ingest Master Data.

## Zasada „naprawiamy stare, nie tworzymy nowe”
- W BQ utrzymujemy stałe nazwy tabel i view.
- Zmiany robimy przez `CREATE OR REPLACE VIEW` / kontrolowane `CREATE OR REPLACE TABLE`.
- Repo jest „single source of truth” dla SQL i skryptów ingest.
