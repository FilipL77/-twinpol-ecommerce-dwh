# twinpol-ecommerce-dwh

Centralny Data Warehouse dla e-commerce (BigQuery + Looker Studio),
zaprojektowany jako skalowalna architektura pod wiele kanałów sprzedaży.

Aktualnie obsługiwany kanał:
- **TEMU_DE**

Repozytorium jest **single source of truth** dla:
- logiki biznesowej (SQL),
- struktury danych,
- zasad liczenia revenue, kosztów i marży.

---

## 🎯 Cel projektu

Celem projektu jest:
- jednoznaczne liczenie **revenue, COGS, kosztów i profitu**,
- pełna kontrola nad marżą per dzień i per produkt,
- oddzielenie:
  - ingestu danych,
  - logiki biznesowej,
  - warstwy raportowej,
- przygotowanie architektury „na lata” i pod kolejne kanały.

---

## 🧱 Architektura danych (high-level)
Google Sheets (RAW exports)
↓
BigQuery RAW tables
↓
BigQuery CLEAN / ETL
↓
BigQuery VIEWS (business logic)
↓
Looker Studio (dashboards)
**Zasada:**  
> Google Sheets = RAW only  
> BigQuery = cała logika  
> Looker = tylko prezentacja

---

## 📂 Źródła danych (Google Sheets)

### SALES — TEMU_DE
- Sheet: `IMPORT_TEMU_RAW`
- Zawiera:
  - pozycje zamówień,
  - cenę netto produktu,
  - kwotę zapłaconą przez klienta za wysyłkę,
  - ilości,
  - statusy zamówień.

### ADS — TEMU_DE
- Sheet: `ads_temu_raw`
- Dzienne dane reklamowe:
  - spend,
  - sprzedaż po cenie bazowej,
  - ROAS, ACOS,
  - orders, products,
  - impressions, clicks.

### SHIPPING COSTS — TEMU_DE
- Sheet: `import_temu_shipping_costs`
- Koszty wysyłki (shipping labels) ponoszone przez sprzedawcę:
  - poziom transakcji / order item,
  - wartości netto.

### MASTER DATA
- Sheet: `MAIN_DATABASE`
  - produkty,
  - SKU,
  - COGS,
  - nazwy produktów,
  - EAN.
- Sheet: `FX_RATES`
  - miesięczne kursy EUR → PLN.

---

## 🗃️ BigQuery – dataset i warstwy

Dataset:
twinpol-ecommerce.ecommerce_db
### RAW (mirror danych źródłowych)
- `sales_temu_raw`
- `ads_temu_raw`
- `shipping_costs_temu_raw`
- `products_raw`
- `fx_rates_raw`

RAW = brak transformacji, brak logiki biznesowej.

---

### CLEAN / ETL
- `ads_temu_clean`
- `shipping_costs_temu_clean`
- `products`
- `fx_rates_clean`

Charakterystyka:
- czyszczenie formatów liczb i dat,
- standaryzacja typów,
- przygotowanie danych do joinów.

---

### VIEWS (business / Looker-ready)

#### Kluczowe widoki:

- `sales_temu_view`
  - grain: **order item**
  - revenue = *(cena netto produktu + kwota zapłacona przez klienta za wysyłkę)*

- `sales_temu_profit_view`
  - revenue_pln
  - cogs_pln (join z `products`)
  - profit po COGS (bez ads i shipping)

- `shipping_costs_temu_daily_pln_view`
  - **1 rekord = 1 dzień**
  - koszt wysyłki w PLN

- `sales_temu_product_profit_final_view`
  - profit per SKU
  - uwzględnia COGS i alokację shipping cost

- `sales_temu_product_profit_daily_view`
  - agregacja dzienna per produkt (TOP produkty)

- `daily_temu_finance_view`
  - **główna tabela finansowa**
  - zawiera:
    - revenue,
    - COGS,
    - ads cost (PLN),
    - shipping cost,
    - profit final,
    - margin final.

---

## 💰 Definicje finansowe (OBOWIĄZUJĄCE)

### Revenue
Revenue jest liczone jako **pełna kwota zapłacona przez klienta**:
unit_revenue_eur
= cena netto produktu
	•	kwota zapłacona przez klienta za wysyłkę

line_revenue_eur
= unit_revenue_eur * quantity
Nie odejmujemy:
- prowizji platformy,
- podatków.

---

### Profit
- **Profit po COGS** (`sales_temu_profit_view`)
revenue_pln - cogs_pln
- **Profit final** (`daily_temu_finance_view`)
(revenue - cogs)
	•	ads_cost_pln
	•	shipping_cost_pln

- **Margin final**
profit_pln_final / revenue_pln
---

## 🔄 Automatyzacja (Apps Script)

Folder:
apps_script/temu_de_ingest/

Apps Script:
- pobiera dane z Google Sheets,
- ładuje je do BigQuery,
- **dopisywane są tylko nowe rekordy** (brak nadpisywania historii).

Funkcje:
- `run_hourly_ingest__temu_de_sales`
- `run_hourly_ingest__temu_de_ads`
- `run_hourly_ingest__temu_de_shipping_costs`

**WAŻNE:**
- Apps Script NIE zawiera logiki biznesowej,
- cała logika znajduje się w SQL (BigQuery).

---

## 🧪 QA / Sanity checks

Folder:
sql/qa/

Zawiera:
- sprawdzenie kompletności COGS (products),
- sprawdzenie pokrycia kursów FX,
- sanity checki używane przy zmianach ingestu.

---

## 🛑 Zasady projektu

- Repo = **single source of truth**
- Naprawiamy istniejące obiekty, **nie tworzymy duplikatów**
- RAW ≠ CLEAN ≠ VIEW
- Looker:
  - nie liczy logiki,
  - tylko prezentuje dane z VIEWS.

---

## 🔜 Kolejne etapy

- automatyczny ingest **master data (products, fx)**,
- kolejne kanały sprzedaży,
- unified multi-channel schema.

