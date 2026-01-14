/***** CONFIG *****/
var PROJECT_ID = 'twinpol-ecommerce';
var DATASET_ID = 'ecommerce_db';
var LOCATION = 'EU';

/*** SOURCE SPREADSHEETS (IDs you gave) ***/
var SALES_SPREADSHEET_ID = '1Y1P2HxNUM4H9Jg5YC6SwZFwCgBc1Iw0b5d2vRpNT4s0';
var ADS_SPREADSHEET_ID = '1i2eJntg15U6PEoEryJ_rq94I-cckQP0HVIA2be1fXsc';
var SHIPPING_SPREADSHEET_ID = '1NuMPV5_Me4IcsXxoZiixsgWnaU3L_eZa7In_pfbBnts';

/*** SHEET TABS (exact names) ***/
var SALES_SHEET_TAB = 'IMPORT_TEMU_RAW';
var ADS_SHEET_TAB = 'ads_temu_raw';
var SHIPPING_SHEET_TAB = 'import_temu_shipping_costs';

/*** TARGET TABLES ***/
var SALES_TARGET_TABLE = 'sales_temu_raw';
var ADS_TARGET_TABLE = 'ads_temu_raw_ingested';
var SHIPPING_TARGET_TABLE = 'shipping_costs_temu_raw_ingested';

/*** STAGING TABLES ***/
var SALES_STAGING_TABLE = 'sales_temu_raw_staging';
var ADS_STAGING_TABLE = 'ads_temu_raw_staging';
var SHIPPING_STAGING_TABLE = 'shipping_costs_temu_raw_staging';

/*** SHIPPING CANONICAL COLS (23) - WITHOUT ingested_at ***/
var SHIPPING_COLS = [
  'date_time',
  'transaction_type',
  'related_id',
  'order_id',
  'order_item_id',
  'sku',
  'sku_id',
  'quantity',
  'ship_city',
  'ship_state',
  'retail_price',
  'platform_discount',
  'seller_discount',
  'service_fee__tax_incl__',
  'platform_incentive',
  'subtotal',
  'shipping',
  'platform_incentive___shipping',
  'product_tax',
  'shipping_tax',
  'others',
  'total',
  'currency'
];

/***** RUNNERS *****/

function run_hourly_ingest__temu_de_sales() {
  Logger.log('START TEMU_DE SALES INGEST');

  var ss = SpreadsheetApp.openById(SALES_SPREADSHEET_ID);
  var sheet = ss.getSheetByName(SALES_SHEET_TAB);
  if (!sheet) throw new Error('Sheet "' + SALES_SHEET_TAB + '" not found (SALES spreadsheet).');

  var obj = mapSales_IMPORT_TEMU_RAW_toCanonical_(sheet);

  ingestGeneric_(
    'sales',
    obj.headers,
    obj.rows,
    SALES_STAGING_TABLE,
    SALES_TARGET_TABLE,
    getSchema_sales_temu_raw_(),
    getMergeSql_sales_(),
    true,
    true // include ingested_at in CSV
  );
}

function run_hourly_ingest__temu_de_ads() {
  Logger.log('START TEMU_DE ADS INGEST');

  var ss = SpreadsheetApp.openById(ADS_SPREADSHEET_ID);
  var sheet = ss.getSheetByName(ADS_SHEET_TAB);
  if (!sheet) throw new Error('Sheet "' + ADS_SHEET_TAB + '" not found (ADS spreadsheet).');

  var obj = mapGenericSheetToCanonical_(sheet);
  var keyCol = detectAdsKeyColumn_(obj.headers);
  Logger.log('ADS key column: ' + keyCol);

  ensureBaseTableExists_(ADS_TARGET_TABLE, buildAllStringSchema_(appendIngestedAtHeader_(obj.headers)));

  ingestGeneric_(
    'ads',
    obj.headers,
    obj.rows,
    ADS_STAGING_TABLE,
    ADS_TARGET_TABLE,
    buildAllStringSchema_(appendIngestedAtHeader_(obj.headers)),
    getMergeSql_ads_(keyCol),
    false,
    true // include ingested_at in CSV (STRING ok)
  );
}

function run_hourly_ingest__temu_de_shipping_costs() {
  Logger.log('START TEMU_DE SHIPPING COSTS INGEST');

  var ss = SpreadsheetApp.openById(SHIPPING_SPREADSHEET_ID);
  var sheet = ss.getSheetByName(SHIPPING_SHEET_TAB);
  if (!sheet) throw new Error('Sheet "' + SHIPPING_SHEET_TAB + '" not found (SHIPPING spreadsheet).');

  var obj = mapGenericSheetToCanonical_(sheet);

  // Ensure target exists with correct schema (23 strings + ingested_at timestamp)
  ensureBaseTableExists_(SHIPPING_TARGET_TABLE, getSchema_shipping_target_());

  // IMPORTANT: staging has ONLY 23 columns (no ingested_at)
  ingestGeneric_(
    'shipping_costs',
    obj.headers,
    obj.rows,
    SHIPPING_STAGING_TABLE,
    SHIPPING_TARGET_TABLE,
    buildAllStringSchema_(obj.headers),       // staging all strings
    getMergeSql_shipping_costs_fixed_(),      // explicit INSERT with ingested_at
    false,
    false // do NOT include ingested_at in CSV
  );
}

/***** GENERIC INGEST *****/

function ingestGeneric_(entity, canonicalHeaders, canonicalRows, stagingTable, targetTable, schema, mergeSql, ensureIngestedAtOnTarget, includeIngestedAtInCsv) {
  if (!canonicalHeaders || !canonicalRows) throw new Error('Missing headers/rows for ' + entity);

  Logger.log('Rows read: ' + canonicalRows.length);
  if (canonicalRows.length === 0) throw new Error('0 rows read for ' + entity);

  if (ensureIngestedAtOnTarget) ensureTargetHasIngestedAt_(targetTable);

  var headers = canonicalHeaders.slice(0);
  var rows = canonicalRows;

  if (includeIngestedAtInCsv) {
    headers = appendIngestedAtHeader_(headers);
    var nowIso = new Date().toISOString();
    var rows2 = [];
    for (var i = 0; i < rows.length; i++) {
      var r = rows[i].slice(0);
      r.push(nowIso);
      rows2.push(r);
    }
    rows = rows2;
  }

  var csv = buildCsv_(headers, rows);
  var blob = Utilities.newBlob(csv, 'text/csv', entity + '_staging.csv');

  bqLoadCsvToStaging_(stagingTable, headers, blob, schema);
  runBqQuery_(mergeSql);

  Logger.log('DONE: ' + entity);
}

function appendIngestedAtHeader_(headers) {
  var out = headers.slice(0);
  if (indexOf_(out, 'ingested_at') === -1) out.push('ingested_at');
  return out;
}

/***** BIGQUERY IO *****/

function ensureTargetHasIngestedAt_(targetTable) {
  var sql =
    'ALTER TABLE `' + PROJECT_ID + '.' + DATASET_ID + '.' + targetTable + '` ' +
    'ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMP;';
  runBqQuery_(sql);
}

function ensureBaseTableExists_(tableId, schemaFields) {
  var fq = PROJECT_ID + '.' + DATASET_ID + '.' + tableId;
  var createSql = 'CREATE TABLE IF NOT EXISTS `' + fq + '` (\n' + schemaFieldsToDDL_(schemaFields) + '\n);';
  runBqQuery_(createSql);
}

function schemaFieldsToDDL_(fields) {
  var parts = [];
  for (var i = 0; i < fields.length; i++) {
    var f = fields[i];
    var t = String(f.type || 'STRING').toUpperCase();
    parts.push('  ' + f.name + ' ' + t);
  }
  return parts.join(',\n');
}

function bqLoadCsvToStaging_(stagingTable, headers, csvBlob, schemaOverride) {
  runBqQuery_('DROP TABLE IF EXISTS `' + PROJECT_ID + '.' + DATASET_ID + '.' + stagingTable + '`');

  var job = {
    configuration: {
      load: {
        destinationTable: { projectId: PROJECT_ID, datasetId: DATASET_ID, tableId: stagingTable },
        schema: { fields: schemaOverride ? schemaOverride : buildAllStringSchema_(headers) },
        sourceFormat: 'CSV',
        writeDisposition: 'WRITE_TRUNCATE',
        skipLeadingRows: 1,
        fieldDelimiter: ',',
        quote: '"',
        allowQuotedNewlines: true,
        encoding: 'UTF-8'
      }
    }
  };

  var insertJob = BigQuery.Jobs.insert(job, PROJECT_ID, csvBlob);
  waitForJobDone_(insertJob.jobReference.jobId);

  Logger.log('Loaded staging: ' + PROJECT_ID + ':' + DATASET_ID + '.' + stagingTable);
}

function runBqQuery_(sql) {
  var request = { query: sql, useLegacySql: false, location: LOCATION };
  var resp = BigQuery.Jobs.query(request, PROJECT_ID);
  waitForJobDone_(resp.jobReference.jobId);
  return resp.jobReference.jobId;
}

function waitForJobDone_(jobId) {
  for (var i = 0; i < 120; i++) {
    var job = BigQuery.Jobs.get(PROJECT_ID, jobId, { location: LOCATION });
    var state = job.status && job.status.state ? job.status.state : '';
    if (state === 'DONE') {
      if (job.status.errorResult) throw new Error('BigQuery job error: ' + JSON.stringify(job.status.errorResult));
      return;
    }
    Utilities.sleep(1000);
  }
  throw new Error('BigQuery job timeout: ' + jobId);
}

/***** SALES MAPPER (IMPORT_TEMU_RAW) *****/

function mapSales_IMPORT_TEMU_RAW_toCanonical_(sheet) {
  var values = sheet.getDataRange().getValues();
  if (!values || values.length < 2) throw new Error('No data rows found.');

  function colIndex_(letters) {
    var s = String(letters).toUpperCase();
    var n = 0;
    for (var i = 0; i < s.length; i++) n = n * 26 + (s.charCodeAt(i) - 64);
    return n - 1;
  }

  var IDX_ITEM_PRICE_AH = colIndex_('AH');
  var IDX_CUST_SHIP_AR = colIndex_('AR');

  var IDX_ORDER_ID = colIndex_('A');
  var IDX_SKU = colIndex_('F');
  var IDX_PRODUCT_NAME = colIndex_('E');
  var IDX_QTY = colIndex_('D');
  var IDX_STATUS = colIndex_('H');
  var IDX_PURCHASE_DT = colIndex_('G');

  var headers = [
    'order_id',
    'sku_raw',
    'product_name_raw',
    'quantity_raw',
    'item_price_eur_raw',
    'customer_shipping_eur_raw',
    'order_status_raw',
    'purchase_datetime_raw'
  ];

  var out = [];
  for (var r = 1; r < values.length; r++) {
    var row = values[r];
    var orderId = safeToString_(row[IDX_ORDER_ID]);
    if (!orderId) continue;

    out.push([
      orderId,
      safeToString_(row[IDX_SKU]),
      safeToString_(row[IDX_PRODUCT_NAME]),
      safeToString_(row[IDX_QTY]),
      safeToString_(row[IDX_ITEM_PRICE_AH]),
      safeToString_(row[IDX_CUST_SHIP_AR]),
      safeToString_(row[IDX_STATUS]),
      safeToString_(row[IDX_PURCHASE_DT])
    ]);
  }

  return { headers: headers, rows: out };
}

/***** GENERIC MAPPER (ADS / SHIPPING) *****/

function mapGenericSheetToCanonical_(sheet) {
  var values = sheet.getDataRange().getValues();
  if (!values || values.length < 2) throw new Error('No data rows found.');

  var rawHeaders = values[0].map(function (h) { return String(h).trim(); });
  var headers = canonicalizeHeaders_(rawHeaders);

  var rows = [];
  for (var i = 1; i < values.length; i++) {
    var row = values[i];
    var out = [];
    for (var c = 0; c < headers.length; c++) out.push(safeToString_(row[c]));
    if (!isAllEmpty_(out)) rows.push(out);
  }

  return { headers: headers, rows: rows };
}

/***** MERGE SQL *****/

function getMergeSql_sales_() {
  var T = '`' + PROJECT_ID + '.' + DATASET_ID + '.' + SALES_TARGET_TABLE + '`';
  var S = '`' + PROJECT_ID + '.' + DATASET_ID + '.' + SALES_STAGING_TABLE + '`';

  return ''
    + 'MERGE ' + T + ' T\n'
    + 'USING (\n'
    + '  SELECT * EXCEPT(rn)\n'
    + '  FROM (\n'
    + '    SELECT S.*,\n'
    + '      ROW_NUMBER() OVER (\n'
    + '        PARTITION BY order_id, IFNULL(sku_raw, \'\'), IFNULL(purchase_datetime_raw, \'\')\n'
    + '        ORDER BY ingested_at DESC\n'
    + '      ) AS rn\n'
    + '    FROM ' + S + ' S\n'
    + '  )\n'
    + '  WHERE rn = 1\n'
    + ') S\n'
    + 'ON T.order_id = S.order_id\n'
    + 'AND IFNULL(T.sku_raw, \'\') = IFNULL(S.sku_raw, \'\')\n'
    + 'AND IFNULL(T.purchase_datetime_raw, \'\') = IFNULL(S.purchase_datetime_raw, \'\')\n'
    + 'WHEN MATCHED THEN UPDATE SET\n'
    + '  product_name_raw = S.product_name_raw,\n'
    + '  quantity_raw = S.quantity_raw,\n'
    + '  item_price_eur_raw = S.item_price_eur_raw,\n'
    + '  customer_shipping_eur_raw = S.customer_shipping_eur_raw,\n'
    + '  order_status_raw = S.order_status_raw,\n'
    + '  ingested_at = S.ingested_at\n'
    + 'WHEN NOT MATCHED THEN\n'
    + '  INSERT (order_id, sku_raw, product_name_raw, quantity_raw, item_price_eur_raw, customer_shipping_eur_raw, order_status_raw, purchase_datetime_raw, ingested_at)\n'
    + '  VALUES (S.order_id, S.sku_raw, S.product_name_raw, S.quantity_raw, S.item_price_eur_raw, S.customer_shipping_eur_raw, S.order_status_raw, S.purchase_datetime_raw, S.ingested_at);\n';
}

function detectAdsKeyColumn_(headers) {
  if (indexOf_(headers, 'data') !== -1) return 'data';
  if (indexOf_(headers, 'date') !== -1) return 'date';
  throw new Error('ADS sheet has no "data" or "date" column. Headers: ' + headers.join(', '));
}

function getMergeSql_ads_(keyCol) {
  var T = '`' + PROJECT_ID + '.' + DATASET_ID + '.' + ADS_TARGET_TABLE + '`';
  var S = '`' + PROJECT_ID + '.' + DATASET_ID + '.' + ADS_STAGING_TABLE + '`';

  return ''
    + 'MERGE ' + T + ' T\n'
    + 'USING ' + S + ' S\n'
    + 'ON IFNULL(T.' + keyCol + ', \'\') = IFNULL(S.' + keyCol + ', \'\')\n'
    + 'WHEN MATCHED THEN UPDATE SET ingested_at = S.ingested_at\n'
    + 'WHEN NOT MATCHED THEN INSERT ROW;\n';
}

/*** ✅ SHIPPING MERGE: explicit INSERT to add ingested_at ***/
function getMergeSql_shipping_costs_fixed_() {
  var T = '`' + PROJECT_ID + '.' + DATASET_ID + '.' + SHIPPING_TARGET_TABLE + '`';
  var S = '`' + PROJECT_ID + '.' + DATASET_ID + '.' + SHIPPING_STAGING_TABLE + '`';

  var colList = SHIPPING_COLS.join(', ');
  var sColList = SHIPPING_COLS.map(function(c){ return 'S.' + c; }).join(', ');

  // dedupe staging to avoid "MERGE must match at most one"
  return ''
    + 'MERGE ' + T + ' T\n'
    + 'USING (\n'
    + '  SELECT * EXCEPT(rn)\n'
    + '  FROM (\n'
    + '    SELECT S.*,\n'
    + '      ROW_NUMBER() OVER (\n'
    + '        PARTITION BY COALESCE(CAST(date_time AS STRING), \'\'), COALESCE(CAST(transaction_type AS STRING), \'\'), COALESCE(CAST(order_id AS STRING), \'\'), COALESCE(CAST(order_item_id AS STRING), \'\')\n'
    + '        ORDER BY 1\n'
    + '      ) AS rn\n'
    + '    FROM ' + S + ' S\n'
    + '  )\n'
    + '  WHERE rn = 1\n'
    + ') S\n'
    + 'ON COALESCE(CAST(T.date_time AS STRING), \'\') = COALESCE(CAST(S.date_time AS STRING), \'\')\n'
    + 'AND COALESCE(CAST(T.transaction_type AS STRING), \'\') = COALESCE(CAST(S.transaction_type AS STRING), \'\')\n'
    + 'AND COALESCE(CAST(T.order_id AS STRING), \'\') = COALESCE(CAST(S.order_id AS STRING), \'\')\n'
    + 'AND COALESCE(CAST(T.order_item_id AS STRING), \'\') = COALESCE(CAST(S.order_item_id AS STRING), \'\')\n'
    + 'WHEN MATCHED THEN UPDATE SET ingested_at = CURRENT_TIMESTAMP()\n'
    + 'WHEN NOT MATCHED THEN\n'
    + '  INSERT (' + colList + ', ingested_at)\n'
    + '  VALUES (' + sColList + ', CURRENT_TIMESTAMP());\n';
}

/***** SCHEMAS *****/

function getSchema_sales_temu_raw_() {
  return [
    { name: 'order_id', type: 'STRING' },
    { name: 'sku_raw', type: 'STRING' },
    { name: 'product_name_raw', type: 'STRING' },
    { name: 'quantity_raw', type: 'STRING' },
    { name: 'item_price_eur_raw', type: 'STRING' },
    { name: 'customer_shipping_eur_raw', type: 'STRING' },
    { name: 'order_status_raw', type: 'STRING' },
    { name: 'purchase_datetime_raw', type: 'STRING' },
    { name: 'ingested_at', type: 'TIMESTAMP' }
  ];
}

function getSchema_shipping_target_() {
  // target: same 23 cols as staging (STRING) + ingested_at TIMESTAMP
  var fields = [];
  for (var i = 0; i < SHIPPING_COLS.length; i++) fields.push({ name: SHIPPING_COLS[i], type: 'STRING' });
  fields.push({ name: 'ingested_at', type: 'TIMESTAMP' });
  return fields;
}

function buildAllStringSchema_(headers) {
  var fields = [];
  for (var i = 0; i < headers.length; i++) fields.push({ name: headers[i], type: 'STRING' });
  return fields;
}

/***** CSV + HELPERS *****/

function buildCsv_(headers, rows) {
  var lines = [];
  lines.push(headers.map(csvEscape_).join(','));
  for (var i = 0; i < rows.length; i++) {
    var out = [];
    for (var c = 0; c < headers.length; c++) out.push(csvEscape_(rows[i][c]));
    lines.push(out.join(','));
  }
  return lines.join('\n');
}

function csvEscape_(v) {
  var s = v === null || v === undefined ? '' : String(v);
  s = s.replace(/\r\n/g, '\n').replace(/\r/g, '\n');
  if (s.indexOf('"') !== -1) s = s.replace(/"/g, '""');
  if (s.indexOf(',') !== -1 || s.indexOf('\n') !== -1 || s.indexOf('"') !== -1) s = '"' + s + '"';
  return s;
}

function canonicalizeHeaders_(rawHeaders) {
  var out = [];
  for (var i = 0; i < rawHeaders.length; i++) out.push(canonicalizeHeader_(rawHeaders[i]));
  return out;
}

function canonicalizeHeader_(h) {
  var s = safeToString_(h).toLowerCase();
  s = s.replace(/ą/g,'a').replace(/ć/g,'c').replace(/ę/g,'e').replace(/ł/g,'l').replace(/ń/g,'n').replace(/ó/g,'o').replace(/ś/g,'s').replace(/ż/g,'z').replace(/ź/g,'z');
  s = s.replace(/[\s\-\/]+/g, '_');
  s = s.replace(/[^a-z0-9_]/g, '');
  if (/^[0-9]/.test(s)) s = 'col_' + s;
  if (!s) s = 'col_unnamed';
  return s;
}

function safeToString_(v) {
  if (v === null || v === undefined) return '';
  return String(v).trim();
}

function isAllEmpty_(arr) {
  for (var i = 0; i < arr.length; i++) if (String(arr[i] || '').trim() !== '') return false;
  return true;
}

function indexOf_(arr, val) {
  for (var i = 0; i < arr.length; i++) if (arr[i] === val) return i;
  return -1;
}
