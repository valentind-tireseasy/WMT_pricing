# WalmartPricing NLC Pipeline — Process Overview

## What This Pipeline Does

Computes optimal Node Level Cost (NLC) prices for ~100K SKUs across Walmart B2B warehouse nodes, applies business rules and A/B tests, generates a DSV file for upload, and validates ingestion results. Unlike Amazon (price per SKU), Walmart prices at the **SKU-Node** level — each warehouse gets its own cost.

---

## Entry Points

- **Notebook (interactive):** `notebooks/01_nlc_pricing.ipynb` — parameter cells at top, execute sequentially
- **Programmatic:** `src/pipeline.py` → `run_pipeline(**kwargs)` — same parameters, end-to-end execution

### Pipeline Parameters

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `date_str` | today | Date for the pricing run |
| `test` | False | Include A/B test group pricing |
| `save` | True | Persist output files |
| `local_output` | True | Save to local `outputs/` instead of shared drive |
| `apply_rollbacks` | True | **[Optional]** Toggle rollback handling |
| `update_national_prices` | False | **[Optional]** Override national prices from external Excel |
| `upload_to_hybris` | False | **[Optional]** Auto-upload DSV via Selenium |
| `margin_test_start_dates` | None | Filter margin tests by start date |
| `rollbacks_path` | *(monthly)* | Path to rollbacks Excel (must update each month) |

---

## Pipeline Steps

### Step 1 — Data Collection (`src/data/loader.py`)

Loads all required data sources, driven by `config/data_sources.yaml`:

| Source | Origin | Notes |
|--------|--------|-------|
| Current DSV | Google Drive folder | Latest date-named CSV |
| Warehouse Node Mapping | Google Drive folder | Semicolon CSV, filtered to WalmartB2B + ENABLED |
| Walmart Item Report | Data Warehouse | Current offer prices by SKU |
| MAP Prices | Data Warehouse | Minimum Advertised Prices |
| Shipping Costs | Local Excel (`H:\...`) | Per-node shipping costs |
| Rollbacks | Local Excel (`H:\...`) | **Optional** — active rollbacks (End date > today) |
| Sales Data | Data Warehouse | 90-day WalmartB2B sales (`new_credentials=false`) |
| Inventory | Shared module (`pricing_module`) | 7-day lookback, excludes rollback SKUs |
| Tests Tracker | Local CSV (`H:\...`) | Current test assignments and margin history |

The DSV is split into **NLC rows** (have a Source/Identifier) and **National rows** (no Source). Sales are binned into 12 percentile categories.

---

### Step 2 — NLC Model: Two-Pass Computation (`src/models/nlc_model.py`)

Computes the optimal NLC price for each SKU-Node using a two-pass strategy.

**Pass 1 (primary, min_units=8):**
1. Filter inventory to nodes with Available >= 8 units AND >= Zero Out Threshold
2. Per SKU-Node-date: take minimum Purchase Price + FET; then per SKU-Node: take most recent date
3. Compute NLC at 8 margin levels: 6%, 8%, 11%, 12%, 13%, 14%, 15%, 20%
4. **NLC cascade** — use 11% if feasible → else 8% → else 6% → else N/A
5. **MAP check:** NLC must be < MAP × 0.95. **Walmart margin check:** WmMargin > 0 if no MAP

**Pass 2 (secondary, min_units=4):**
- Same logic but lower threshold (4 units). Only fills SKU-Nodes that got no result from Pass 1.

**Merge:** Pass 1 results take priority; Pass 2 fills gaps. Output is ~2.27M SKU-Node rows with final NLC price, margins, and target assignments.

---

### Step 3 — Apply Pricing Rules (`src/rules/pricing_rules.py`)

Categorizes SKU-Nodes into 5 update types (configured in `config/pricing_rules.yaml`):

1. **Wm Margin Split Test** — SKU-Nodes tagged "Wm margin split test". Sub-groups get 60% split, 50% split, or baseline pricing. Only updates if price change >= 1%.

2. **Brand Margin Test** — SKU-Nodes tagged "Margin test". Each sub-group (11%–15%) uses its corresponding margin column. Optionally filtered by `margin_test_start_dates`. Only updates if price change >= 1%.

3. **Low Price Updates** — SKU-Nodes with current NLC margin < 5.9%, excluding protected targets. Price increases to the cascaded Final NLC. Sub-categorized as "Not showing inventory" (< 4%) or "Below 6% margin".

4. **High Price Updates** — SKU-Nodes with current NLC margin > 20.3%, excluding protected targets. Price decreased to the 20% margin level.

5. **New SKU-Nodes** — SKU-Nodes not in the current DSV. Priced at the Final NLC.

**Protected targets** (excluded from rules 3–4): "Margin test", "Wm margin split test", "Shipping cost added".

---

### Step 4 — Build New DSV (`src/dsv/dsv_builder.py`)

Constructs the final DSV file starting from the current DSV:

1. **Start** with current DSV (~3.29M rows)
2. **[Optional] National Price Updates** (`update_national_prices=True`) — Override national-row prices from an external Excel file
3. **[Optional] Rollback Handling** (`apply_rollbacks=True`) — Remove all NLC rows for rollback SKUs; override national-row prices with rollback unit costs
4. **Apply all pricing rule updates** — Merge the ~75K updated rows from Step 3 by SKU-Node key (`{SKU}-{Source}`)
5. **Validate** — Compare new vs current prices, log counts of increases/decreases/unchanged

Output: DSV CSV with columns `SKU, Price, Minimum margin ("4%"), Source`.

---

### Step 5 — Update Tests Tracker (`src/tracker/tracker_updater.py`)

Maintains the master tracker with one row per SKU-Node:

1. **Refresh margins** — Drop stale margin columns, merge fresh values from Step 2 output (margin, date, sales category, in-stock flag)
2. **Append updates** — Add/replace rows for all SKU-Nodes that were updated or newly created in Step 3
3. **Deduplicate** — By (Product Code, Identifier), keeping the last entry

---

### Step 6 — Save Outputs

When `save=True`:

| Output | Local Path (`local_output=True`) | Shared Drive Path |
|--------|----------------------------------|-------------------|
| DSV CSV | `outputs/New WalmartB2B DSV to upload {date}.csv` | `H:\...\DSV Files\{YYYY-MM}\...` |
| Tracker CSV | `outputs/Final node level costs tracker.csv` | `H:\...\Final node level costs tracker.csv` |
| Tracker Backup | *(not created locally)* | `H:\...\Bk tracker\..._{date}.csv` |

---

### Step 7 — [Optional] Upload to Hybris (`src/dsv/hybris_uploader.py`)

When `upload_to_hybris=True`:
- Launches Selenium (Chrome), signs into hybris backoffice
- Navigates to DSV prices page, selects "WalmartB2B - EXTERNAL_WAREHOUSE"
- Uploads the DSV CSV, polls for completion (every 30s, up to 1 hour)
- Verifies Status=FINISHED, Result=SUCCESS

---

### Post-Upload — FTP Validation (`src/dsv/ftp_validator.py`)

Run **separately**, ~3 hours after upload via `run_ftp_validation(today_str)`:

1. Connect to Walmart FTP (`upload.tires-easy.com`)
2. Download today's `*_response*.xml` files
3. Parse ingestion status per line item (SUCCESS / DATA_ERROR)
4. Generate summary Excel with success/failure rates
5. **Alert** if failure rate exceeds 1.5%

Output: `H:\...\Price updates check\{date}\NLC Price update response summary {date}.xlsx`

---

## Optional Steps Summary

| Step | Toggle | Default | What It Does |
|------|--------|---------|--------------|
| Rollback handling | `apply_rollbacks` | True | Remove RB SKUs from NLC; apply RB prices to National |
| National price updates | `update_national_prices` | False | Override national prices from external Excel |
| Hybris upload | `upload_to_hybris` | False | Auto-upload DSV via Selenium |
| Tracker backup | `tracker_backup` | True (shared) | Dated backup of tracker before overwrite |
| Margin test date filter | `margin_test_start_dates` | None | Only process margin tests with matching start dates |

---

## Key Business Rules

- **Two-pass inventory:** Always Pass 1 (8 units) then Pass 2 (4 units) for gaps — never merge differently
- **NLC cascade order:** 11% → 8% → 6% → N/A (fixed)
- **MAP constraint:** NLC < MAP × 95%
- **1% price change minimum:** Updates below 1% change are discarded
- **Protected targets** are excluded from low/high margin rules
- **Tracker deduplication:** One row per SKU-Node, keep last entry
- **FTP alert threshold:** 1.5% failure rate

## Monthly Maintenance

- **Rollbacks path** must be updated each month (path includes month folder name)
- **DSV file naming** is date-based, handled automatically
- **Tracker** is overwritten each run; backup created on shared drive
