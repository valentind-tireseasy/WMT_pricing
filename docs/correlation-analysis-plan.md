# Correlation Analysis Sub-Project — Implementation Plan

**Project:** WalmartPricing NLC Pipeline
**Sub-project:** SKU-Node Correlation Analysis
**Date:** 2026-03-25
**Deliverable:** Jupyter notebook at `notebooks/correlation_analysis.ipynb`

---

## 1. Context & Objective

The pricing team needs to understand what drives sales at the SKU-Node level on Walmart B2B. Currently, pricing decisions (NLC cascade, margin tests, split tests) are made based on cost/margin thresholds, but there is no systematic analysis of how price changes, inventory coverage, margin levels, and other factors correlate with actual sales volume.

**Business questions:**
- How do NLC price changes affect subsequent sales volume?
- What factors (price, margin, inventory, brand, geography) most influence sales?
- What margin levels maximize total revenue/profit across SKU-Nodes?

**Approach:** Build a daily SKU-Node-Date dataset with ~20 features, then run broad exploratory correlation and statistical analysis.

---

## 2. Dataset Specification

### 2.1 Granularity

**One row = one SKU-Node-Date** (daily)

- **Date range:** Parameterized, default 30 days
- **SKU filter:** Top SKUs accounting for 90% of qty sold over past 2 months
- **Node filter:** All nodes with at least 1 sale for those SKUs
- **Rollback exclusion:** SKUs in active rollbacks (End date > analysis start) are excluded
- **Expected size:** ~5,000 SKU-Nodes x 30 days = ~150,000 rows

### 2.2 Core Columns

| # | Column | Source | Computation |
|---|--------|--------|-------------|
| 1 | SKU | Sales query | Product Code |
| 2 | Node | Sales query | externalwarehouseid (= Identifier) |
| 3 | Date | Generated | Daily within analysis window |
| 4 | Qty sold | DW: `warehouse.vw_virtual_node_tracker` | Sum of `quantity` per SKU-Node-Date |
| 4b | Revenue | DW: `warehouse.vw_virtual_node_tracker` | Sum of `total_inv_amount` per SKU-Node-Date |
| 4c | Profit | DW: `warehouse.vw_virtual_node_tracker` | Sum of `profit` per SKU-Node-Date |
| 5 | Cost to Walmart | Google Drive DSV files | Price from DSV; national fallback if no node-specific price |
| 6 | Walmart offer price | DW: `pricing_tests.walmart_item_report` | `offer_price` where status = PUBLISHED |
| 7 | Walmart margin | Computed | `(offer_price - cost_to_walmart) / offer_price` |
| 8 | TE Margin | Computed | `(cost_to_walmart - min_purchase_price_fet) / cost_to_walmart` |
| 9 | Brand | Derived | First 4 characters of Product Code |
| 10 | Size | DW/Inventory | Best-effort, not central to analysis |
| 11 | Town | Warehouse Addresses CSV | City of warehouse node |
| 12 | State | Warehouse Addresses CSV | State of warehouse node |
| 13 | Can show inv? | Inventory (pricing_module) | 1 if inventory available at node on that date, else 0 |
| 14 | Min Purchase Price + FET | Inventory | Min daily purchase price at node |
| 15 | Shipping cost | Local Excel | Per-node shipping cost |
| 16 | MAP | DW: `pricing_tests.map_prices` | Minimum Advertised Price |

### 2.3 Additional Variables (Claude-suggested, user-approved)

| # | Column | Computation |
|---|--------|-------------|
| 17 | Day of week | `date.dt.dayofweek` (0=Mon, 6=Sun) |
| 18 | MAP proximity | `cost_to_walmart / (MAP * 0.95)` — how close to MAP floor; NaN if no MAP |
| 19 | Active nodes per SKU | Count of nodes with `can_show_inv=1` per SKU per date |
| 20 | Days since last price change | Days since `cost_to_walmart` changed for that SKU-Node (from DSV history) |

### 2.4 Rolling 7-Day Comparison Columns

For each of **qty_sold, TE margin, cost_to_walmart, offer_price, walmart_margin**:

- `{metric}_7d_avg` — Mean of prior 7 days for that SKU-Node (excludes current day)
- `{metric}_vs_7d` — Absolute delta: today's value minus 7-day average
- `{metric}_vs_7d_pct` — Percentage change vs 7-day average

---

## 3. Data Sources & Loading Strategy

### 3.1 Sales Data

- **Source:** `warehouse.vw_virtual_node_tracker` via DataLoader
- **Query:** Existing config `dw_walmart_sales` with `start_date` parameter
- **Credential mode:** `new_credentials=false` (warehouse.* schema)
- **Two loads:**
  1. 2-month lookback for SKU filtering (top 90%)
  2. Analysis window + 7-day warmup for rolling averages

### 3.2 DSV Files (Cost to Walmart)

- **Source:** Google Drive folder `1piuawZRpppmoD-Qdkd1IUj3x4rs-LKny`
- **Strategy:** List all files, parse dates from filenames, load only those within range (~6-8 files)
- **Assignment:** For each analysis date, use most recent DSV on or before that date (`merge_asof`)
- **National fallback:** If no node-specific price (Source = node ID), use row where Source is null
- **Rate limiting:** 0.5s between API calls (handled by adapter)

### 3.3 Walmart Item Report (Offer Prices)

- **Source:** `pricing_tests.walmart_item_report` via DataLoader
- **Strategy:** Single snapshot for END_DATE (gets `MAX(date) <= date_str`)
- **Assumption:** Offer prices are relatively stable over 30 days (documented in notebook)
- **Filter:** `1p_offer_status = 'PUBLISHED'`

### 3.4 Inventory

- **Source:** `pricing_module.get_inventory()` shared module (lazy import)
- **Strategy:** Sample every 4 days (~8 calls instead of 30) to manage runtime
- **Processing:** Filter Available >= 4 (secondary threshold) AND >= Zero Out Threshold
- **Forward-fill:** Assign to all analysis dates from nearest inventory date
- **Runtime:** ~5-15 minutes (dominant cost of the notebook)

### 3.5 Static Reference Data

| Data | Source | Load Method |
|------|--------|-------------|
| MAP prices | `pricing_tests.map_prices` | `loader.load("dw_map_prices")` |
| Shipping costs | Local Excel | `loader.load("shipping_costs_by_node")` |
| Warehouse node mapping | Google Drive folder | `loader.load("warehouse_node_mapping")` |
| Warehouse addresses | Local CSV | `pd.read_csv(WAREHOUSE_ADDRESSES_PATH)` |
| Rollbacks | Local Excel | `loader.load("rollbacks")` (optional) |

---

## 4. Join Chain for City Mapping

```
Sales.externalwarehouseid = Identifier (node ID)
        | (warehouse_node_mapping table)
Warehouse Code
        | (Warehouse Addresses CSV)
Code -> Town, State
```

---

## 5. Notebook Cell Structure

### Section 1: Parameters & Setup
- Configurable parameters at top of notebook
- Imports (pandas, numpy, matplotlib, seaborn, scipy, statsmodels)
- Project path setup, DataLoader initialization

### Section 2: SKU Filtering
- Load 2-month sales, compute cumulative qty %, select top 90%
- Optionally exclude rollback SKUs

### Section 3: Daily Sales
- Load sales for analysis window + 7-day warmup
- Filter to top SKUs, aggregate by SKU-Node-Date

### Section 4: Date Scaffold
- Cross-join unique SKU-Nodes x date range
- Left-join sales (fill missing with 0)

### Section 5: DSV History
- List and load DSV files from Google Drive
- Build date-stamped price table
- Assign cost_to_walmart via merge_asof with national fallback

### Section 6: Walmart Offer Prices
- Load item report snapshot
- Join to scaffold on Product Code

### Section 7: Supporting Data
- MAP, shipping costs, warehouse mapping, addresses

### Section 8: Inventory
- Lazy import pricing_module
- Load sampled inventory dates
- Process into can_show_inv flag + min_purchase_price_fet
- Forward-fill to all dates

### Section 9: Assemble Master DataFrame
- Merge all sources
- Compute derived columns (margins, brand, day_of_week, MAP proximity, active nodes, days since price change)

### Section 10: Rolling Comparisons
- 7-day rolling averages with shift(1)
- Delta and percentage change columns
- Trim warmup period

### Section 11: Exploratory Data Analysis
- Dataset summary and missing values
- Full Spearman correlation matrix heatmap
- Top correlations with qty_sold (bar chart)
- Scatter plots for top 6 features vs qty_sold
- Distribution plots for key variables

### Section 12: Statistical Tests
- Price change impact on sales (Mann-Whitney U)
- Margin level vs revenue (decile binning)
- Inventory availability vs sales
- OLS regression (statsmodels summary)

### Section 13: Geographic & Brand Analysis
- Sales by state
- Top 20 brands by volume and margin
- Node distribution breadth vs sales

### Section 14: Summary & Cleanup
- Key findings markdown cell
- Export assembled dataset as parquet for reuse (`outputs/correlation_dataset_{date}.parquet`)
- Close loader

---

## 6. Edge Cases

| Issue | Handling |
|-------|----------|
| Not every day has a DSV file | Forward-fill from most recent DSV via `merge_asof(direction="backward")` |
| No node-specific DSV price | Fall back to national price (Source = NaN row) |
| Zero-inflated sales distribution | Use Spearman correlation; log-transform with `np.log1p()`; analyze non-zero subset separately |
| Rolling window warmup | Pull 7 extra days before START_DATE, trim after computing features |
| Inventory load performance | Sample every 4 days, forward-fill between samples |
| Offer price changes within window | Single snapshot assumed stable; document assumption |
| Node/Identifier type mismatch | Cast all join keys to str early |
| No MAP for many SKUs | MAP proximity = NaN; document partial coverage |

---

## 7. Dependencies

**Already available:** pandas, numpy, matplotlib, openpyxl

**May need installation:** seaborn, scipy, statsmodels

---

## 8. Existing Code Reused

| Component | File | What's Reused |
|-----------|------|---------------|
| DataLoader | `src/data/loader.py` | All data loading, `load_dsv_by_date()` pattern |
| GoogleAPIAdapter | `src/adapters/google_api_adapter.py` | `get_folder_files()`, `get_file_as_df()` |
| DataWarehouseAdapter | `src/adapters/dw_adapter.py` | SQL query execution |
| Module loader | `src/adapters/module_loader.py` | `ensure_modules_path()`, `load_yaml()` |
| Data source configs | `config/data_sources.yaml` | All SQL queries and source definitions |
| NLC model config | `config/nlc_model.yaml` | Inventory thresholds, margin parameters |
| Inventory loading pattern | `src/models/nlc_model.py` | `_load_inventory()` method as template |

---

## 9. Verification Checklist

- [ ] Run notebook with `ANALYSIS_DAYS=7` first (smaller, faster)
- [ ] Check scaffold shape matches n_sku_nodes x n_days
- [ ] Verify no unexpected NaN patterns in cost_to_walmart, offer_price
- [ ] Confirm rolling averages are clean after warmup trim
- [ ] Spot-check a few SKU-Nodes against actual DSV files
- [ ] Correlation matrix renders correctly
- [ ] Top correlations make business sense
- [ ] Statistical tests produce interpretable p-values
