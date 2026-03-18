# WalmartPricing — Claude Code Context

## What is this project?
Node Level Cost (NLC) pricing pipeline for a B2B tire company selling ~100K SKUs on Walmart B2B. Unlike Amazon (per-SKU pricing), Walmart uses per-SKU-Node pricing where each warehouse node gets its own cost. Takes data from Google Drive, Data Warehouse, and local shared drive files; computes optimal NLC prices via a two-pass inventory/margin model; applies business rules and A/B tests; generates DSV files for Walmart upload via hybris.

## Key difference from AmazonPricing
- **Amazon:** Price per SKU across 4 regions → Contra COGs Excel upload
- **Walmart:** Price per SKU-Node (warehouse node) → DSV CSV upload via hybris/FTP
- SKU-Node = `{Product Code}-{Identifier}` (Identifier = warehouse node ID)

## Pipeline steps
1. **Data Collection** (`src/data/`) — Load DSV, inventory, sales, Walmart item report, MAP, warehouse node mapping, shipping costs, rollbacks, tests tracker
2. **NLC Model** (`src/models/`) — Two-pass computation (min_units=8 first, then min_units=4 for gaps). Computes NLC at 8 margin levels [6,8,11,12,13,14,15,20%]. Cascading final NLC: try 11% → 8% → 6% → N/A
3. **Pricing Rules** (`src/rules/`) — Categorize into update types: Wm margin split test, brand margin test, low price updates (margin < 5.9%), high price updates (margin > 20.3%), new SKU-nodes
4. **DSV Generation** (`src/dsv/`) — Build new DSV from current + updates. Handles rollback exclusion from NLC, rollback price override on national. Validates and saves
5. **Tracker Update** (`src/tracker/`) — Update tests tracker with refreshed margins, new entries, and date stamps. Backup before save
6. **FTP Validation** (`src/dsv/ftp_validator.py`) — Download XML responses from Walmart FTP ~3 hours after upload, parse ingestion status, generate summary Excel, alert on high failure rate

## Architecture
- **Config-driven:** `config/data_sources.yaml` (sources), `config/nlc_model.yaml` (parameters), `config/pricing_rules.yaml` (rules)
- **Adapters:** `src/adapters/` wraps shared modules via thin adapter classes
- **Shared modules:** Live on `G:\Shared drives\DevOps Projects\Python projecs\python-automations\modules` — accessed via `sys.path`, never copied
- **Credentials:** Resolved by the shared modules from their sibling `credentials/` folder

## NLC computation logic (core algorithm)
1. Load 7 days of inventory via `pricing_module.get_inventory()`, exclude rollback SKUs
2. Filter to SKUs in current national DSV, filter nodes by Available >= min_units AND >= Zero Out Threshold, map to warehouse nodes
3. Per SKU-Node-date: take min Purchase Price+FET. Then per SKU-Node: take most recent date
4. Merge with Walmart offer prices, MAP, shipping costs
5. For each margin m: `NLC = PurchasePrice+FET / (1-m) + ShippingCost`
6. MAP check: `NLC < MAP * 0.95` required (if has MAP). Walmart margin check: `WmMargin > 0` required (if no MAP)
7. Final NLC cascade: 11% feasible → use 11%; else 8% → use 8%; else 6% → use 6%; else N/A
8. Current NLC margin = `(current_nlc_price - Cost+Shipping) / current_nlc_price`
9. Margin splits: `TotalMargin = WmMarginAtNLC + NLCMargin`, then `Price = offer_price * (1 - TotalMargin * (1-split%))`, capped at 20% NLC

## Key parameters
- `min_margin_update_prices`: 0.059 (5.9%) — below this, price increase
- `max_margin_update_prices`: 0.203 (20.3%) — above this, decrease to 20%
- `days_before`: 7 (inventory lookback)
- `days_sales`: 90 (sales analysis window)
- `min_units`: 8 primary, 4 secondary (two-pass)
- `distance_map`: 0.05 (MAP buffer)
- `min_price_change_pct`: 0.01 (1% — minimum change to trigger update)
- DSV minimum margin column: "4%"
- FTP failure rate alert: 1.5%

## Data sources
- **Walmart DSV folder:** Google Drive `1piuawZRpppmoD-Qdkd1IUj3x4rs-LKny` (date-named CSVs)
- **Walmart item report:** `pricing_tests.walmart_item_report` (DW, newCredentials=true)
- **MAP:** `pricing_tests.map_prices` (DW)
- **Sales:** `warehouse.vw_virtual_node_tracker` (DW, newCredentials=false, WalmartB2B sales only)
- **Warehouse nodes:** Google Drive folder `1Y4drFI84j2eNQM_XTNb9nY9vBCngszZq` (semicolon CSV)
- **Shipping costs:** `H:\...\Config files\Costs to add by node.xlsx`
- **Inventory:** Via `pricing_module.get_inventory()` shared module (lazy import)
- **Rollbacks:** Excel from `H:\...\Rollbacks\` folder (filter End date > today)
- **Tests tracker:** `H:\...\Final node level costs tracker.csv`
- **FTP:** `/external-merchants/WalmartB2B/price` on Walmart FTP server

## Key conventions
- No hybris import at startup (requires Selenium)
- No pricing_module import at startup (triggers Google OAuth)
- 0.5s sleep between Google API calls during batch operations
- DW `new_credentials=false` for `warehouse.*` schema queries
- SKU-Node key: `{Product Code}-{Identifier}`
- Brand code: first 4 chars of Product Code
- Targets to exclude from regular updates: "Margin test", "Wm margin split test", "Shipping cost added"
- Original notebook in `original_code/` — reference only, do not modify

## Output files
- DSV CSV: `H:\...\Node level costs\DSV Files\{YYYY-MM}\New WalmartB2B DSV to upload {date}.csv`
- Tests tracker: `H:\...\Node level costs\Final node level costs tracker.csv`
- Tracker backup: `H:\...\Node level costs\Bk tracker\Final node level costs tracker_{date}.csv`
- FTP validation Excel: `H:\...\Node level costs\Price updates check\{date}\NLC Price update response summary {date}.xlsx`

## Reference
- `original_code/node_level_costs_pricing.ipynb` — original 242-cell notebook (reference only)
