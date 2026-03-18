# WalmartPricing — Claude Code Context

## What is this project?
Node Level Cost (NLC) pricing pipeline for a B2B tire company selling ~100K SKUs on Walmart B2B. Unlike Amazon (per-SKU pricing), Walmart uses per-SKU-Node pricing where each warehouse node gets its own cost. Takes 20+ data sources, computes optimal NLC prices via inventory/margin model, applies business rules and tests, generates DSV files for Walmart upload.

## Key difference from AmazonPricing
- **Amazon:** Price per SKU across 4 regions → Contra COGs upload
- **Walmart:** Price per SKU-Node (warehouse node) → DSV file upload via hybris/FTP
- SKU-Node = `{Product Code}-{Identifier}` (Identifier = warehouse node ID)

## Pipeline steps
1. **Data Collection** (`src/data/`) — Load inventory, sales, BuyBox, MAP, DSV, warehouse mappings, rollbacks, shipping costs, test tracker
2. **NLC Model** (`src/models/`) — Compute node level costs per SKU-Node using inventory + margin parameters
3. **Rules & Tests** (`src/rules/`) — Apply pricing rules: margin tests, less-sales adjustments, Wm margin split tests, brand margin tests
4. **Price Updates** (`src/nlc/`) — Categorize updates: new SKU-nodes, low price corrections, high margin decreases, test updates
5. **DSV Generation** (`src/dsv/`) — Build new DSV file from current DSV + price updates, export CSV
6. **Tracker Update** (`src/tracker/`) — Update the NLC tests tracker with all changes
7. **Upload Validation** — (Manual) Upload DSV via hybris, check FTP XML responses after 3 hours

## Architecture
- **Config-driven:** All data sources defined in `config/data_sources.yaml`
- **Adapters:** `src/adapters/` wraps shared modules (`GoogleAPI_functions`, `DW_connection`, `pricing_module`, `hybris`, `ftp_server`) via thin adapter classes
- **Shared modules:** Live on `G:\Shared drives\DevOps Projects\Python projecs\python-automations\modules` — accessed via `sys.path`, never copied
- **Credentials:** Resolved by the shared modules from their sibling `credentials/` folder on the shared drive

## Key pricing concepts
- **NLC margin:** `(NLC price - Purchase Price+FET) / NLC price`
- **Final node level cost:** Computed from inventory coverage model with margin targets (6%, 11%, 15%, 20%)
- **Walmart margin at NLC:** Walmart's margin calculated at the node level cost
- **Wm margin split test:** Testing different margin split percentages (50%, 60%)
- **Rollbacks:** SKUs with temporary price rollbacks that must be excluded from NLC updates
- **MAP (Minimum Advertised Price):** Floor price constraint — test prices must respect MAP + 5% buffer

## Key parameters
- `min_margin_update_prices`: 0.059 (5.9%)
- `max_margin_update_prices`: 0.203 (20.3%)
- `days_before`: 7 (lookback window)
- `days_sales`: 90 (sales analysis window)
- `min_units`: 4 or 8 (minimum inventory units for NLC eligibility)
- Minimum margin in DSV: 4%

## Data sources
- **BuyBox/Pricing:** `pricing_tests.walmart_item_report` (DW), current DSV from Google Drive folder
- **MAP:** `pricing_tests.map_prices` (DW)
- **Inventory:** Via `pricing_module.get_inventory()` shared module
- **Sales:** `pricing_tests.wmt_order` (DW), 90-day window
- **Warehouse nodes:** Google Drive folder `1Y4drFI84j2eNQM_XTNb9nY9vBCngszZq`
- **Shipping costs:** Excel config `Costs to add by node.xlsx`
- **Rollbacks:** Excel from `H:\...\Rollbacks\` folder
- **Tests tracker:** CSV from `H:\...\Node level costs\`
- **FTP responses:** Walmart FTP server for upload validation

## Key conventions
- No hybris import at startup (requires Selenium)
- No pricing_module import at startup (triggers Google OAuth)
- 0.5s sleep between Google API calls during batch operations
- SKU-Node key format: `{Product Code}-{Identifier}`
- Brand code: first 4 chars of Product Code
- Original notebook in `original_code/` — reference only, do not modify

## Output files
- DSV CSV: `H:\...\Node level costs\DSV Files\{YYYY-MM}\DSV {date}.csv`
- Tests tracker: `H:\...\Node level costs\Final node level costs tracker.csv`
- Tracker backup: `H:\...\Node level costs\Bk tracker\`
- Upload validation Excel: `H:\...\Node level costs\Price updates check\{date}\`

## Reference docs
- `PROJECT_DOCUMENTATION.md` — full project documentation
- `original_code/node_level_costs_pricing.ipynb` — original notebook (reference)
