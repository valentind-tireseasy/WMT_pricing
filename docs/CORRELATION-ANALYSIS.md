# WalmartPricing NLC Pipeline — Correlation Analysis

## Context & Objective

The pricing team needs to understand what drives sales at the SKU-Node level on Walmart B2B. Currently, pricing decisions (NLC cascade, margin tests, split tests) are made based on cost/margin thresholds, but there is no systematic analysis of how price changes, inventory coverage, margin levels, and other factors correlate with actual sales volume.

### Business questions

- How do NLC price changes affect subsequent sales volume?
- What factors (price, margin, inventory, brand, geography) most influence sales?
- What margin levels maximize total revenue/profit across SKU-Nodes?
- Which brand-state segments should increase, decrease, or hold pricing?

### Approach

Build a daily SKU-Node-Date dataset from 6 data sources, then run exploratory correlation analysis, statistical hypothesis tests, elasticity estimation, difference-in-differences causal inference, margin optimization, and revenue/profit simulation. All analyses include confidence intervals via bootstrap, analytical, or delta-method estimation. Originally a monolithic 91-cell notebook, the analysis was refactored on 2026-03-31 into 14 Python modules under `src/analysis/` orchestrated by a 29-cell notebook.

---

## Architecture

### Modular design

The analysis was refactored from a single monolithic notebook into a modular architecture:

```
src/analysis/
  ci_utils.py          # Bootstrap, delta-method, rank-biserial CIs
  plot_utils.py         # CI-aware plotting helpers
  config.py             # Loads correlation_analysis.yaml
  data_prep.py          # AnalysisDataPrep: data loading & feature engineering
  eda.py                # Exploratory analysis with bootstrap CIs
  statistical_tests.py  # Mann-Whitney, OLS, decile analysis with CIs
  geographic_brand.py   # State/brand aggregations with CI error bars
  elasticity.py         # Log-log OLS elasticity with analytical CIs
  did_effects.py        # Difference-in-Differences with HC1 robust CIs
  segmented.py          # Tire size, MAP, inventory visibility segments
  optimization.py       # Quadratic margin-sales/profit fits + delta-method CIs
  simulation.py         # What-if scenarios with CI bands from elasticity SE
  strategy.py           # Master brand-state strategy table with confidence levels
  summary.py            # Executive summary with CI ranges, parquet export

config/
  correlation_analysis.yaml   # Centralized parameters

notebooks/
  correlation_analysis.ipynb  # 29-cell orchestration notebook
  legacy/                     # 3 legacy notebooks (reference only)
```

### Design principles

- **Config-driven:** All parameters (date range, bootstrap settings, thresholds) live in `config/correlation_analysis.yaml`, not hardcoded in cells.
- **CI-first:** Every statistical result includes a confidence interval. No point estimates without uncertainty quantification.
- **Orchestration notebook:** The notebook imports modules and calls their public APIs. No inline analysis logic in cells.
- **Reusable utilities:** `ci_utils.py` and `plot_utils.py` provide shared CI computation and visualization across all modules.

---

## Confidence Intervals

Every analysis section produces confidence intervals using one of four methods, selected based on the statistic being estimated.

### Bootstrap CIs

Used for: correlation coefficients, mean differences, effect sizes.

- **Method:** BCa (bias-corrected and accelerated) bootstrap with 1,000 resamples.
- **Max sample:** 50,000 rows subsampled before bootstrapping to manage compute time (20,000 for the correlation matrix).
- **Functions:** `bootstrap_ci()`, `bootstrap_correlation_ci()`, `bootstrap_mean_diff_ci()` in `ci_utils.py`.
- **Example:** Spearman correlation between price change and sales = -0.006 [-0.028, 0.018].

### Analytical CIs

Used for: OLS regression coefficients, elasticity estimates.

- **Method:** Standard errors from statsmodels OLS with HC1 (heteroskedasticity-robust) covariance. 95% confidence intervals from the t-distribution.
- **Function:** `ols_ci_df()` in `ci_utils.py`.
- **Example:** FERE brand-state elasticity in US-CA = -2.247 [-3.685, -0.809].

### Delta method CIs

Used for: optimal margin vertex from quadratic regression.

- **Method:** The optimal margin is a non-linear function of quadratic coefficients (vertex = -b/2a). The delta method propagates coefficient covariance to obtain a CI for the vertex.
- **Function:** `delta_method_ci()` in `ci_utils.py`.
- **Example:** FERE optimal TE margin = 11.9% [9.9%, 13.9%].

### SE propagation CIs

Used for: simulation revenue/profit projections.

- **Method:** Elasticity standard errors are propagated through the revenue/profit formulas to produce confidence bands around simulated outcomes.
- **Example:** FERE decrease 5% -> +5.7% revenue [+3.3%, +8.1%].

### Rank-biserial effect size

Used for: Mann-Whitney U tests (binary group comparisons).

- **Method:** r = 1 - 2U/(n1*n2), with bootstrap CI on r.
- **Example:** Price change impact effect size r = -0.006 (negligible).

---

## Dataset

### Scope

| Parameter | Value |
|-----------|-------|
| Granularity | One row per SKU-Node-Date (daily) |
| Analysis window | 30 days (2026-02-24 to 2026-03-25) |
| SKU filter | Top SKUs accounting for 90% of qty sold (~6,497 SKUs, ~44,800 SKU-Nodes) |
| Node filter | All nodes with at least 1 sale for those SKUs |
| Rollback exclusion | SKUs in active rollbacks excluded |
| Final shape | 1,343,970 rows x 41 columns |
| Rolling warmup | 7 days prior to analysis window for rolling feature computation |

### Core columns

| # | Column | Source | Computation |
|---|--------|--------|-------------|
| 1 | SKU | Sales query | Product Code |
| 2 | Node | Sales query | externalwarehouseid (= Identifier) |
| 3 | Date | Generated | Daily within analysis window |
| 4 | Qty sold | DW: warehouse.vw_virtual_node_tracker | Sum of quantity per SKU-Node-Date |
| 4b | Revenue | DW: warehouse.vw_virtual_node_tracker | Sum of total_inv_amount per SKU-Node-Date |
| 4c | Profit | DW: warehouse.vw_virtual_node_tracker | Sum of profit per SKU-Node-Date |
| 5 | Cost to Walmart | Google Drive DSV files | Price from DSV; national fallback if no node-specific price |
| 6 | Walmart offer price | DW: pricing_tests.walmart_item_report | offer_price where status = PUBLISHED |
| 7 | Walmart margin | Computed | (offer_price - cost_to_walmart) / offer_price |
| 8 | TE margin | Computed | (cost_to_walmart - min_purchase_price_fet) / cost_to_walmart |
| 9 | Brand | Derived | First 4 characters of Product Code |
| 10 | Town / State | Warehouse Addresses CSV | City and state of warehouse node |
| 11 | Can show inv? | Inventory (pricing_module) | 1 if inventory available at node on date, else 0 |
| 12 | Min Purchase Price + FET | Inventory | Min daily purchase price at node |
| 13 | Shipping cost | Local Excel | Per-node shipping cost |
| 14 | MAP | DW: pricing_tests.map_prices | Minimum Advertised Price |
| 15 | Day of week | Derived | date.dt.dayofweek (0=Mon, 6=Sun) |
| 16 | MAP proximity | Derived | cost_to_walmart / (MAP * 0.95); NaN if no MAP |
| 17 | Active nodes per SKU | Derived | Count of nodes with can_show_inv=1 per SKU per date |
| 18 | Days since price change | Derived | Days since cost_to_walmart changed for that SKU-Node |

### Rolling 7-day features

For each of **qty_sold, TE margin, cost_to_walmart, offer_price, walmart_margin**:

- `{metric}_7d_avg` — Mean of prior 7 days for that SKU-Node (excludes current day)
- `{metric}_vs_7d` — Absolute delta: today's value minus 7-day average
- `{metric}_vs_7d_pct` — Percentage change vs 7-day average

Computed via numpy reshape + cumsum (see Performance Optimizations below).

---

## Data Sources & Loading Strategy

| Data | Source | Access Method | Strategy |
|------|--------|---------------|----------|
| Sales | warehouse.vw_virtual_node_tracker | DW (new_credentials=false) | Two loads: lookback for SKU filtering, then analysis window + 7-day warmup |
| DSV files | Google Drive folder | Google API with 0.5s rate limiting | List all files, parse dates from filenames, load within range; merge_asof for assignment |
| Offer prices | pricing_tests.walmart_item_report | DW (new_credentials=true) | Single snapshot; filter 1p_offer_status = PUBLISHED |
| Inventory | pricing_module.get_inventory() | Shared module (lazy import) | Sample every 4 days, forward-fill between samples |
| MAP | pricing_tests.map_prices | DW | loader.load('dw_map_prices') |
| Shipping costs | Local Excel | loader.load() | Per-node shipping cost |
| Warehouse mapping | Google Drive folder | Google API | Node ID to Warehouse Code |
| Warehouse addresses | Local CSV | pd.read_csv() | Warehouse Code to Town/State |
| Rollbacks | Local Excel | loader.load() | Filter End date > analysis start |

---

## Analysis Structure

### Module 1 — Data Preparation (`data_prep.py`)

`AnalysisDataPrep` class loads all data sources, builds the daily SKU-Node-Date scaffold, computes margins and derived columns, and generates rolling 7-day features. Outputs the master DataFrame (1.3M rows x 41 columns).

### Module 2 — Exploratory Data Analysis (`eda.py`)

Spearman correlation matrix with bootstrap CIs on each coefficient. Scatter plots with regression CI bands. Distribution plots with mean CI annotation. Identifies top feature correlations with qty_sold.

### Module 3 — Statistical Tests (`statistical_tests.py`)

- **Price change impact:** Mann-Whitney U with rank-biserial effect size + bootstrap CI
- **Inventory impact:** Mann-Whitney U with effect size + CI
- **Pre/post revenue comparison:** Mean difference with bootstrap CI
- **OLS regression:** Full coefficient table with HC1 robust CIs, forest plot
- **Margin decile analysis:** Revenue by decile with CI error bars
- **Price-revenue correlation:** Bucketed analysis with CIs

### Module 4 — Geographic & Brand Analysis (`geographic_brand.py`)

State-level and brand-level aggregations with bootstrap CI error bars. Distribution breadth (number of active nodes per SKU) with Spearman r + CI.

### Module 5 — Elasticity Estimation (`elasticity.py`)

Generic `estimate_elasticity()` function using log-log OLS, replacing 5 separate implementations from the legacy notebook. Produces elasticity estimates for state, brand-state, city, and brand rankings. All estimates include analytical CIs from statsmodels.

### Module 6 — Difference-in-Differences (`did_effects.py`)

DiD panel construction with brand-matched controls. Heterogeneous treatment effects by brand tier, state, and MAP status. All ATT estimates use HC1 robust standard errors.

### Module 7 — Segmented Analysis (`segmented.py`)

Tire size, MAP vs non-MAP, and inventory visibility comparisons with effect sizes + CIs. Segmented elasticity estimation by segment.

### Module 8 — Margin Optimization (`optimization.py`)

Quadratic margin-sales and margin-profit fits. Optimal margin vertex estimated via delta-method CIs. Compares optimal vs current margin for each brand.

### Module 9 — Simulation (`simulation.py`)

What-if price change scenarios (+/- 1%, 3%, 5%, 10%) per brand. Revenue and profit projections with CI bands propagated from elasticity standard errors.

### Module 10 — Strategy (`strategy.py`)

Master brand-state strategy table combining elasticity, CI width, sample size, and simulation results into actionable recommendations with CI-derived confidence levels (high/medium/low).

### Module 11 — Summary (`summary.py`)

Executive summary with CI ranges. Parquet export of the final dataset.

### Foundation Modules

- **`ci_utils.py`** — `bootstrap_ci`, `bootstrap_correlation_ci`, `bootstrap_mean_diff_ci`, `rank_biserial_effect_size`, `delta_method_ci`, `ols_ci_df`
- **`plot_utils.py`** — `bar_chart_with_ci`, `scatter_with_regression_ci`, `line_with_ci_band`, `coefficient_ci_plot`, `heatmap_with_ci_annotation`
- **`config.py`** — Loads `config/correlation_analysis.yaml`

---

## Key Findings (2026-03-31 Run)

### 1. Price change impact is statistically significant but small

Mann-Whitney U test: p = 1.74e-11 (significant). Mean difference = 0.016 [95% CI: -0.028, 0.018]. Rank-biserial effect size r = -0.006 (negligible). Price changes are detectable in aggregate but the per-SKU-Node effect is tiny relative to natural variation.

### 2. Inventory availability remains the strongest sales driver

Mann-Whitney U: p < 0.001. Mean difference = 0.107 [95% CI: 0.085, 0.120]. Effect size r = -0.030. Inventory availability continues to dominate all other predictors.

### 3. Pre/post revenue shows a meaningful increase

+10.3% revenue change (p = 6.33e-04). Mean difference = $1.85 [95% CI: $0.36, $3.22]. The CI excludes zero, confirming a real positive revenue effect from recent pricing actions.

### 4. Price elasticity varies dramatically by brand and geography

42 states analyzed, 146 brand-state segments. Key findings:

| Brand-State | Elasticity | 95% CI | Interpretation |
|-------------|-----------|--------|----------------|
| FERE in US-CA | -2.247 | [-3.685, -0.809] | Highly elastic — price cuts drive large volume gains |
| ARRO in US-FL | -1.218 | — | Elastic |
| TRAV in US-FL | -1.021 | — | Unit elastic |

8 brand-state segments have high city-level variation (std > 0.5), indicating within-state heterogeneity that warrants city-level pricing.

**Seasonal shift:** Elasticity decreases over time (P1: -0.080, P2: -0.076, P3: -0.051), suggesting customers become less price-sensitive as the period progresses. Brands with the largest seasonal shift: LION (0.490), SUMM (0.289).

### 5. DiD treatment effects are significant for budget tier and select states

| Segment | ATT | 95% CI | p-value | Significant? |
|---------|-----|--------|---------|--------------|
| Budget tier | +0.156 | [+0.044, +0.267] | 0.006 | Yes |
| US-GA | +0.283 | [+0.018, +0.547] | 0.036 | Yes |
| Non-MAP SKUs | +0.087 | [-0.024, +0.198] | — | No |
| MAP SKUs | +0.033 | [-0.062, +0.128] | — | No |

Budget-tier SKUs respond positively to pricing treatment. Georgia shows the strongest state-level effect.

### 6. Margin optimization reveals under-priced brands

| Brand | Metric | Optimal | 95% CI | Current | Gap |
|-------|--------|---------|--------|---------|-----|
| FERE | TE margin | 11.9% | [9.9%, 13.9%] | 11.5% | +0.4pp |
| LION | WM margin | 18.2% | [11.6%, 24.7%] | 10.6% | +7.5pp |
| KEND | TE margin | 18.6% | [14.2%, 23.1%] | 12.2% | +6.4pp |

LION and KEND have the largest gaps between optimal and current margin, suggesting significant room for margin improvement.

### 7. Simulation identifies revenue and profit sweet spots

| Brand | Scenario | Revenue Impact | 95% CI |
|-------|----------|---------------|--------|
| FERE | Decrease 5% | +5.7% revenue | [+3.3%, +8.1%] |
| ARIS | Increase 5% | +58.4% profit | [+56.8%, +60.0%] |

FERE benefits from price decreases (elastic demand), while ARIS can absorb a price increase with a large profit gain (inelastic demand with tight CI).

### 8. Strategy recommendations span 146 brand-state segments

- 124 segments: increase pricing
- 5 segments: decrease pricing
- 17 segments: hold current pricing
- 21 segments classified as high-confidence recommendations
- Top quick win: GTRA in US-KY, increase 1% -> +1.5% revenue (n=394, high confidence)

---

## Performance Optimizations

| Optimization | Before | After | Improvement |
|--------------|--------|-------|-------------|
| Rolling 7-day features | 12 min (pandas rolling per group) | 10 sec (numpy reshape + cumsum) | ~72x faster |
| Bootstrap max_sample | Full dataset | 50K (20K for correlation matrix) | Bounded compute time |
| Bootstrap resamples | — | 500 (correlation matrix), 1000 (other) | Tuned per analysis |
| Total local compute | ~52 min | ~8 min | ~6.5x faster |

The numpy optimization for rolling features works by reshaping the grouped data into a fixed-width array and using cumulative sums to compute windowed means without Python-level loops.

---

## Edge Cases & Design Decisions

| Issue | Handling |
|-------|----------|
| Not every day has a DSV file | Forward-fill from most recent DSV via merge_asof(direction='backward') |
| No node-specific DSV price | Fall back to national price (Source = NaN row) |
| Rolling window warmup | Pull 7 extra days before START_DATE, trim after computing features |
| Inventory load performance | Sample every 4 days, forward-fill between samples |
| Offer price changes within window | Single snapshot assumed stable; documented assumption |
| Node/Identifier type mismatch | Cast all join keys to str early |
| No MAP for many SKUs | MAP proximity = NaN; document partial coverage |
| Bootstrap on large groups | Subsample to max_sample before resampling to bound runtime |
| Wide CIs on small segments | Strategy table uses CI width to assign confidence levels (high/medium/low) |

---

## Existing Pipeline Code Reused

| Component | File | What's Reused |
|-----------|------|---------------|
| DataLoader | src/data/loader.py | All data loading, load_dsv_by_date() pattern |
| GoogleAPIAdapter | src/adapters/google_api_adapter.py | get_folder_files(), get_file_as_df() |
| DataWarehouseAdapter | src/adapters/dw_adapter.py | SQL query execution |
| Module loader | src/adapters/module_loader.py | ensure_modules_path(), load_yaml() |
| Data source configs | config/data_sources.yaml | All SQL queries and source definitions |
| NLC model config | config/nlc_model.yaml | Inventory thresholds, margin parameters |
| Inventory pattern | src/models/nlc_model.py | _load_inventory() method as template |

---

## Output Files

| Artifact | Path | Description |
|----------|------|-------------|
| Orchestration notebook | notebooks/correlation_analysis.ipynb | 29-cell notebook that imports and calls src/analysis/ modules |
| Legacy notebooks | notebooks/legacy/ | 3 legacy notebooks (reference only) |
| Analysis modules | src/analysis/ | 14 Python modules (see Architecture section) |
| Config | config/correlation_analysis.yaml | Centralized analysis parameters |
| Dataset (parquet) | outputs/correlation_dataset_2026-03-25.parquet | 1.3M rows x 41 columns |
| This document | docs/CORRELATION-ANALYSIS.md | Post-analysis documentation |

---

## Git History Reference

| Date | Commit | Phase | Description |
|------|--------|-------|-------------|
| 2026-03-25 | 23ad91f | Phase 1-2 | Add SKU-Node correlation analysis sub-project (template + initial run) |
| 2026-03-25 | 43fe847 | Phase 3 | Extend correlation analysis with advanced statistical methods (Sections 15-29) |
| 2026-03-25 | 90f1a4b | Bugfix | Fix offer_price KeyError, add price-change-to-revenue analysis, optimize slow cells |
| 2026-03-31 | 5538a8c | Refactor | Refactor correlation analysis into modular src/analysis/ with built-in CIs |
| 2026-03-31 | 5ac6078 | Cleanup | Move 01_nlc_pricing.ipynb back to notebooks root |
| 2026-03-31 | 4076c60 | Cleanup | Normalize notebook metadata |

---

## Actionable Recommendations

Based on the 2026-03-31 analysis with confidence intervals, ordered by expected impact and confidence:

1. **Prioritize inventory coverage over price optimization** — Inventory availability remains the strongest predictor of sales (mean diff = 0.107 [0.085, 0.120], r = -0.030). Ensuring SKU-Nodes have available inventory is more impactful than adjusting margins. Consider alerting when key SKU-Nodes lose inventory coverage.

2. **Increase margins on LION and KEND** — Margin optimization with delta-method CIs shows LION is 7.5pp below its optimal WM margin (18.2% [11.6%, 24.7%] vs 10.6%) and KEND is 6.4pp below its optimal TE margin (18.6% [14.2%, 23.1%] vs 12.2%). These are the largest gaps with CIs that exclude the current margin, giving high confidence in the direction.

3. **Decrease FERE prices by 5% in US-CA** — FERE in US-CA has the highest measured elasticity (-2.247 [-3.685, -0.809]). Simulation projects a 5% price decrease yields +5.7% revenue [+3.3%, +8.1%]. The CI excludes zero, confirming the direction.

4. **Implement the 21 high-confidence brand-state recommendations** — The strategy table identifies 21 segments where the CI width is narrow enough to act with high confidence. Start with the top quick win: GTRA in US-KY (increase 1% -> +1.5% revenue, n=394).

5. **Focus budget-tier pricing treatment** — DiD analysis confirms budget-tier SKUs respond positively to pricing treatment (ATT = +0.156 [+0.044, +0.267], p = 0.006). Prioritize pricing experiments in this tier.

6. **Investigate city-level pricing for 8 high-variation segments** — 8 brand-state segments have city-level elasticity std > 0.5, indicating that state-level pricing leaves value on the table. These segments may benefit from city-level or node-cluster-level differentiation.

7. **Account for seasonal elasticity shifts** — Elasticity decreases over the period (P1: -0.080 to P3: -0.051). Brands like LION (shift = 0.490) and SUMM (0.289) show the largest seasonal changes. Consider adjusting margin targets by season.

8. **Rerun analysis monthly** — The 30-day analysis window and modular architecture support monthly refresh. The config-driven design means updating `correlation_analysis.yaml` with new dates is sufficient to rerun the full pipeline in ~8 minutes.
