# Change-Based Analysis Sub-Project — Implementation Plan

**Project:** WalmartPricing NLC Pipeline
**Sub-project:** Change-based correlation analysis (% price, % TE margin, % WMT margin → sales)
**Date:** 2026-04-24
**Deliverable:** Standalone Jupyter notebook at `notebooks/change_based_analysis.ipynb`

---

## 1. Context & Motivation

The existing `notebooks/correlation_analysis.ipynb` is a 29-cell orchestration notebook with ~14 `src/analysis/*` modules behind it. An audit (2026-04-24) found that every headline regression / causal / elasticity analysis in that notebook uses **absolute** levels of `cost_to_walmart`, `te_margin`, and `walmart_margin` as predictors. The `*_vs_7d_pct` percent-change columns are computed in `data_prep.py:630–678` but only one secondary test (`price_change_revenue_analysis`) touches `cost_to_walmart_vs_7d_pct`. The two margin-% change columns (`te_margin_vs_7d_pct`, `walmart_margin_vs_7d_pct`) are referenced by **zero** downstream analyses.

**Business question this notebook answers:**
Controlling for context, do **changes** in price, TE margin, and Walmart margin (expressed as % vs 7-day average) predict changes in `qty_sold` and `revenue` at the SKU-Node-Date level?

**Why a separate notebook:** The existing notebook is already large (29 cells + 14 modules + 30 config params). Adding another full analysis track inline would make orchestration and cell-by-cell review unwieldy. A standalone notebook lets the change-based analysis evolve independently and be run without the elasticity/DiD/segmented/optimization/strategy machinery.

---

## 2. Dataset Specification

### 2.1 Granularity & scope

- **Row = SKU-Node-Date** (same grain as the existing notebook).
- **Analysis window:** 90 days (configurable, default matches `correlation_analysis.yaml: data_prep.analysis_days: 90`).
- **SKU filter:** Top 90% by qty over 60-day lookback (reuse existing filter).
- **Rollback exclusion:** Same as the existing notebook.
- **Expected size:** ~1.3M rows before filtering; ~30–100K rows after restricting to change events (V2 / V3 variants).

### 2.2 Data input

- **Primary path:** Re-run `DataPreparation.build_master()` fresh at notebook start — 5–15 min cost, uses DW / Google / `pricing_module` / inventory, guarantees current data.
- **No new data sources.** All three `_vs_7d_pct` predictors and both outcome variables are already produced by the existing `data_prep` pipeline.

### 2.3 Predictors

| Variable | Definition | Source |
|---|---|---|
| `cost_to_walmart_vs_7d_pct` | `(cost_to_walmart - cost_to_walmart_7d_avg) / cost_to_walmart_7d_avg` | `data_prep.py:661` |
| `te_margin_vs_7d_pct` | Same formula, applied to `te_margin` | `data_prep.py:661` |
| `walmart_margin_vs_7d_pct` | Same formula, applied to `walmart_margin` | `data_prep.py:661` |

### 2.4 Controls

| Variable | Role |
|---|---|
| `can_show_inv` | Binary inventory availability |
| `day_of_week` | Demand seasonality |
| `n_active_nodes` | Network breadth at the SKU level |
| `days_since_price_change` | Recency (for discount-decay controls) |
| `price_tier` | Pre-existing quartile bucket (for segmentation) |

### 2.5 Outcomes

Primary: `qty_sold`, `revenue` (= `total_inv_amount`).
Secondary (robustness only): `P(qty_sold > 0)` (binary).

---

## 3. Event-Definition Variants

Three variant definitions are produced and carried through all relevant analyses. Sensitivity across variants is the key robustness check.

| Variant | Column / rule | Primary use |
|---|---|---|
| **V1 — Continuous** | Raw `_vs_7d_pct` values (includes 0 and small fluctuations) | Regression predictor (OLS, NegBin, elasticity) |
| **V2 — Threshold** | Binary: `|_vs_7d_pct| >= 0.01` (matches pipeline's `min_price_change_pct = 0.01`) | DiD treatment flag; ITS event marker |
| **V3 — Directional buckets** | 5-bin categorical: `[-inf, -5%] / (-5%, -1%] / (-1%, 1%] / (1%, 5%] / (5%, +inf)` | Categorical regressor; ANOVA-style group comparison |

All three variants are constructed for each of the three change variables → 9 variant columns total.

---

## 4. Analyses

| # | Analysis | Predictors | Outcome | Spec | Reuses |
|---|---|---|---|---|---|
| 1 | OLS main effects | 3× V1 + controls | `qty_sold` | `qty_sold ~ cost_pct + te_pct + wm_pct + controls` | `statistical_tests.ols_regression` (override `feature_cols`) |
| 2 | Log-OLS on revenue | 3× V1 + controls | `log(1+revenue)` | Same as (1), log outcome | `statistical_tests.ols_regression` |
| 3 | Two-way FE OLS | 3× V1 | `qty_sold` | SKU-Node + date fixed effects | New inline helper (via `linearmodels.PanelOLS`) |
| 4 | Negative Binomial | 3× V1 | `qty_sold` on `qty_sold > 0` subset | NegBin with log link | `statsmodels.NegativeBinomial` (inline) |
| 5 | Semi-log own-price elasticity | `cost_to_walmart_vs_7d_pct` (V1) | `log(1 + qty_sold)` | `log_qty ~ β·pct_price` — semi-log (log-log fails with signed % changes) | Adapt `elasticity.estimate_elasticity` |
| 6 | FE panel elasticity | V1 price only | `log(1 + qty_sold)` | Two-way FE semi-log | Adapt `elasticity.estimate_elasticity_fe` |
| 7 | DiD on threshold events | V2 price-threshold treatment | `qty_sold`, `revenue` | Treated = first V2 event; controls = same-brand no-change SKU-Nodes | `did_effects.build_did_panel` + `heterogeneous_did` (override `treatment_col`) |
| 8 | Interrupted time series | V2 event markers | `qty_sold` | Segmented regression, top 20 SKU-Nodes by event count, HAC SEs | New inline helper (adapts existing ITS from legacy notebook) |
| 9 | Directional bucket comparison | V3 buckets | `qty_sold`, `revenue` | One-way ANOVA + Tukey HSD, per change variable | New inline helper (scipy + statsmodels) |
| 10 | Interaction sensitivity (one-off) | `price_pct × te_pct`, `price_pct × wm_pct` | `qty_sold` | Single OLS with two interaction terms, main effects retained | `statistical_tests.ols_regression` extended |

**Deliberately excluded** (keeping notebook tight): logistic P(sale>0), propensity matching, clustering, Lorenz/Gini, optimization, strategy recommendations. These remain in the main notebook if needed.

---

## 5. Notebook Structure (~18 cells)

| Section | Cells | Purpose |
|---|---|---|
| 1. Setup | 1–2 | Imports, config, path setup |
| 2. Data load | 3–4 | `DataPreparation.build_master()`; sanity-check three `_vs_7d_pct` columns are present and non-degenerate |
| 3. Variant construction | 5 | Build V2 threshold flags and V3 directional buckets for the three change variables |
| 4. Sample characterization | 6 | Counts of change vs no-change days; distribution plots of the three `_vs_7d_pct` columns; bucket sizes; VIF table |
| 5. OLS suite | 7–8 | Analysis 1, 2, 3 — qty_sold and revenue, pooled and FE |
| 6. NegBin on sale-days | 9 | Analysis 4 |
| 7. Price elasticity | 10–11 | Analysis 5, 6 — semi-log + FE panel |
| 8. DiD | 12 | Analysis 7 — threshold treatment on `cost_to_walmart_vs_7d_pct` |
| 9. ITS | 13 | Analysis 8 — top-20 SKU-Node segmented regressions |
| 10. Directional buckets | 14–15 | Analysis 9 — ANOVA + pairwise comparisons for each of three change vars |
| 11. Interaction robustness | 16 | Analysis 10 — one-off sensitivity model |
| 12. Summary | 17 | Coefficient forest plot across all specs; findings markdown; export |
| 13. Cleanup | 18 | `loader.close()`; export summary parquet |

**Summary export:** `outputs/change_based_coefficients_{end_date}.parquet` — one row per (analysis, variant, predictor, outcome) with `coef`, `se`, `pvalue`, `ci_low`, `ci_high`, `n_obs`, `r2_or_pseudo`.

---

## 6. Config Additions

New section in `config/correlation_analysis.yaml`:

```yaml
change_based:
  change_cols:
    - cost_to_walmart_vs_7d_pct
    - te_margin_vs_7d_pct
    - walmart_margin_vs_7d_pct
  outcome_cols:
    - qty_sold
    - revenue
  controls:
    - can_show_inv
    - day_of_week
    - n_active_nodes
    - days_since_price_change
  variants:
    threshold_pct: 0.01            # V2: matches pipeline min_price_change_pct
    directional_bins: [-1.0, -0.05, -0.01, 0.01, 0.05, 1.0]
    directional_labels:
      - "big_decrease (<-5%)"
      - "small_decrease (-5%..-1%)"
      - "stable (-1%..1%)"
      - "small_increase (1%..5%)"
      - "big_increase (>5%)"
  its:
    top_n_sku_nodes: 20
    min_events: 2
    pre_window: 7
    post_window: 14
  did:
    treatment_col: "cost_to_walmart_vs_7d_pct"   # overrides correlation main-notebook default
    threshold: 0.01
  min_obs_nb: 200                  # NegBin fit guard
  interactions:
    - ["cost_to_walmart_vs_7d_pct", "te_margin_vs_7d_pct"]
    - ["cost_to_walmart_vs_7d_pct", "walmart_margin_vs_7d_pct"]
```

No new data-loading config keys needed — inherits from `data_prep` block.

---

## 7. Module Reuse & New Code

### 7.1 Reused as-is (via config override)

| Module.function | Reuse pattern |
|---|---|
| `data_prep.DataPreparation.build_master` | Called unchanged |
| `statistical_tests.ols_regression` | Called with `feature_cols` swapped to the 3 `_vs_7d_pct` predictors + controls |
| `ci_utils.*` | All CI helpers used unchanged |

### 7.2 Adapted via thin wrappers (inline in notebook)

| What | How |
|---|---|
| Semi-log elasticity (analysis 5) | 20-line inline wrapper: fit `log1p(qty) ~ pct` via statsmodels OLS; report β, SE, 95% CI |
| FE panel semi-log (analysis 6) | 30-line inline wrapper around `linearmodels.PanelOLS` with two-way FE |
| DiD with % threshold treatment (analysis 7) | Call `did_effects.build_did_panel` with `treatment_col = cost_to_walmart_vs_7d_pct` and derive treated/control from V2 flag |
| NegBin (analysis 4) | 15-line inline statsmodels call |
| ITS (analysis 8) | 40-line inline function: for each top-N SKU-Node, fit segmented regression pre/post V2 events with HAC SEs |
| ANOVA + Tukey (analysis 9) | 20-line scipy + statsmodels wrapper iterating over V3 buckets |

### 7.3 No new modules under `src/analysis/`

Deliberate choice: the notebook stays self-contained. If any of the inline helpers proves reusable (likely ITS), it graduates to `src/analysis/change_based.py` in a follow-up — not as part of this plan.

---

## 8. Caveats & Decisions

| Issue | Decision |
|---|---|
| Log-log elasticity fails with signed % changes | Use **semi-log** (`log(1+qty) ~ pct_price`) — coefficient interpreted as % change in qty per 1pp change in price |
| Zero / NaN denominator in `_vs_7d_pct` | Already handled in `data_prep.py:663` (`replace(0, nan)`); rows with NaN predictor are dropped per-analysis |
| Most SKU-Node-days have zero % change | V1 keeps all rows (zeros = "no change" baseline); V2/V3 filter or bucket explicitly |
| Rolling window induces autocorrelation in the predictor | Use HC1 robust SEs in OLS; HAC (Newey-West) in ITS; clustered SEs at SKU-Node level in FE panel |
| Multicollinearity between `cost_pct`, `te_pct`, `wm_pct` | Report VIF in characterization cell; if VIF > 10, run single-predictor variants as robustness |
| Large zero-sales mass | Analysis 4 (NegBin) restricts to `qty_sold > 0`; Analysis 9 compares means across buckets regardless of sparsity |
| Interpretation of interaction terms | Analysis 10 is marked explicitly as "robustness only" in notebook; headline conclusions rest on main-effect models |

---

## 9. Verification Checklist

- [ ] Notebook runs end-to-end with default 90-day window in < 25 min
- [ ] All three `_vs_7d_pct` columns exist in master df with > 50% non-NaN
- [ ] VIF reported in characterization cell; all < 10 for chosen main-effect spec
- [ ] Coefficient signs across three variants (V1, V2, V3) point in the same direction for `cost_to_walmart_vs_7d_pct`
- [ ] DiD parallel-trends plot rendered; visual inspection passes
- [ ] ITS runs successfully on ≥ 15 of top-20 SKU-Nodes
- [ ] Summary parquet written with ≥ one row per analysis
- [ ] No import of `src/analysis/*` beyond `data_prep`, `statistical_tests`, `did_effects`, `ci_utils`
- [ ] Dependencies `linearmodels` (FE panel) and `statsmodels` (NegBin) verified at notebook start

---

## 10. Deliverables

| File | Status |
|---|---|
| `docs/change-based-analysis-plan.md` | This document |
| `docs/change-based-analysis-plan.docx` | Generated from `docs/generate_change_based_analysis_doc.py` |
| `notebooks/change_based_analysis.ipynb` | To build after plan approval |
| `config/correlation_analysis.yaml` | Add `change_based:` section (non-breaking) |
| `outputs/change_based_coefficients_{date}.parquet` | Generated on notebook run |

---

## 11. Out of Scope (for this sub-project)

- Absolute-level analyses (stay in the main correlation notebook)
- Logistic regression for P(sale>0) (already in main notebook)
- A/B tests for Wm margin split / brand margin arms (already in main notebook)
- Propensity score matching, clustering, node efficiency (already in main notebook)
- Writing change-based results back into the pricing rules / simulation / strategy modules
