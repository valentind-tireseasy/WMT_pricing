"""Build notebooks/change_based_analysis.ipynb via nbformat.

Run this script any time the plan is updated; the resulting notebook is the
change-based analysis deliverable described in docs/change-based-analysis-plan.md.
"""

from pathlib import Path

import nbformat as nbf


def md(text):
    return nbf.v4.new_markdown_cell(text)


def code(text):
    return nbf.v4.new_code_cell(text)


def build_notebook():
    nb = nbf.v4.new_notebook()
    nb.metadata = {
        "kernelspec": {
            "display_name": "Python 3",
            "language": "python",
            "name": "python3",
        },
        "language_info": {"name": "python", "version": "3.11"},
    }

    cells = []

    # --- 1. Title ---
    cells.append(md(
        "# Change-Based Correlation Analysis\n\n"
        "Sales response to **% changes** (vs 7-day average) in price, TE margin, and "
        "Walmart margin — companion to `correlation_analysis.ipynb` which analyzes "
        "absolute levels. See `docs/change-based-analysis-plan.md` for design.\n\n"
        "**Predictors:** `cost_to_walmart_vs_7d_pct`, `te_margin_vs_7d_pct`, "
        "`walmart_margin_vs_7d_pct`  \n"
        "**Outcomes:** `qty_sold`, `revenue`  \n"
        "**Variants:** V1 continuous · V2 threshold (|%| ≥ 1%) · V3 directional buckets"
    ))

    # --- 2. Params & imports ---
    cells.append(code(
        "# === PARAMETERS ===\n"
        "END_DATE = \"2026-03-25\"\n"
        "USE_CACHED = True   # True: load outputs/correlation_dataset_{END_DATE}.parquet\n"
        "                    # False: fresh DataPreparation (~15 min, needs DW/Google/pricing_module)\n"
        "\n"
        "THRESHOLD_PCT = 0.01             # V2 threshold (matches pipeline min_price_change_pct)\n"
        "DIRECTIONAL_BINS = [-1.0, -0.05, -0.01, 0.01, 0.05, 1.0]\n"
        "DIRECTIONAL_LABELS = [\n"
        "    \"big_decrease (<-5%)\",\n"
        "    \"small_decrease (-5..-1%)\",\n"
        "    \"stable (-1..1%)\",\n"
        "    \"small_increase (1..5%)\",\n"
        "    \"big_increase (>5%)\",\n"
        "]\n"
        "CHANGE_COLS = [\n"
        "    \"cost_to_walmart_vs_7d_pct\",\n"
        "    \"te_margin_vs_7d_pct\",\n"
        "    \"walmart_margin_vs_7d_pct\",\n"
        "]\n"
        "CONTROLS = [\"can_show_inv\", \"day_of_week\", \"n_active_nodes\", \"days_since_price_change\"]\n"
        "OUTCOME_COLS = [\"qty_sold\", \"revenue\"]\n"
        "\n"
        "ITS_TOP_N = 20\n"
        "ITS_MIN_EVENTS = 2\n"
        "NB_MIN_OBS = 200\n"
        "\n"
        "# === IMPORTS ===\n"
        "import sys, os, warnings, logging\n"
        "from pathlib import Path\n"
        "sys.path.insert(0, os.path.abspath(\"..\"))\n"
        "\n"
        "import numpy as np\n"
        "import pandas as pd\n"
        "import matplotlib.pyplot as plt\n"
        "import seaborn as sns\n"
        "import statsmodels.api as sm\n"
        "from scipy import stats as sp_stats\n"
        "from statsmodels.stats.multicomp import pairwise_tukeyhsd\n"
        "from statsmodels.discrete.discrete_model import NegativeBinomial\n"
        "from statsmodels.stats.outliers_influence import variance_inflation_factor\n"
        "\n"
        "from src.analysis.config import load_analysis_config\n"
        "from src.analysis import statistical_tests, did_effects\n"
        "\n"
        "warnings.filterwarnings(\"ignore\")\n"
        "logging.basicConfig(level=logging.INFO, format=\"%(asctime)s %(levelname)s %(message)s\")\n"
        "pd.set_option(\"display.max_columns\", 50)\n"
        "sns.set_theme(style=\"whitegrid\")\n"
        "\n"
        "cfg = load_analysis_config()\n"
        "ci_level = cfg[\"ci\"][\"level\"]\n"
        "print(f\"Setup complete. CI level={ci_level}, USE_CACHED={USE_CACHED}\")"
    ))

    # --- 3. Load data ---
    cells.append(code(
        "# === LOAD DATA ===\n"
        "project_root = Path(\"..\").resolve()\n"
        "cached_parquet = project_root / \"outputs\" / f\"correlation_dataset_{END_DATE}.parquet\"\n"
        "\n"
        "if USE_CACHED and cached_parquet.exists():\n"
        "    print(f\"Loading cached parquet: {cached_parquet.name}\")\n"
        "    df = pd.read_parquet(cached_parquet)\n"
        "    prep = None\n"
        "else:\n"
        "    print(\"Building master DataFrame fresh via AnalysisDataPrep (~15 min)...\")\n"
        "    from src.analysis.data_prep import AnalysisDataPrep\n"
        "    ROLLBACKS_PATH = r\"G:\\Shared drives\\07_Finance\\07_10_Pricing_department\\07_11_01_WalmartB2B\\07_10_01_03 Rollbacks\\WalmartB2B Rollbacks tracker.xlsx\"\n"
        "    ROLLBACKS_EXCLUDE_START_DATE = \"2026-03-18\"\n"
        "    WAREHOUSE_ADDRESSES_PATH = str(project_root / \"Warehouse Addresses 03-25-2026 01-43-16 PM.csv\")\n"
        "    prep = AnalysisDataPrep(\n"
        "        end_date=END_DATE,\n"
        "        rollbacks_path=ROLLBACKS_PATH,\n"
        "        rollbacks_exclude_start_date=ROLLBACKS_EXCLUDE_START_DATE,\n"
        "        warehouse_addresses_path=WAREHOUSE_ADDRESSES_PATH,\n"
        "    )\n"
        "    df = prep.run()\n"
        "\n"
        "# Sanity checks\n"
        "assert all(c in df.columns for c in CHANGE_COLS), f\"Missing change cols in df\"\n"
        "assert all(c in df.columns for c in OUTCOME_COLS), f\"Missing outcome cols in df\"\n"
        "print(f\"Shape: {df.shape}  Date range: {df['date'].min().date()} to {df['date'].max().date()}\")\n"
        "print(\"\\nChange-column non-null counts:\")\n"
        "for c in CHANGE_COLS:\n"
        "    n = df[c].notna().sum()\n"
        "    print(f\"  {c}: {n:,} ({n/len(df):.1%})\")"
    ))

    # --- 4. Variant construction ---
    cells.append(md(
        "## 1. Build event-definition variants\n\n"
        "- **V1** continuous — the raw `_vs_7d_pct` columns, used directly as regressors.\n"
        "- **V2** threshold binary — `|pct| >= 1%`, used as DiD treatment and ITS marker.\n"
        "- **V3** directional 5-bin — used as categorical regressor and for ANOVA."
    ))

    cells.append(code(
        "# V2: threshold flags\n"
        "for col in CHANGE_COLS:\n"
        "    df[f\"{col}_v2\"] = (df[col].abs() >= THRESHOLD_PCT).astype(int)\n"
        "    df.loc[df[col].isna(), f\"{col}_v2\"] = np.nan\n"
        "\n"
        "# V3: directional buckets\n"
        "for col in CHANGE_COLS:\n"
        "    df[f\"{col}_v3\"] = pd.cut(\n"
        "        df[col], bins=DIRECTIONAL_BINS, labels=DIRECTIONAL_LABELS, include_lowest=True,\n"
        "    )\n"
        "\n"
        "summary = {}\n"
        "for col in CHANGE_COLS:\n"
        "    v1_nonzero = (df[col].fillna(0) != 0).sum()\n"
        "    v2_treated = df[f\"{col}_v2\"].sum()\n"
        "    v3_sizes = df[f\"{col}_v3\"].value_counts().reindex(DIRECTIONAL_LABELS).fillna(0).astype(int)\n"
        "    summary[col] = {\n"
        "        \"V1 non-null\": df[col].notna().sum(),\n"
        "        \"V1 non-zero\": v1_nonzero,\n"
        "        \"V2 treated\": int(v2_treated),\n"
        "        **{f\"V3 {lbl}\": v3_sizes[lbl] for lbl in DIRECTIONAL_LABELS},\n"
        "    }\n"
        "variant_summary = pd.DataFrame(summary).T\n"
        "variant_summary"
    ))

    # --- 5. Sample characterization ---
    cells.append(md(
        "## 2. Sample characterization\n\n"
        "Distributions of the three % change predictors and a VIF check before the\n"
        "multi-predictor regressions."
    ))

    cells.append(code(
        "fig, axes = plt.subplots(1, 3, figsize=(15, 4))\n"
        "for ax, col in zip(axes, CHANGE_COLS):\n"
        "    vals = df[col].dropna()\n"
        "    clipped = vals.clip(-0.2, 0.2)\n"
        "    ax.hist(clipped, bins=50, edgecolor=\"white\")\n"
        "    ax.axvline(0, color=\"red\", linestyle=\"--\", linewidth=1)\n"
        "    ax.set_title(col)\n"
        "    ax.set_xlabel(\"% change (clipped at ±20%)\")\n"
        "plt.tight_layout()\n"
        "plt.show()\n"
        "\n"
        "# VIF on continuous predictors where all three are non-null\n"
        "sub = df[CHANGE_COLS + CONTROLS].dropna()\n"
        "X = sm.add_constant(sub[CHANGE_COLS].astype(float).values)\n"
        "vif_rows = []\n"
        "for i, col in enumerate([\"const\"] + CHANGE_COLS):\n"
        "    try:\n"
        "        v = variance_inflation_factor(X, i)\n"
        "    except Exception:\n"
        "        v = np.nan\n"
        "    vif_rows.append({\"variable\": col, \"VIF\": v})\n"
        "vif_df = pd.DataFrame(vif_rows)\n"
        "print(f\"Joint-non-null sample for VIF/regression: {len(sub):,} rows\")\n"
        "print(vif_df.to_string(index=False))"
    ))

    # --- 6. OLS + log-OLS revenue ---
    cells.append(md(
        "## 3. OLS regressions (V1 continuous main effects)\n\n"
        "Two specifications: `qty_sold` and `log(1+revenue)`, both on the three % changes +\n"
        "controls. Reuses `statistical_tests.ols_regression`."
    ))

    cells.append(code(
        "features = CHANGE_COLS + CONTROLS\n"
        "\n"
        "ols_qty = statistical_tests.ols_regression(\n"
        "    df, feature_cols=features, target_col=\"qty_sold\", ci_level=ci_level,\n"
        ")\n"
        "ols_rev = statistical_tests.ols_regression(\n"
        "    df, feature_cols=features, target_col=\"revenue\",\n"
        "    log_transform=True, ci_level=ci_level,\n"
        ")\n"
        "\n"
        "for label, res in [(\"qty_sold (linear)\", ols_qty), (\"log(1+revenue)\", ols_rev)]:\n"
        "    print(f\"\\n=== OLS: {label}  n={res['n_obs']:,}  R2={res['r_squared']:.4f} ===\")\n"
        "    c = res[\"coefficients\"]\n"
        "    display(c[c[\"variable\"].isin(CHANGE_COLS)].round(5))"
    ))

    # --- 7. Two-way FE ---
    cells.append(md(
        "## 4. Two-way fixed-effects OLS (SKU-Node + date demeaning)\n\n"
        "Controls for unobserved SKU-Node and date heterogeneity via within-entity +\n"
        "within-time demeaning (same approach as `elasticity.estimate_elasticity_fe`).\n"
        "Clustered SEs at the SKU-Node level."
    ))

    cells.append(code(
        "def two_way_fe_ols(data, y_col, x_cols, entity_col=\"SKU_Node\", time_col=\"date\", ci_level=0.95):\n"
        "    d = data[[y_col, entity_col, time_col] + x_cols].dropna().copy()\n"
        "    if len(d) == 0:\n"
        "        return None\n"
        "    # Two-way demeaning: iterate until convergence (simple 2-pass for speed)\n"
        "    y = np.ascontiguousarray(d[y_col].astype(float).values, dtype=float)\n"
        "    X = np.ascontiguousarray(d[x_cols].astype(float).values, dtype=float).copy()\n"
        "    entity_idx = d[entity_col].astype(\"category\").cat.codes.values.astype(np.int64)\n"
        "    time_idx = d[time_col].astype(\"category\").cat.codes.values.astype(np.int64)\n"
        "\n"
        "    def demean(arr, idx):\n"
        "        n = int(idx.max()) + 1\n"
        "        sums = np.bincount(idx, weights=arr, minlength=n)\n"
        "        counts = np.bincount(idx, minlength=n).astype(float)\n"
        "        means = np.divide(sums, counts, out=np.zeros_like(sums), where=counts > 0)\n"
        "        return arr - means[idx]\n"
        "\n"
        "    # 3-pass alternating demean approximates the two-way within transform\n"
        "    for _ in range(3):\n"
        "        y = demean(y, entity_idx)\n"
        "        y = demean(y, time_idx)\n"
        "        for j in range(X.shape[1]):\n"
        "            col = demean(X[:, j].copy(), entity_idx)\n"
        "            X[:, j] = demean(col, time_idx)\n"
        "\n"
        "    model = sm.OLS(y, X).fit(cov_type=\"cluster\", cov_kwds={\"groups\": entity_idx})\n"
        "    z = sp_stats.norm.ppf(1 - (1 - ci_level) / 2)\n"
        "    p = np.asarray(model.params); b = np.asarray(model.bse); pv = np.asarray(model.pvalues)\n"
        "    rows = []\n"
        "    for i, name in enumerate(x_cols):\n"
        "        rows.append({\n"
        "            \"variable\": name,\n"
        "            \"coef\": float(p[i]),\n"
        "            \"se\": float(b[i]),\n"
        "            \"ci_lower\": float(p[i] - z * b[i]),\n"
        "            \"ci_upper\": float(p[i] + z * b[i]),\n"
        "            \"p_value\": float(pv[i]),\n"
        "        })\n"
        "    return {\"coef_df\": pd.DataFrame(rows), \"n_obs\": int(model.nobs), \"r_squared\": float(model.rsquared)}\n"
        "\n"
        "if \"SKU_Node\" not in df.columns:\n"
        "    df[\"SKU_Node\"] = df[\"sku\"].astype(str) + \"-\" + df[\"node\"].astype(str)\n"
        "\n"
        "fe_res = two_way_fe_ols(df, \"qty_sold\", CHANGE_COLS, ci_level=ci_level)\n"
        "print(f\"Two-way FE OLS: n={fe_res['n_obs']:,}  within-R2={fe_res['r_squared']:.4f}\")\n"
        "display(fe_res[\"coef_df\"].round(5))"
    ))

    # --- 8. NegBin ---
    cells.append(md(
        "## 5. Negative Binomial on `qty_sold | qty_sold > 0`\n\n"
        "Count model restricted to sale days, avoids the 97% zero-inflation issue."
    ))

    cells.append(code(
        "sale_days = df[df[\"qty_sold\"] > 0].dropna(subset=CHANGE_COLS + [\"qty_sold\"])\n"
        "print(f\"Sale-day sample: {len(sale_days):,}\")\n"
        "\n"
        "nb_res = None\n"
        "if len(sale_days) >= NB_MIN_OBS:\n"
        "    X = sm.add_constant(sale_days[CHANGE_COLS].astype(float))\n"
        "    y = sale_days[\"qty_sold\"].astype(float)\n"
        "    try:\n"
        "        nb_model = NegativeBinomial(y, X).fit(disp=False, maxiter=200)\n"
        "        z = sp_stats.norm.ppf(1 - (1 - ci_level) / 2)\n"
        "        p = np.asarray(nb_model.params); b = np.asarray(nb_model.bse); pv = np.asarray(nb_model.pvalues)\n"
        "        nb_rows = []\n"
        "        for i, name in enumerate([\"const\"] + CHANGE_COLS):\n"
        "            nb_rows.append({\n"
        "                \"variable\": name,\n"
        "                \"coef\": float(p[i]),\n"
        "                \"se\": float(b[i]),\n"
        "                \"ci_lower\": float(p[i] - z * b[i]),\n"
        "                \"ci_upper\": float(p[i] + z * b[i]),\n"
        "                \"p_value\": float(pv[i]),\n"
        "            })\n"
        "        nb_res = {\n"
        "            \"coef_df\": pd.DataFrame(nb_rows),\n"
        "            \"n_obs\": int(nb_model.nobs),\n"
        "            \"pseudo_r2\": float(nb_model.prsquared),\n"
        "        }\n"
        "        print(f\"NegBin: n={nb_res['n_obs']:,}  pseudo-R2={nb_res['pseudo_r2']:.4f}\")\n"
        "        display(nb_res[\"coef_df\"][nb_res[\"coef_df\"][\"variable\"].isin(CHANGE_COLS)].round(5))\n"
        "    except Exception as e:\n"
        "        print(f\"NegBin failed: {e}\")\n"
        "else:\n"
        "    print(f\"Skipping NegBin: fewer than {NB_MIN_OBS} sale-day rows\")"
    ))

    # --- 9. Semi-log elasticity ---
    cells.append(md(
        "## 6. Semi-log own-price elasticity\n\n"
        "Pooled and within-entity (FE) specifications of `log(1+qty_sold) ~ β·pct_price`.\n"
        "Semi-log is used because signed % changes break log-log. The coefficient β is\n"
        "interpreted as the % change in (1+qty) per 1pp change in price."
    ))

    cells.append(code(
        "def semilog_elasticity(data, pct_col, y_col=\"qty_sold\", cluster_col=None, ci_level=0.95):\n"
        "    d = data[[pct_col, y_col] + ([cluster_col] if cluster_col else [])].dropna()\n"
        "    if len(d) < 100:\n"
        "        return None\n"
        "    y = np.log1p(d[y_col].astype(float).clip(lower=0))\n"
        "    X = sm.add_constant(d[pct_col].astype(float))\n"
        "    if cluster_col is not None:\n"
        "        groups = d[cluster_col].astype(\"category\").cat.codes.values\n"
        "        model = sm.OLS(y, X).fit(cov_type=\"cluster\", cov_kwds={\"groups\": groups})\n"
        "    else:\n"
        "        model = sm.OLS(y, X).fit(cov_type=\"HC1\")\n"
        "    z = sp_stats.norm.ppf(1 - (1 - ci_level) / 2)\n"
        "    beta = float(model.params.iloc[1])\n"
        "    se = float(model.bse.iloc[1])\n"
        "    return {\n"
        "        \"beta\": beta, \"se\": se,\n"
        "        \"ci_lower\": beta - z * se, \"ci_upper\": beta + z * se,\n"
        "        \"p_value\": float(model.pvalues.iloc[1]), \"n_obs\": int(model.nobs),\n"
        "    }\n"
        "\n"
        "def semilog_elasticity_fe(data, pct_col, y_col=\"qty_sold\", entity_col=\"SKU_Node\", ci_level=0.95):\n"
        "    d = data[[pct_col, y_col, entity_col]].dropna()\n"
        "    if len(d) < 100:\n"
        "        return None\n"
        "    y = np.ascontiguousarray(np.log1p(d[y_col].astype(float).clip(lower=0)).values, dtype=float)\n"
        "    x = np.ascontiguousarray(d[pct_col].astype(float).values, dtype=float)\n"
        "    idx = d[entity_col].astype(\"category\").cat.codes.values.astype(np.int64)\n"
        "    def demean(arr, idx):\n"
        "        n = int(idx.max()) + 1\n"
        "        sums = np.bincount(idx, weights=arr, minlength=n)\n"
        "        counts = np.bincount(idx, minlength=n).astype(float)\n"
        "        means = np.divide(sums, counts, out=np.zeros_like(sums), where=counts > 0)\n"
        "        return arr - means[idx]\n"
        "    y_dm = demean(y, idx)\n"
        "    x_dm = demean(x, idx)\n"
        "    # Keep entities with within-variation\n"
        "    mask = np.abs(x_dm) > 1e-12\n"
        "    if mask.sum() < 100:\n"
        "        return None\n"
        "    model = sm.OLS(y_dm[mask], x_dm[mask]).fit(cov_type=\"cluster\", cov_kwds={\"groups\": idx[mask]})\n"
        "    z = sp_stats.norm.ppf(1 - (1 - ci_level) / 2)\n"
        "    params = np.asarray(model.params)\n"
        "    bse = np.asarray(model.bse)\n"
        "    pvals = np.asarray(model.pvalues)\n"
        "    beta = float(params[0])\n"
        "    se = float(bse[0])\n"
        "    return {\n"
        "        \"beta\": beta, \"se\": se,\n"
        "        \"ci_lower\": beta - z * se, \"ci_upper\": beta + z * se,\n"
        "        \"p_value\": float(pvals[0]), \"n_obs\": int(model.nobs),\n"
        "    }\n"
        "\n"
        "elast_pooled = semilog_elasticity(df, \"cost_to_walmart_vs_7d_pct\", cluster_col=\"SKU_Node\", ci_level=ci_level)\n"
        "elast_fe = semilog_elasticity_fe(df, \"cost_to_walmart_vs_7d_pct\", ci_level=ci_level)\n"
        "elast_df = pd.DataFrame({\n"
        "    \"spec\": [\"Pooled (SKU-Node clustered)\", \"Within-entity FE\"],\n"
        "    **{k: [elast_pooled[k] if elast_pooled else np.nan, elast_fe[k] if elast_fe else np.nan] for k in [\"beta\", \"se\", \"ci_lower\", \"ci_upper\", \"p_value\", \"n_obs\"]},\n"
        "})\n"
        "print(\"=== Semi-log own-price elasticity (β = Δlog(1+qty) per 1pp change in cost) ===\")\n"
        "display(elast_df.round(5))"
    ))

    # --- 10. DiD ---
    cells.append(md(
        "## 7. Diff-in-Diff on threshold (V2) price-change events\n\n"
        "Uses `did_effects.build_did_panel` + `heterogeneous_did`. Treatment column is\n"
        "`cost_to_walmart_vs_7d_pct` **masked to zero** where `|pct| < 1%` so only material\n"
        "changes define treated SKU-Nodes."
    ))

    cells.append(code(
        "# build_did_panel is O(n_treated * n_rows) — pre-subsample the universe\n"
        "# to a tractable set of top-event SKU-Nodes + random controls.\n"
        "DID_TOP_TREATED = 300      # top SKU-Nodes by V2 event count\n"
        "DID_N_CONTROLS = 600       # random never-V2 SKU-Nodes as control pool\n"
        "\n"
        "df_did_src = df.copy()\n"
        "if \"SKU_Node\" not in df_did_src.columns:\n"
        "    df_did_src[\"SKU_Node\"] = df_did_src[\"sku\"].astype(str) + \"-\" + df_did_src[\"node\"].astype(str)\n"
        "\n"
        "event_counts = (\n"
        "    df_did_src.assign(_v2=(df_did_src[\"cost_to_walmart_vs_7d_pct\"].abs() >= THRESHOLD_PCT).astype(int))\n"
        "    .groupby(\"SKU_Node\")[\"_v2\"].sum()\n"
        ")\n"
        "treated_universe = event_counts.sort_values(ascending=False).head(DID_TOP_TREATED).index\n"
        "control_pool = event_counts[event_counts == 0].index\n"
        "rng = np.random.default_rng(42)\n"
        "control_universe = rng.choice(control_pool, size=min(DID_N_CONTROLS, len(control_pool)), replace=False)\n"
        "keep = set(treated_universe) | set(control_universe)\n"
        "print(f\"DiD universe: {len(treated_universe)} treated + {len(control_universe)} controls = {len(keep)} SKU-Nodes\")\n"
        "\n"
        "df_did = df_did_src[df_did_src[\"SKU_Node\"].isin(keep)].copy()\n"
        "mask_small = df_did[\"cost_to_walmart_vs_7d_pct\"].abs() < THRESHOLD_PCT\n"
        "df_did[\"treatment_pct_v2\"] = df_did[\"cost_to_walmart_vs_7d_pct\"].where(~mask_small, 0.0).fillna(0.0)\n"
        "print(f\"DiD source df: {len(df_did):,} rows\")\n"
        "\n"
        "panel = did_effects.build_did_panel(\n"
        "    df_did, treatment_col=\"treatment_pct_v2\",\n"
        "    max_controls_per_brand=5, pre_window_days=7, post_window_days=14, min_event_buffer_days=14,\n"
        ")\n"
        "print(f\"DiD panel: {len(panel):,} rows\")\n"
        "\n"
        "did_overall = None\n"
        "if len(panel) == 0:\n"
        "    print(\"DiD panel empty - no eligible events within buffered window.\")\n"
        "    did_brand = pd.DataFrame()\n"
        "    did_ptier = pd.DataFrame()\n"
        "else:\n"
        "    # Pooled overall ATT: qty_sold ~ treated + post + treated_x_post + dow dummies\n"
        "    dow_d = pd.get_dummies(panel[\"day_of_week\"], prefix=\"dow\", drop_first=True, dtype=float)\n"
        "    X_pool = pd.concat([panel[[\"treated\", \"post\", \"treated_x_post\"]].astype(float), dow_d], axis=1)\n"
        "    X_pool = sm.add_constant(X_pool, has_constant=\"add\")\n"
        "    y_pool = panel[\"qty_sold\"].astype(float)\n"
        "    pool_model = sm.OLS(y_pool.values, X_pool.values).fit(cov_type=\"HC1\")\n"
        "    z = sp_stats.norm.ppf(1 - (1 - ci_level) / 2)\n"
        "    pp = np.asarray(pool_model.params); pb = np.asarray(pool_model.bse); ppv = np.asarray(pool_model.pvalues)\n"
        "    att = float(pp[3]); se = float(pb[3])\n"
        "    did_overall = {\"ATT\": att, \"se\": se, \"ci_lower\": att - z*se, \"ci_upper\": att + z*se,\n"
        "                   \"p_value\": float(ppv[3]), \"n_obs\": int(pool_model.nobs)}\n"
        "    print(f\"\\n=== Pooled overall ATT (V2 threshold treatment on % price change) ===\")\n"
        "    print(f\"ATT={att:+.4f} [{did_overall['ci_lower']:+.4f}, {did_overall['ci_upper']:+.4f}] p={did_overall['p_value']:.4f}  n={did_overall['n_obs']:,}\")\n"
        "\n"
        "    did_brand = did_effects.heterogeneous_did(panel, \"brand\", top_n=10, min_obs=30, ci_level=ci_level)\n"
        "    did_ptier = did_effects.heterogeneous_did(panel, \"price_tier\", top_n=4, min_obs=30, ci_level=ci_level)\n"
        "    print(\"\\n=== Brand ATT subgroups ===\")\n"
        "    display(did_brand.round(5) if len(did_brand) else \"(no subgroup met min_obs)\")\n"
        "    print(\"\\n=== Price-tier ATT subgroups ===\")\n"
        "    display(did_ptier.round(5) if len(did_ptier) else \"(no subgroup met min_obs)\")"
    ))

    # --- 11. ITS ---
    cells.append(md(
        "## 8. Interrupted time series on top-20 SKU-Nodes by V2 event count\n\n"
        "Per SKU-Node, fit a segmented regression around the first V2 price-change event\n"
        "with HAC (Newey-West, lag=7) standard errors. Coefficient of interest = level\n"
        "shift post-event."
    ))

    cells.append(code(
        "def fit_its(sub, event_date):\n"
        "    sub = sub.sort_values(\"date\").copy()\n"
        "    sub[\"t\"] = (sub[\"date\"] - sub[\"date\"].min()).dt.days.astype(float)\n"
        "    sub[\"post\"] = (sub[\"date\"] >= event_date).astype(float)\n"
        "    sub[\"t_post\"] = sub[\"post\"] * ((sub[\"date\"] - event_date).dt.days.astype(float))\n"
        "    X = sm.add_constant(sub[[\"t\", \"post\", \"t_post\"]])\n"
        "    y = sub[\"qty_sold\"].astype(float)\n"
        "    try:\n"
        "        return sm.OLS(y, X).fit(cov_type=\"HAC\", cov_kwds={\"maxlags\": 7})\n"
        "    except Exception:\n"
        "        return None\n"
        "\n"
        "# Identify first V2 event per SKU-Node\n"
        "ev = df[df[\"cost_to_walmart_vs_7d_pct_v2\"] == 1].groupby(\"SKU_Node\")[\"date\"].min().rename(\"first_event\")\n"
        "ev_count = df[df[\"cost_to_walmart_vs_7d_pct_v2\"] == 1].groupby(\"SKU_Node\").size().rename(\"n_events\")\n"
        "ev = pd.concat([ev, ev_count], axis=1).dropna()\n"
        "ev = ev[ev[\"n_events\"] >= ITS_MIN_EVENTS].sort_values(\"n_events\", ascending=False).head(ITS_TOP_N)\n"
        "\n"
        "its_rows = []\n"
        "z = sp_stats.norm.ppf(1 - (1 - ci_level) / 2)\n"
        "for sku_node, row in ev.iterrows():\n"
        "    sub = df[df[\"SKU_Node\"] == sku_node][[\"date\", \"qty_sold\"]].dropna()\n"
        "    if len(sub) < 20:\n"
        "        continue\n"
        "    res = fit_its(sub, row[\"first_event\"])\n"
        "    if res is None or \"post\" not in res.params.index:\n"
        "        continue\n"
        "    coef = res.params[\"post\"]\n"
        "    se = res.bse[\"post\"]\n"
        "    its_rows.append({\n"
        "        \"SKU_Node\": sku_node, \"n_events\": int(row[\"n_events\"]),\n"
        "        \"level_shift\": coef, \"se\": se,\n"
        "        \"ci_lower\": coef - z * se, \"ci_upper\": coef + z * se,\n"
        "        \"p_value\": res.pvalues[\"post\"], \"n_obs\": int(res.nobs),\n"
        "    })\n"
        "\n"
        "its_df = pd.DataFrame(its_rows).sort_values(\"p_value\") if its_rows else pd.DataFrame()\n"
        "print(f\"ITS fitted for {len(its_df)} / {len(ev)} top SKU-Nodes\")\n"
        "if len(its_df):\n"
        "    display(its_df.head(20).round(4))"
    ))

    # --- 12. Directional bucket ANOVA ---
    cells.append(md(
        "## 9. Directional bucket (V3) comparison\n\n"
        "One-way ANOVA + Tukey HSD on `qty_sold` and `revenue` across the 5 directional\n"
        "buckets, for each of the three change variables."
    ))

    cells.append(code(
        "bucket_rows = []\n"
        "for change_col in CHANGE_COLS:\n"
        "    bcol = f\"{change_col}_v3\"\n"
        "    for outcome in OUTCOME_COLS:\n"
        "        sub = df[[bcol, outcome]].dropna()\n"
        "        groups = [sub.loc[sub[bcol] == lbl, outcome].values for lbl in DIRECTIONAL_LABELS if (sub[bcol] == lbl).sum() > 0]\n"
        "        if len(groups) < 2:\n"
        "            continue\n"
        "        f_stat, p_val = sp_stats.f_oneway(*groups)\n"
        "        means = sub.groupby(bcol, observed=False)[outcome].agg([\"mean\", \"count\"]).reindex(DIRECTIONAL_LABELS)\n"
        "        row = {\"change_col\": change_col, \"outcome\": outcome, \"F\": f_stat, \"p_value\": p_val}\n"
        "        for lbl in DIRECTIONAL_LABELS:\n"
        "            row[f\"mean_{lbl.split(' ')[0]}\"] = means.loc[lbl, \"mean\"] if lbl in means.index else np.nan\n"
        "            row[f\"n_{lbl.split(' ')[0]}\"] = int(means.loc[lbl, \"count\"]) if lbl in means.index else 0\n"
        "        bucket_rows.append(row)\n"
        "\n"
        "bucket_df = pd.DataFrame(bucket_rows)\n"
        "display(bucket_df.round(5))\n"
        "\n"
        "# One Tukey example for the headline variable\n"
        "print(\"\\n=== Tukey HSD: qty_sold ~ cost_to_walmart_vs_7d_pct_v3 ===\")\n"
        "sub = df[[\"cost_to_walmart_vs_7d_pct_v3\", \"qty_sold\"]].dropna().sample(n=min(200_000, len(df)), random_state=42)\n"
        "tukey = pairwise_tukeyhsd(endog=sub[\"qty_sold\"], groups=sub[\"cost_to_walmart_vs_7d_pct_v3\"].astype(str), alpha=0.05)\n"
        "print(tukey.summary())"
    ))

    # --- 13. Interaction robustness ---
    cells.append(md(
        "## 10. Interaction sensitivity (one-off robustness)\n\n"
        "Adds `price_pct × te_pct` and `price_pct × wm_pct` to the main OLS. Reported as a\n"
        "robustness check only — headline inference rests on the main-effect models above."
    ))

    cells.append(code(
        "df_int = df.dropna(subset=CHANGE_COLS + [\"qty_sold\"]).copy()\n"
        "df_int[\"price_x_te\"] = df_int[\"cost_to_walmart_vs_7d_pct\"] * df_int[\"te_margin_vs_7d_pct\"]\n"
        "df_int[\"price_x_wm\"] = df_int[\"cost_to_walmart_vs_7d_pct\"] * df_int[\"walmart_margin_vs_7d_pct\"]\n"
        "int_features = CHANGE_COLS + [\"price_x_te\", \"price_x_wm\"] + CONTROLS\n"
        "int_res = statistical_tests.ols_regression(df_int, feature_cols=int_features, target_col=\"qty_sold\", ci_level=ci_level)\n"
        "print(f\"Interaction OLS: n={int_res['n_obs']:,}  R2={int_res['r_squared']:.4f}\")\n"
        "display(int_res[\"coefficients\"][int_res[\"coefficients\"][\"variable\"].isin(CHANGE_COLS + [\"price_x_te\", \"price_x_wm\"])].round(5))"
    ))

    # --- 14. Summary + export ---
    cells.append(md(
        "## 11. Summary: coefficient forest plot + parquet export"
    ))

    cells.append(code(
        "# Collect headline coefficients across specs\n"
        "rows = []\n"
        "for name, res in [(\"OLS qty_sold\", ols_qty), (\"log-OLS revenue\", ols_rev)]:\n"
        "    for _, r in res[\"coefficients\"].iterrows():\n"
        "        if r[\"variable\"] in CHANGE_COLS:\n"
        "            rows.append({\"analysis\": name, \"predictor\": r[\"variable\"], \"coef\": r[\"coef\"],\n"
        "                         \"ci_lower\": r[\"ci_lower\"], \"ci_upper\": r[\"ci_upper\"],\n"
        "                         \"p_value\": r[\"p_value\"], \"n_obs\": res[\"n_obs\"]})\n"
        "for _, r in fe_res[\"coef_df\"].iterrows():\n"
        "    rows.append({\"analysis\": \"Two-way FE OLS qty\", \"predictor\": r[\"variable\"], \"coef\": r[\"coef\"],\n"
        "                 \"ci_lower\": r[\"ci_lower\"], \"ci_upper\": r[\"ci_upper\"],\n"
        "                 \"p_value\": r[\"p_value\"], \"n_obs\": fe_res[\"n_obs\"]})\n"
        "if nb_res is not None:\n"
        "    for _, r in nb_res[\"coef_df\"].iterrows():\n"
        "        if r[\"variable\"] in CHANGE_COLS:\n"
        "            rows.append({\"analysis\": \"NegBin qty|qty>0\", \"predictor\": r[\"variable\"], \"coef\": r[\"coef\"],\n"
        "                         \"ci_lower\": r[\"ci_lower\"], \"ci_upper\": r[\"ci_upper\"],\n"
        "                         \"p_value\": r[\"p_value\"], \"n_obs\": nb_res[\"n_obs\"]})\n"
        "if elast_pooled:\n"
        "    rows.append({\"analysis\": \"Semi-log elasticity (pooled)\", \"predictor\": \"cost_to_walmart_vs_7d_pct\",\n"
        "                 \"coef\": elast_pooled[\"beta\"], \"ci_lower\": elast_pooled[\"ci_lower\"],\n"
        "                 \"ci_upper\": elast_pooled[\"ci_upper\"], \"p_value\": elast_pooled[\"p_value\"],\n"
        "                 \"n_obs\": elast_pooled[\"n_obs\"]})\n"
        "if elast_fe:\n"
        "    rows.append({\"analysis\": \"Semi-log elasticity (FE)\", \"predictor\": \"cost_to_walmart_vs_7d_pct\",\n"
        "                 \"coef\": elast_fe[\"beta\"], \"ci_lower\": elast_fe[\"ci_lower\"],\n"
        "                 \"ci_upper\": elast_fe[\"ci_upper\"], \"p_value\": elast_fe[\"p_value\"],\n"
        "                 \"n_obs\": elast_fe[\"n_obs\"]})\n"
        "if did_overall is not None:\n"
        "    rows.append({\"analysis\": \"DiD pooled ATT (V2 threshold)\", \"predictor\": \"cost_to_walmart_vs_7d_pct\",\n"
        "                 \"coef\": did_overall[\"ATT\"], \"ci_lower\": did_overall[\"ci_lower\"],\n"
        "                 \"ci_upper\": did_overall[\"ci_upper\"], \"p_value\": did_overall[\"p_value\"],\n"
        "                 \"n_obs\": did_overall[\"n_obs\"]})\n"
        "\n"
        "summary_df = pd.DataFrame(rows)\n"
        "display(summary_df.round(5))\n"
        "\n"
        "# Forest plot\n"
        "fig, ax = plt.subplots(figsize=(10, max(4, 0.4 * len(summary_df))))\n"
        "colors = {c: col for c, col in zip(CHANGE_COLS, [\"#1f77b4\", \"#ff7f0e\", \"#2ca02c\"])}\n"
        "for i, r in summary_df.reset_index(drop=True).iterrows():\n"
        "    c = colors.get(r[\"predictor\"], \"#666\")\n"
        "    ax.errorbar(r[\"coef\"], i, xerr=[[r[\"coef\"] - r[\"ci_lower\"]], [r[\"ci_upper\"] - r[\"coef\"]]],\n"
        "                fmt=\"o\", color=c, capsize=3)\n"
        "ax.axvline(0, color=\"red\", linestyle=\"--\", linewidth=1)\n"
        "ax.set_yticks(range(len(summary_df)))\n"
        "ax.set_yticklabels([f\"{r['analysis']} :: {r['predictor']}\" for _, r in summary_df.iterrows()], fontsize=8)\n"
        "ax.set_xlabel(\"coefficient (95% CI)\")\n"
        "ax.set_title(\"Change-based predictors: coefficients across specifications\")\n"
        "plt.tight_layout()\n"
        "plt.show()\n"
        "\n"
        "# Export\n"
        "out_path = project_root / \"outputs\" / f\"change_based_coefficients_{END_DATE}.parquet\"\n"
        "summary_df.to_parquet(out_path, index=False)\n"
        "print(f\"\\nSaved: {out_path}\")"
    ))

    # --- 15. Cleanup ---
    cells.append(code(
        "if prep is not None:\n"
        "    prep.close()\n"
        "print(\"Done.\")"
    ))

    nb.cells = cells
    return nb


def main():
    nb = build_notebook()
    out = Path(__file__).resolve().parent.parent / "notebooks" / "change_based_analysis.ipynb"
    out.parent.mkdir(parents=True, exist_ok=True)
    nbf.write(nb, str(out))
    print(f"Wrote {out}  ({len(nb.cells)} cells)")


if __name__ == "__main__":
    main()
