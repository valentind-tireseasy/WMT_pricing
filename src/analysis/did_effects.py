"""Difference-in-Differences treatment effect estimation.

Builds a DiD panel from daily SKU-Node sales data, matches treated
SKU-Nodes to brand-matched controls, and runs heterogeneous OLS
regressions with HC1 robust standard errors.

Extracted from original notebook cells 63-64.
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import statsmodels.api as sm
from scipy import stats as sp_stats

from src.analysis import ci_utils, plot_utils


# ---------------------------------------------------------------------------
# Panel construction
# ---------------------------------------------------------------------------

def build_did_panel(
    df,
    treatment_col="cost_to_walmart_vs_7d",
    max_controls_per_brand=5,
    pre_window_days=7,
    post_window_days=14,
    min_event_buffer_days=14,
):
    """Build a Difference-in-Differences panel dataset.

    Parameters
    ----------
    df : DataFrame
        Daily SKU-Node level data.  Must contain columns: ``sku``
        (or ``Product Code``), ``node`` (or ``Identifier``), ``date``,
        ``qty_sold``, ``offer_price``, ``brand`` (or ``Brand code``),
        and *treatment_col*.
    treatment_col : str
        Column indicating the treatment intensity.  A SKU-Node is
        considered treated if its value is non-zero on any date.
    max_controls_per_brand : int
        Maximum number of brand-matched control SKU-Nodes per brand.
    pre_window_days : int
        Days before event to include in panel.
    post_window_days : int
        Days after event to include in panel.
    min_event_buffer_days : int
        Minimum number of days an event must be from the analysis
        boundaries (earliest / latest date) to be included.

    Returns
    -------
    DataFrame
        Panel with columns: ``SKU_Node``, ``date``, ``qty_sold``,
        ``offer_price``, ``brand``, ``treated``, ``post``,
        ``treated_x_post``, ``price_tier``, ``day_of_week``, plus
        any additional columns from the original data.
    """
    df = df.copy()

    # Normalise column names -------------------------------------------------
    if "Product Code" in df.columns and "sku" not in df.columns:
        df.rename(columns={"Product Code": "sku"}, inplace=True)
    if "Identifier" in df.columns and "node" not in df.columns:
        df.rename(columns={"Identifier": "node"}, inplace=True)
    if "Brand code" in df.columns and "brand" not in df.columns:
        df.rename(columns={"Brand code": "brand"}, inplace=True)

    # Ensure date is datetime
    df["date"] = pd.to_datetime(df["date"])

    # SKU_Node key -----------------------------------------------------------
    if "SKU_Node" not in df.columns:
        df["SKU_Node"] = df["sku"].astype(str) + "-" + df["node"].astype(str)

    # Identify treated candidates (non-zero treatment_col on any date) -------
    treatment_flags = (
        df.groupby("SKU_Node")[treatment_col]
        .apply(lambda s: (s != 0).any())
    )
    treated_nodes = set(treatment_flags[treatment_flags].index)

    # First event date per treated node (first date with non-zero value) -----
    treated_df = df[df["SKU_Node"].isin(treated_nodes)].copy()
    treated_df = treated_df[treated_df[treatment_col] != 0]
    first_event = (
        treated_df.groupby("SKU_Node")["date"]
        .min()
        .rename("event_date")
    )

    # Filter events with sufficient pre/post buffer from data boundaries -----
    date_min = df["date"].min()
    date_max = df["date"].max()

    buffer = pd.Timedelta(days=min_event_buffer_days)
    first_event = first_event[
        (first_event >= date_min + buffer)
        & (first_event <= date_max - buffer)
    ]

    if first_event.empty:
        return pd.DataFrame()

    treated_nodes = set(first_event.index)

    # Brand lookup for treated nodes -----------------------------------------
    node_brand = (
        df[df["SKU_Node"].isin(treated_nodes)]
        .drop_duplicates("SKU_Node")[["SKU_Node", "brand"]]
        .set_index("SKU_Node")["brand"]
    )

    # Control matching: brand-matched, never treated -------------------------
    all_nodes = set(df["SKU_Node"].unique())
    control_candidates = all_nodes - treated_nodes

    # Map every node to its brand
    all_node_brand = (
        df.drop_duplicates("SKU_Node")[["SKU_Node", "brand"]]
        .set_index("SKU_Node")["brand"]
    )

    rng = np.random.default_rng(42)
    control_nodes = set()
    for brand_val in node_brand.unique():
        brand_controls = [
            n for n in control_candidates
            if all_node_brand.get(n) == brand_val
        ]
        if len(brand_controls) > max_controls_per_brand:
            chosen = rng.choice(
                brand_controls, size=max_controls_per_brand, replace=False
            )
            control_nodes.update(chosen)
        else:
            control_nodes.update(brand_controls)

    # Build panel ------------------------------------------------------------
    pre_td = pd.Timedelta(days=pre_window_days)
    post_td = pd.Timedelta(days=post_window_days)

    panels = []

    # Treated panel: window around each node's event_date
    for node, event_date in first_event.items():
        mask = (
            (df["SKU_Node"] == node)
            & (df["date"] >= event_date - pre_td)
            & (df["date"] <= event_date + post_td)
        )
        node_panel = df.loc[mask].copy()
        node_panel["treated"] = 1
        node_panel["post"] = (node_panel["date"] >= event_date).astype(int)
        node_panel["event_date"] = event_date
        panels.append(node_panel)

    # Control panel: use each treated node's event_date for its brand-matched
    # controls so the calendar window aligns
    brand_event_dates = first_event.to_frame().join(node_brand.rename("brand"))

    for brand_val, grp in brand_event_dates.groupby("brand"):
        brand_controls_set = [
            n for n in control_nodes
            if all_node_brand.get(n) == brand_val
        ]
        if not brand_controls_set:
            continue

        # Use the median event date for this brand's controls
        median_event = grp["event_date"].sort_values().iloc[len(grp) // 2]

        for ctrl_node in brand_controls_set:
            mask = (
                (df["SKU_Node"] == ctrl_node)
                & (df["date"] >= median_event - pre_td)
                & (df["date"] <= median_event + post_td)
            )
            ctrl_panel = df.loc[mask].copy()
            ctrl_panel["treated"] = 0
            ctrl_panel["post"] = (ctrl_panel["date"] >= median_event).astype(int)
            ctrl_panel["event_date"] = median_event
            panels.append(ctrl_panel)

    if not panels:
        return pd.DataFrame()

    panel = pd.concat(panels, ignore_index=True)

    # Interaction term -------------------------------------------------------
    panel["treated_x_post"] = panel["treated"] * panel["post"]

    # Day of week control ----------------------------------------------------
    panel["day_of_week"] = panel["date"].dt.dayofweek

    # Price tier via safe qcut -----------------------------------------------
    panel["price_tier"] = _safe_price_tier(panel)

    return panel


def _safe_price_tier(panel):
    """Assign price_tier via qcut with graceful fallback.

    Tries 4 tiers (Budget/Mid/Premium/Luxury), then 2
    (Budget/Premium), then assigns 'Budget' to all rows.
    """
    labels_4 = ["Budget", "Mid", "Premium", "Luxury"]
    labels_2 = ["Budget", "Premium"]

    for brand_val in panel["brand"].unique():
        mask = panel["brand"] == brand_val
        prices = panel.loc[mask, "offer_price"]

        assigned = False
        for labels in (labels_4, labels_2):
            try:
                panel.loc[mask, "price_tier"] = pd.qcut(
                    prices, q=len(labels), labels=labels, duplicates="drop"
                )
                # Verify we actually got labels assigned
                if panel.loc[mask, "price_tier"].notna().any():
                    assigned = True
                    break
            except (ValueError, TypeError):
                continue

        if not assigned:
            panel.loc[mask, "price_tier"] = "Budget"

    return panel["price_tier"]


# ---------------------------------------------------------------------------
# Heterogeneous DiD regressions
# ---------------------------------------------------------------------------

def heterogeneous_did(
    df_panel,
    dimension_col,
    top_n=10,
    min_obs=50,
    ci_level=0.95,
):
    """Run DiD OLS regressions by subgroup within *dimension_col*.

    Parameters
    ----------
    df_panel : DataFrame
        Panel built by :func:`build_did_panel`.
    dimension_col : str
        Column to segment by (e.g. ``brand``, ``price_tier``).
    top_n : int
        Only analyse the *top_n* most frequent segments.
    min_obs : int
        Minimum observations required per segment.
    ci_level : float
        Confidence level for CIs.

    Returns
    -------
    DataFrame
        Columns: ``segment_value``, ``ATT``, ``se``, ``ci_lower``,
        ``ci_upper``, ``p_value``, ``n_obs``.
    """
    if dimension_col not in df_panel.columns:
        return pd.DataFrame(
            columns=["segment_value", "ATT", "se", "ci_lower",
                     "ci_upper", "p_value", "n_obs"]
        )

    # Handle boolean columns (e.g. is_MAP_tire)
    col_series = df_panel[dimension_col].copy()
    if col_series.dtype == bool or set(col_series.dropna().unique()).issubset({True, False}):
        col_series = col_series.map({True: "True", False: "False"})
        df_panel = df_panel.copy()
        df_panel[dimension_col] = col_series

    # Top N segments by frequency
    top_segments = (
        df_panel[dimension_col]
        .value_counts()
        .head(top_n)
        .index.tolist()
    )

    z = sp_stats.norm.ppf(1 - (1 - ci_level) / 2)
    results = []

    for seg in top_segments:
        seg_data = df_panel[df_panel[dimension_col] == seg].copy()

        if len(seg_data) < min_obs:
            continue

        # Ensure required columns exist and have variance
        y = seg_data["qty_sold"]
        if y.std() == 0:
            continue

        # Build design matrix: treated + post + treated_x_post + day_of_week dummies
        dow_dummies = pd.get_dummies(
            seg_data["day_of_week"], prefix="dow", drop_first=True, dtype=float,
        )
        X = seg_data[["treated", "post", "treated_x_post"]].astype(float)
        X = pd.concat([X, dow_dummies], axis=1)
        X = sm.add_constant(X, has_constant="add")

        try:
            model = sm.OLS(y.values, X.values).fit(cov_type="HC1")
        except Exception:
            continue

        # treated_x_post is column index 3 (const=0, treated=1, post=2, treated_x_post=3)
        att_idx = 3
        att = model.params[att_idx]
        se = model.bse[att_idx]
        p_value = model.pvalues[att_idx]

        results.append({
            "segment_value": str(seg),
            "ATT": att,
            "se": se,
            "ci_lower": att - z * se,
            "ci_upper": att + z * se,
            "p_value": p_value,
            "n_obs": len(seg_data),
        })

    return pd.DataFrame(results)


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_all_did(
    df,
    dimensions=None,
    treatment_col="cost_to_walmart_vs_7d",
    max_controls_per_brand=5,
    pre_window_days=7,
    post_window_days=14,
    min_event_buffer_days=14,
    top_n=10,
    min_obs=50,
    ci_level=0.95,
):
    """Build panel and run heterogeneous DiD for every dimension.

    Parameters
    ----------
    df : DataFrame
        Raw daily SKU-Node data (see :func:`build_did_panel`).
    dimensions : list[str], optional
        Columns to segment by.  Defaults to
        ``["brand", "price_tier", "State", "is_MAP_tire"]``.

    Returns
    -------
    dict
        ``{"panel": DataFrame, "results": {dim: DataFrame, ...}}``
    """
    if dimensions is None:
        dimensions = ["brand", "price_tier", "State", "is_MAP_tire"]

    panel = build_did_panel(
        df,
        treatment_col=treatment_col,
        max_controls_per_brand=max_controls_per_brand,
        pre_window_days=pre_window_days,
        post_window_days=post_window_days,
        min_event_buffer_days=min_event_buffer_days,
    )

    if panel.empty:
        return {"panel": panel, "results": {dim: pd.DataFrame() for dim in dimensions}}

    results = {}
    for dim in dimensions:
        results[dim] = heterogeneous_did(
            panel,
            dimension_col=dim,
            top_n=top_n,
            min_obs=min_obs,
            ci_level=ci_level,
        )

    return {"panel": panel, "results": results}


# ---------------------------------------------------------------------------
# Plotting & summary
# ---------------------------------------------------------------------------

def plot_did_results(results_dict, ci_level=0.95):
    """Plot DiD treatment effects for every dimension and print summary.

    Parameters
    ----------
    results_dict : dict
        Output of :func:`run_all_did`.
    ci_level : float
        Confidence level (used only in title annotation).
    """
    dim_results = results_dict.get("results", {})

    for dim_name, df_res in dim_results.items():
        if df_res.empty:
            print(f"\n{'='*60}")
            print(f"DiD Treatment Effect by {dim_name}: no results")
            print(f"{'='*60}")
            continue

        # -- Bar chart with CI error bars --
        plot_utils.bar_chart_with_ci(
            df_res,
            x_col="segment_value",
            y_col="ATT",
            ci_lower_col="ci_lower",
            ci_upper_col="ci_upper",
            title=f"DiD Treatment Effect by {dim_name}",
            xlabel=dim_name,
            ylabel="ATT (qty_sold)",
            horizontal=True,
            color_by_sign=True,
        )
        plt.tight_layout()
        plt.show()

        # -- Text summary --
        print(f"\n{'='*60}")
        print(f"DiD Treatment Effect by {dim_name} ({ci_level:.0%} CI)")
        print(f"{'='*60}")

        for _, row in df_res.iterrows():
            sig = "*" if row["p_value"] < 0.05 else ""
            sig += "*" if row["p_value"] < 0.01 else ""
            sig += "*" if row["p_value"] < 0.001 else ""
            print(
                f"  {row['segment_value']:>20s}  "
                f"ATT={row['ATT']:+.4f}  "
                f"[{row['ci_lower']:+.4f}, {row['ci_upper']:+.4f}]  "
                f"p={row['p_value']:.4f} {sig}  "
                f"(n={int(row['n_obs'])})"
            )
        print()
