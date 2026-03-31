"""Geographic and brand analysis module for the correlation analysis pipeline.

Extracts and refactors cells 50-52 from the original notebook into
reusable functions.  All confidence intervals come from
``src.analysis.ci_utils``; all plots are rendered via helpers in
``src.analysis.plot_utils``.
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
from src.analysis import ci_utils, plot_utils


# ---------------------------------------------------------------------------
# State-level sales analysis
# ---------------------------------------------------------------------------

def state_sales_analysis(df, top_n=20, n_boot=2000, ci_level=0.95, seed=42):
    """Aggregate sales metrics by State with bootstrap CIs on mean daily qty.

    Parameters
    ----------
    df : pd.DataFrame
        Must contain columns: ``State``, ``qty_sold``, ``wm_margin``,
        ``te_margin``, ``node``.
    top_n : int
        Number of top states to return (by total quantity).
    n_boot : int
        Bootstrap resamples.
    ci_level : float
        Confidence level.
    seed : int
        Random seed.

    Returns
    -------
    pd.DataFrame
        Columns: ``State``, ``total_qty``, ``avg_daily_qty``, ``ci_lower``,
        ``ci_upper``, ``avg_wm_margin``, ``avg_te_margin``, ``n_nodes``.
    """
    grouped = df.groupby("State", as_index=False).agg(
        total_qty=("qty_sold", "sum"),
        avg_wm_margin=("walmart_margin", "mean"),
        avg_te_margin=("te_margin", "mean"),
        n_nodes=("node", "nunique"),
    )

    # Bootstrap CI on mean daily qty_sold per state
    ci_rows = []
    for state in grouped["State"]:
        state_data = df.loc[df["State"] == state, "qty_sold"].values
        result = ci_utils.bootstrap_ci(
            state_data,
            statistic=np.mean,
            n_boot=n_boot,
            ci_level=ci_level,
            seed=seed,
        )
        ci_rows.append({
            "State": state,
            "avg_daily_qty": result["estimate"],
            "ci_lower": result["ci_lower"],
            "ci_upper": result["ci_upper"],
        })
    ci_df = pd.DataFrame(ci_rows)

    result_df = grouped.merge(ci_df, on="State", how="left")
    result_df = result_df.sort_values("total_qty", ascending=False).head(top_n)
    result_df = result_df[
        ["State", "total_qty", "avg_daily_qty", "ci_lower", "ci_upper",
         "avg_wm_margin", "avg_te_margin", "n_nodes"]
    ].reset_index(drop=True)
    return result_df


# ---------------------------------------------------------------------------
# Brand-level analysis
# ---------------------------------------------------------------------------

def brand_analysis(df, top_n=20, n_boot=2000, ci_level=0.95, seed=42):
    """Aggregate sales and margin metrics by brand with bootstrap CIs.

    Parameters
    ----------
    df : pd.DataFrame
        Must contain columns: ``brand``, ``qty_sold``, ``revenue``,
        ``te_margin``, ``wm_margin``, ``sku``, ``node``.
    top_n : int
        Number of top brands to return (by total quantity).
    n_boot : int
        Bootstrap resamples.
    ci_level : float
        Confidence level.
    seed : int
        Random seed.

    Returns
    -------
    pd.DataFrame
        Columns: ``brand``, ``total_qty``, ``total_revenue``,
        ``avg_te_margin``, ``te_margin_ci_lower``, ``te_margin_ci_upper``,
        ``avg_wm_margin``, ``wm_margin_ci_lower``, ``wm_margin_ci_upper``,
        ``n_skus``, ``n_nodes``.
    """
    grouped = df.groupby("brand", as_index=False).agg(
        total_qty=("qty_sold", "sum"),
        total_revenue=("revenue", "sum"),
        avg_te_margin=("te_margin", "mean"),
        avg_wm_margin=("walmart_margin", "mean"),
        n_skus=("sku", "nunique"),
        n_nodes=("node", "nunique"),
    )

    # Bootstrap CIs on avg_te_margin and avg_wm_margin per brand
    ci_rows = []
    for brand in grouped["brand"]:
        brand_data = df.loc[df["brand"] == brand]

        te_ci = ci_utils.bootstrap_ci(
            brand_data["te_margin"].values,
            statistic=np.mean,
            n_boot=n_boot,
            ci_level=ci_level,
            seed=seed,
        )
        wm_ci = ci_utils.bootstrap_ci(
            brand_data["walmart_margin"].values,
            statistic=np.mean,
            n_boot=n_boot,
            ci_level=ci_level,
            seed=seed,
        )
        ci_rows.append({
            "brand": brand,
            "te_margin_ci_lower": te_ci["ci_lower"],
            "te_margin_ci_upper": te_ci["ci_upper"],
            "wm_margin_ci_lower": wm_ci["ci_lower"],
            "wm_margin_ci_upper": wm_ci["ci_upper"],
        })
    ci_df = pd.DataFrame(ci_rows)

    result_df = grouped.merge(ci_df, on="brand", how="left")
    result_df = result_df.sort_values("total_qty", ascending=False).head(top_n)
    result_df = result_df[
        ["brand", "total_qty", "total_revenue", "avg_te_margin",
         "te_margin_ci_lower", "te_margin_ci_upper", "avg_wm_margin",
         "wm_margin_ci_lower", "wm_margin_ci_upper", "n_skus", "n_nodes"]
    ].reset_index(drop=True)
    return result_df


# ---------------------------------------------------------------------------
# Distribution breadth analysis
# ---------------------------------------------------------------------------

def distribution_breadth_analysis(df, n_boot=2000, ci_level=0.95, seed=42):
    """Analyse Spearman correlation between distribution breadth and sales.

    Parameters
    ----------
    df : pd.DataFrame
        Must contain columns: ``sku``, ``n_active_nodes``, ``qty_sold``.
    n_boot : int
        Bootstrap resamples.
    ci_level : float
        Confidence level.
    seed : int
        Random seed.

    Returns
    -------
    dict
        ``"correlation"`` : dict from ``ci_utils.bootstrap_correlation_ci``
        with keys ``r``, ``ci_lower``, ``ci_upper``, ``p_value``, ``se``.
        ``"breadth_df"`` : pd.DataFrame with columns ``sku``,
        ``avg_active_nodes``, ``total_qty``.
    """
    breadth_df = df.groupby("sku", as_index=False).agg(
        avg_active_nodes=("n_active_nodes", "mean"),
        total_qty=("qty_sold", "sum"),
    )

    corr_result = ci_utils.bootstrap_correlation_ci(
        breadth_df["avg_active_nodes"].values,
        breadth_df["total_qty"].values,
        method="spearman",
        n_boot=n_boot,
        ci_level=ci_level,
        seed=seed,
    )

    return {
        "correlation": corr_result,
        "breadth_df": breadth_df,
    }


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------

def plot_geographic_brand(state_df, brand_df, breadth_result):
    """Generate three figures for geographic and brand analysis.

    Parameters
    ----------
    state_df : pd.DataFrame
        Output of :func:`state_sales_analysis`.
    brand_df : pd.DataFrame
        Output of :func:`brand_analysis`.
    breadth_result : dict
        Output of :func:`distribution_breadth_analysis`.

    Returns
    -------
    tuple of (fig1, fig2, fig3)
    """
    # ------------------------------------------------------------------
    # Figure 1: Top states — total qty bars + avg daily qty with CI on
    #           secondary axis
    # ------------------------------------------------------------------
    fig1, ax1 = plt.subplots(figsize=(14, 6))

    x_pos = np.arange(len(state_df))
    bar_width = 0.45

    # Total qty bars on primary axis
    bars1 = ax1.bar(
        x_pos - bar_width / 2,
        state_df["total_qty"],
        width=bar_width,
        color="steelblue",
        alpha=0.8,
        label="Total Qty",
    )
    ax1.set_ylabel("Total Quantity Sold", color="steelblue")
    ax1.tick_params(axis="y", labelcolor="steelblue")

    # Avg daily qty with CI error bars on secondary axis
    ax1b = ax1.twinx()
    yerr_lo = state_df["avg_daily_qty"].values - state_df["ci_lower"].values
    yerr_hi = state_df["ci_upper"].values - state_df["avg_daily_qty"].values
    yerr = np.array([np.abs(yerr_lo), np.abs(yerr_hi)])

    bars2 = ax1b.bar(
        x_pos + bar_width / 2,
        state_df["avg_daily_qty"],
        width=bar_width,
        yerr=yerr,
        color="#e74c3c",
        alpha=0.7,
        capsize=3,
        ecolor="gray",
        label="Avg Daily Qty (95% CI)",
    )
    ax1b.set_ylabel("Avg Daily Qty Sold", color="#e74c3c")
    ax1b.tick_params(axis="y", labelcolor="#e74c3c")

    ax1.set_xticks(x_pos)
    ax1.set_xticklabels(state_df["State"], rotation=45, ha="right")
    ax1.set_title("Top States by Sales Volume", fontsize=14)

    # Combined legend
    handles = [bars1, bars2]
    labels = ["Total Qty", "Avg Daily Qty (95% CI)"]
    ax1.legend(handles, labels, loc="upper right")
    fig1.tight_layout()

    # ------------------------------------------------------------------
    # Figure 2: 1x2 subplot — brands by qty and margins with CIs
    # ------------------------------------------------------------------
    fig2, (ax2a, ax2b) = plt.subplots(1, 2, figsize=(18, 6))

    # Left: top brands by total_qty
    ax2a.barh(
        brand_df["brand"],
        brand_df["total_qty"],
        color="steelblue",
        alpha=0.8,
    )
    ax2a.invert_yaxis()
    ax2a.set_xlabel("Total Quantity Sold")
    ax2a.set_title("Top Brands by Sales Volume", fontsize=13)

    # Right: grouped bar chart — avg te_margin and wm_margin with CI error bars
    y_pos = np.arange(len(brand_df))
    bar_h = 0.35

    # TE margin bars
    te_err_lo = brand_df["avg_te_margin"].values - brand_df["te_margin_ci_lower"].values
    te_err_hi = brand_df["te_margin_ci_upper"].values - brand_df["avg_te_margin"].values
    te_yerr = np.array([np.abs(te_err_lo), np.abs(te_err_hi)])

    ax2b.barh(
        y_pos - bar_h / 2,
        brand_df["avg_te_margin"],
        height=bar_h,
        xerr=te_yerr,
        color="#2ecc71",
        alpha=0.8,
        capsize=3,
        ecolor="gray",
        label="TE Margin",
    )

    # WM margin bars
    wm_err_lo = brand_df["avg_wm_margin"].values - brand_df["wm_margin_ci_lower"].values
    wm_err_hi = brand_df["wm_margin_ci_upper"].values - brand_df["avg_wm_margin"].values
    wm_yerr = np.array([np.abs(wm_err_lo), np.abs(wm_err_hi)])

    ax2b.barh(
        y_pos + bar_h / 2,
        brand_df["avg_wm_margin"],
        height=bar_h,
        xerr=wm_yerr,
        color="#3498db",
        alpha=0.8,
        capsize=3,
        ecolor="gray",
        label="WM Margin",
    )

    ax2b.set_yticks(y_pos)
    ax2b.set_yticklabels(brand_df["brand"])
    ax2b.invert_yaxis()
    ax2b.set_xlabel("Average Margin")
    ax2b.set_title("Avg Margins by Brand (95% CI)", fontsize=13)
    ax2b.legend(loc="lower right")
    fig2.tight_layout()

    # ------------------------------------------------------------------
    # Figure 3: Distribution breadth scatter with regression CI band
    # ------------------------------------------------------------------
    breadth_df = breadth_result["breadth_df"]
    corr_info = breadth_result["correlation"]

    fig3, ax3 = plt.subplots(figsize=(10, 6))
    plot_utils.scatter_with_regression_ci(
        breadth_df["avg_active_nodes"].values,
        breadth_df["total_qty"].values,
        title="Distribution Breadth vs Sales Volume",
        xlabel="Avg Active Nodes",
        ylabel="Total Quantity Sold",
        corr_info=corr_info,
        ax=ax3,
    )
    fig3.tight_layout()

    return fig1, fig2, fig3
