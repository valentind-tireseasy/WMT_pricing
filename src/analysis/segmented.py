"""Segmented analysis: tire size, MAP vs non-MAP, inventory visibility, and
segmented elasticity.

Extracts and refactors cells 71-77 from the original notebook.  Uses
``src.analysis.ci_utils``, ``src.analysis.plot_utils``, and
``src.analysis.elasticity.estimate_elasticity`` for consistent CI-aware
computation and visualisation.
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats as sp_stats

from src.analysis import ci_utils, plot_utils
from src.analysis.elasticity import estimate_elasticity


# ---------------------------------------------------------------------------
# Tire size analysis
# ---------------------------------------------------------------------------

def tire_size_analysis(
    df: pd.DataFrame,
    top_n: int = 15,
    n_boot: int = 2000,
    ci_level: float = 0.95,
    seed: int = 42,
) -> dict:
    """Analyse performance metrics grouped by tire diameter.

    Parameters
    ----------
    df : DataFrame
        Must contain ``tire_diameter``, ``qty_sold``, ``revenue``,
        ``te_margin``, ``walmart_margin``, ``sku`` (or ``sku_node``),
        ``cost_to_walmart``, ``offer_price``.
    top_n : int
        Number of top diameters (by total qty) to include in *stats*.
    n_boot, ci_level, seed
        Bootstrap parameters forwarded to ``ci_utils``.

    Returns
    -------
    dict with keys:
        ``stats`` : DataFrame  -- per-diameter aggregates with bootstrap CIs
        ``correlations`` : DataFrame  -- Spearman correlations of tire_diameter
            with key metrics
    """
    dft = df[df["tire_diameter"].notna()].copy()

    # --- Per-diameter aggregated statistics ---
    sku_col = "sku" if "sku" in dft.columns else "sku_node"

    grouped = dft.groupby("tire_diameter").agg(
        total_qty=("qty_sold", "sum"),
        avg_qty=("qty_sold", "mean"),
        total_revenue=("revenue", "sum"),
        avg_te_margin=("te_margin", "mean"),
        avg_wm_margin=("walmart_margin", "mean"),
        n_sku_nodes=(sku_col, "nunique"),
    ).reset_index()

    # Bootstrap CIs for avg_qty and avg_te_margin per diameter
    ci_records = []
    for diameter, grp in dft.groupby("tire_diameter"):
        qty_ci = ci_utils.bootstrap_ci(
            grp["qty_sold"].values,
            statistic=np.mean,
            n_boot=n_boot,
            ci_level=ci_level,
            seed=seed,
        )
        margin_ci = ci_utils.bootstrap_ci(
            grp["te_margin"].dropna().values,
            statistic=np.mean,
            n_boot=n_boot,
            ci_level=ci_level,
            seed=seed,
        )
        ci_records.append({
            "tire_diameter": diameter,
            "avg_qty_ci_lower": qty_ci["ci_lower"],
            "avg_qty_ci_upper": qty_ci["ci_upper"],
            "avg_te_margin_ci_lower": margin_ci["ci_lower"],
            "avg_te_margin_ci_upper": margin_ci["ci_upper"],
        })

    ci_df = pd.DataFrame(ci_records)
    grouped = grouped.merge(ci_df, on="tire_diameter", how="left")

    # Sort by total_qty descending, take top_n
    stats = (
        grouped.sort_values("total_qty", ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )

    # --- Spearman correlations of tire_diameter with key metrics ---
    corr_metrics = [
        "qty_sold", "revenue", "te_margin", "walmart_margin",
        "cost_to_walmart", "offer_price",
    ]
    corr_records = []
    for metric in corr_metrics:
        if metric not in dft.columns:
            continue
        result = ci_utils.bootstrap_correlation_ci(
            dft["tire_diameter"].values,
            dft[metric].values,
            method="spearman",
            n_boot=n_boot,
            ci_level=ci_level,
            seed=seed,
        )
        corr_records.append({
            "metric": metric,
            "r": result["r"],
            "ci_lower": result["ci_lower"],
            "ci_upper": result["ci_upper"],
            "p_value": result["p_value"],
        })

    correlations = pd.DataFrame(corr_records)

    return {"stats": stats, "correlations": correlations}


# ---------------------------------------------------------------------------
# MAP vs non-MAP comparison
# ---------------------------------------------------------------------------

def map_vs_nonmap_comparison(
    df: pd.DataFrame,
    compare_cols: list[str] | None = None,
    n_boot: int = 2000,
    ci_level: float = 0.95,
    seed: int = 42,
) -> pd.DataFrame:
    """Compare MAP vs non-MAP SKU-Nodes across multiple metrics.

    Parameters
    ----------
    df : DataFrame
        Must contain ``is_MAP_tire`` (bool) and each column in *compare_cols*.
    compare_cols : list[str], optional
        Metrics to compare. Defaults to qty_sold, revenue, te_margin,
        walmart_margin, cost_to_walmart, offer_price.
    n_boot, ci_level, seed
        Bootstrap parameters.

    Returns
    -------
    DataFrame with one row per metric.
    """
    if compare_cols is None:
        compare_cols = [
            "qty_sold", "revenue", "te_margin", "walmart_margin",
            "cost_to_walmart", "offer_price",
        ]

    map_mask = df["is_MAP_tire"] == True  # noqa: E712
    records = []

    for col in compare_cols:
        if col not in df.columns:
            continue
        map_vals = df.loc[map_mask, col].dropna().values
        nonmap_vals = df.loc[~map_mask, col].dropna().values

        # Bootstrap mean difference CI + effect size
        boot = ci_utils.bootstrap_mean_diff_ci(
            map_vals, nonmap_vals,
            n_boot=n_boot, ci_level=ci_level, seed=seed,
        )

        # Mann-Whitney U
        if len(map_vals) >= 2 and len(nonmap_vals) >= 2:
            U_stat, p_value = sp_stats.mannwhitneyu(
                map_vals, nonmap_vals, alternative="two-sided",
            )
        else:
            U_stat, p_value = np.nan, np.nan

        records.append({
            "metric": col,
            "MAP_mean": np.nanmean(map_vals) if len(map_vals) else np.nan,
            "NonMAP_mean": np.nanmean(nonmap_vals) if len(nonmap_vals) else np.nan,
            "MAP_median": np.nanmedian(map_vals) if len(map_vals) else np.nan,
            "NonMAP_median": np.nanmedian(nonmap_vals) if len(nonmap_vals) else np.nan,
            "diff": boot["diff"],
            "ci_lower": boot["ci_lower"],
            "ci_upper": boot["ci_upper"],
            "effect_size_r": boot["effect_size_r"],
            "effect_r_ci_lower": boot["effect_r_ci_lower"],
            "effect_r_ci_upper": boot["effect_r_ci_upper"],
            "U_stat": U_stat,
            "p_value": p_value,
        })

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Inventory visibility analysis
# ---------------------------------------------------------------------------

def inventory_visibility_analysis(
    df: pd.DataFrame,
    compare_cols: list[str] | None = None,
    n_boot: int = 2000,
    ci_level: float = 0.95,
    seed: int = 42,
) -> pd.DataFrame:
    """Compare inventory-visible vs invisible SKU-Nodes across metrics.

    Parameters
    ----------
    df : DataFrame
        Must contain ``can_show_inventory`` (bool), ``min_purchase_price_fet``,
        ``cost_to_walmart``, and each column in *compare_cols*.
    compare_cols : list[str], optional
        Metrics to compare. Defaults to qty_sold, revenue, profit,
        te_margin, walmart_margin.
    n_boot, ci_level, seed
        Bootstrap parameters.

    Returns
    -------
    DataFrame with one row per metric (same structure as
    ``map_vs_nonmap_comparison``).
    """
    if compare_cols is None:
        compare_cols = [
            "qty_sold", "revenue", "profit", "te_margin", "walmart_margin",
        ]

    # Filter to rows with valid cost data
    dfi = df[
        df["min_purchase_price_fet"].notna() & df["cost_to_walmart"].notna()
    ].copy()

    vis_mask = dfi["can_show_inventory"] == True  # noqa: E712
    records = []

    for col in compare_cols:
        if col not in dfi.columns:
            continue
        vis_vals = dfi.loc[vis_mask, col].dropna().values
        invis_vals = dfi.loc[~vis_mask, col].dropna().values

        boot = ci_utils.bootstrap_mean_diff_ci(
            vis_vals, invis_vals,
            n_boot=n_boot, ci_level=ci_level, seed=seed,
        )

        if len(vis_vals) >= 2 and len(invis_vals) >= 2:
            U_stat, p_value = sp_stats.mannwhitneyu(
                vis_vals, invis_vals, alternative="two-sided",
            )
        else:
            U_stat, p_value = np.nan, np.nan

        records.append({
            "metric": col,
            "MAP_mean": np.nanmean(vis_vals) if len(vis_vals) else np.nan,
            "NonMAP_mean": np.nanmean(invis_vals) if len(invis_vals) else np.nan,
            "MAP_median": np.nanmedian(vis_vals) if len(vis_vals) else np.nan,
            "NonMAP_median": np.nanmedian(invis_vals) if len(invis_vals) else np.nan,
            "diff": boot["diff"],
            "ci_lower": boot["ci_lower"],
            "ci_upper": boot["ci_upper"],
            "effect_size_r": boot["effect_size_r"],
            "effect_r_ci_lower": boot["effect_r_ci_lower"],
            "effect_r_ci_upper": boot["effect_r_ci_upper"],
            "U_stat": U_stat,
            "p_value": p_value,
        })

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Segmented elasticity
# ---------------------------------------------------------------------------

def segmented_elasticity(
    df: pd.DataFrame,
    segment_col: str,
    min_obs: int = 30,
    ci_level: float = 0.95,
) -> pd.DataFrame:
    """Estimate price elasticity segmented by brand and an additional column.

    Parameters
    ----------
    df : DataFrame
        Must contain ``brand``, ``offer_price``, ``qty_sold``, and
        *segment_col*.
    segment_col : str
        Column to segment on (e.g. ``is_MAP_tire``,
        ``can_show_inventory``).
    min_obs : int
        Minimum observations per group.
    ci_level : float
        Confidence level.

    Returns
    -------
    DataFrame from ``estimate_elasticity`` grouped by
    ``["brand", segment_col]``.
    """
    dfs = df[(df["qty_sold"] > 0) & (df["offer_price"] > 0)].copy()

    result = estimate_elasticity(
        dfs,
        groupby_cols=["brand", segment_col],
        min_obs=min_obs,
        ci_level=ci_level,
    )
    return result


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------

def plot_segmented_results(
    tire_result: dict,
    map_df: pd.DataFrame,
    vis_df: pd.DataFrame,
    seg_elast_dict: dict,
    *,
    df: pd.DataFrame | None = None,
) -> list[plt.Figure]:
    """Generate all segmented-analysis figures.

    Parameters
    ----------
    tire_result : dict
        Output of ``tire_size_analysis``.
    map_df : pd.DataFrame
        Output of ``map_vs_nonmap_comparison``.  The underlying *df* is
        needed for histograms -- pass via *df*.
    vis_df : pd.DataFrame
        Output of ``inventory_visibility_analysis``.
    seg_elast_dict : dict[str, DataFrame]
        Mapping of ``{segment_col: segmented_elasticity_result}``.
    df : DataFrame, optional
        Full analysis DataFrame, used for MAP histograms and brand
        visibility rate.

    Returns
    -------
    list of matplotlib Figures.
    """
    figures: list[plt.Figure] = []

    # ---- 1. Tire diameter stats (1x3, 20x6) ----
    tire_stats = tire_result["stats"]
    if not tire_stats.empty:
        fig_tire, axes = plt.subplots(1, 3, figsize=(20, 6))

        # Total qty bars
        axes[0].bar(
            tire_stats["tire_diameter"].astype(str),
            tire_stats["total_qty"],
            color="steelblue", alpha=0.8,
        )
        axes[0].set_title("Total Qty Sold by Tire Diameter")
        axes[0].set_xlabel("Tire Diameter")
        axes[0].set_ylabel("Total Qty")
        axes[0].tick_params(axis="x", rotation=45)

        # Avg TE margin bars with CI error bars
        yerr_margin = np.array([
            tire_stats["avg_te_margin"].values - tire_stats["avg_te_margin_ci_lower"].values,
            tire_stats["avg_te_margin_ci_upper"].values - tire_stats["avg_te_margin"].values,
        ])
        yerr_margin = np.abs(yerr_margin)
        axes[1].bar(
            tire_stats["tire_diameter"].astype(str),
            tire_stats["avg_te_margin"],
            yerr=yerr_margin,
            color="#2ecc71", alpha=0.8, capsize=3, ecolor="gray",
        )
        axes[1].set_title("Avg TE Margin by Tire Diameter")
        axes[1].set_xlabel("Tire Diameter")
        axes[1].set_ylabel("Avg TE Margin")
        axes[1].tick_params(axis="x", rotation=45)

        # Avg Walmart margin bars
        axes[2].bar(
            tire_stats["tire_diameter"].astype(str),
            tire_stats["avg_wm_margin"],
            color="#e67e22", alpha=0.8,
        )
        axes[2].set_title("Avg Walmart Margin by Tire Diameter")
        axes[2].set_xlabel("Tire Diameter")
        axes[2].set_ylabel("Avg Walmart Margin")
        axes[2].tick_params(axis="x", rotation=45)

        fig_tire.suptitle("Tire Diameter Analysis", fontsize=14, y=1.02)
        fig_tire.tight_layout()
        figures.append(fig_tire)

    # ---- 2. MAP comparison histograms (2x3, 18x10) ----
    if df is not None and not map_df.empty:
        compare_cols = map_df["metric"].tolist()
        n_cols = 3
        n_rows = (len(compare_cols) + n_cols - 1) // n_cols
        fig_map, axes_map = plt.subplots(n_rows, n_cols, figsize=(18, 10))
        axes_map = np.atleast_2d(axes_map)

        map_mask = df["is_MAP_tire"] == True  # noqa: E712

        for idx, col in enumerate(compare_cols):
            row_i, col_i = divmod(idx, n_cols)
            ax = axes_map[row_i, col_i]

            if col not in df.columns:
                ax.set_visible(False)
                continue

            map_vals = df.loc[map_mask, col].dropna()
            nonmap_vals = df.loc[~map_mask, col].dropna()

            # Clip to 1st-99th percentile for display
            all_vals = pd.concat([map_vals, nonmap_vals])
            if len(all_vals) > 0:
                lo_clip = np.percentile(all_vals, 1)
                hi_clip = np.percentile(all_vals, 99)
                map_clipped = map_vals[(map_vals >= lo_clip) & (map_vals <= hi_clip)]
                nonmap_clipped = nonmap_vals[(nonmap_vals >= lo_clip) & (nonmap_vals <= hi_clip)]
            else:
                map_clipped = map_vals
                nonmap_clipped = nonmap_vals

            if len(map_clipped) > 0:
                ax.hist(
                    map_clipped, bins=40, density=True, alpha=0.5,
                    color="red", label="MAP",
                )
            if len(nonmap_clipped) > 0:
                ax.hist(
                    nonmap_clipped, bins=40, density=True, alpha=0.5,
                    color="blue", label="Non-MAP",
                )
            ax.set_title(col)
            ax.legend(fontsize=8)

        # Hide unused axes
        for idx in range(len(compare_cols), n_rows * n_cols):
            row_i, col_i = divmod(idx, n_cols)
            axes_map[row_i, col_i].set_visible(False)

        fig_map.suptitle("MAP vs Non-MAP Distributions", fontsize=14, y=1.02)
        fig_map.tight_layout()
        figures.append(fig_map)

    # ---- 3. Inventory visibility (1x3, 18x5) ----
    if df is not None and not vis_df.empty:
        fig_vis, axes_vis = plt.subplots(1, 3, figsize=(18, 5))

        vis_mask = df["can_show_inventory"] == True  # noqa: E712
        dfi = df[df["min_purchase_price_fet"].notna() & df["cost_to_walmart"].notna()]
        vis_mask_f = dfi["can_show_inventory"] == True  # noqa: E712

        # (a) Avg qty bars: visible vs invisible
        avg_qty_vis = dfi.loc[vis_mask_f, "qty_sold"].mean()
        avg_qty_invis = dfi.loc[~vis_mask_f, "qty_sold"].mean()
        axes_vis[0].bar(
            ["Visible", "Invisible"],
            [avg_qty_vis, avg_qty_invis],
            color=["#2ecc71", "#e74c3c"], alpha=0.8,
        )
        axes_vis[0].set_title("Avg Qty Sold by Inventory Visibility")
        axes_vis[0].set_ylabel("Avg Qty Sold")

        # (b) Avg margins grouped bars with CI error bars
        margin_cols = ["te_margin", "walmart_margin"]
        avail_margin_cols = [c for c in margin_cols if c in dfi.columns]
        x_pos = np.arange(len(avail_margin_cols))
        bar_width = 0.35

        vis_means = []
        invis_means = []
        vis_errs = []
        invis_errs = []

        for mc in avail_margin_cols:
            v_vals = dfi.loc[vis_mask_f, mc].dropna().values
            i_vals = dfi.loc[~vis_mask_f, mc].dropna().values

            v_ci = ci_utils.bootstrap_ci(v_vals, statistic=np.mean, n_boot=2000, seed=42)
            i_ci = ci_utils.bootstrap_ci(i_vals, statistic=np.mean, n_boot=2000, seed=42)

            vis_means.append(v_ci["estimate"])
            invis_means.append(i_ci["estimate"])
            vis_errs.append([
                [v_ci["estimate"] - v_ci["ci_lower"]],
                [v_ci["ci_upper"] - v_ci["estimate"]],
            ])
            invis_errs.append([
                [i_ci["estimate"] - i_ci["ci_lower"]],
                [i_ci["ci_upper"] - i_ci["estimate"]],
            ])

        if avail_margin_cols:
            vis_lo = [e[0][0] for e in vis_errs]
            vis_hi = [e[1][0] for e in vis_errs]
            invis_lo = [e[0][0] for e in invis_errs]
            invis_hi = [e[1][0] for e in invis_errs]

            axes_vis[1].bar(
                x_pos - bar_width / 2, vis_means,
                bar_width, yerr=[vis_lo, vis_hi],
                label="Visible", color="#2ecc71", alpha=0.8,
                capsize=3, ecolor="gray",
            )
            axes_vis[1].bar(
                x_pos + bar_width / 2, invis_means,
                bar_width, yerr=[invis_lo, invis_hi],
                label="Invisible", color="#e74c3c", alpha=0.8,
                capsize=3, ecolor="gray",
            )
            axes_vis[1].set_xticks(x_pos)
            axes_vis[1].set_xticklabels(avail_margin_cols)
            axes_vis[1].set_title("Avg Margins by Visibility")
            axes_vis[1].set_ylabel("Margin")
            axes_vis[1].legend(fontsize=8)

        # (c) Brand visibility rate -- bottom 10 brands by visibility
        if "brand" in df.columns:
            brand_vis = (
                dfi.groupby("brand")["can_show_inventory"]
                .mean()
                .sort_values()
                .head(10)
            )
            axes_vis[2].barh(
                brand_vis.index.astype(str),
                brand_vis.values,
                color="#3498db", alpha=0.8,
            )
            axes_vis[2].set_title("Bottom 10 Brands by Visibility Rate")
            axes_vis[2].set_xlabel("Visibility Rate")
        else:
            axes_vis[2].set_visible(False)

        fig_vis.suptitle("Inventory Visibility Analysis", fontsize=14, y=1.02)
        fig_vis.tight_layout()
        figures.append(fig_vis)

    # ---- 4. Segmented elasticity (per segment) ----
    for seg_col, seg_df in seg_elast_dict.items():
        if seg_df.empty:
            continue

        segment_values = seg_df[seg_col].dropna().unique()

        if len(segment_values) == 2:
            # Two-segment: horizontal grouped bar chart by brand with CI
            fig_seg, ax_seg = plt.subplots(
                figsize=(14, max(6, seg_df["brand"].nunique() * 0.4))
            )

            seg_a, seg_b = sorted(segment_values, key=str)
            df_a = seg_df[seg_df[seg_col] == seg_a].set_index("brand")
            df_b = seg_df[seg_df[seg_col] == seg_b].set_index("brand")

            # Use brands present in both segments
            common_brands = sorted(
                set(df_a.index) & set(df_b.index), key=str,
            )
            if not common_brands:
                common_brands = sorted(seg_df["brand"].unique(), key=str)

            y_pos = np.arange(len(common_brands))
            bar_h = 0.35

            # Segment A bars
            vals_a = [df_a.loc[b, "elasticity"] if b in df_a.index else np.nan for b in common_brands]
            errs_a_lo = [
                abs(df_a.loc[b, "elasticity"] - df_a.loc[b, "ci_lower"]) if b in df_a.index else 0
                for b in common_brands
            ]
            errs_a_hi = [
                abs(df_a.loc[b, "ci_upper"] - df_a.loc[b, "elasticity"]) if b in df_a.index else 0
                for b in common_brands
            ]
            ax_seg.barh(
                y_pos - bar_h / 2, vals_a, bar_h,
                xerr=[errs_a_lo, errs_a_hi],
                label=str(seg_a), color="#3498db", alpha=0.8,
                capsize=3, ecolor="gray",
            )

            # Segment B bars
            vals_b = [df_b.loc[b, "elasticity"] if b in df_b.index else np.nan for b in common_brands]
            errs_b_lo = [
                abs(df_b.loc[b, "elasticity"] - df_b.loc[b, "ci_lower"]) if b in df_b.index else 0
                for b in common_brands
            ]
            errs_b_hi = [
                abs(df_b.loc[b, "ci_upper"] - df_b.loc[b, "elasticity"]) if b in df_b.index else 0
                for b in common_brands
            ]
            ax_seg.barh(
                y_pos + bar_h / 2, vals_b, bar_h,
                xerr=[errs_b_lo, errs_b_hi],
                label=str(seg_b), color="#e74c3c", alpha=0.8,
                capsize=3, ecolor="gray",
            )

            ax_seg.set_yticks(y_pos)
            ax_seg.set_yticklabels(common_brands)
            ax_seg.axvline(0, color="black", linewidth=0.5, linestyle="--")
            ax_seg.set_title(f"Price Elasticity by Brand and {seg_col}")
            ax_seg.set_xlabel("Elasticity")
            ax_seg.legend(fontsize=9)
            fig_seg.tight_layout()
            figures.append(fig_seg)

        else:
            # Multi-segment: heatmap via plot_utils
            val_pivot = seg_df.pivot_table(
                index="brand", columns=seg_col, values="elasticity",
                aggfunc="first",
            )
            lo_pivot = seg_df.pivot_table(
                index="brand", columns=seg_col, values="ci_lower",
                aggfunc="first",
            )
            hi_pivot = seg_df.pivot_table(
                index="brand", columns=seg_col, values="ci_upper",
                aggfunc="first",
            )

            fig_seg, ax_seg = plt.subplots(
                figsize=(
                    max(10, len(val_pivot.columns) * 1.8),
                    max(6, len(val_pivot) * 0.5),
                )
            )
            plot_utils.heatmap_with_ci_annotation(
                val_pivot,
                ci_lower=lo_pivot,
                ci_upper=hi_pivot,
                title=f"Elasticity by Brand x {seg_col}",
                cmap="RdYlGn",
                center=0,
                ax=ax_seg,
            )
            fig_seg.tight_layout()
            figures.append(fig_seg)

    return figures
