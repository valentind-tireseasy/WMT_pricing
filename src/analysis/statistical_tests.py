"""Statistical testing module for correlation analysis.

Extracts and enhances the Mann-Whitney, OLS, margin-decile, and
price-change-revenue analyses from notebook cells 42-48, adding
bootstrap confidence intervals via ``ci_utils`` and CI-aware plots
via ``plot_utils``.
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import statsmodels.api as sm
from scipy import stats

from src.analysis import ci_utils, plot_utils


# ---------------------------------------------------------------------------
# Mann-Whitney U test (core building block)
# ---------------------------------------------------------------------------

def mann_whitney_test(
    group_a,
    group_b,
    label_a: str = "Group A",
    label_b: str = "Group B",
    n_boot: int = 2000,
    ci_level: float = 0.95,
    seed: int = 42,
) -> dict:
    """Run a two-sided Mann-Whitney U test with bootstrap CIs.

    Parameters
    ----------
    group_a, group_b : array-like
        Samples to compare.
    label_a, label_b : str
        Human-readable labels (used in printed summaries).
    n_boot : int
        Bootstrap resamples for mean-difference and effect-size CIs.
    ci_level : float
        Confidence level (e.g. 0.95).
    seed : int
        Random seed.

    Returns
    -------
    dict with keys: U_stat, p_value, mean_a, mean_b, median_a, median_b,
    n_a, n_b, mean_diff, mean_diff_ci_lower, mean_diff_ci_upper,
    effect_size_r, effect_r_ci_lower, effect_r_ci_upper, significant.
    """
    a = np.asarray(group_a, dtype=float)
    b = np.asarray(group_b, dtype=float)
    a = a[~np.isnan(a)]
    b = b[~np.isnan(b)]

    if len(a) == 0 or len(b) == 0:
        return {
            "U_stat": np.nan, "p_value": np.nan,
            "mean_a": np.nan, "mean_b": np.nan,
            "median_a": np.nan, "median_b": np.nan,
            "n_a": len(a), "n_b": len(b),
            "mean_diff": np.nan,
            "mean_diff_ci_lower": np.nan, "mean_diff_ci_upper": np.nan,
            "effect_size_r": np.nan,
            "effect_r_ci_lower": np.nan, "effect_r_ci_upper": np.nan,
            "significant": False,
        }

    U_stat, p_value = stats.mannwhitneyu(a, b, alternative="two-sided")

    effect_r = ci_utils.rank_biserial_effect_size(U_stat, len(a), len(b))

    boot = ci_utils.bootstrap_mean_diff_ci(
        a, b, n_boot=n_boot, ci_level=ci_level, seed=seed,
    )

    alpha = 1 - ci_level
    significant = p_value < alpha

    return {
        "U_stat": float(U_stat),
        "p_value": float(p_value),
        "mean_a": float(a.mean()),
        "mean_b": float(b.mean()),
        "median_a": float(np.median(a)),
        "median_b": float(np.median(b)),
        "n_a": len(a),
        "n_b": len(b),
        "mean_diff": boot["diff"],
        "mean_diff_ci_lower": boot["ci_lower"],
        "mean_diff_ci_upper": boot["ci_upper"],
        "effect_size_r": boot["effect_size_r"],
        "effect_r_ci_lower": boot["effect_r_ci_lower"],
        "effect_r_ci_upper": boot["effect_r_ci_upper"],
        "significant": significant,
    }


# ---------------------------------------------------------------------------
# Test 1: Price change impact on sales
# ---------------------------------------------------------------------------

def price_change_impact_test(
    df: pd.DataFrame,
    days_recent: int = 3,
    days_no_change: int = 7,
    n_boot: int = 2000,
    ci_level: float = 0.95,
    seed: int = 42,
) -> dict:
    """Compare qty_sold for recent price-change vs no-change groups.

    Groups:
    - Recent change: ``days_since_price_change <= days_recent``
    - No change:     ``days_since_price_change > days_no_change``

    Parameters
    ----------
    df : DataFrame
        Must contain ``days_since_price_change`` and ``qty_sold``.

    Returns
    -------
    dict from :func:`mann_whitney_test`.
    """
    recent = df.loc[df["days_since_price_change"] <= days_recent, "qty_sold"]
    no_change = df.loc[df["days_since_price_change"] > days_no_change, "qty_sold"]

    result = mann_whitney_test(
        recent, no_change,
        label_a=f"Recent change (<={days_recent} days)",
        label_b=f"No change (>{days_no_change} days)",
        n_boot=n_boot, ci_level=ci_level, seed=seed,
    )

    pct = ci_level * 100
    print("=== Price Change Impact on Sales ===")
    print(f"Recent change (<={days_recent} days): "
          f"n={result['n_a']:,}, mean={result['mean_a']:.3f}, "
          f"median={result['median_a']:.1f}")
    print(f"No recent change (>{days_no_change} days): "
          f"n={result['n_b']:,}, mean={result['mean_b']:.3f}, "
          f"median={result['median_b']:.1f}")
    print(f"Mann-Whitney U stat={result['U_stat']:.0f}, "
          f"p-value={result['p_value']:.4e}")
    print(f"Mean diff={result['mean_diff']:.4f} "
          f"[{result['mean_diff_ci_lower']:.4f}, "
          f"{result['mean_diff_ci_upper']:.4f}] ({pct:.0f}% CI)")
    print(f"Effect size r={result['effect_size_r']:.4f} "
          f"[{result['effect_r_ci_lower']:.4f}, "
          f"{result['effect_r_ci_upper']:.4f}]")
    print(f"Significant at {pct:.0f}%: "
          f"{'Yes' if result['significant'] else 'No'}")

    return result


# ---------------------------------------------------------------------------
# Test 2: Inventory availability vs sales
# ---------------------------------------------------------------------------

def inventory_impact_test(
    df: pd.DataFrame,
    n_boot: int = 2000,
    ci_level: float = 0.95,
    seed: int = 42,
) -> dict:
    """Compare qty_sold for can_show_inv==1 vs ==0.

    Parameters
    ----------
    df : DataFrame
        Must contain ``can_show_inv`` and ``qty_sold``.

    Returns
    -------
    dict from :func:`mann_whitney_test`.
    """
    inv_yes = df.loc[df["can_show_inv"] == 1, "qty_sold"]
    inv_no = df.loc[df["can_show_inv"] == 0, "qty_sold"]

    result = mann_whitney_test(
        inv_yes, inv_no,
        label_a="With inventory",
        label_b="Without inventory",
        n_boot=n_boot, ci_level=ci_level, seed=seed,
    )

    pct = ci_level * 100
    print("=== Inventory Availability vs Sales ===")
    print(f"With inventory (can_show_inv=1):    "
          f"n={result['n_a']:,}, mean={result['mean_a']:.3f}, "
          f"median={result['median_a']:.1f}")
    print(f"Without inventory (can_show_inv=0): "
          f"n={result['n_b']:,}, mean={result['mean_b']:.3f}, "
          f"median={result['median_b']:.1f}")
    print(f"Mann-Whitney U stat={result['U_stat']:.0f}, "
          f"p-value={result['p_value']:.4e}")
    print(f"Mean diff={result['mean_diff']:.4f} "
          f"[{result['mean_diff_ci_lower']:.4f}, "
          f"{result['mean_diff_ci_upper']:.4f}] ({pct:.0f}% CI)")
    print(f"Effect size r={result['effect_size_r']:.4f} "
          f"[{result['effect_r_ci_lower']:.4f}, "
          f"{result['effect_r_ci_upper']:.4f}]")
    print(f"Significant at {pct:.0f}%: "
          f"{'Yes' if result['significant'] else 'No'}")

    return result


# ---------------------------------------------------------------------------
# Test 3: Margin decile analysis
# ---------------------------------------------------------------------------

def margin_decile_analysis(
    df: pd.DataFrame,
    margin_cols: list = None,
    n_quantiles: int = 10,
    n_boot: int = 2000,
    ci_level: float = 0.95,
    seed: int = 42,
) -> pd.DataFrame:
    """Compute mean qty_sold per margin quantile bin with bootstrap CIs.

    Parameters
    ----------
    df : DataFrame
        Must contain ``qty_sold`` and each column in *margin_cols*.
    margin_cols : list of str
        Margin columns to analyse (default: ``["te_margin", "walmart_margin"]``).
    n_quantiles : int
        Number of quantile bins.

    Returns
    -------
    DataFrame with columns: margin_col, margin_bin, avg_qty, ci_lower,
    ci_upper, total_qty, count.
    """
    if margin_cols is None:
        margin_cols = ["te_margin", "walmart_margin"]

    rows = []
    for col in margin_cols:
        df_valid = df[df[col].notna() & (df["qty_sold"] > 0)].copy()
        if len(df_valid) < n_quantiles:
            continue

        df_valid["margin_bin"] = pd.qcut(
            df_valid[col], n_quantiles, duplicates="drop",
        )

        for bin_label, grp in df_valid.groupby("margin_bin", observed=True):
            qty_values = grp["qty_sold"].values
            boot = ci_utils.bootstrap_ci(
                qty_values,
                statistic=np.mean,
                n_boot=n_boot,
                ci_level=ci_level,
                seed=seed,
            )
            rows.append({
                "margin_col": col,
                "margin_bin": str(bin_label),
                "avg_qty": boot["estimate"],
                "ci_lower": boot["ci_lower"],
                "ci_upper": boot["ci_upper"],
                "total_qty": qty_values.sum(),
                "count": len(qty_values),
            })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Test 4: OLS regression
# ---------------------------------------------------------------------------

def ols_regression(
    df: pd.DataFrame,
    feature_cols: list,
    target_col: str = "qty_sold",
    log_transform: bool = False,
    ci_level: float = 0.95,
) -> dict:
    """Fit an OLS model and extract coefficients with CIs.

    Parameters
    ----------
    df : DataFrame
    feature_cols : list of str
        Predictor column names (must exist in *df*).
    target_col : str
        Response variable.
    log_transform : bool
        If True, filter to target > 0 and use ``log1p(target)``.
    ci_level : float

    Returns
    -------
    dict with keys: model (OLSResults), coefficients (DataFrame with
    variable/coef/se/ci_lower/ci_upper/p_value), r_squared, n_obs,
    summary_text.
    """
    cols = [c for c in feature_cols if c in df.columns]
    df_reg = df[cols + [target_col]].dropna()

    if log_transform:
        df_reg = df_reg[df_reg[target_col] > 0].copy()
        y = np.log1p(df_reg[target_col])
    else:
        y = df_reg[target_col]

    if len(df_reg) < len(cols) + 2:
        return {
            "model": None,
            "coefficients": pd.DataFrame(),
            "r_squared": np.nan,
            "n_obs": len(df_reg),
            "summary_text": "Insufficient data for OLS regression",
        }

    X = sm.add_constant(df_reg[cols])
    model = sm.OLS(y, X).fit()
    coef_df = ci_utils.ols_ci_df(model, ci_level=ci_level)

    return {
        "model": model,
        "coefficients": coef_df,
        "r_squared": float(model.rsquared),
        "n_obs": int(model.nobs),
        "summary_text": str(model.summary()),
    }


# ---------------------------------------------------------------------------
# Test 5: Price change impact on revenue
# ---------------------------------------------------------------------------

def price_change_revenue_analysis(
    df: pd.DataFrame,
    pct_chg_col: str = None,
    n_boot: int = 2000,
    ci_level: float = 0.95,
    seed: int = 42,
) -> dict:
    """Analyse correlation between price changes and revenue.

    Computes Pearson/Spearman correlations with bootstrap CIs, bucketed
    revenue statistics, and a pre/post Mann-Whitney comparison.

    Parameters
    ----------
    df : DataFrame
        Must contain a price-change % column, ``revenue``, ``qty_sold``,
        and ``days_since_price_change``.
    pct_chg_col : str, optional
        Name of the price-change percentage column.  Auto-detected from
        ``cost_to_walmart_pct_chg`` or ``cost_to_walmart_vs_7d_pct``.

    Returns
    -------
    dict with keys: pearson, spearman (bootstrap_correlation_ci results),
    bucketed (DataFrame with ci_lower/ci_upper), pre_post (mann_whitney_test
    result or None).
    """
    # Auto-detect price change column
    if pct_chg_col is None:
        for candidate in ["cost_to_walmart_pct_chg", "cost_to_walmart_vs_7d_pct"]:
            if candidate in df.columns:
                pct_chg_col = candidate
                break
    if pct_chg_col is None or pct_chg_col not in df.columns:
        print("No price-change % column found; skipping price-revenue analysis.")
        return {"pearson": None, "spearman": None, "bucketed": pd.DataFrame(), "pre_post": None}

    df_pc = df[
        df[pct_chg_col].notna()
        & df["revenue"].notna()
    ].copy()
    df_pc = df_pc[df_pc[pct_chg_col] != 0]

    # -- Correlations with bootstrap CIs --
    pearson = ci_utils.bootstrap_correlation_ci(
        df_pc[pct_chg_col], df_pc["revenue"],
        method="pearson", n_boot=n_boot, ci_level=ci_level, seed=seed,
    )
    spearman = ci_utils.bootstrap_correlation_ci(
        df_pc[pct_chg_col], df_pc["revenue"],
        method="spearman", n_boot=n_boot, ci_level=ci_level, seed=seed,
    )

    # -- Bucketed analysis --
    bucket_labels = [
        "Large decrease (<-5%)",
        "Small decrease (-5% to -1%)",
        "Minimal (-1% to 1%)",
        "Small increase (1% to 5%)",
        "Large increase (>5%)",
    ]
    df_pc["price_change_bucket"] = pd.cut(
        df_pc[pct_chg_col],
        bins=[-np.inf, -0.05, -0.01, 0.01, 0.05, np.inf],
        labels=bucket_labels,
    )

    bucket_rows = []
    for bucket, grp in df_pc.groupby("price_change_bucket", observed=True):
        rev_vals = grp["revenue"].values
        qty_vals = grp["qty_sold"].values

        rev_boot = ci_utils.bootstrap_ci(
            rev_vals, statistic=np.mean,
            n_boot=n_boot, ci_level=ci_level, seed=seed,
        )
        qty_boot = ci_utils.bootstrap_ci(
            qty_vals, statistic=np.mean,
            n_boot=n_boot, ci_level=ci_level, seed=seed,
        )
        bucket_rows.append({
            "price_change_bucket": str(bucket),
            "n": len(grp),
            "mean_revenue": rev_boot["estimate"],
            "ci_lower": rev_boot["ci_lower"],
            "ci_upper": rev_boot["ci_upper"],
            "median_revenue": float(np.median(rev_vals)),
            "total_revenue": float(rev_vals.sum()),
            "mean_qty": qty_boot["estimate"],
            "qty_ci_lower": qty_boot["ci_lower"],
            "qty_ci_upper": qty_boot["ci_upper"],
        })

    bucketed = pd.DataFrame(bucket_rows)

    # -- Pre/post revenue around price change events --
    pre_post = None
    df_events = df[df["days_since_price_change"].notna()].copy()
    post_rev = df_events.loc[
        df_events["days_since_price_change"].between(0, 3), "revenue"
    ]
    pre_rev = df_events.loc[
        df_events["days_since_price_change"].between(4, 7), "revenue"
    ]
    if len(post_rev) > 30 and len(pre_rev) > 30:
        pre_post = mann_whitney_test(
            post_rev, pre_rev,
            label_a="Post-change (0-3 days)",
            label_b="Pre-change (4-7 days)",
            n_boot=n_boot, ci_level=ci_level, seed=seed,
        )

    return {
        "pearson": pearson,
        "spearman": spearman,
        "bucketed": bucketed,
        "pre_post": pre_post,
    }


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------

def plot_statistical_tests(
    price_test: dict,
    inv_test: dict,
    margin_deciles: pd.DataFrame,
    ols_result: dict,
    ols_log_result: dict,
    pc_revenue: dict,
) -> None:
    """Generate all statistical-test figures.

    Creates four separate figures:
    1. Margin decile bar charts (1x2 subplots, one per margin column).
    2. OLS coefficient forest plots (regular + log).
    3. Price-revenue analysis (scatter, mean revenue by bucket, mean qty
       by bucket) as 1x3 subplots.
    4. Printed text summaries for Mann-Whitney tests.
    """
    # ---------------------------------------------------------------
    # Figure 1: Margin decile bar charts with CI error bars
    # ---------------------------------------------------------------
    unique_cols = margin_deciles["margin_col"].unique() if len(margin_deciles) else []
    n_cols = max(len(unique_cols), 1)
    fig1, axes1 = plt.subplots(1, n_cols, figsize=(8 * n_cols, 6))
    if n_cols == 1:
        axes1 = [axes1]

    for ax, col in zip(axes1, unique_cols):
        sub = margin_deciles[margin_deciles["margin_col"] == col].copy()
        x_pos = range(len(sub))
        avg = sub["avg_qty"].values
        lo = avg - sub["ci_lower"].values
        hi = sub["ci_upper"].values - avg
        yerr = np.array([np.abs(lo), np.abs(hi)])

        ax.bar(x_pos, avg, yerr=yerr, capsize=3, ecolor="gray",
               color="steelblue", alpha=0.8)
        ax.set_xticks(list(x_pos))
        ax.set_xticklabels(sub["margin_bin"], rotation=45, ha="right",
                           fontsize=8)
        ax.set_ylabel("Avg Qty Sold")
        label = col.replace("_", " ").title()
        ax.set_title(f"Avg Sales by {label} Decile (with 95% CI)")

    fig1.tight_layout()
    plt.show()

    # ---------------------------------------------------------------
    # Figure 2: OLS coefficient forest plots
    # ---------------------------------------------------------------
    if (ols_result.get("model") is not None
            or ols_log_result.get("model") is not None):
        fig2, axes2 = plt.subplots(1, 2, figsize=(16, 6))

        if ols_result.get("model") is not None:
            plot_utils.coefficient_ci_plot(
                ols_result["coefficients"],
                title=f"OLS Coefficients (R\u00b2={ols_result['r_squared']:.4f})",
                ax=axes2[0],
            )
        else:
            axes2[0].text(0.5, 0.5, "Insufficient data",
                          ha="center", va="center",
                          transform=axes2[0].transAxes)
            axes2[0].set_title("OLS Coefficients (linear)")

        if ols_log_result.get("model") is not None:
            plot_utils.coefficient_ci_plot(
                ols_log_result["coefficients"],
                title=(f"OLS Coefficients — log(1+qty) "
                       f"(R\u00b2={ols_log_result['r_squared']:.4f})"),
                ax=axes2[1],
            )
        else:
            axes2[1].text(0.5, 0.5, "Insufficient data",
                          ha="center", va="center",
                          transform=axes2[1].transAxes)
            axes2[1].set_title("OLS Coefficients (log)")

        fig2.tight_layout()
        plt.show()

    # ---------------------------------------------------------------
    # Figure 3: Price-revenue analysis (1x3)
    # ---------------------------------------------------------------
    bucketed = pc_revenue.get("bucketed", pd.DataFrame())
    spearman = pc_revenue.get("spearman", {})

    if len(bucketed) > 0:
        fig3, axes3 = plt.subplots(1, 3, figsize=(18, 5))

        # 3a — Scatter: price change vs revenue
        # (uses spearman r in title for consistency with original notebook)
        r_val = spearman.get("r", np.nan)
        axes3[0].set_title(f"Price Change vs Revenue (r={r_val:.3f})")
        axes3[0].set_xlabel("Price Change (%)")
        axes3[0].set_ylabel("Revenue ($)")
        # We don't have raw data here, so annotate with correlation info
        txt = (f"Pearson  r={pc_revenue['pearson']['r']:.4f} "
               f"[{pc_revenue['pearson']['ci_lower']:.4f}, "
               f"{pc_revenue['pearson']['ci_upper']:.4f}]"
               f"\nSpearman r={spearman['r']:.4f} "
               f"[{spearman['ci_lower']:.4f}, "
               f"{spearman['ci_upper']:.4f}]")
        axes3[0].text(0.05, 0.95, txt, transform=axes3[0].transAxes,
                      va="top", fontsize=9,
                      bbox=dict(facecolor="white", alpha=0.8))
        axes3[0].axvline(0, color="red", linestyle="--", alpha=0.5)

        # 3b — Mean revenue by bucket with CI
        x_pos = range(len(bucketed))
        rev_avg = bucketed["mean_revenue"].values
        rev_lo = rev_avg - bucketed["ci_lower"].values
        rev_hi = bucketed["ci_upper"].values - rev_avg
        axes3[1].bar(x_pos, rev_avg,
                     yerr=[np.abs(rev_lo), np.abs(rev_hi)],
                     color="steelblue", alpha=0.8, capsize=3, ecolor="gray")
        axes3[1].set_xticks(list(x_pos))
        axes3[1].set_xticklabels(bucketed["price_change_bucket"],
                                 rotation=30, ha="right", fontsize=8)
        axes3[1].set_ylabel("Mean Revenue ($)")
        axes3[1].set_title("Mean Revenue by Price Change Bucket")

        # 3c — Mean qty by bucket with CI
        qty_avg = bucketed["mean_qty"].values
        qty_lo = qty_avg - bucketed["qty_ci_lower"].values
        qty_hi = bucketed["qty_ci_upper"].values - qty_avg
        axes3[2].bar(x_pos, qty_avg,
                     yerr=[np.abs(qty_lo), np.abs(qty_hi)],
                     color="darkorange", alpha=0.8, capsize=3, ecolor="gray")
        axes3[2].set_xticks(list(x_pos))
        axes3[2].set_xticklabels(bucketed["price_change_bucket"],
                                 rotation=30, ha="right", fontsize=8)
        axes3[2].set_ylabel("Mean Qty Sold")
        axes3[2].set_title("Mean Qty by Price Change Bucket")

        fig3.tight_layout()
        plt.show()

    # ---------------------------------------------------------------
    # Figure 4 (text): Mann-Whitney test summaries
    # ---------------------------------------------------------------
    print()
    print("=" * 60)
    print("MANN-WHITNEY TEST SUMMARIES")
    print("=" * 60)

    print()
    print("--- Price Change Impact on Sales ---")
    _print_mw_summary(price_test, "Recent change", "No change")

    print()
    print("--- Inventory Availability vs Sales ---")
    _print_mw_summary(inv_test, "With inventory", "Without inventory")

    pre_post = pc_revenue.get("pre_post")
    if pre_post is not None:
        print()
        print("--- Pre/Post Revenue Around Price Change ---")
        _print_mw_summary(pre_post, "Post-change (0-3d)", "Pre-change (4-7d)")
        if pre_post["mean_b"] != 0:
            pct_chg = ((pre_post["mean_a"] - pre_post["mean_b"])
                       / pre_post["mean_b"] * 100)
            print(f"  Revenue change: {pct_chg:+.1f}%")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _print_mw_summary(result: dict, label_a: str, label_b: str) -> None:
    """Print a formatted Mann-Whitney result dict."""
    print(f"  {label_a}: n={result['n_a']:,}, "
          f"mean={result['mean_a']:.3f}, median={result['median_a']:.1f}")
    print(f"  {label_b}: n={result['n_b']:,}, "
          f"mean={result['mean_b']:.3f}, median={result['median_b']:.1f}")
    print(f"  U={result['U_stat']:.0f}, p={result['p_value']:.4e}")
    print(f"  Mean diff={result['mean_diff']:.4f} "
          f"[{result['mean_diff_ci_lower']:.4f}, "
          f"{result['mean_diff_ci_upper']:.4f}]")
    print(f"  Effect size r={result['effect_size_r']:.4f} "
          f"[{result['effect_r_ci_lower']:.4f}, "
          f"{result['effect_r_ci_upper']:.4f}]")
    print(f"  Significant: {'Yes' if result['significant'] else 'No'}")
