"""Margin optimization via quadratic OLS models with delta-method CIs.

Extracts the margin-sales and profit-maximizing optimization analyses
(original notebook cells 79-80) into reusable functions.
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import statsmodels.api as sm

from src.analysis import ci_utils, plot_utils


# ---------------------------------------------------------------------------
# Margin-sales optimization
# ---------------------------------------------------------------------------

def margin_sales_optimization(
    df,
    brands,
    margin_cols=None,
    margin_bounds=(-0.5, 0.8),
    optimal_range=(-0.1, 0.5),
    min_obs=50,
    ci_level=0.95,
):
    """Fit quadratic OLS (qty_sold ~ margin + margin^2) per brand/margin type.

    For each brand and margin column, finds the revenue-maximizing margin
    (vertex of the fitted parabola) with delta-method confidence intervals.

    Parameters
    ----------
    df : DataFrame
        Must contain ``qty_sold`` and the columns listed in *margin_cols*.
    brands : list of str
        Brand codes to iterate over. Column ``brand`` must exist in *df*.
    margin_cols : list of str, optional
        Margin columns to analyse. Defaults to ``["te_margin", "walmart_margin"]``.
    margin_bounds : tuple of float
        (lower, upper) filter on the margin column values.
    optimal_range : tuple of float
        Only keep results where the optimal margin falls in this range.
    min_obs : int
        Minimum observations required to fit a model.
    ci_level : float
        Confidence level for the delta-method CI.

    Returns
    -------
    DataFrame with columns: brand, margin_type, optimal_margin, ci_lower,
    ci_upper, se, current_avg, gap_pct, b1, b2, R2, n_obs, is_concave.
    """
    if margin_cols is None:
        margin_cols = ["te_margin", "walmart_margin"]

    _MARGIN_LABELS = {
        "te_margin": "TE Margin",
        "walmart_margin": "Walmart Margin",
    }

    rows = []
    for brand in brands:
        brand_df = df[df["brand"] == brand]
        for mcol in margin_cols:
            subset = brand_df[
                (brand_df["qty_sold"] > 0)
                & brand_df[mcol].notna()
                & brand_df[mcol].between(*margin_bounds)
            ].copy()

            if len(subset) < min_obs:
                continue

            y = subset["qty_sold"].values
            x_margin = subset[mcol].values
            X = sm.add_constant(np.column_stack([x_margin, x_margin ** 2]))

            try:
                model = sm.OLS(y, X).fit()
            except Exception:
                continue

            params = model.params.values if hasattr(model.params, "values") else np.asarray(model.params)
            vcov = model.cov_params().values if hasattr(model.cov_params(), "values") else np.asarray(model.cov_params())

            ci_result = ci_utils.delta_method_ci(params, vcov, ci_level)
            optimal_x = ci_result["optimal_x"]

            if np.isnan(optimal_x):
                continue
            if not (optimal_range[0] <= optimal_x <= optimal_range[1]):
                continue

            current_avg = float(subset[mcol].mean())
            margin_label = _MARGIN_LABELS.get(mcol, mcol)

            rows.append({
                "brand": brand,
                "margin_type": margin_label,
                "optimal_margin": optimal_x,
                "ci_lower": ci_result["ci_lower"],
                "ci_upper": ci_result["ci_upper"],
                "se": ci_result["se"],
                "current_avg": current_avg,
                "gap_pct": optimal_x - current_avg,
                "b1": float(params[1]),
                "b2": float(params[2]),
                "R2": float(model.rsquared),
                "n_obs": len(subset),
                "is_concave": float(params[2]) < 0,
            })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Profit-maximizing margin
# ---------------------------------------------------------------------------

def profit_maximizing_margin(
    df,
    brands,
    margin_bounds=(-0.5, 0.8),
    optimal_range=(-0.1, 0.5),
    min_obs=50,
    ci_level=0.95,
):
    """Fit quadratic OLS (profit_proxy ~ te_margin + te_margin^2) per brand.

    profit_proxy = qty_sold * te_margin * cost_to_walmart, approximating
    total gross profit for each SKU-node observation.

    Parameters
    ----------
    df : DataFrame
        Must contain ``qty_sold``, ``te_margin``, ``cost_to_walmart``, ``brand``.
    brands : list of str
        Brand codes to iterate over.
    margin_bounds : tuple of float
        Filter on te_margin values.
    optimal_range : tuple of float
        Only keep results where the optimal margin falls in this range.
    min_obs : int
        Minimum observations required to fit a model.
    ci_level : float
        Confidence level for the delta-method CI.

    Returns
    -------
    DataFrame with columns: brand, profit_max_margin, ci_lower, ci_upper,
    se, current_avg, is_concave, R2, n_obs.
    """
    rows = []
    for brand in brands:
        brand_df = df[df["brand"] == brand]
        subset = brand_df[
            (brand_df["qty_sold"] > 0)
            & brand_df["te_margin"].notna()
            & brand_df["cost_to_walmart"].notna()
            & brand_df["te_margin"].between(*margin_bounds)
        ].copy()

        if len(subset) < min_obs:
            continue

        subset["profit_proxy"] = (
            subset["qty_sold"] * subset["te_margin"] * subset["cost_to_walmart"]
        )

        y = subset["profit_proxy"].values
        x_margin = subset["te_margin"].values
        X = sm.add_constant(np.column_stack([x_margin, x_margin ** 2]))

        try:
            model = sm.OLS(y, X).fit()
        except Exception:
            continue

        params = model.params.values if hasattr(model.params, "values") else np.asarray(model.params)
        vcov = model.cov_params().values if hasattr(model.cov_params(), "values") else np.asarray(model.cov_params())

        ci_result = ci_utils.delta_method_ci(params, vcov, ci_level)
        optimal_x = ci_result["optimal_x"]

        if np.isnan(optimal_x):
            continue
        if not (optimal_range[0] <= optimal_x <= optimal_range[1]):
            continue

        current_avg = float(subset["te_margin"].mean())

        rows.append({
            "brand": brand,
            "profit_max_margin": optimal_x,
            "ci_lower": ci_result["ci_lower"],
            "ci_upper": ci_result["ci_upper"],
            "se": ci_result["se"],
            "current_avg": current_avg,
            "is_concave": float(params[2]) < 0,
            "R2": float(model.rsquared),
            "n_obs": len(subset),
        })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Plotting / reporting
# ---------------------------------------------------------------------------

def plot_optimization_results(margin_opt_df, profit_opt_df):
    """Print summary tables and plot revenue-vs-profit optimal margin scatter.

    Parameters
    ----------
    margin_opt_df : DataFrame
        Output of :func:`margin_sales_optimization`.
    profit_opt_df : DataFrame
        Output of :func:`profit_maximizing_margin`.
    """
    # ---- 1. Formatted margin-optimization table ----
    if len(margin_opt_df) > 0:
        print("=" * 90)
        print("MARGIN-SALES OPTIMIZATION RESULTS")
        print("=" * 90)
        print(
            f"{'Brand':<10} {'Type':<18} {'Optimal':>8} {'95% CI':>20} "
            f"{'Current':>8} {'Gap':>7} {'R2':>6} {'N':>6} {'Flag':>5}"
        )
        print("-" * 90)
        for _, row in margin_opt_df.iterrows():
            gap_pp = row["gap_pct"] * 100
            flag = " ***" if abs(gap_pp) > 3 else ""
            ci_str = f"[{row['ci_lower']:.3f}, {row['ci_upper']:.3f}]"
            print(
                f"{row['brand']:<10} {row['margin_type']:<18} "
                f"{row['optimal_margin']:>8.3f} {ci_str:>20} "
                f"{row['current_avg']:>8.3f} {gap_pp:>6.1f}pp "
                f"{row['R2']:>6.3f} {row['n_obs']:>6.0f}{flag}"
            )
        print("-" * 90)
        print("*** = gap from optimal exceeds 3 percentage points")
        print()
    else:
        print("No margin-sales optimization results to display.\n")

    # ---- 2. Revenue vs Profit scatter ----
    if (
        len(margin_opt_df) > 0
        and len(profit_opt_df) > 0
    ):
        # Use TE Margin rows from margin_opt for the revenue side
        rev_te = margin_opt_df[margin_opt_df["margin_type"] == "TE Margin"].copy()
        rev_te = rev_te.rename(columns={
            "optimal_margin": "rev_max_margin",
            "ci_lower": "rev_ci_lower",
            "ci_upper": "rev_ci_upper",
        })

        merged = rev_te.merge(
            profit_opt_df[["brand", "profit_max_margin", "ci_lower", "ci_upper"]].rename(
                columns={"ci_lower": "profit_ci_lower", "ci_upper": "profit_ci_upper"}
            ),
            on="brand",
            how="inner",
        )

        if len(merged) > 0:
            fig, ax = plt.subplots(figsize=(10, 8))

            # Error bars
            rev_err_lo = merged["rev_max_margin"] - merged["rev_ci_lower"]
            rev_err_hi = merged["rev_ci_upper"] - merged["rev_max_margin"]
            prof_err_lo = merged["profit_max_margin"] - merged["profit_ci_lower"]
            prof_err_hi = merged["profit_ci_upper"] - merged["profit_max_margin"]

            ax.errorbar(
                merged["rev_max_margin"],
                merged["profit_max_margin"],
                xerr=[rev_err_lo.values, rev_err_hi.values],
                yerr=[prof_err_lo.values, prof_err_hi.values],
                fmt="o",
                markersize=8,
                capsize=4,
                ecolor="gray",
                color="steelblue",
                zorder=3,
            )

            # Brand labels
            for _, row in merged.iterrows():
                ax.annotate(
                    row["brand"],
                    (row["rev_max_margin"], row["profit_max_margin"]),
                    textcoords="offset points",
                    xytext=(8, 5),
                    fontsize=9,
                )

            # 45-degree reference line
            all_vals = np.concatenate([
                merged["rev_max_margin"].values,
                merged["profit_max_margin"].values,
            ])
            lo, hi = all_vals.min() - 0.02, all_vals.max() + 0.02
            ax.plot([lo, hi], [lo, hi], "k--", alpha=0.4, label="Equal line")

            ax.set_xlabel("Revenue-Maximizing Margin (TE)", fontsize=12)
            ax.set_ylabel("Profit-Maximizing Margin (TE)", fontsize=12)
            ax.set_title("Revenue vs Profit Optimal Margins by Brand", fontsize=14)
            ax.legend()
            plt.tight_layout()
            plt.show()

    # ---- 3. Profit optimization table ----
    if len(profit_opt_df) > 0:
        print("=" * 80)
        print("PROFIT-MAXIMIZING MARGIN RESULTS")
        print("=" * 80)
        print(
            f"{'Brand':<10} {'Optimal':>10} {'95% CI':>22} "
            f"{'Current':>8} {'Concave':>8} {'R2':>6} {'N':>6}"
        )
        print("-" * 80)
        for _, row in profit_opt_df.iterrows():
            ci_str = f"[{row['ci_lower']:.3f}, {row['ci_upper']:.3f}]"
            concave_str = "Yes" if row["is_concave"] else "No"
            print(
                f"{row['brand']:<10} {row['profit_max_margin']:>10.3f} "
                f"{ci_str:>22} {row['current_avg']:>8.3f} "
                f"{concave_str:>8} {row['R2']:>6.3f} {row['n_obs']:>6.0f}"
            )
        print("-" * 80)
        print()
    else:
        print("No profit-maximizing margin results to display.\n")
