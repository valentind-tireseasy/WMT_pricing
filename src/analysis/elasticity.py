"""Price elasticity estimation via log-log OLS.

Replaces five separate implementations from the original notebook
(cells 59-61, 66-67, 69) with one generic ``estimate_elasticity()``
function.  All variants (state, brand-state, city, seasonal, brand
rankings) call the same core and differ only in ``groupby_cols``.

Uses ``src.analysis.ci_utils`` for analytical CIs and
``src.analysis.plot_utils`` for CI-aware visualisations.
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import statsmodels.api as sm
from scipy import stats as sp_stats

from src.analysis import ci_utils, plot_utils


# ---------------------------------------------------------------------------
# Core estimator
# ---------------------------------------------------------------------------

def estimate_elasticity(
    df: pd.DataFrame,
    groupby_cols: list[str],
    price_col: str = "offer_price",
    qty_col: str = "qty_sold",
    min_obs: int = 50,
    ci_level: float = 0.95,
) -> pd.DataFrame:
    """Run log-log OLS within each group and return elasticity estimates.

    Model: ``log1p(qty) ~ const + log(price)``

    Parameters
    ----------
    df : DataFrame
        Must contain *price_col*, *qty_col*, and every column in
        *groupby_cols*.
    groupby_cols : list[str]
        Columns that define groups (e.g. ``["state"]``,
        ``["brand", "state"]``).
    price_col, qty_col : str
        Column names for price and quantity.
    min_obs : int
        Minimum observations per group (groups below this are skipped).
    ci_level : float
        Confidence level for the coefficient interval.

    Returns
    -------
    DataFrame with columns
        ``[*groupby_cols, elasticity, se, ci_lower, ci_upper, p_value,
        n_obs, r_squared]``
    """
    # Pre-filter: positive price and quantity
    mask = (df[qty_col] > 0) & (df[price_col] > 0)
    data = df.loc[mask].copy()

    z = sp_stats.norm.ppf(1 - (1 - ci_level) / 2)

    records: list[dict] = []
    for group_key, grp in data.groupby(groupby_cols, dropna=False):
        # Normalise group_key to tuple
        if not isinstance(group_key, tuple):
            group_key = (group_key,)

        # Skip groups with NaN in the key
        if any(pd.isna(k) for k in group_key):
            continue

        if len(grp) < min_obs:
            continue

        y = np.log1p(grp[qty_col].values)
        X = sm.add_constant(np.log(grp[price_col].values))

        try:
            model = sm.OLS(y, X).fit()
        except Exception:
            continue

        coef = model.params[-1]  # elasticity (slope on log-price)
        se = model.bse[-1]
        p_value = model.pvalues[-1]
        ci_lower = coef - z * se
        ci_upper = coef + z * se

        row = dict(zip(groupby_cols, group_key))
        row.update({
            "elasticity": coef,
            "se": se,
            "ci_lower": ci_lower,
            "ci_upper": ci_upper,
            "p_value": p_value,
            "n_obs": int(len(grp)),
            "r_squared": model.rsquared,
        })
        records.append(row)

    if not records:
        cols = groupby_cols + [
            "elasticity", "se", "ci_lower", "ci_upper",
            "p_value", "n_obs", "r_squared",
        ]
        return pd.DataFrame(columns=cols)

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Seasonal elasticity
# ---------------------------------------------------------------------------

def estimate_seasonal_elasticity(
    df: pd.DataFrame,
    start_date,
    end_date,
    n_periods: int = 3,
    brands: list[str] | None = None,
    min_obs_overall: int = 50,
    min_obs_brand: int = 30,
    ci_level: float = 0.95,
) -> pd.DataFrame:
    """Estimate elasticity across sub-periods for overall and per-brand.

    Parameters
    ----------
    df : DataFrame
        Must contain a ``date`` column (or coercible to datetime), plus
        ``offer_price``, ``qty_sold``, and ``brand``.
    start_date, end_date : date-like
        Boundaries for the analysis window.
    n_periods : int
        Number of equal-length sub-periods.
    brands : list[str], optional
        Brands to include.  ``None`` means use all brands present.
    min_obs_overall : int
        Minimum observations for the overall (ALL) estimate per period.
    min_obs_brand : int
        Minimum observations for a brand-level estimate per period.
    ci_level : float
        Confidence level.

    Returns
    -------
    DataFrame with columns
        ``[period, brand, elasticity, se, ci_lower, ci_upper, p_value,
        n_obs]``
    """
    data = df.copy()
    data["date"] = pd.to_datetime(data["date"])

    # Build period boundaries and labels
    boundaries = pd.date_range(start=start_date, end=end_date, periods=n_periods + 1)
    labels = []
    for i in range(n_periods):
        lo_str = boundaries[i].strftime("%b %d")
        hi_str = boundaries[i + 1].strftime("%b %d")
        labels.append(f"P{i + 1} ({lo_str}-{hi_str})")

    data["sub_period"] = pd.cut(
        data["date"],
        bins=boundaries,
        labels=labels,
        include_lowest=True,
    )

    if brands is not None:
        brand_list = list(brands)
    else:
        brand_list = sorted(data["brand"].dropna().unique())

    records: list[dict] = []

    for period_label in labels:
        period_data = data[data["sub_period"] == period_label]

        # --- Overall estimate ---
        if len(period_data) >= min_obs_overall:
            period_data_tagged = period_data.assign(_dummy="ALL")
            overall = estimate_elasticity(
                period_data_tagged,
                groupby_cols=["_dummy"],
                min_obs=min_obs_overall,
                ci_level=ci_level,
            )
            for _, row in overall.iterrows():
                records.append({
                    "period": period_label,
                    "brand": "ALL",
                    "elasticity": row["elasticity"],
                    "se": row["se"],
                    "ci_lower": row["ci_lower"],
                    "ci_upper": row["ci_upper"],
                    "p_value": row["p_value"],
                    "n_obs": row["n_obs"],
                })

        # --- Per-brand estimates ---
        for brand in brand_list:
            brand_data = period_data[period_data["brand"] == brand]
            if len(brand_data) < min_obs_brand:
                continue
            brand_data_tagged = brand_data.assign(_dummy=brand)
            brand_result = estimate_elasticity(
                brand_data_tagged,
                groupby_cols=["_dummy"],
                min_obs=min_obs_brand,
                ci_level=ci_level,
            )
            for _, row in brand_result.iterrows():
                records.append({
                    "period": period_label,
                    "brand": brand,
                    "elasticity": row["elasticity"],
                    "se": row["se"],
                    "ci_lower": row["ci_lower"],
                    "ci_upper": row["ci_upper"],
                    "p_value": row["p_value"],
                    "n_obs": row["n_obs"],
                })

    cols = ["period", "brand", "elasticity", "se", "ci_lower", "ci_upper",
            "p_value", "n_obs"]
    if not records:
        return pd.DataFrame(columns=cols)

    return pd.DataFrame(records, columns=cols)


# ---------------------------------------------------------------------------
# Brand sensitivity rankings
# ---------------------------------------------------------------------------

def brand_sensitivity_rankings(
    df: pd.DataFrame,
    min_obs: int = 30,
    ci_level: float = 0.95,
) -> pd.DataFrame:
    """Rank brands by price sensitivity with context metrics and tiers.

    Parameters
    ----------
    df : DataFrame
        Must contain ``brand``, ``offer_price``, ``qty_sold``.
        Optionally: ``te_margin``, ``wm_margin``, ``revenue``,
        ``sku_node``, ``is_MAP_tire``, ``can_show_inventory``.

    Returns
    -------
    DataFrame sorted by elasticity (most elastic first), with columns
        ``brand, elasticity, se, ci_lower, ci_upper, p_value, n_obs,
        r_squared, avg_te_margin, avg_wm_margin, total_qty, total_revenue,
        n_sku_nodes, [pct_MAP], [pct_can_show_inv], sensitivity_tier,
        recommendation``
    """
    elast = estimate_elasticity(
        df, groupby_cols=["brand"], min_obs=min_obs, ci_level=ci_level,
    )

    if elast.empty:
        return elast

    # Build context aggregations
    agg_dict: dict = {}
    if "te_margin" in df.columns:
        agg_dict["avg_te_margin"] = ("te_margin", "mean")
    if "walmart_margin" in df.columns:
        agg_dict["avg_wm_margin"] = ("walmart_margin", "mean")
    agg_dict["total_qty"] = ("qty_sold", "sum")
    if "revenue" in df.columns:
        agg_dict["total_revenue"] = ("revenue", "sum")
    if "sku_node" in df.columns:
        agg_dict["n_sku_nodes"] = ("sku_node", "nunique")

    if agg_dict:
        ctx = df.groupby("brand").agg(**agg_dict).reset_index()
        elast = elast.merge(ctx, on="brand", how="left")

    if "is_MAP_tire" in df.columns:
        map_pct = (
            df.groupby("brand")["is_MAP_tire"]
            .mean()
            .reset_index()
            .rename(columns={"is_MAP_tire": "pct_MAP"})
        )
        elast = elast.merge(map_pct, on="brand", how="left")

    if "can_show_inventory" in df.columns:
        inv_pct = (
            df.groupby("brand")["can_show_inventory"]
            .mean()
            .reset_index()
            .rename(columns={"can_show_inventory": "pct_can_show_inv"})
        )
        elast = elast.merge(inv_pct, on="brand", how="left")

    # Sensitivity tiers
    def _tier(e):
        if e < -1.5:
            return "Highly Elastic"
        elif e < -0.8:
            return "Elastic"
        elif e < -0.3:
            return "Unit Elastic"
        else:
            return "Inelastic"

    def _rec(e):
        if e < -0.8:
            return "Decrease price to capture volume"
        elif e >= -0.3:
            return "Increase price to capture margin"
        else:
            return "Monitor - near unit elastic"

    elast["sensitivity_tier"] = elast["elasticity"].apply(_tier)
    elast["recommendation"] = elast["elasticity"].apply(_rec)

    elast = elast.sort_values("elasticity").reset_index(drop=True)
    return elast


# ---------------------------------------------------------------------------
# Plotting helpers
# ---------------------------------------------------------------------------

def plot_elasticity_bars(
    df_elast: pd.DataFrame,
    groupby_col: str,
    title: str = "",
    top_n: int = 10,
) -> plt.Figure:
    """Side-by-side horizontal bar charts: most vs least price-sensitive.

    Parameters
    ----------
    df_elast : DataFrame
        Output of ``estimate_elasticity`` with a single *groupby_col*.
    groupby_col : str
        Column used as the category axis.
    title : str
        Super-title for the figure.
    top_n : int
        Number of groups to show on each side.

    Returns
    -------
    matplotlib Figure
    """
    df_sorted = df_elast.sort_values("elasticity")
    most_sensitive = df_sorted.head(top_n).copy()
    least_sensitive = df_sorted.tail(top_n).copy()

    fig, (ax_left, ax_right) = plt.subplots(1, 2, figsize=(16, 6))

    plot_utils.bar_chart_with_ci(
        most_sensitive,
        x_col=groupby_col,
        y_col="elasticity",
        ci_lower_col="ci_lower",
        ci_upper_col="ci_upper",
        title=f"Top {top_n} Most Price-Sensitive",
        xlabel="Elasticity",
        horizontal=True,
        color_by_sign=True,
        ax=ax_left,
    )

    plot_utils.bar_chart_with_ci(
        least_sensitive,
        x_col=groupby_col,
        y_col="elasticity",
        ci_lower_col="ci_lower",
        ci_upper_col="ci_upper",
        title=f"Top {top_n} Least Price-Sensitive",
        xlabel="Elasticity",
        horizontal=True,
        color_by_sign=True,
        ax=ax_right,
    )

    if title:
        fig.suptitle(title, fontsize=14, y=1.02)
    fig.tight_layout()
    return fig


def plot_elasticity_heatmap(
    df_elast: pd.DataFrame,
    row_col: str,
    col_col: str,
    title: str = "",
) -> plt.Figure:
    """Pivot elasticity results into a heatmap with CI annotations.

    Parameters
    ----------
    df_elast : DataFrame
        Output of ``estimate_elasticity`` containing *row_col*, *col_col*,
        ``elasticity``, ``ci_lower``, ``ci_upper``.
    row_col, col_col : str
        Columns to use as heatmap rows and columns.
    title : str
        Plot title.

    Returns
    -------
    matplotlib Figure
    """
    val_pivot = df_elast.pivot_table(
        index=row_col, columns=col_col, values="elasticity", aggfunc="first",
    )
    lo_pivot = df_elast.pivot_table(
        index=row_col, columns=col_col, values="ci_lower", aggfunc="first",
    )
    hi_pivot = df_elast.pivot_table(
        index=row_col, columns=col_col, values="ci_upper", aggfunc="first",
    )

    fig, ax = plt.subplots(
        figsize=(max(10, len(val_pivot.columns) * 1.8),
                 max(6, len(val_pivot) * 0.6)),
    )
    plot_utils.heatmap_with_ci_annotation(
        val_pivot,
        ci_lower=lo_pivot,
        ci_upper=hi_pivot,
        title=title,
        cmap="RdYlGn",
        center=0,
        ax=ax,
    )
    fig.tight_layout()
    return fig


def plot_seasonal_elasticity(
    df_seasonal: pd.DataFrame,
    top_brands_n: int = 8,
) -> plt.Figure:
    """Line chart + heatmap of seasonal elasticity by brand.

    Parameters
    ----------
    df_seasonal : DataFrame
        Output of ``estimate_seasonal_elasticity``.
    top_brands_n : int
        Number of brands (by observation count) to include.

    Returns
    -------
    matplotlib Figure
    """
    # Exclude ALL for brand selection
    brand_data = df_seasonal[df_seasonal["brand"] != "ALL"]
    if brand_data.empty:
        fig, ax = plt.subplots(figsize=(16, 7))
        ax.text(0.5, 0.5, "No brand-level seasonal data available",
                ha="center", va="center", transform=ax.transAxes)
        return fig

    # Select top brands by total observations
    top_brands = (
        brand_data.groupby("brand")["n_obs"]
        .sum()
        .nlargest(top_brands_n)
        .index.tolist()
    )
    plot_data = brand_data[brand_data["brand"].isin(top_brands)]

    fig, (ax_left, ax_right) = plt.subplots(1, 2, figsize=(16, 7))

    # --- Left: line chart with CI bands ---
    colors = plt.cm.tab10(np.linspace(0, 1, len(top_brands)))
    for brand, color in zip(top_brands, colors):
        bdf = plot_data[plot_data["brand"] == brand].sort_values("period")
        if bdf.empty:
            continue
        plot_utils.line_with_ci_band(
            x=bdf["period"].values,
            y=bdf["elasticity"].values,
            ci_lower=bdf["ci_lower"].values,
            ci_upper=bdf["ci_upper"].values,
            ax=ax_left,
            label=brand,
            color=color,
        )
    ax_left.set_title("Elasticity by Period (Top Brands)", fontsize=13)
    ax_left.set_xlabel("Period")
    ax_left.set_ylabel("Elasticity")
    ax_left.axhline(-1, color="gray", linestyle="--", alpha=0.5, label="Unit elastic")
    ax_left.legend(fontsize=8, loc="best")
    ax_left.tick_params(axis="x", rotation=30)

    # --- Right: heatmap of brand x period ---
    heat_pivot = plot_data.pivot_table(
        index="brand", columns="period", values="elasticity", aggfunc="first",
    )
    lo_pivot = plot_data.pivot_table(
        index="brand", columns="period", values="ci_lower", aggfunc="first",
    )
    hi_pivot = plot_data.pivot_table(
        index="brand", columns="period", values="ci_upper", aggfunc="first",
    )
    plot_utils.heatmap_with_ci_annotation(
        heat_pivot,
        ci_lower=lo_pivot,
        ci_upper=hi_pivot,
        title="Brand × Period Elasticity",
        cmap="RdYlGn",
        center=0,
        ax=ax_right,
    )

    fig.tight_layout()

    # Print brands with largest seasonal shift
    seasonal_range = (
        plot_data.groupby("brand")["elasticity"]
        .agg(lambda x: x.max() - x.min())
        .sort_values(ascending=False)
    )
    if not seasonal_range.empty:
        print("\nBrands with largest seasonal elasticity shift:")
        for brand, shift in seasonal_range.head(5).items():
            print(f"  {brand}: {shift:.3f}")

    return fig
