"""Price elasticity estimation via log-log OLS and fixed effects.

Replaces five separate implementations from the original notebook
(cells 59-61, 66-67, 69) with one generic ``estimate_elasticity()``
function.  All variants (state, brand-state, city, seasonal, brand
rankings) call the same core and differ only in ``groupby_cols``.

Includes a fixed-effects estimator (``estimate_elasticity_fe``) that
exploits within-SKU-node price variation from historical DSV snapshots.

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
    print_detail_table: bool = False,
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
    print_detail_table : bool
        When *True*, the heatmap shows only central values and a detailed
        table with CI limits is printed below the plot.

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
        show_ci=not print_detail_table,
        ax=ax,
    )
    fig.tight_layout()

    if print_detail_table:
        detail = (
            df_elast[[row_col, col_col, "elasticity", "ci_lower", "ci_upper"]]
            .sort_values([row_col, col_col])
            .reset_index(drop=True)
        )
        detail.columns = [row_col, col_col, "Elasticity", "CI Lower", "CI Upper"]
        print(f"\n{title} — Detail Table")
        print(detail.to_string(index=False, float_format="%.3f"))

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


# ---------------------------------------------------------------------------
# Fixed-effects panel elasticity
# ---------------------------------------------------------------------------

def diagnose_fe_feasibility(
    df: pd.DataFrame,
    entity_cols: list[str] = ("sku", "node"),
    price_col: str = "cost_to_walmart",
    qty_col: str = "qty_sold",
) -> dict:
    """Check whether within-entity price variation supports FE estimation.

    Parameters
    ----------
    df : DataFrame
        Full panel (including zero-sales rows).
    entity_cols : list[str]
        Columns defining the panel entity (default ``["sku", "node"]``).
    price_col : str
        Time-varying price column (from DSV snapshots).
    qty_col : str
        Quantity column.

    Returns
    -------
    dict with diagnostic statistics printed to stdout.
    """
    data = df.dropna(subset=[price_col]).copy()
    data["_entity"] = data[list(entity_cols)].astype(str).agg("-".join, axis=1)

    total_entities = data["_entity"].nunique()

    # Within-entity price variation
    entity_stats = data.groupby("_entity")[price_col].agg(["std", "nunique", "count"])
    entities_with_variation = (entity_stats["std"] > 0).sum()
    entities_multi_price = (entity_stats["nunique"] > 1).sum()

    # Sales days per entity
    sales_data = data[data[qty_col] > 0]
    sales_per_entity = sales_data.groupby("_entity").size()

    # Entities with both price variation AND sales
    entities_with_var_set = set(entity_stats[entity_stats["std"] > 0].index)
    entities_with_sales = set(sales_per_entity[sales_per_entity >= 2].index)
    usable_entities = entities_with_var_set & entities_with_sales

    stats = {
        "total_entities": total_entities,
        "entities_with_price_variation": entities_with_variation,
        "entities_multiple_prices": int(entities_multi_price),
        "pct_with_variation": entities_with_variation / max(total_entities, 1) * 100,
        "entities_with_2plus_sales_days": len(entities_with_sales),
        "usable_entities_for_fe": len(usable_entities),
        "median_sales_days_per_entity": float(sales_per_entity.median()) if len(sales_per_entity) > 0 else 0,
        "median_unique_prices_per_entity": float(entity_stats["nunique"].median()),
        "total_rows": len(data),
        "rows_with_sales": len(sales_data),
    }

    print("=== Fixed-Effects Feasibility Diagnostic ===")
    print(f"  Total entities ({', '.join(entity_cols)}): {stats['total_entities']:,}")
    print(f"  Entities with price variation:  {stats['entities_with_price_variation']:,} "
          f"({stats['pct_with_variation']:.1f}%)")
    print(f"  Entities with multiple prices:  {stats['entities_multiple_prices']:,}")
    print(f"  Entities with 2+ sales days:    {stats['entities_with_2plus_sales_days']:,}")
    print(f"  Usable for FE (variation+sales): {stats['usable_entities_for_fe']:,}")
    print(f"  Median sales days per entity:   {stats['median_sales_days_per_entity']:.0f}")
    print(f"  Median unique prices per entity: {stats['median_unique_prices_per_entity']:.0f}")
    print(f"  Total rows: {stats['total_rows']:,}  |  Rows with sales: {stats['rows_with_sales']:,}")

    return stats


def estimate_elasticity_fe(
    df: pd.DataFrame,
    entity_cols: list[str] = ("sku", "node"),
    groupby_cols: list[str] | None = None,
    price_col: str = "cost_to_walmart",
    qty_col: str = "qty_sold",
    min_obs_per_entity: int = 3,
    min_entities: int = 5,
    ci_level: float = 0.95,
) -> pd.DataFrame:
    """Fixed-effects elasticity using within-entity price variation.

    Uses the within (demeaning) estimator: for each entity *i*, subtract
    the entity mean from ``log(price)`` and ``log1p(qty)``, then pool and
    run OLS (no constant).  The slope is the within-entity elasticity.

    Parameters
    ----------
    df : DataFrame
        Must contain *entity_cols*, *price_col*, *qty_col*, and optionally
        columns in *groupby_cols* for group-level reporting.
    entity_cols : list[str]
        Columns defining the panel entity (default ``["sku", "node"]``).
    groupby_cols : list[str] or None
        Columns to report results by (e.g. ``["brand"]``).
        If ``None``, returns a single overall estimate.
    price_col : str
        Time-varying price column (``cost_to_walmart`` from DSV history).
    qty_col : str
        Quantity sold column.
    min_obs_per_entity : int
        Minimum sales-day observations per entity to include it.
    min_entities : int
        Minimum usable entities per reporting group.
    ci_level : float
        Confidence level for the coefficient interval.

    Returns
    -------
    DataFrame with columns
        ``[*groupby_cols, elasticity_fe, se, ci_lower, ci_upper, p_value,
        n_entities, n_obs, r_squared_within]``
    """
    # Filter to positive sales and valid prices
    mask = (df[qty_col] > 0) & (df[price_col] > 0)
    data = df.loc[mask].copy()

    data["_entity"] = data[list(entity_cols)].astype(str).agg("-".join, axis=1)
    data["_log_price"] = np.log(data[price_col])
    data["_log_qty"] = np.log1p(data[qty_col])

    # Keep only entities with price variation AND enough observations
    entity_stats = data.groupby("_entity").agg(
        _price_std=("_log_price", "std"),
        _n_obs=("_log_price", "size"),
    )
    usable = entity_stats[
        (entity_stats["_price_std"] > 0) & (entity_stats["_n_obs"] >= min_obs_per_entity)
    ].index
    data = data[data["_entity"].isin(usable)].copy()

    if len(data) == 0:
        cols = (list(groupby_cols) if groupby_cols else []) + [
            "elasticity_fe", "se", "ci_lower", "ci_upper",
            "p_value", "n_entities", "n_obs", "r_squared_within",
        ]
        return pd.DataFrame(columns=cols)

    # Demean within entity
    entity_means = data.groupby("_entity")[["_log_price", "_log_qty"]].transform("mean")
    data["_dm_price"] = data["_log_price"] - entity_means["_log_price"]
    data["_dm_qty"] = data["_log_qty"] - entity_means["_log_qty"]

    z = sp_stats.norm.ppf(1 - (1 - ci_level) / 2)

    def _fe_ols(grp):
        """Run demeaned OLS (no constant) on a group."""
        y = grp["_dm_qty"].values
        X = grp["_dm_price"].values.reshape(-1, 1)
        n_ent = grp["_entity"].nunique()

        if n_ent < min_entities or len(grp) < min_entities * 2:
            return None

        try:
            model = sm.OLS(y, X).fit()
        except Exception:
            return None

        coef = model.params[0]
        se = model.bse[0]
        # Adjust SE for entity-level clustering (Moulton factor approximation)
        avg_cluster_size = len(grp) / n_ent
        if avg_cluster_size > 1:
            resid_by_entity = grp.copy()
            resid_by_entity["_resid"] = model.resid
            rho = resid_by_entity.groupby("_entity")["_resid"].apply(
                lambda r: r.autocorr() if len(r) > 1 else 0
            ).mean()
            rho = max(rho, 0)
            moulton = np.sqrt(1 + rho * (avg_cluster_size - 1))
            se_adj = se * moulton
        else:
            se_adj = se

        return {
            "elasticity_fe": coef,
            "se": se_adj,
            "ci_lower": coef - z * se_adj,
            "ci_upper": coef + z * se_adj,
            "p_value": 2 * (1 - sp_stats.norm.cdf(abs(coef / se_adj))),
            "n_entities": n_ent,
            "n_obs": len(grp),
            "r_squared_within": float(model.rsquared),
        }

    records = []
    if groupby_cols:
        for group_key, grp in data.groupby(groupby_cols, dropna=False):
            if not isinstance(group_key, tuple):
                group_key = (group_key,)
            if any(pd.isna(k) for k in group_key):
                continue
            result = _fe_ols(grp)
            if result is None:
                continue
            row = dict(zip(groupby_cols, group_key))
            row.update(result)
            records.append(row)
    else:
        result = _fe_ols(data)
        if result is not None:
            records.append(result)

    cols = (list(groupby_cols) if groupby_cols else []) + [
        "elasticity_fe", "se", "ci_lower", "ci_upper",
        "p_value", "n_entities", "n_obs", "r_squared_within",
    ]
    if not records:
        return pd.DataFrame(columns=cols)
    return pd.DataFrame(records, columns=cols)


def plot_fe_vs_ols_comparison(
    df_ols: pd.DataFrame,
    df_fe: pd.DataFrame,
    groupby_col: str,
    title: str = "OLS vs Fixed-Effects Elasticity Comparison",
) -> plt.Figure:
    """Scatter plot comparing OLS (cross-sectional) vs FE (within) elasticity.

    Parameters
    ----------
    df_ols : DataFrame
        Output of ``estimate_elasticity`` with ``elasticity`` column.
    df_fe : DataFrame
        Output of ``estimate_elasticity_fe`` with ``elasticity_fe`` column.
    groupby_col : str
        Column to join on (e.g. ``"brand"``).
    title : str
        Plot title.

    Returns
    -------
    matplotlib Figure
    """
    merged = df_ols[[groupby_col, "elasticity", "ci_lower", "ci_upper"]].merge(
        df_fe[[groupby_col, "elasticity_fe", "ci_lower", "ci_upper"]],
        on=groupby_col,
        suffixes=("_ols", "_fe"),
    )

    if merged.empty:
        fig, ax = plt.subplots(figsize=(8, 6))
        ax.text(0.5, 0.5, "No overlapping groups for comparison",
                ha="center", va="center", transform=ax.transAxes)
        return fig

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7))

    # --- Left: scatter comparison ---
    ax1.errorbar(
        merged["elasticity"], merged["elasticity_fe"],
        xerr=[merged["elasticity"] - merged["ci_lower_ols"],
              merged["ci_upper_ols"] - merged["elasticity"]],
        yerr=[merged["elasticity_fe"] - merged["ci_lower_fe"],
              merged["ci_upper_fe"] - merged["elasticity_fe"]],
        fmt="o", alpha=0.6, markersize=5, elinewidth=0.8, capsize=2,
    )
    lims = [
        min(merged["elasticity"].min(), merged["elasticity_fe"].min()) - 0.3,
        max(merged["elasticity"].max(), merged["elasticity_fe"].max()) + 0.3,
    ]
    ax1.plot(lims, lims, "--", color="gray", alpha=0.5, label="45° line")
    ax1.set_xlabel("OLS Elasticity (cross-sectional)")
    ax1.set_ylabel("FE Elasticity (within SKU-node)")
    ax1.set_title("OLS vs Fixed-Effects")
    ax1.legend()

    for _, row in merged.iterrows():
        ax1.annotate(
            row[groupby_col], (row["elasticity"], row["elasticity_fe"]),
            fontsize=6, alpha=0.7,
            xytext=(3, 3), textcoords="offset points",
        )

    # --- Right: paired bar chart of top differences ---
    merged["diff"] = merged["elasticity_fe"] - merged["elasticity"]
    merged["abs_diff"] = merged["diff"].abs()
    top_diff = merged.nlargest(min(15, len(merged)), "abs_diff").sort_values("diff")

    y_pos = range(len(top_diff))
    ax2.barh(y_pos, top_diff["elasticity"], height=0.35, label="OLS",
             color="steelblue", alpha=0.7)
    ax2.barh([y + 0.35 for y in y_pos], top_diff["elasticity_fe"], height=0.35,
             label="FE", color="coral", alpha=0.7)
    ax2.set_yticks([y + 0.175 for y in y_pos])
    ax2.set_yticklabels(top_diff[groupby_col], fontsize=8)
    ax2.set_xlabel("Elasticity")
    ax2.set_title("Largest OLS vs FE Differences")
    ax2.legend()
    ax2.axvline(0, color="gray", linewidth=0.5)

    fig.suptitle(title, fontsize=14, y=1.02)
    fig.tight_layout()

    # Print summary stats
    corr = merged["elasticity"].corr(merged["elasticity_fe"])
    mean_diff = merged["diff"].mean()
    print(f"\nOLS vs FE comparison ({len(merged)} groups):")
    print(f"  Correlation: {corr:.3f}")
    print(f"  Mean difference (FE - OLS): {mean_diff:.3f}")
    print(f"  FE more negative (demand more elastic) in "
          f"{(merged['diff'] < 0).sum()}/{len(merged)} groups")

    return fig
