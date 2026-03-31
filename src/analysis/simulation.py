"""What-if price change simulation module.

Extracted from original notebook cells 82-83.  Simulates the impact of
hypothetical price changes on revenue and profit using constant-elasticity
demand models, with confidence intervals propagated from the standard error
of the elasticity estimates.
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from src.analysis import ci_utils, plot_utils


# ---------------------------------------------------------------------------
# Core simulation
# ---------------------------------------------------------------------------

def simulate_price_changes(df_brand_rank, df, pct_changes=None, min_obs_brand=30):
    """Project revenue and profit for a range of price changes per brand.

    Parameters
    ----------
    df_brand_rank : DataFrame
        Output of ``elasticity.brand_sensitivity_rankings`` with columns
        ``brand``, ``elasticity``, ``se``.
    df : DataFrame
        Transaction-level data with columns ``brand``, ``offer_price``,
        ``qty_sold``, and optionally ``cost_to_walmart``.
    pct_changes : array-like or None
        Fractional price changes to simulate (e.g. -0.05 = 5% decrease).
        Defaults to ``np.arange(-0.05, 0.06, 0.01)``.
    min_obs_brand : int
        Minimum rows with ``qty_sold > 0`` required per brand.

    Returns
    -------
    DataFrame with columns: brand, pct_change, new_price, projected_qty,
        projected_revenue, revenue_ci_lower, revenue_ci_upper,
        projected_profit, profit_ci_lower, profit_ci_upper,
        revenue_change_pct, profit_change_pct, base_revenue, base_profit.
    """
    if pct_changes is None:
        pct_changes = np.arange(-0.05, 0.06, 0.01)

    rows = []

    for _, row in df_brand_rank.iterrows():
        brand = row["brand"]
        elasticity = row["elasticity"]
        se = row["se"]

        df_brand = df[df["brand"] == brand]
        df_pos = df_brand[df_brand["qty_sold"] > 0]

        if len(df_pos) < min_obs_brand:
            continue

        base_price = df_brand["offer_price"].mean()
        base_qty = df_brand["qty_sold"].mean()

        if "cost_to_walmart" in df_brand.columns and df_brand["cost_to_walmart"].notna().any():
            base_cost = df_brand["cost_to_walmart"].mean()
        else:
            base_cost = base_price * 0.85

        base_revenue = base_qty * base_price
        base_profit = base_qty * (base_price - base_cost)

        for pct_change in pct_changes:
            new_price = base_price * (1 + pct_change)
            price_ratio = new_price / base_price

            # Constant elasticity model: Q_new = Q_base * (P_new / P_base)^e
            projected_qty = base_qty * (price_ratio ** elasticity)
            projected_qty_lo = base_qty * (price_ratio ** (elasticity - 1.96 * se))
            projected_qty_hi = base_qty * (price_ratio ** (elasticity + 1.96 * se))

            projected_revenue = projected_qty * new_price
            revenue_ci_lower = projected_qty_lo * new_price
            revenue_ci_upper = projected_qty_hi * new_price

            # Ensure lower <= upper after propagation
            revenue_ci_lower, revenue_ci_upper = (
                min(revenue_ci_lower, revenue_ci_upper),
                max(revenue_ci_lower, revenue_ci_upper),
            )

            projected_profit = projected_qty * (new_price - base_cost)
            profit_ci_lower = projected_qty_lo * (new_price - base_cost)
            profit_ci_upper = projected_qty_hi * (new_price - base_cost)

            profit_ci_lower, profit_ci_upper = (
                min(profit_ci_lower, profit_ci_upper),
                max(profit_ci_lower, profit_ci_upper),
            )

            revenue_change_pct = (
                (projected_revenue - base_revenue) / base_revenue
                if base_revenue != 0 else np.nan
            )
            profit_change_pct = (
                (projected_profit - base_profit) / base_profit
                if base_profit != 0 else np.nan
            )

            rows.append({
                "brand": brand,
                "pct_change": pct_change,
                "new_price": new_price,
                "projected_qty": projected_qty,
                "projected_revenue": projected_revenue,
                "revenue_ci_lower": revenue_ci_lower,
                "revenue_ci_upper": revenue_ci_upper,
                "projected_profit": projected_profit,
                "profit_ci_lower": profit_ci_lower,
                "profit_ci_upper": profit_ci_upper,
                "revenue_change_pct": revenue_change_pct,
                "profit_change_pct": profit_change_pct,
                "base_revenue": base_revenue,
                "base_profit": base_profit,
            })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Sweet-spot finder
# ---------------------------------------------------------------------------

def find_sweet_spots(df_sim, df_brand_rank, elastic_threshold=-0.8,
                     inelastic_threshold=-0.3):
    """Identify optimal price change for elastic and inelastic brands.

    Parameters
    ----------
    df_sim : DataFrame
        Output of :func:`simulate_price_changes`.
    df_brand_rank : DataFrame
        Brand rankings with ``brand``, ``elasticity``, ``se``.
    elastic_threshold : float
        Brands with elasticity below this are considered elastic.
    inelastic_threshold : float
        Brands with elasticity above this are considered inelastic.

    Returns
    -------
    DataFrame with columns: brand, strategy, recommended_pct,
        expected_change_pct, change_ci_lower, change_ci_upper.
    """
    results = []

    # --- Elastic brands: maximize revenue via price *decreases* -----------
    elastic_brands = df_brand_rank[
        df_brand_rank["elasticity"] < elastic_threshold
    ]["brand"].tolist()

    for brand in elastic_brands:
        brand_sim = df_sim[
            (df_sim["brand"] == brand) & (df_sim["pct_change"] < 0)
        ]
        if brand_sim.empty:
            continue

        best_idx = brand_sim["revenue_change_pct"].idxmax()
        best = brand_sim.loc[best_idx]

        base_rev = best["base_revenue"]
        if base_rev != 0:
            ci_lo = (best["revenue_ci_lower"] - base_rev) / base_rev
            ci_hi = (best["revenue_ci_upper"] - base_rev) / base_rev
        else:
            ci_lo, ci_hi = np.nan, np.nan

        results.append({
            "brand": brand,
            "strategy": "price_decrease_for_revenue",
            "recommended_pct": best["pct_change"],
            "expected_change_pct": best["revenue_change_pct"],
            "change_ci_lower": ci_lo,
            "change_ci_upper": ci_hi,
        })

    # --- Inelastic brands: maximize profit via price *increases* ----------
    inelastic_brands = df_brand_rank[
        df_brand_rank["elasticity"] > inelastic_threshold
    ]["brand"].tolist()

    for brand in inelastic_brands:
        brand_sim = df_sim[
            (df_sim["brand"] == brand) & (df_sim["pct_change"] > 0)
        ]
        if brand_sim.empty:
            continue

        best_idx = brand_sim["profit_change_pct"].idxmax()
        best = brand_sim.loc[best_idx]

        base_prof = best["base_profit"]
        if base_prof != 0:
            ci_lo = (best["profit_ci_lower"] - base_prof) / base_prof
            ci_hi = (best["profit_ci_upper"] - base_prof) / base_prof
        else:
            ci_lo, ci_hi = np.nan, np.nan

        results.append({
            "brand": brand,
            "strategy": "price_increase_for_profit",
            "recommended_pct": best["pct_change"],
            "expected_change_pct": best["profit_change_pct"],
            "change_ci_lower": ci_lo,
            "change_ci_upper": ci_hi,
        })

    return pd.DataFrame(results)


# ---------------------------------------------------------------------------
# Visualization
# ---------------------------------------------------------------------------

def plot_simulation_results(df_sim, df_brand_rank, elastic_brands=None,
                            inelastic_brands=None, top_n=5):
    """Plot simulation curves with CI bands and print the sweet-spot table.

    Parameters
    ----------
    df_sim : DataFrame
        Output of :func:`simulate_price_changes`.
    df_brand_rank : DataFrame
        Brand rankings with ``brand``, ``elasticity``, ``se``.
    elastic_brands : list or None
        Brands to plot on the elastic panel.  Defaults to *top_n* most
        elastic brands present in *df_sim*.
    inelastic_brands : list or None
        Brands to plot on the inelastic panel.  Defaults to *top_n* most
        inelastic brands present in *df_sim*.
    top_n : int
        Number of brands to select when *elastic_brands* or
        *inelastic_brands* is ``None``.
    """
    sim_brands = df_sim["brand"].unique()
    rank_in_sim = df_brand_rank[df_brand_rank["brand"].isin(sim_brands)]

    if elastic_brands is None:
        elastic_brands = (
            rank_in_sim.nsmallest(top_n, "elasticity")["brand"].tolist()
        )
    if inelastic_brands is None:
        inelastic_brands = (
            rank_in_sim.nlargest(top_n, "elasticity")["brand"].tolist()
        )

    # ---- Figure 1: two-panel line plots with CI bands --------------------
    fig, (ax_left, ax_right) = plt.subplots(1, 2, figsize=(16, 7))

    # Left panel — elastic brands, price decreases, revenue impact
    for brand in elastic_brands:
        bsim = df_sim[
            (df_sim["brand"] == brand) & (df_sim["pct_change"] <= 0)
        ].sort_values("pct_change")
        if bsim.empty:
            continue

        x = bsim["pct_change"].values * 100
        base_rev = bsim["base_revenue"].iloc[0]
        if base_rev == 0:
            continue

        y = bsim["revenue_change_pct"].values * 100
        ci_lo = ((bsim["revenue_ci_lower"].values - base_rev) / base_rev) * 100
        ci_hi = ((bsim["revenue_ci_upper"].values - base_rev) / base_rev) * 100

        plot_utils.line_with_ci_band(
            x, y, ci_lo, ci_hi,
            ax=ax_left, label=brand,
        )

    ax_left.set_title("Elastic Brands: Revenue Impact of Price Decrease",
                       fontsize=13)
    ax_left.set_xlabel("Price Change (%)", fontsize=11)
    ax_left.set_ylabel("Revenue Change (%)", fontsize=11)
    ax_left.axhline(0, color="black", linewidth=0.5)
    ax_left.legend(fontsize=9)

    # Right panel — inelastic brands, price increases, profit impact
    for brand in inelastic_brands:
        bsim = df_sim[
            (df_sim["brand"] == brand) & (df_sim["pct_change"] >= 0)
        ].sort_values("pct_change")
        if bsim.empty:
            continue

        x = bsim["pct_change"].values * 100
        base_prof = bsim["base_profit"].iloc[0]
        if base_prof == 0:
            continue

        y = bsim["profit_change_pct"].values * 100
        ci_lo = ((bsim["profit_ci_lower"].values - base_prof) / base_prof) * 100
        ci_hi = ((bsim["profit_ci_upper"].values - base_prof) / base_prof) * 100

        plot_utils.line_with_ci_band(
            x, y, ci_lo, ci_hi,
            ax=ax_right, label=brand,
        )

    ax_right.set_title("Inelastic Brands: Profit Impact of Price Increase",
                        fontsize=13)
    ax_right.set_xlabel("Price Change (%)", fontsize=11)
    ax_right.set_ylabel("Profit Change (%)", fontsize=11)
    ax_right.axhline(0, color="black", linewidth=0.5)
    ax_right.legend(fontsize=9)

    fig.tight_layout()
    plt.show()

    # ---- Figure 2: sweet-spot table --------------------------------------
    df_sweet = find_sweet_spots(df_sim, df_brand_rank)
    if df_sweet.empty:
        print("No sweet spots identified.")
        return fig, df_sweet

    print("\n=== Price Change Sweet Spots ===\n")
    display_df = df_sweet.copy()
    display_df["recommended_pct"] = display_df["recommended_pct"].map(
        lambda v: f"{v:+.1%}"
    )
    display_df["expected_change_pct"] = display_df["expected_change_pct"].map(
        lambda v: f"{v:+.1%}"
    )
    display_df["change_ci_lower"] = display_df["change_ci_lower"].map(
        lambda v: f"{v:+.1%}" if pd.notna(v) else "N/A"
    )
    display_df["change_ci_upper"] = display_df["change_ci_upper"].map(
        lambda v: f"{v:+.1%}" if pd.notna(v) else "N/A"
    )
    print(display_df.to_string(index=False))

    return fig, df_sweet
