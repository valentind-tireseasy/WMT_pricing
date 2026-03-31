"""Master pricing strategy table and narrative recommendations.

Extracts cells 85-86 from the original notebook.
Combines elasticity, DiD, and optimization results into an actionable table.
"""

import numpy as np
import pandas as pd
from scipy import stats as sp_stats


def build_strategy_table(
    df_brand_state_elasticity: pd.DataFrame,
    df_brand_rank: pd.DataFrame,
    df_margin_opt: pd.DataFrame = None,
    df_did_results: dict = None,
    *,
    elastic_threshold: float = -0.8,
    inelastic_threshold: float = -0.3,
    max_recommended_change: float = 0.05,
    high_confidence_min_n: int = 200,
    medium_confidence_min_n: int = 50,
    confidence_threshold: float = 0.05,
    ci_level: float = 0.95,
) -> pd.DataFrame:
    """Build master brand-state strategy table combining all prior results.

    Parameters
    ----------
    df_brand_state_elasticity : DataFrame
        From ``elasticity.estimate_elasticity(df, ["brand", "State"])``.
        Must have columns: brand, State, elasticity, se, ci_lower, ci_upper,
        p_value, n_obs.
    df_brand_rank : DataFrame
        From ``elasticity.brand_sensitivity_rankings``.
        Must have: brand, avg_te_margin, avg_wm_margin.
    df_margin_opt : DataFrame, optional
        From ``optimization.margin_sales_optimization``.
    df_did_results : dict, optional
        From ``did_effects.run_all_did``.  Expected key: ``"results"``
        containing a dict of DataFrames keyed by dimension.
    """
    if df_brand_state_elasticity is None or len(df_brand_state_elasticity) == 0:
        return pd.DataFrame()

    bs = df_brand_state_elasticity.dropna(subset=["elasticity"]).copy()
    rows = []

    for _, r in bs.iterrows():
        brand, state = r["brand"], r["State"]
        elast = r["elasticity"]
        se = r.get("se", np.nan)
        ci_lo = r.get("ci_lower", np.nan)
        ci_hi = r.get("ci_upper", np.nan)
        pval = r["p_value"]
        n = r["n_obs"]

        # Brand-level context
        br_info = (
            df_brand_rank[df_brand_rank["brand"] == brand]
            if df_brand_rank is not None and len(df_brand_rank) > 0
            else pd.DataFrame()
        )
        avg_te = br_info["avg_te_margin"].iloc[0] if len(br_info) > 0 else np.nan
        avg_wm = br_info["avg_wm_margin"].iloc[0] if len(br_info) > 0 else np.nan

        # Treatment effect from DiD (brand dimension)
        att = np.nan
        if df_did_results and "results" in df_did_results:
            brand_het = df_did_results["results"].get("brand", pd.DataFrame())
            if len(brand_het) > 0 and "segment_value" in brand_het.columns:
                match = brand_het[brand_het["segment_value"] == brand]
                if len(match) > 0:
                    att = match["ATT"].iloc[0]

        # Optimal margin
        opt_margin = np.nan
        if df_margin_opt is not None and len(df_margin_opt) > 0:
            opt = df_margin_opt[
                (df_margin_opt["brand"] == brand)
                & (df_margin_opt["margin_type"] == "TE Margin")
            ]
            if len(opt) > 0:
                opt_margin = opt["optimal_margin"].iloc[0]

        # Recommendation logic
        if elast < elastic_threshold:
            action = "Decrease"
            rec_pct = max(-max_recommended_change, elast * 0.02)
        elif elast > inelastic_threshold:
            action = "Increase"
            rec_pct = min(max_recommended_change, abs(elast) * 0.03)
        else:
            action = "Hold"
            rec_pct = 0.0

        # Projected impact (constant elasticity)
        if rec_pct != 0:
            expected_qty_pct = (1 + rec_pct) ** elast - 1
            expected_rev_pct = ((1 + rec_pct) * (1 + expected_qty_pct)) - 1
            # CI on projected impact using elasticity CI
            if pd.notna(ci_lo) and pd.notna(ci_hi):
                rev_pct_lo = ((1 + rec_pct) * (1 + (1 + rec_pct) ** ci_hi - 1)) - 1
                rev_pct_hi = ((1 + rec_pct) * (1 + (1 + rec_pct) ** ci_lo - 1)) - 1
                if rev_pct_lo > rev_pct_hi:
                    rev_pct_lo, rev_pct_hi = rev_pct_hi, rev_pct_lo
            else:
                rev_pct_lo = rev_pct_hi = np.nan
        else:
            expected_qty_pct = 0.0
            expected_rev_pct = 0.0
            rev_pct_lo = rev_pct_hi = 0.0

        # Confidence level
        if pval < confidence_threshold and n > high_confidence_min_n:
            confidence = "High"
        elif pval < confidence_threshold and n > medium_confidence_min_n:
            confidence = "Medium"
        else:
            confidence = "Low"

        rows.append({
            "brand": brand,
            "State": state,
            "elasticity": elast,
            "elasticity_ci_lower": ci_lo,
            "elasticity_ci_upper": ci_hi,
            "treatment_effect": att,
            "optimal_te_margin": opt_margin,
            "current_te_margin": avg_te,
            "current_wm_margin": avg_wm,
            "action": action,
            "recommended_change": rec_pct,
            "expected_qty_impact": expected_qty_pct,
            "expected_revenue_impact": expected_rev_pct,
            "rev_impact_ci_lower": rev_pct_lo,
            "rev_impact_ci_upper": rev_pct_hi,
            "confidence": confidence,
            "n_obs": n,
            "p_value": pval,
        })

    df_strategy = pd.DataFrame(rows)
    if len(df_strategy) > 0:
        df_strategy = df_strategy.sort_values(
            "expected_revenue_impact", ascending=False
        )
    return df_strategy


def generate_narrative(df_strategy: pd.DataFrame) -> str:
    """Generate text narrative of top recommendations.

    Returns the narrative string (also prints it).
    """
    if df_strategy is None or len(df_strategy) == 0:
        msg = "No strategy recommendations available."
        print(msg)
        return msg

    lines = []
    lines.append("=" * 70)
    lines.append("ACTIONABLE PRICING RECOMMENDATIONS")
    lines.append("=" * 70)

    # Top 10 highest impact
    top10 = df_strategy.head(10)
    lines.append("\n--- TOP 10 HIGHEST REVENUE IMPACT ---\n")
    for i, (_, r) in enumerate(top10.iterrows(), 1):
        ci_str = ""
        if pd.notna(r.get("rev_impact_ci_lower")) and pd.notna(
            r.get("rev_impact_ci_upper")
        ):
            ci_str = (
                f" [{r['rev_impact_ci_lower']:+.1%}, {r['rev_impact_ci_upper']:+.1%}]"
            )
        lines.append(
            f"{i}. **{r['brand']} in {r['State']}** "
            f"(elasticity={r['elasticity']:.3f} "
            f"[{r['elasticity_ci_lower']:.3f}, {r['elasticity_ci_upper']:.3f}], "
            f"n={r['n_obs']:,.0f})"
        )
        lines.append(
            f"   {r['action']} price by {abs(r['recommended_change']):.0%} -> "
            f"Expected revenue: {r['expected_revenue_impact']:+.1%}{ci_str}  "
            f"[{r['confidence']} confidence]"
        )

    # Quick wins
    quick_wins = df_strategy[
        (df_strategy["confidence"] == "High")
        & (df_strategy["expected_revenue_impact"] > 0)
    ].head(5)
    if len(quick_wins) > 0:
        lines.append("\n--- QUICK WINS (High Confidence, Positive Revenue) ---\n")
        for _, r in quick_wins.iterrows():
            lines.append(
                f"  {r['brand']} in {r['State']}: {r['action']} "
                f"{abs(r['recommended_change']):.0%} "
                f"-> +{r['expected_revenue_impact']:.1%} revenue "
                f"(n={r['n_obs']:,.0f})"
            )

    # Caution
    caution = df_strategy[df_strategy["confidence"] == "Low"].head(5)
    if len(caution) > 0:
        lines.append("\n--- CAUTION (Low Confidence - Need More Data) ---\n")
        for _, r in caution.iterrows():
            lines.append(
                f"  {r['brand']} in {r['State']}: elasticity={r['elasticity']:.3f} "
                f"(p={r['p_value']:.3f}, n={r['n_obs']:,.0f}) "
                f"- insufficient evidence"
            )

    text = "\n".join(lines)
    print(text)
    return text


def plot_strategy_overview(df_strategy: pd.DataFrame):
    """Strategy summary visualizations."""
    import matplotlib.pyplot as plt

    if df_strategy is None or len(df_strategy) == 0:
        print("No strategy data to plot.")
        return

    fig, axes = plt.subplots(1, 2, figsize=(16, 7))

    # Left: Action distribution
    action_counts = df_strategy["action"].value_counts()
    colors = {"Increase": "#2ecc71", "Decrease": "#e74c3c", "Hold": "#95a5a6"}
    bar_colors = [colors.get(a, "steelblue") for a in action_counts.index]
    axes[0].bar(action_counts.index, action_counts.values, color=bar_colors, alpha=0.8)
    axes[0].set_title("Recommended Actions Distribution")
    axes[0].set_ylabel("Number of Brand-State Segments")

    # Right: Confidence breakdown
    conf_action = df_strategy.groupby(["confidence", "action"]).size().unstack(
        fill_value=0
    )
    conf_order = ["High", "Medium", "Low"]
    conf_action = conf_action.reindex(
        [c for c in conf_order if c in conf_action.index]
    )
    conf_action.plot(
        kind="bar", ax=axes[1], color=[colors.get(c, "steelblue") for c in conf_action.columns],
        alpha=0.8,
    )
    axes[1].set_title("Actions by Confidence Level")
    axes[1].set_ylabel("Count")
    axes[1].tick_params(axis="x", rotation=0)
    axes[1].legend(title="Action")

    plt.tight_layout()
    plt.show()
