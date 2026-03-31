"""Executive summary generation and output saving.

Extracts cells 88 and 90 from the original notebook.
"""

import os
import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def generate_executive_summary(
    df: pd.DataFrame,
    results: dict,
    start_date: str,
    end_date: str,
) -> str:
    """Generate extended executive summary text.

    Parameters
    ----------
    df : DataFrame
        The master analysis DataFrame.
    results : dict
        Aggregated results from all analysis modules.  Expected keys
        (all optional):
        ``state_elasticity``, ``brand_state_elasticity``,
        ``brand_rank``, ``did_results``, ``seasonal``,
        ``strategy``, ``corr_result``.
    start_date, end_date : str
        Analysis window boundaries.

    Returns
    -------
    str  The summary text (also printed).
    """
    lines = []
    lines.append("=" * 70)
    lines.append("EXTENDED EXECUTIVE SUMMARY")
    lines.append(f"Analysis Window: {start_date} to {end_date}")
    lines.append(f"Dataset: {df.shape[0]:,} rows, {df.shape[1]} columns")
    lines.append("=" * 70)

    # --- New features ---
    lines.append("\n1. NEW FEATURES")
    if "tire_size" in df.columns:
        lines.append(f"   - Tire size coverage: {df['tire_size'].notna().mean():.1%}")
    if "is_MAP_tire" in df.columns:
        lines.append(f"   - MAP-governed tires: {df['is_MAP_tire'].mean():.1%}")
    if "tire_diameter" in df.columns and df["tire_diameter"].notna().any():
        top5 = df["tire_diameter"].value_counts().head(5).to_dict()
        lines.append(f"   - Most common diameters: {top5}")

    # --- State elasticity ---
    lines.append("\n2. STATE-LEVEL PRICE SENSITIVITY")
    df_state = results.get("state_elasticity")
    if df_state is not None and len(df_state) > 0:
        most = df_state.iloc[0]
        least = df_state.iloc[-1]
        lines.append(
            f"   - Most price-sensitive state: {most['State']} "
            f"(elasticity={most['elasticity']:.3f} "
            f"[{most.get('ci_lower', np.nan):.3f}, {most.get('ci_upper', np.nan):.3f}])"
        )
        lines.append(
            f"   - Least price-sensitive state: {least['State']} "
            f"(elasticity={least['elasticity']:.3f} "
            f"[{least.get('ci_lower', np.nan):.3f}, {least.get('ci_upper', np.nan):.3f}])"
        )
        lines.append(f"   - States analyzed: {len(df_state)}")
    else:
        lines.append("   - No state-level elasticity data available.")

    # --- Brand-State highlights ---
    lines.append("\n3. BRAND-STATE HIGHLIGHTS")
    df_bs = results.get("brand_state_elasticity")
    if df_bs is not None and len(df_bs) > 0:
        bs_valid = df_bs.dropna(subset=["elasticity"])
        lines.append(f"   - Brand-State segments analyzed: {len(bs_valid)}")
        if len(bs_valid) > 0:
            top3 = bs_valid.nsmallest(3, "elasticity")
            for _, r in top3.iterrows():
                ci_str = ""
                if pd.notna(r.get("ci_lower")) and pd.notna(r.get("ci_upper")):
                    ci_str = f" [{r['ci_lower']:.3f}, {r['ci_upper']:.3f}]"
                lines.append(
                    f"   - {r['brand']} in {r['State']}: "
                    f"elasticity={r['elasticity']:.3f}{ci_str} (highly sensitive)"
                )

    # --- Treatment effects ---
    lines.append("\n4. HETEROGENEOUS TREATMENT EFFECTS")
    did = results.get("did_results")
    if did and "results" in did:
        map_effects = did["results"].get("is_MAP_tire", pd.DataFrame())
        if len(map_effects) > 0:
            for _, r in map_effects.iterrows():
                sig = (
                    "***" if r["p_value"] < 0.001
                    else "**" if r["p_value"] < 0.01
                    else "*" if r["p_value"] < 0.05
                    else ""
                )
                ci_str = ""
                if pd.notna(r.get("ci_lower")) and pd.notna(r.get("ci_upper")):
                    ci_str = f" [{r['ci_lower']:+.4f}, {r['ci_upper']:+.4f}]"
                lines.append(
                    f"   - {r['segment_value']}: ATT={r['ATT']:+.4f}{sig}{ci_str}"
                )
        else:
            lines.append("   - No MAP-status treatment effects available.")
    else:
        lines.append("   - DiD analysis not run.")

    # --- Seasonal ---
    lines.append("\n5. SEASONAL PRICE SENSITIVITY")
    df_seas = results.get("seasonal")
    if df_seas is not None and len(df_seas) > 0:
        overall = df_seas[df_seas["brand"] == "ALL"]
        for _, r in overall.iterrows():
            ci_str = ""
            if pd.notna(r.get("ci_lower")) and pd.notna(r.get("ci_upper")):
                ci_str = f" [{r['ci_lower']:.4f}, {r['ci_upper']:.4f}]"
            lines.append(
                f"   - {r['period']}: overall elasticity={r['elasticity']:.4f}{ci_str}"
            )
    else:
        lines.append("   - Seasonal analysis not run.")

    # --- Strategy highlights ---
    lines.append("\n6. TOP RECOMMENDATIONS")
    df_strat = results.get("strategy")
    if df_strat is not None and len(df_strat) > 0:
        n_increase = (df_strat["action"] == "Increase").sum()
        n_decrease = (df_strat["action"] == "Decrease").sum()
        n_hold = (df_strat["action"] == "Hold").sum()
        lines.append(f"   - {n_increase} segments: increase price")
        lines.append(f"   - {n_decrease} segments: decrease price")
        lines.append(f"   - {n_hold} segments: hold current price")
        high_conf = df_strat[df_strat["confidence"] == "High"]
        lines.append(f"   - High-confidence recommendations: {len(high_conf)}")
    else:
        lines.append("   - Strategy not generated.")

    lines.append("\n" + "=" * 70)
    lines.append("END OF EXTENDED ANALYSIS")
    lines.append("=" * 70)

    text = "\n".join(lines)
    print(text)
    return text


def save_analysis_dataset(
    df: pd.DataFrame,
    output_dir: str,
    end_date: str,
    fmt: str = "parquet",
    fallback_fmt: str = "csv",
) -> str:
    """Save the analysis dataset to disk.

    Parameters
    ----------
    df : DataFrame
        The master analysis DataFrame (with extended features).
    output_dir : str
        Directory to write to (created if needed).
    end_date : str
        Used in the filename.
    fmt : str
        Preferred format ("parquet" or "csv").
    fallback_fmt : str
        Used if the preferred format fails.

    Returns
    -------
    str  Path to the saved file.
    """
    os.makedirs(output_dir, exist_ok=True)
    base = f"correlation_dataset_{end_date}"

    if fmt == "parquet":
        path = os.path.join(output_dir, f"{base}.parquet")
        try:
            df.to_parquet(path, index=False)
            logger.info("Saved analysis dataset: %s", path)
            return path
        except Exception as e:
            logger.warning("Parquet save failed (%s), falling back to %s", e, fallback_fmt)
            fmt = fallback_fmt

    path = os.path.join(output_dir, f"{base}.csv")
    df.to_csv(path, index=False)
    logger.info("Saved analysis dataset: %s", path)
    return path
