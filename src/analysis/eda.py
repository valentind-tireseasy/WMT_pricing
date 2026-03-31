"""Exploratory Data Analysis module for the correlation analysis pipeline.

Extracts and refactors cells 35-40 from the original notebook into
reusable functions.  All confidence intervals come from
``src.analysis.ci_utils``; all plots are rendered via helpers in
``src.analysis.plot_utils``.
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from src.analysis import ci_utils, plot_utils


# ---------------------------------------------------------------------------
# Correlation matrix with bootstrap CIs
# ---------------------------------------------------------------------------

def compute_correlation_matrix(
    df,
    method="spearman",
    target_col="qty_sold",
    exclude_cols=None,
    n_boot=2000,
    ci_level=0.95,
    seed=42,
):
    """Compute a full correlation matrix and per-feature bootstrap CIs
    against *target_col*.

    Parameters
    ----------
    df : pd.DataFrame
    method : str
        ``"spearman"`` or ``"pearson"``.
    target_col : str
        Column to compute pairwise correlations against.
    exclude_cols : list[str] | None
        Columns to drop before computing correlations.
    n_boot : int
        Bootstrap resamples for CI estimation.
    ci_level : float
        Confidence level (e.g. 0.95).
    seed : int
        Random seed for reproducibility.

    Returns
    -------
    dict
        ``"corr_matrix"`` : pd.DataFrame – full correlation matrix.
        ``"target_corrs"`` : pd.DataFrame – columns
        ``[feature, r, ci_lower, ci_upper, p_value]`` sorted by ``|r|``
        descending.
    """
    exclude_cols = set(exclude_cols or [])
    numeric_df = df.select_dtypes(include="number").drop(
        columns=[c for c in exclude_cols if c in df.columns], errors="ignore"
    )

    # Full correlation matrix
    corr_matrix = numeric_df.corr(method=method)

    # Per-feature bootstrap CIs against target_col
    if target_col not in numeric_df.columns:
        raise ValueError(
            f"target_col '{target_col}' not found in numeric columns"
        )

    target_data = numeric_df[target_col].values
    records = []
    features = [c for c in numeric_df.columns if c != target_col]
    # Use fewer bootstrap samples for the full matrix scan (many features)
    matrix_n_boot = min(n_boot, 500)
    for feat in features:
        feat_data = numeric_df[feat].values
        ci = ci_utils.bootstrap_correlation_ci(
            feat_data,
            target_data,
            method=method,
            n_boot=matrix_n_boot,
            max_sample=10000,
            ci_level=ci_level,
            seed=seed,
        )
        records.append(
            {
                "feature": feat,
                "r": ci["r"],
                "ci_lower": ci["ci_lower"],
                "ci_upper": ci["ci_upper"],
                "p_value": ci["p_value"],
            }
        )

    target_corrs = (
        pd.DataFrame(records)
        .assign(abs_r=lambda d: d["r"].abs())
        .sort_values("abs_r", ascending=False)
        .drop(columns="abs_r")
        .reset_index(drop=True)
    )

    return {"corr_matrix": corr_matrix, "target_corrs": target_corrs}


# ---------------------------------------------------------------------------
# Heatmap + top-N bar chart
# ---------------------------------------------------------------------------

def plot_correlation_heatmap(corr_result, top_n=15):
    """Draw a two-panel correlation summary.

    Parameters
    ----------
    corr_result : dict
        Output of :func:`compute_correlation_matrix`.
    top_n : int
        Number of features to show in the bar chart.

    Returns
    -------
    tuple[matplotlib.figure.Figure, matplotlib.figure.Figure]
        The heatmap figure and the bar-chart figure.
    """
    corr_matrix = corr_result["corr_matrix"]
    target_corrs = corr_result["target_corrs"]

    # --- Panel 1: lower-triangle Spearman heatmap ---
    mask = np.triu(np.ones_like(corr_matrix, dtype=bool))
    fig1, ax1 = plt.subplots(figsize=(18, 15))
    sns.heatmap(
        corr_matrix,
        mask=mask,
        annot=True,
        fmt=".2f",
        cmap="RdBu_r",
        center=0,
        linewidths=0.5,
        ax=ax1,
        cbar_kws={"label": "Correlation"},
    )
    ax1.set_title("Spearman Correlation Matrix (lower triangle)", fontsize=14)
    fig1.tight_layout()

    # --- Panel 2: top-N bar chart with CI error bars ---
    top_df = target_corrs.head(top_n).copy()
    fig2, ax2 = plt.subplots(
        figsize=(12, max(6, top_n * 0.4))
    )
    plot_utils.bar_chart_with_ci(
        top_df,
        x_col="feature",
        y_col="r",
        ci_lower_col="ci_lower",
        ci_upper_col="ci_upper",
        title=f"Top {top_n} Features Correlated with Target (with 95% CI)",
        xlabel="Spearman r",
        horizontal=True,
        color_by_sign=True,
        ax=ax2,
    )
    fig2.tight_layout()

    return fig1, fig2


# ---------------------------------------------------------------------------
# Scatter plots with regression CI
# ---------------------------------------------------------------------------

def plot_scatter_with_ci(
    df,
    features,
    target_col="qty_sold",
    sample_n=10000,
    n_boot=2000,
    ci_level=0.95,
    seed=42,
):
    """Scatter plot per feature against *target_col* with bootstrap CI
    annotation.

    Parameters
    ----------
    df : pd.DataFrame
    features : list[str]
        Feature columns to plot.
    target_col : str
    sample_n : int
        Max rows to plot (sampled from non-zero target rows).
    n_boot : int
        Bootstrap resamples for the correlation CI.
    ci_level : float
    seed : int

    Returns
    -------
    list[matplotlib.figure.Figure]
        One figure per feature.
    """
    figs = []
    for feat in features:
        sub = df[[feat, target_col]].dropna()
        sub = sub[sub[target_col] > 0]

        # Sample if needed
        if len(sub) > sample_n:
            sub = sub.sample(n=sample_n, random_state=seed)

        # Bootstrap correlation CI
        corr_info = ci_utils.bootstrap_correlation_ci(
            sub[feat].values,
            sub[target_col].values,
            method="spearman",
            n_boot=n_boot,
            ci_level=ci_level,
            seed=seed,
        )

        fig, ax = plt.subplots(figsize=(10, 6))
        plot_utils.scatter_with_regression_ci(
            sub[feat].values,
            sub[target_col].values,
            ci_level=ci_level,
            title=f"{feat} vs {target_col}",
            xlabel=feat,
            ylabel=target_col,
            corr_info=corr_info,
            ax=ax,
            sample_n=sample_n,
            seed=seed,
        )
        fig.tight_layout()
        figs.append(fig)

    return figs


# ---------------------------------------------------------------------------
# Distribution plots
# ---------------------------------------------------------------------------

def plot_distributions(df, columns):
    """Histogram + KDE for each column, annotated with bootstrap CI of
    the mean.

    Parameters
    ----------
    df : pd.DataFrame
    columns : list[str]
        Columns to plot.

    Returns
    -------
    list[matplotlib.figure.Figure]
        One figure per column.
    """
    figs = []
    for col in columns:
        series = df[col].dropna()

        # For qty_sold, filter to positive values only
        if col == "qty_sold":
            series = series[series > 0]

        if len(series) < 2:
            continue

        # Bootstrap CI of the mean
        ci = ci_utils.bootstrap_ci(series.values, statistic=np.mean)

        fig, ax = plt.subplots(figsize=(10, 6))
        ax.hist(series, bins=50, density=True, alpha=0.6, color="steelblue",
                edgecolor="white", label="Histogram")
        series.plot.kde(ax=ax, color="navy", linewidth=2, label="KDE")

        # Annotate mean + CI
        annotation = (
            f"Mean: {ci['estimate']:.4f}\n"
            f"95% CI: [{ci['ci_lower']:.4f}, {ci['ci_upper']:.4f}]\n"
            f"SE: {ci['se']:.4f}\n"
            f"n = {len(series):,}"
        )
        ax.text(
            0.95, 0.95, annotation, transform=ax.transAxes, va="top",
            ha="right", fontsize=10,
            bbox=dict(facecolor="white", alpha=0.8, edgecolor="gray"),
        )

        ax.set_title(f"Distribution of {col}", fontsize=13)
        ax.set_xlabel(col, fontsize=11)
        ax.set_ylabel("Density", fontsize=11)
        ax.legend(loc="upper left")
        fig.tight_layout()
        figs.append(fig)

    return figs
