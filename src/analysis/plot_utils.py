"""Shared CI-aware plotting utilities.

Every plot function accepts pre-computed results (DataFrames / dicts)
with CI columns and renders error bars, shaded bands, or annotations.
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


# ---------------------------------------------------------------------------
# Bar charts
# ---------------------------------------------------------------------------

def bar_chart_with_ci(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    ci_lower_col: str,
    ci_upper_col: str,
    *,
    title: str = "",
    xlabel: str = "",
    ylabel: str = "",
    ax=None,
    horizontal: bool = False,
    color_by_sign: bool = False,
    color_positive: str = "#2ecc71",
    color_negative: str = "#e74c3c",
    color_default: str = "steelblue",
    alpha: float = 0.8,
) -> plt.Axes:
    """Bar chart with asymmetric CI error bars.

    Parameters
    ----------
    df : DataFrame
        Must contain *x_col*, *y_col*, *ci_lower_col*, *ci_upper_col*.
    color_by_sign : bool
        If True, bars with negative *y_col* are red, positive are green.

    Returns
    -------
    matplotlib Axes
    """
    if ax is None:
        _, ax = plt.subplots(figsize=(12, max(6, len(df) * 0.4) if horizontal else 6))

    vals = df[y_col].values
    lo = vals - df[ci_lower_col].values   # distance below
    hi = df[ci_upper_col].values - vals   # distance above
    yerr = np.array([np.abs(lo), np.abs(hi)])

    if color_by_sign:
        colors = [color_negative if v < 0 else color_positive for v in vals]
    else:
        colors = color_default

    if horizontal:
        ax.barh(df[x_col], vals, xerr=yerr, color=colors, alpha=alpha,
                capsize=3, ecolor="gray")
        ax.axvline(0, color="black", linewidth=0.5)
        if xlabel:
            ax.set_xlabel(xlabel)
    else:
        ax.bar(df[x_col], vals, yerr=yerr, color=colors, alpha=alpha,
               capsize=3, ecolor="gray")
        ax.axhline(0, color="black", linewidth=0.5)
        if ylabel:
            ax.set_ylabel(ylabel)
        ax.tick_params(axis="x", rotation=45)

    if title:
        ax.set_title(title)
    return ax


# ---------------------------------------------------------------------------
# Scatter + regression
# ---------------------------------------------------------------------------

def scatter_with_regression_ci(
    x,
    y,
    *,
    ci_level: float = 0.95,
    title: str = "",
    xlabel: str = "",
    ylabel: str = "",
    corr_info: dict = None,
    ax=None,
    sample_n: int = 10000,
    seed: int = 42,
) -> plt.Axes:
    """Scatter plot with OLS regression line and CI band.

    Parameters
    ----------
    corr_info : dict, optional
        If provided, annotates the plot with r, CI, and p-value.
        Expected keys: ``r``, ``ci_lower``, ``ci_upper``, ``p_value``.
    sample_n : int
        Max points to plot (random sample for performance).
    """
    if ax is None:
        _, ax = plt.subplots(figsize=(10, 6))

    x, y = np.asarray(x, dtype=float), np.asarray(y, dtype=float)
    mask = ~(np.isnan(x) | np.isnan(y))
    x, y = x[mask], y[mask]

    # Subsample
    if len(x) > sample_n:
        rng = np.random.default_rng(seed)
        idx = rng.choice(len(x), size=sample_n, replace=False)
        x_plot, y_plot = x[idx], y[idx]
    else:
        x_plot, y_plot = x, y

    sns.regplot(
        x=x_plot, y=y_plot, ax=ax, ci=int(ci_level * 100),
        scatter_kws={"alpha": 0.2, "s": 8},
        line_kws={"color": "red"},
    )

    if corr_info:
        txt = (f"r={corr_info['r']:.3f} "
               f"[{corr_info['ci_lower']:.3f}, {corr_info['ci_upper']:.3f}]"
               f"\np={corr_info['p_value']:.2e}")
        ax.text(0.05, 0.95, txt, transform=ax.transAxes, va="top",
                fontsize=10, bbox=dict(facecolor="white", alpha=0.8))

    if title:
        ax.set_title(title, fontsize=13)
    if xlabel:
        ax.set_xlabel(xlabel, fontsize=11)
    if ylabel:
        ax.set_ylabel(ylabel, fontsize=11)
    return ax


# ---------------------------------------------------------------------------
# Line + CI band
# ---------------------------------------------------------------------------

def line_with_ci_band(
    x,
    y,
    ci_lower,
    ci_upper,
    *,
    ax=None,
    label: str = "",
    color=None,
    linewidth: float = 2,
    alpha_band: float = 0.2,
) -> plt.Axes:
    """Line plot with shaded CI band."""
    if ax is None:
        _, ax = plt.subplots(figsize=(10, 6))

    line, = ax.plot(x, y, marker="o", label=label, color=color,
                    linewidth=linewidth)
    ax.fill_between(x, ci_lower, ci_upper, alpha=alpha_band,
                    color=line.get_color())
    return ax


# ---------------------------------------------------------------------------
# Coefficient forest plot
# ---------------------------------------------------------------------------

def coefficient_ci_plot(
    df_coefs: pd.DataFrame,
    *,
    title: str = "OLS Coefficients with 95% CI",
    ax=None,
) -> plt.Axes:
    """Horizontal forest plot of regression coefficients with CI whiskers.

    Parameters
    ----------
    df_coefs : DataFrame
        Columns: ``variable``, ``coef``, ``ci_lower``, ``ci_upper``.
    """
    if ax is None:
        _, ax = plt.subplots(figsize=(10, max(4, len(df_coefs) * 0.4)))

    # Exclude intercept for readability
    df_plot = df_coefs[df_coefs["variable"] != "const"].copy()
    df_plot = df_plot.sort_values("coef")

    y_pos = range(len(df_plot))
    xerr_lo = df_plot["coef"].values - df_plot["ci_lower"].values
    xerr_hi = df_plot["ci_upper"].values - df_plot["coef"].values

    ax.barh(y_pos, df_plot["coef"], xerr=[xerr_lo, xerr_hi],
            color="steelblue", alpha=0.7, capsize=3, ecolor="gray")
    ax.set_yticks(list(y_pos))
    ax.set_yticklabels(df_plot["variable"])
    ax.axvline(0, color="black", linewidth=0.8, linestyle="--")
    ax.set_title(title)
    ax.set_xlabel("Coefficient")
    return ax


# ---------------------------------------------------------------------------
# Heatmap with CI text
# ---------------------------------------------------------------------------

def heatmap_with_ci_annotation(
    values: pd.DataFrame,
    ci_lower: pd.DataFrame = None,
    ci_upper: pd.DataFrame = None,
    *,
    title: str = "",
    cmap: str = "RdYlGn",
    center: float = 0,
    fmt: str = ".2f",
    ax=None,
) -> plt.Axes:
    """Heatmap where each cell shows ``value [lo, hi]`` when CIs are given."""
    if ax is None:
        _, ax = plt.subplots(figsize=(16, max(6, len(values) * 0.5)))

    if ci_lower is not None and ci_upper is not None:
        # Build annotation strings
        annot = values.copy().astype(str)
        for r in values.index:
            for c in values.columns:
                v = values.loc[r, c]
                lo = ci_lower.loc[r, c] if r in ci_lower.index and c in ci_lower.columns else np.nan
                hi = ci_upper.loc[r, c] if r in ci_upper.index and c in ci_upper.columns else np.nan
                if pd.notna(v):
                    if pd.notna(lo) and pd.notna(hi):
                        annot.loc[r, c] = f"{v:{fmt[1:]}}\n[{lo:{fmt[1:]}},{hi:{fmt[1:]}}]"
                    else:
                        annot.loc[r, c] = f"{v:{fmt[1:]}}"
                else:
                    annot.loc[r, c] = ""
        sns.heatmap(values, annot=annot, fmt="", cmap=cmap, center=center,
                    linewidths=0.5, ax=ax, cbar_kws={"label": "Value"},
                    annot_kws={"size": 7})
    else:
        sns.heatmap(values, annot=True, fmt=fmt, cmap=cmap, center=center,
                    linewidths=0.5, ax=ax, cbar_kws={"label": "Value"})

    if title:
        ax.set_title(title, fontsize=14)
    return ax
