"""Bootstrap and analytical confidence interval utilities.

Every analysis function in ``src/analysis/`` uses these helpers to attach
CI columns to result DataFrames.
"""

import numpy as np
import pandas as pd
from scipy import stats as sp_stats


# ---------------------------------------------------------------------------
# Bootstrap helpers
# ---------------------------------------------------------------------------

def bootstrap_ci(
    data,
    statistic=np.mean,
    n_boot: int = 2000,
    ci_level: float = 0.95,
    seed: int = 42,
    max_sample: int = 50000,
) -> dict:
    """Percentile bootstrap CI for any scalar statistic.

    Parameters
    ----------
    data : array-like
        1-D sample.
    statistic : callable
        Function that returns a scalar from a 1-D array.
    n_boot : int
        Number of bootstrap resamples.
    ci_level : float
        Confidence level (e.g. 0.95 for 95% CI).
    seed : int
        Random seed for reproducibility.
    max_sample : int
        Subsample to this size before bootstrapping (for performance on
        large datasets).  The point estimate is computed on the full data.

    Returns
    -------
    dict with keys ``estimate``, ``ci_lower``, ``ci_upper``, ``se``.
    """
    data = np.asarray(data)
    data = data[~np.isnan(data)]
    if len(data) < 2:
        est = statistic(data) if len(data) == 1 else np.nan
        return {"estimate": est, "ci_lower": np.nan, "ci_upper": np.nan, "se": np.nan}

    rng = np.random.default_rng(seed)

    # Subsample for bootstrap efficiency; point estimate uses full data
    full_estimate = statistic(data)
    if max_sample and len(data) > max_sample:
        data = data[rng.choice(len(data), size=max_sample, replace=False)]

    boot_stats = np.empty(n_boot)
    n = len(data)
    for i in range(n_boot):
        sample = data[rng.integers(0, n, size=n)]
        boot_stats[i] = statistic(sample)

    alpha = 1 - ci_level
    lo = np.nanpercentile(boot_stats, 100 * alpha / 2)
    hi = np.nanpercentile(boot_stats, 100 * (1 - alpha / 2))
    return {
        "estimate": full_estimate,
        "ci_lower": lo,
        "ci_upper": hi,
        "se": np.nanstd(boot_stats, ddof=1),
    }


def bootstrap_correlation_ci(
    x,
    y,
    method: str = "spearman",
    n_boot: int = 2000,
    ci_level: float = 0.95,
    seed: int = 42,
    max_sample: int = 20000,
) -> dict:
    """Bootstrap CI for a correlation coefficient.

    Parameters
    ----------
    max_sample : int
        Subsample to this size before bootstrapping.  The point estimate
        and p-value are computed on the full (non-NaN) data.

    Returns
    -------
    dict with keys ``r``, ``ci_lower``, ``ci_upper``, ``p_value``, ``se``.
    """
    x, y = np.asarray(x, dtype=float), np.asarray(y, dtype=float)
    mask = ~(np.isnan(x) | np.isnan(y))
    x, y = x[mask], y[mask]

    corr_fn = sp_stats.spearmanr if method == "spearman" else sp_stats.pearsonr
    if len(x) < 3:
        return {"r": np.nan, "ci_lower": np.nan, "ci_upper": np.nan,
                "p_value": np.nan, "se": np.nan}

    r_obs, p_obs = corr_fn(x, y)

    rng = np.random.default_rng(seed)

    # Subsample for bootstrap efficiency
    if max_sample and len(x) > max_sample:
        idx_sub = rng.choice(len(x), size=max_sample, replace=False)
        x, y = x[idx_sub], y[idx_sub]

    boot_rs = np.empty(n_boot)
    n = len(x)
    for i in range(n_boot):
        idx = rng.integers(0, n, size=n)
        boot_rs[i] = corr_fn(x[idx], y[idx])[0]

    alpha = 1 - ci_level
    lo = np.nanpercentile(boot_rs, 100 * alpha / 2)
    hi = np.nanpercentile(boot_rs, 100 * (1 - alpha / 2))
    return {
        "r": float(r_obs),
        "ci_lower": float(lo),
        "ci_upper": float(hi),
        "p_value": float(p_obs),
        "se": float(np.nanstd(boot_rs, ddof=1)),
    }


def bootstrap_mean_diff_ci(
    group_a,
    group_b,
    n_boot: int = 2000,
    ci_level: float = 0.95,
    seed: int = 42,
    max_sample: int = 50000,
) -> dict:
    """Bootstrap CI for the difference in means (A - B).

    Also computes rank-biserial effect size with bootstrap CI.

    Parameters
    ----------
    max_sample : int
        Subsample each group to this size before bootstrapping.

    Returns
    -------
    dict with keys ``diff``, ``ci_lower``, ``ci_upper``,
    ``effect_size_r``, ``effect_r_ci_lower``, ``effect_r_ci_upper``.
    """
    a = np.asarray(group_a, dtype=float)
    b = np.asarray(group_b, dtype=float)
    a, b = a[~np.isnan(a)], b[~np.isnan(b)]

    if len(a) < 2 or len(b) < 2:
        return {"diff": np.nan, "ci_lower": np.nan, "ci_upper": np.nan,
                "effect_size_r": np.nan, "effect_r_ci_lower": np.nan,
                "effect_r_ci_upper": np.nan}

    obs_diff = a.mean() - b.mean()
    U, _ = sp_stats.mannwhitneyu(a, b, alternative="two-sided")
    obs_r = rank_biserial_effect_size(U, len(a), len(b))

    rng = np.random.default_rng(seed)

    # Subsample for bootstrap efficiency
    if max_sample:
        if len(a) > max_sample:
            a = a[rng.choice(len(a), size=max_sample, replace=False)]
        if len(b) > max_sample:
            b = b[rng.choice(len(b), size=max_sample, replace=False)]

    # Bootstrap only the mean diff (fast); effect size CI via separate
    # lightweight bootstrap that avoids mannwhitneyu per iteration
    boot_diffs = np.empty(n_boot)
    for i in range(n_boot):
        sa = a[rng.integers(0, len(a), size=len(a))]
        sb = b[rng.integers(0, len(b), size=len(b))]
        boot_diffs[i] = sa.mean() - sb.mean()

    # Effect size CI: bootstrap rank-biserial via fast U approximation
    # U = sum of ranks trick: instead of mannwhitneyu, use the fast
    # formula U = n1*n2 + n1*(n1+1)/2 - R1 where R1 = sum of ranks of a
    na, nb = len(a), len(b)
    boot_rs = np.empty(min(n_boot, 500))  # fewer iterations for effect size
    combined = np.empty(na + nb)
    for i in range(len(boot_rs)):
        sa = a[rng.integers(0, na, size=na)]
        sb = b[rng.integers(0, nb, size=nb)]
        combined[:na] = sa
        combined[na:] = sb
        ranks = sp_stats.rankdata(combined)
        u = na * nb + na * (na + 1) / 2 - ranks[:na].sum()
        boot_rs[i] = rank_biserial_effect_size(u, na, nb)

    alpha = 1 - ci_level
    return {
        "diff": float(obs_diff),
        "ci_lower": float(np.nanpercentile(boot_diffs, 100 * alpha / 2)),
        "ci_upper": float(np.nanpercentile(boot_diffs, 100 * (1 - alpha / 2))),
        "effect_size_r": float(obs_r),
        "effect_r_ci_lower": float(np.nanpercentile(boot_rs, 100 * alpha / 2)),
        "effect_r_ci_upper": float(np.nanpercentile(boot_rs, 100 * (1 - alpha / 2))),
    }


# ---------------------------------------------------------------------------
# Effect size
# ---------------------------------------------------------------------------

def rank_biserial_effect_size(U: float, n1: int, n2: int) -> float:
    """Rank-biserial correlation *r* from Mann-Whitney U.

    r = 1 - (2U)/(n1*n2).  Range [-1, 1].
    """
    denom = n1 * n2
    if denom == 0:
        return np.nan
    return 1 - (2 * U) / denom


# ---------------------------------------------------------------------------
# Analytical CIs (OLS / delta method)
# ---------------------------------------------------------------------------

def ols_ci_df(model, ci_level: float = 0.95) -> pd.DataFrame:
    """Extract OLS coefficients with CIs into a tidy DataFrame.

    Parameters
    ----------
    model : statsmodels OLSResults
    ci_level : float

    Returns
    -------
    DataFrame with columns
        ``variable``, ``coef``, ``se``, ``ci_lower``, ``ci_upper``, ``p_value``.
    """
    ci = model.conf_int(alpha=1 - ci_level)
    return pd.DataFrame({
        "variable": model.params.index,
        "coef": model.params.values,
        "se": model.bse.values,
        "ci_lower": ci.iloc[:, 0].values,
        "ci_upper": ci.iloc[:, 1].values,
        "p_value": model.pvalues.values,
    })


def delta_method_ci(
    params,
    vcov,
    ci_level: float = 0.95,
) -> dict:
    """Delta-method CI for the vertex of a quadratic  y = a + b*x + c*x^2.

    The vertex is  x* = -b / (2c)  where ``params = [a, b, c]``
    and ``vcov`` is the 3x3 variance-covariance matrix from OLS.

    The gradient of g(b, c) = -b/(2c) is:
        dg/db = -1/(2c)
        dg/dc =  b/(2c^2)

    Returns
    -------
    dict with keys ``optimal_x``, ``ci_lower``, ``ci_upper``, ``se``.
    """
    params = np.asarray(params, dtype=float)
    vcov = np.asarray(vcov, dtype=float)

    b, c = params[1], params[2]
    if c == 0:
        return {"optimal_x": np.nan, "ci_lower": np.nan,
                "ci_upper": np.nan, "se": np.nan}

    x_star = -b / (2 * c)

    # Gradient w.r.t. [b, c]
    grad = np.array([-1 / (2 * c), b / (2 * c**2)])

    # Subset of vcov for [b, c] (indices 1, 2)
    V = vcov[np.ix_([1, 2], [1, 2])]
    var_x_star = grad @ V @ grad
    se = np.sqrt(max(var_x_star, 0))

    z = sp_stats.norm.ppf(1 - (1 - ci_level) / 2)
    return {
        "optimal_x": float(x_star),
        "ci_lower": float(x_star - z * se),
        "ci_upper": float(x_star + z * se),
        "se": float(se),
    }
