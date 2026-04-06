# How to Add New Test Groups / Recurrent Update Types

## Purpose

This guide explains how to introduce a new pricing test group (like "DSVD test" or "Increase test") into the WalmartPricing pipeline. It is written for a technical user who works with the live notebook and needs to mirror those changes in the modular project code.

When a new test group is added in the notebook, 4 files in the project need to be updated. This document walks through each one.

---

## Overview: What makes a test group work

A test group is a label stored in the `Final target` column of the tests tracker (e.g., "Margin test", "DSVD test", "Increase test"). Each pipeline run:

1. Loads the tracker and merges `Final target` + `Sub-group` onto the NLC model output
2. Applies **recurrent update logic** for each group (computes new prices based on group-specific rules)
3. Excludes those tagged SKU-Nodes from the regular low/high price updates
4. Includes the group's DSV updates in the final DSV file
5. Includes the group's tracker updates in the tracker save

To add a new group, you touch these 4 files:

| # | File | What to change |
|---|------|----------------|
| 1 | `config/nlc_model.yaml` | Add the group name to `dont_update_targets` |
| 2 | `src/rules/pricing_rules.py` | Add a new method that computes prices for the group |
| 3 | `src/pipeline.py` | Call the new method, wire its outputs into DSV + tracker |
| 4 | `config/pricing_rules.yaml` | (Optional) Add config entries if the group has configurable parameters |

---

## Step 1: Add to the exclusion list

**File:** `config/nlc_model.yaml`

**Section:** `dont_update_targets` (near the bottom)

```yaml
dont_update_targets:
  - "Margin test"
  - "Wm margin split test"
  - "Shipping cost added"
  - "DSVD test"
  - "Increase test"
  - "YOUR NEW GROUP"       # <-- add here
```

**Why:** This list tells the low price updates and high price updates methods to skip SKU-Nodes that belong to a test group. Without this, those SKU-Nodes would get repriced by the regular update logic AND by the test logic, causing conflicts.

**No code changes needed** — `pricing_rules.py` reads this list from config at runtime.

---

## Step 2: Add the pricing method

**File:** `src/rules/pricing_rules.py`

**Where:** Add a new method to the `PricingRulesEngine` class, before `get_new_sku_nodes()`.

### Template

```python
def get_your_group_updates(self) -> tuple:
    """Get updates for SKU-Nodes in [your group name].

    [Describe the pricing logic in plain English]

    Returns:
        (df_dsv, df_tracker) — DSV-format updates and tracker updates
    """
    # 1. Filter to rows tagged with your group
    df = self.df_output[
        self.df_output["Final target"] == "Your group name"
    ].copy()

    if len(df) == 0:
        logger.info("No [your group] SKU-Nodes found.")
        return pd.DataFrame(), pd.DataFrame()

    # 2. Compute the new Price based on sub-group logic
    #    Use np.where() for conditional pricing, e.g.:
    df["Price"] = np.where(
        df["Sub-group"] == "Sub A",
        df["Final node level cost"],           # Sub A gets the NLC price
        df["current_nlc_price"],               # Others keep current price
    )

    # 3. Compute price change % and category (always the same pattern)
    df["Price change %"] = round(
        (df["Price"] - df["current_nlc_price"]) / df["current_nlc_price"], 4
    )
    df["Price change category"] = np.where(
        df["Price change %"] < 0,
        "Decrease",
        np.where(df["Price change %"] > 0, "Increase", "No change"),
    )

    # 4. Filter to meaningful changes (|delta| >= 1%)
    df_update = df[abs(df["Price change %"]) >= self.min_price_change_pct].copy()

    logger.info("[Your group] updates: %d SKU-Nodes", len(df_update))

    # 5. Build DSV format (always the same 4 columns)
    df_dsv = df_update[
        ["Product Code", "Identifier", "Price", "SKU-Node"]
    ].rename(columns={"Product Code": "SKU", "Identifier": "Source"})

    # 6. Build tracker format (always the same pattern)
    df_tracker = df_update[["SKU-Node"]].merge(
        self.df_current_tests, on="SKU-Node", how="left"
    )
    df_tracker["Last price update"] = self.today_str

    return df_dsv, df_tracker
```

### If the method needs external data

If your group needs data that isn't already in `df_output` (e.g., DSVD test needs shipping costs from an external Excel file), add the external data as a parameter:

```python
def get_your_group_updates(self, df_external: pd.DataFrame) -> tuple:
    ...
    df = df.merge(df_external, how="left", on="Identifier")
    ...
```

The loading of that external data happens in `pipeline.py` (Step 3).

---

## Step 3: Wire into the pipeline

**File:** `src/pipeline.py`

Three places need changes:

### 3a. Call the new method (Step 3 — Pricing Rules section)

Find the block where the other `engine.get_*` calls are made. Add yours:

```python
df_yours_dsv, df_yours_tracker = engine.get_your_group_updates()
```

If your method needs external data, load it here and pass it in:

```python
# Add a pipeline parameter: your_data_path: str = None
if your_data_path:
    df_external = pd.read_excel(your_data_path, ...)
    df_yours_dsv, df_yours_tracker = engine.get_your_group_updates(df_external)
else:
    df_yours_dsv, df_yours_tracker = pd.DataFrame(), pd.DataFrame()
```

### 3b. Add to the DSV update list (Step 4 — Build DSV section)

Find `list_dsv_updates = [...]` and add your DSV DataFrame:

```python
list_dsv_updates = [
    df_incr_dsv, df_dsvd_dsv,
    df_wm_split_dsv, df_margin_dsv,
    df_low_dsv, df_high_dsv,
    df_yours_dsv,                    # <-- add here
]
```

### 3c. Add to the tracker append list (Step 5 — Update Tracker section)

Find `updater.append_entries([...])` and add your tracker DataFrame:

```python
updater.append_entries([
    df_new_tracker,
    df_low_tracker,
    df_high_tracker,
    df_wm_split_tracker,
    df_margin_tracker,
    df_dsvd_tracker,
    df_incr_tracker,
    df_yours_tracker,                # <-- add here
])
```

### 3d. Update logging and Slack (optional but recommended)

Add your group to the `slack.notify_pricing_rules()` dict and the summary logger lines:

```python
slack.notify_pricing_rules({
    ...
    "Your group": len(df_yours_dsv),
})
```

And in the summary dict:

```python
summary = {
    ...
    "your_group": len(df_yours_dsv),
}
```

---

## Step 4 (Optional): Add config entries

**File:** `config/pricing_rules.yaml`

If your group has parameters that might change (thresholds, column names, sub-group labels), add them under the `rules:` section:

```yaml
rules:
  your_group:
    target_label: "Your group name"
    description: "What this group does"
    some_threshold: 0.06
```

Then read them in your method:

```python
cfg = self._config["rules"]["your_group"]
threshold = cfg["some_threshold"]
```

---

## Checklist

When adding a new test group, verify:

- [ ] Group name added to `dont_update_targets` in `config/nlc_model.yaml`
- [ ] New method added to `PricingRulesEngine` in `src/rules/pricing_rules.py`
- [ ] Method called in `src/pipeline.py` Step 3
- [ ] DSV DataFrame added to `list_dsv_updates` in `src/pipeline.py` Step 4
- [ ] Tracker DataFrame added to `updater.append_entries()` in `src/pipeline.py` Step 5
- [ ] Logging and Slack updated in `src/pipeline.py`
- [ ] If external data needed: pipeline parameter added, loading function created
- [ ] Test with `run_pipeline(save=False)` to confirm no errors

---

## Real examples

### Example A: Price Increase test (simple — no external data)

- **Group name:** `"Increase test"`
- **Logic:** If Sub-group is "Increased" or margin < 6%, use Final node level cost; else keep current price
- **External data:** None
- **Method:** `get_price_increase_test_updates()` in `pricing_rules.py`
- **Pipeline wiring:** Direct call, no conditional

### Example B: DSVD test (needs external Excel file)

- **Group name:** `"DSVD test"`
- **Logic:** "No shipping" sub-group gets NLC price; "Shipping cost added" gets NLC + DSVD shipping cost; others keep current
- **External data:** DSVD cost test Excel with shipping costs per node
- **Method:** `get_dsvd_test_updates(df_dsvd_test)` in `pricing_rules.py`
- **Pipeline wiring:** `dsvd_test_path` parameter, `_load_dsvd_test_data()` helper, conditional call
