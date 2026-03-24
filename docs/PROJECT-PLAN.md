# WalmartPricing NLC Pipeline — Project Plan

## Context

The Walmart B2B Node Level Cost (NLC) pricing system manages pricing for ~100K SKUs across warehouse nodes on the Walmart B2B platform. Unlike Amazon (price per SKU, 4 regions, Contra COGs Excel upload), Walmart uses **per-SKU-Node pricing** — each warehouse gets its own cost, delivered as a DSV CSV upload via the hybris platform.

The system was previously a monolithic 242-cell Jupyter notebook (`original_code/node_level_costs_pricing.ipynb`) that was run semi-automatically: the notebook could be executed end-to-end, but required manual parameter toggling, file management, and hybris upload steps between runs. There was no notification system, no automated validation, and no config-driven parameterization — all values were hardcoded in notebook cells.

**Goal**: Transform the notebook into a modular, config-driven Python pipeline with automated upload, validation, notifications, and diagnostic tools — while preserving the exact NLC computation logic. Then extend with A/B testing infrastructure to experiment with margin parameters and track performance.

---

## Prior State — Original Notebook

The original system (`original_code/node_level_costs_pricing.ipynb`) was a single notebook with:

- **242 cells** (194 code, 48 markdown) containing all logic inline
- Hardcoded file paths, Google Drive IDs, DW table names, and margin thresholds
- Manual toggling of optional sections (rollbacks, national prices, tests) via `(no)` labels in section headers
- No separation between data loading, model computation, business rules, and output generation
- No error handling or notifications — failures required manual inspection
- hybris upload done manually through the browser
- FTP validation run manually ~3 hours after upload
- Tests tracker updated in-notebook with no backup mechanism
- Shared module imports (`pricing_module`, `hybris`) mixed into cell execution order

The notebook was functional but fragile: a mis-ordered cell execution, a wrong parameter, or a silent data issue could cascade through the entire run without detection.

---

## Architecture — What Was Built

```
WalmartPricing/
├── config/                          4 YAML config files (all parameters externalized)
│   ├── data_sources.yaml            8 data source definitions with adapters
│   ├── nlc_model.yaml               NLC computation parameters and thresholds
│   ├── pricing_rules.yaml           5 pricing rules + margin bin categories
│   └── settings.yaml                Drive letter, paths, rate limits
│
├── src/
│   ├── adapters/                    Thin wrappers for external data access
│   │   ├── module_loader.py         Shared module loader with {drive} resolution
│   │   ├── google_api_adapter.py    Google Sheets/Drive API wrapper
│   │   └── dw_adapter.py            Data Warehouse SQL wrapper
│   │
│   ├── data/
│   │   ├── loader.py                Config-driven data source dispatcher
│   │   └── inventory_checker.py     Day-over-day inventory cost comparison
│   │
│   ├── models/
│   │   ├── nlc_model.py             Core NLC model (two-pass, 8 margins, cascade)
│   │   └── run_model.py             Model runner wrapper
│   │
│   ├── rules/
│   │   └── pricing_rules.py         5-rule pricing engine
│   │
│   ├── dsv/
│   │   ├── dsv_builder.py           DSV construction and validation
│   │   ├── hybris_uploader.py       Selenium-based hybris automation
│   │   └── ftp_validator.py         FTP response download and parsing
│   │
│   ├── tracker/
│   │   └── tracker_updater.py       Tests tracker maintenance
│   │
│   ├── notifications/
│   │   └── slack_notifier.py        Step-by-step Slack notifications
│   │
│   └── pipeline.py                  Main orchestrator (run_pipeline, run_ftp_validation)
│
├── notebooks/
│   └── 01_nlc_pricing.ipynb         Interactive orchestrator (mirrors pipeline.py)
│
├── original_code/
│   └── node_level_costs_pricing.ipynb   Reference only — original 242-cell notebook
│
├── tests/                           (placeholder — tests not yet implemented)
├── docs/                            Process overview + this document
└── outputs/                         Local output directory (gitignored)
```

---

## Phase 1 — Core Pipeline Extraction `DONE`

*Commits: `1277cba` through `2d3e787` — 2026-03-18*

Extracted the notebook's logic into modular Python with clean separation of concerns.

### What was done

- **Config externalization** — Moved all hardcoded values (Google Drive IDs, DW table names, margin thresholds, inventory parameters, file paths) into 4 YAML config files. Every parameter that was previously buried in a notebook cell is now in `config/`.

- **Adapter layer** — Created `src/adapters/` to abstract data access. Google Sheets, Google Drive, and Data Warehouse calls go through thin adapter classes. Shared modules (`pricing_module`, `hybris`) use lazy imports to avoid triggering Selenium/OAuth at startup. The `{drive}` placeholder in paths allows the pipeline to run on any machine without path editing.

- **Data loader** — Built `src/data/loader.py` as a config-driven dispatcher. Given a source name from `data_sources.yaml`, it routes to the correct adapter (Google API, DW query, or local file read), applies column renames and dtype specs, and returns a clean DataFrame.

- **NLC model** — Ported the two-pass inventory computation into `src/models/nlc_model.py`. Preserves the exact algorithm: filter inventory by min_units threshold and zero-out threshold, compute NLC at 8 margin levels (6–20%), apply MAP and Walmart margin constraints, cascade to final NLC (11% → 8% → 6% → N/A). The two-pass strategy (min_units=8 first, then min_units=4 for gaps) is preserved exactly.

- **Pricing rules engine** — Extracted the 5 update-type categorization into `src/rules/pricing_rules.py`: Walmart margin split test, brand margin test, low price updates, high price updates, new SKU-Nodes. Protected target exclusion and 1% minimum price change threshold are configurable.

- **DSV builder** — `src/dsv/dsv_builder.py` handles DSV construction: starts from current DSV, optionally applies national price overrides and rollback handling, merges pricing rule updates, validates the result. Rollback handling (removing NLC rows for rollback SKUs, overriding national prices with rollback unit costs) was ported from the notebook's optional sections.

- **Tracker updater** — `src/tracker/tracker_updater.py` refreshes margin columns, appends new/updated entries, deduplicates by (Product Code, Identifier), and creates dated backups on the shared drive before overwriting.

### Key decisions

- **Preserve algorithm exactly** — No optimization or logic changes during extraction. The model produces the same output as the notebook.
- **Config over code** — Business parameters live in YAML, not Python. Changes to thresholds, margin levels, or data sources don't require code changes.
- **Lazy imports** — `hybris` and `pricing_module` are imported only when their steps run, avoiding Selenium browser launch and Google OAuth flows on import.
- **0.5s Google API sleep** — Rate limiting between Google API calls during batch operations to avoid quota exhaustion.

---

## Phase 2 — Orchestration and Interactive Notebook `DONE`

*Commits: `2b0a7c5` through `bcc327e` — 2026-03-18*

### What was done

- **Pipeline orchestrator** — Created `src/pipeline.py` with `run_pipeline(**kwargs)` that executes all steps in sequence with parameter-driven toggling of optional steps. Also exposes `run_ftp_validation(today_str)` for the separate post-upload validation run.

- **Interactive notebook** — Built `notebooks/01_nlc_pricing.ipynb` that mirrors `pipeline.py` with parameter cells at the top. Allows toggling individual steps and running interactively while using the same underlying modules.

- **hybris upload automation** — `src/dsv/hybris_uploader.py` automates the previously manual browser workflow: sign in, navigate to DSV prices, select the WalmartB2B channel, upload the CSV, and poll for completion.

- **FTP validation** — `src/dsv/ftp_validator.py` downloads XML response files from Walmart's FTP, parses ingestion status per line item, generates a summary Excel, and flags failure rates above 1.5%.

- **Local output mode** — Added `test_output` / `local_output` flag to save all files to a local `outputs/` directory instead of the shared drive, enabling safe test runs without overwriting production files.

- **First full run** — Executed the notebook end-to-end and saved cell outputs, validating the pipeline produces correct results against the notebook's original output.

### Key decisions

- **Two entry points** — Both `pipeline.py` (programmatic) and the notebook (interactive) use the same underlying modules. The notebook is for exploration and debugging; the pipeline is for production runs.
- **Optional steps via kwargs** — rollbacks, national prices, hybris upload, and inventory check are all toggled by boolean parameters with sensible defaults.

---

## Phase 3 — Diagnostics and Configurability `DONE`

*Commits: `d8f21d0` through `5a4d251` — 2026-03-19*

### What was done

- **Inventory checker** — Added `src/data/inventory_checker.py` to compare today's inventory costs against the previous run. Computes per-SKU-Warehouse cost deltas and summarizes by brand and vendor (increases and decreases), giving visibility into cost shifts that drive price changes.

- **Configurable shared drive letter** — Parameterized the shared drive letter in `config/settings.yaml` (`shared_drive_letter: "G:"`), with `{drive}` placeholder resolution throughout all path templates. The pipeline can run on any machine by changing one config value.

- **Process overview documentation** — Created `docs/process_overview.md` with detailed step-by-step documentation of the pipeline, parameters, data sources, and business rules.

---

## Phase 4 — Notifications and Production Polish `DONE`

*Commits: `083defd` through `e38ac9f` — 2026-03-20 to 2026-03-23*

### What was done

- **Slack notifications** — Built `src/notifications/slack_notifier.py` with step-by-step Slack posts for the entire pipeline lifecycle: start, inventory check, NLC model, pricing rules, national prices, rollbacks, DSV build, tracker update, save, hybris upload, FTP validation, completion, and errors. Disabled steps are silently skipped (no "skipped" clutter).

- **4-table inventory breakdown** — Slack inventory check notifications show 4 formatted tables: top 5 brand increases, top 5 brand decreases, top 5 vendor increases, top 5 vendor decreases. Provides immediate visibility into what's driving price changes.

- **Smart inventory date comparison** — The inventory checker now compares against the **actual last run date** (read from `last_run.txt`) instead of always comparing to yesterday. Handles gaps between runs correctly.

- **hybris polling tuned** — Changed polling interval from 30 seconds to 5 minutes to avoid disrupting Walmart's processing pipeline. Timeout remains at 1 hour.

- **DSV archive** — After successful hybris upload, the DSV file is automatically copied to the shared drive archive folder using `shutil`.

---

## Current State — Summary

### What the pipeline does today

1. **Loads 8 data sources** from Google Drive, Data Warehouse, and shared drive via config-driven adapters
2. **Computes optimal NLC prices** using a two-pass inventory model with 8 margin levels and cascading logic
3. **Applies 5 pricing rules** (margin split test, margin test, low/high price updates, new nodes)
4. **Generates DSV files** (~3.3M rows) for Walmart upload
5. **Automates hybris upload** via Selenium with 5-minute polling
6. **Validates FTP responses** ~3 hours post-upload, alerts on high failure rates
7. **Updates tests tracker** with per-SKU-Node margin history and dated backups
8. **Posts Slack notifications** at each step with detailed breakdowns
9. **Handles optional workflows**: rollbacks, national price overrides, inventory diagnostics

### What changed from the original notebook

| Aspect | Original Notebook | Current Pipeline |
|--------|-------------------|------------------|
| Structure | 242 cells, all inline | 22 modules across 7 packages |
| Configuration | Hardcoded in cells | 4 YAML config files |
| Data access | Direct API calls in cells | Adapter layer with config dispatch |
| Optional steps | Toggle by `(no)` in headers | Boolean kwargs with defaults |
| hybris upload | Manual browser workflow | Automated Selenium with polling |
| FTP validation | Manual, run separately | Automated with failure alerting |
| Notifications | None | Step-by-step Slack with breakdowns |
| Inventory diagnostics | None | Day-over-day cost comparison |
| Tracker backup | None | Dated backup before overwrite |
| Output flexibility | Shared drive only | Local or shared drive |
| Path portability | Hardcoded drive letters | `{drive}` placeholder resolution |
| Error visibility | Silent failures | Slack error notifications |

---

## Phase 5 — A/B Testing and Parameter Experimentation `PLANNED`

### Objective

Build infrastructure to systematically experiment with NLC pricing parameters — margin levels, Walmart margin split percentages, and other model parameters — across defined groups of SKU-Nodes, then measure and compare performance over time.

### Current A/B testing capabilities

The pipeline already supports two test types:

- **Walmart margin split test** — 3 sub-groups (60% split, 50% split, baseline) applied to SKU-Nodes tagged "Wm margin split test" in the tracker
- **Brand margin test** — 5 sub-groups (11%–15% margin levels) applied to SKU-Nodes tagged "Margin test", filterable by start date

These are functional but static: the groups and parameters were defined once and haven't been iterated on.

### Planned work

**5.1 — New experimental groups**
- Define new test groups beyond the existing two, targeting specific SKU segments (by brand, sales category, node, or margin band)
- Assign experimental margin levels, split percentages, or other parameters to each group
- Ensure proper control groups (baseline) for each experiment

**5.2 — Parameter tuning on existing tests**
- Adjust parameters on the existing Wm margin split test (e.g., try 55% or 65% split instead of 60%/50%)
- Adjust margin levels on the Brand margin test sub-groups
- Track when parameter changes were made for before/after comparison

**5.3 — Performance analysis and reporting**
- Build analysis to compare experimental vs. control group performance over time
- Key metrics: revenue, margin, conversion rate, inventory sell-through
- Time-series tracking to account for seasonality and market shifts
- Automated reporting (Slack or Excel) on experiment outcomes

### Open questions

- What SKU segments should be targeted for new experiments?
- How long should each experiment run before drawing conclusions?
- What metrics define "better performance" for a given experiment?
- Should experiments be managed in the tracker CSV or a separate experiment config?

---

## Git History Reference

| Date | Commit | Phase | Description |
|------|--------|-------|-------------|
| 2026-03-18 | `1277cba` | Phase 1 | Initial scaffold: config, adapters, model, rules, DSV, tracker |
| 2026-03-18 | `2d3e787` | Phase 1 | Refine model logic and data sources to match original notebook |
| 2026-03-18 | `2b0a7c5` | Phase 2 | Add orchestrator notebook with toggleable optional steps |
| 2026-03-18 | `55e9c98` | Phase 2 | Add hybris DSV upload automation |
| 2026-03-18 | `70e3c41` | Phase 2 | Add .gitignore and remove cached pycache files |
| 2026-03-18 | `8d8a24c` | Phase 2 | Fix rollbacks loading: use rollbacks_path parameter |
| 2026-03-18 | `9cb9b93` | Phase 2 | Add local output mode for safe test runs |
| 2026-03-18 | `bcc327e` | Phase 2 | First full end-to-end run with saved outputs |
| 2026-03-19 | `d8f21d0` | Phase 3 | Add pipeline process overview documentation |
| 2026-03-19 | `b60670d` | Phase 3 | Add inventory check step and configurable shared drive letter |
| 2026-03-19 | `5a4d251` | Phase 3 | Fran_1 |
| 2026-03-20 | `083defd` | Phase 4 | Add Slack notifications and silence skipped steps |
| 2026-03-23 | `e38ac9f` | Phase 4 | hybris 5min poll, smart inventory date, 4-table Slack, DSV archive |
