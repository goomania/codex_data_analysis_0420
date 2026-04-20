# Foundation Intelligence Dataset Builder

This repository builds a structured grants-intelligence dataset from IRS Form 990-PF XML filings.

## What it does

- Downloads IRS TEOS index files for selected years.
- Filters filings to `990PF` foundations.
- Downloads IRS XML batch ZIP files.
- Parses `GrantOrContributionPdDurYrGrp` entries.
- Keeps grants with higher-education signals in recipient names and/or grant purposes.
- Exports a CSV dataset for prospect research.

## Repository layout

- `src/foundation_intel/build_dataset.py` — main pipeline and CLI.
- `build_foundation_intel.py` — compatibility entrypoint.
- `founation_intel.csv` — generated dataset output.
- `tests/test_parse_qualifying_grants.py` — parser-focused unit tests.

## Quickstart

```bash
python build_foundation_intel.py
```

Generate to a custom path and size:

```bash
python build_foundation_intel.py --output data/founation_intel.csv --target-foundations 1000 --years 2026 2025 2024
```

## Output schema

`founation_intel.csv` contains:

- `foundation_ein`, `foundation_name`
- filing metadata (`filing_tax_period`, `index_year`, `submission_year`, `return_type`, `object_id`, `xml_batch_id`)
- recipient fields (`grant_recipient_name`, address components, relationship/status)
- grant fields (`grant_amount_usd`, `grant_purpose`, `higher_ed_match_basis`)

## Notes

- The output file name intentionally follows the original request spelling: `founation_intel.csv`.
- Matching uses keyword heuristics; review records before external outreach.
