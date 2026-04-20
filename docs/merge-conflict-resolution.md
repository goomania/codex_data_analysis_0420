# Resolving PR merge conflict (`build_foundation_intel.py`)

This PR conflicts on `build_foundation_intel.py` because both branches changed the same file.

## Recommended resolution

Keep `build_foundation_intel.py` as a **small compatibility launcher** and keep real logic in `src/foundation_intel/build_dataset.py`.

### Final desired file content

```python
#!/usr/bin/env python3
"""Backward-compatible entrypoint for building `founation_intel.csv`."""

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from foundation_intel.build_dataset import main  # noqa: E402

if __name__ == "__main__":
    main()
```

## CLI resolution steps

```bash
git checkout codex/collect-foundation-grant-data-for-higher-education
git fetch origin
git merge origin/main
# resolve conflict by replacing build_foundation_intel.py with the content above
git add build_foundation_intel.py
git commit -m "Resolve merge conflict in build_foundation_intel.py"
git push
```

After push, GitHub should allow merge if status checks pass.

## GitHub web UI resolution

1. Open PR → **Resolve conflicts**.
2. For `build_foundation_intel.py`, keep the compatibility launcher version above.
3. Mark resolved → Commit merge.
4. Re-run checks if prompted.

## Validation commands after resolving

```bash
pytest -q
python build_foundation_intel.py --target-foundations 25 --output data/smoke_test.csv
```

These confirm parser tests and launcher wiring are still good.
