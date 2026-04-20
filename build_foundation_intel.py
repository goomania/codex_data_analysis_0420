#!/usr/bin/env python3
"""Backward-compatible entrypoint for building `founation_intel.csv`."""

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from foundation_intel.build_dataset import main  # noqa: E402

if __name__ == "__main__":
    main()
