"""Build a higher-education grant intelligence dataset from IRS 990-PF XML filings."""

from __future__ import annotations

import argparse
import csv
import io
import re
import sys
import urllib.request
import zipfile
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List

DEFAULT_YEARS = [2026, 2025, 2024]
DEFAULT_TARGET_FOUNDATIONS = 1000
DEFAULT_OUTPUT_CSV = "founation_intel.csv"
NS = {"irs": "http://www.irs.gov/efile"}

HIGHER_ED_PATTERN = re.compile(
    r"\b(university|college|institute of technology|polytechnic|community college|school of|law school|medical school|state university|seminary|higher education)\b",
    re.IGNORECASE,
)
PURPOSE_PATTERN = re.compile(
    r"\b(scholarship|fellowship|tuition|higher education|college|university|undergraduate|graduate|academic)\b",
    re.IGNORECASE,
)


def fetch_index_rows(year: int) -> Iterable[Dict[str, str]]:
    """Yield 990-PF rows from the IRS TEOS index CSV for a given year."""
    url = f"https://apps.irs.gov/pub/epostcard/990/xml/{year}/index_{year}.csv"
    print(f"Loading index {year}: {url}", file=sys.stderr)
    try:
        with urllib.request.urlopen(url, timeout=60) as response:
            text_stream = io.TextIOWrapper(response, encoding="utf-8", errors="replace")
            reader = csv.DictReader(text_stream)
            for row in reader:
                if (row.get("RETURN_TYPE") or "").strip().upper() == "990PF":
                    yield row
    except Exception as exc:  # noqa: BLE001 - resilience to remote/source errors
        print(f"  Skipping {year} ({exc})", file=sys.stderr)


def text_or_empty(node: ET.Element, path: str) -> str:
    element = node.find(path, NS)
    return (element.text or "").strip() if element is not None and element.text else ""


def first_nonempty(*values: str) -> str:
    for value in values:
        if value:
            return value
    return ""


def parse_qualifying_grants(xml_bytes: bytes) -> List[Dict[str, str]]:
    """Extract grant rows with higher-education signals from one 990-PF XML payload."""
    lowered = xml_bytes.lower()
    if b"grantorcontributionpdduryrgrp" not in lowered:
        return []

    if not any(
        token in lowered
        for token in [b"university", b"college", b"scholar", b"higher education", b"institute"]
    ):
        return []

    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return []

    grants: List[Dict[str, str]] = []
    for group in root.findall(".//irs:GrantOrContributionPdDurYrGrp", NS):
        recipient_name = first_nonempty(
            text_or_empty(group, "irs:RecipientBusinessName/irs:BusinessNameLine1Txt"),
            text_or_empty(group, "irs:RecipientBusinessName/irs:BusinessNameLine2Txt"),
            text_or_empty(group, "irs:RecipientPersonNm"),
        )
        purpose = first_nonempty(
            text_or_empty(group, "irs:GrantOrContributionPurposeTxt"),
            text_or_empty(group, "irs:PurposeOfGrantTxt"),
        )

        match_basis: List[str] = []
        if recipient_name and HIGHER_ED_PATTERN.search(recipient_name):
            match_basis.append("recipient_name_keyword")
        if purpose and PURPOSE_PATTERN.search(purpose):
            match_basis.append("purpose_keyword")

        if not match_basis:
            continue

        grants.append(
            {
                "grant_recipient_name": recipient_name,
                "recipient_relationship": text_or_empty(group, "irs:RecipientRelationshipTxt"),
                "recipient_foundation_status": text_or_empty(group, "irs:RecipientFoundationStatusTxt"),
                "recipient_city": first_nonempty(
                    text_or_empty(group, "irs:RecipientUSAddress/irs:CityNm"),
                    text_or_empty(group, "irs:RecipientForeignAddress/irs:CityNm"),
                ),
                "recipient_state": text_or_empty(group, "irs:RecipientUSAddress/irs:StateAbbreviationCd"),
                "recipient_country": text_or_empty(group, "irs:RecipientForeignAddress/irs:CountryCd"),
                "recipient_zip": text_or_empty(group, "irs:RecipientUSAddress/irs:ZIPCd"),
                "grant_amount_usd": text_or_empty(group, "irs:Amt"),
                "grant_purpose": purpose,
                "higher_ed_match_basis": ";".join(match_basis),
            }
        )

    return grants


def build_dataset(output_csv: str, target_foundations: int, years: List[int]) -> Dict[str, int]:
    """Build CSV dataset and return summary stats."""
    rows_by_batch: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for year in years:
        for row in fetch_index_rows(year):
            row["index_year"] = str(year)
            batch_id = (row.get("XML_BATCH_ID") or "").strip()
            if not batch_id:
                continue
            rows_by_batch[batch_id].append(row)

    if not rows_by_batch:
        raise RuntimeError("No index data loaded from IRS.")

    batches = sorted(rows_by_batch.keys(), reverse=True)
    foundation_eins = set()
    records: List[Dict[str, str]] = []

    for batch in batches:
        if len(foundation_eins) >= target_foundations:
            break

        year = batch.split("_")[0]
        zip_url = f"https://apps.irs.gov/pub/epostcard/990/xml/{year}/{batch}.zip"
        print(f"Processing batch {batch} ({len(foundation_eins)} foundations so far)", file=sys.stderr)

        try:
            zip_bytes = urllib.request.urlopen(zip_url, timeout=120).read()
        except Exception as exc:  # noqa: BLE001
            print(f"  Failed to download {zip_url}: {exc}", file=sys.stderr)
            continue

        rows_by_object_id = {row["OBJECT_ID"]: row for row in rows_by_batch[batch]}

        try:
            zip_file = zipfile.ZipFile(io.BytesIO(zip_bytes))
        except zipfile.BadZipFile:
            print(f"  Bad ZIP: {batch}", file=sys.stderr)
            continue

        for name in zip_file.namelist():
            if not name.endswith("_public.xml"):
                continue

            object_id = name.replace("_public.xml", "")
            index_row = rows_by_object_id.get(object_id)
            if not index_row:
                continue

            try:
                xml_bytes = zip_file.read(name)
            except Exception:  # noqa: BLE001
                continue

            grants = parse_qualifying_grants(xml_bytes)
            if not grants:
                continue

            foundation_ein = (index_row.get("EIN") or "").strip()
            foundation_eins.add(foundation_ein)

            for grant in grants:
                records.append(
                    {
                        "foundation_ein": foundation_ein,
                        "foundation_name": (index_row.get("TAXPAYER_NAME") or "").strip(),
                        "filing_tax_period": (index_row.get("TAX_PERIOD") or "").strip(),
                        "index_year": index_row.get("index_year", ""),
                        "submission_year": (index_row.get("SUB_DATE") or "").strip(),
                        "return_type": (index_row.get("RETURN_TYPE") or "").strip(),
                        "object_id": object_id,
                        "xml_batch_id": (index_row.get("XML_BATCH_ID") or "").strip(),
                        **grant,
                    }
                )

            if len(foundation_eins) >= target_foundations:
                break

    if not records:
        raise RuntimeError("No qualifying grant records found. Try loosening match patterns.")

    fields = [
        "foundation_ein",
        "foundation_name",
        "filing_tax_period",
        "index_year",
        "submission_year",
        "return_type",
        "object_id",
        "xml_batch_id",
        "grant_recipient_name",
        "recipient_relationship",
        "recipient_foundation_status",
        "recipient_city",
        "recipient_state",
        "recipient_country",
        "recipient_zip",
        "grant_amount_usd",
        "grant_purpose",
        "higher_ed_match_basis",
    ]

    output_path = Path(output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(records)

    return {
        "foundations": len(foundation_eins),
        "grant_rows": len(records),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT_CSV,
        help=f"Output CSV path (default: {DEFAULT_OUTPUT_CSV})",
    )
    parser.add_argument(
        "--target-foundations",
        type=int,
        default=DEFAULT_TARGET_FOUNDATIONS,
        help=f"Stop after this many unique foundations (default: {DEFAULT_TARGET_FOUNDATIONS})",
    )
    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        default=DEFAULT_YEARS,
        help=f"Index years to scan, newest first (default: {' '.join(map(str, DEFAULT_YEARS))})",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    stats = build_dataset(args.output, args.target_foundations, args.years)
    print(
        f"Wrote {stats['grant_rows']} grant rows from {stats['foundations']} foundations to {args.output}",
    )


if __name__ == "__main__":
    main()
