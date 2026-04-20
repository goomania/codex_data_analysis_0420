import csv
import io
import re
import sys
import urllib.request
import zipfile
import xml.etree.ElementTree as ET
from collections import defaultdict

YEARS = [2026, 2025, 2024]
TARGET_FOUNDATIONS = 1000
OUTPUT_CSV = 'founation_intel.csv'
NS = {'irs': 'http://www.irs.gov/efile'}

HIGHER_ED_PATTERN = re.compile(
    r"\b(university|college|institute of technology|polytechnic|community college|school of|law school|medical school|state university|seminary|higher education)\b",
    re.IGNORECASE,
)
PURPOSE_PATTERN = re.compile(
    r"\b(scholarship|fellowship|tuition|higher education|college|university|undergraduate|graduate|academic)\b",
    re.IGNORECASE,
)


def fetch_index_rows(year: int):
    url = f"https://apps.irs.gov/pub/epostcard/990/xml/{year}/index_{year}.csv"
    print(f"Loading index {year}: {url}", file=sys.stderr)
    try:
        with urllib.request.urlopen(url, timeout=60) as resp:
            text_stream = io.TextIOWrapper(resp, encoding='utf-8', errors='replace')
            reader = csv.DictReader(text_stream)
            for row in reader:
                if (row.get('RETURN_TYPE') or '').strip().upper() == '990PF':
                    yield row
    except Exception as e:
        print(f"  Skipping {year} ({e})", file=sys.stderr)


def text_or_empty(node, path):
    el = node.find(path, NS)
    return (el.text or '').strip() if el is not None and el.text else ''


def first_nonempty(*vals):
    for v in vals:
        if v:
            return v
    return ''


def parse_qualifying_grants(xml_bytes):
    # Fast pre-filter before XML parse
    low = xml_bytes.lower()
    if b'grantorcontributionpdduryrgrp' not in low:
        return []
    if not any(k in low for k in [b'university', b'college', b'scholar', b'higher education', b'institute']):
        return []

    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return []

    grants = []
    for grp in root.findall('.//irs:GrantOrContributionPdDurYrGrp', NS):
        recipient_name = first_nonempty(
            text_or_empty(grp, 'irs:RecipientBusinessName/irs:BusinessNameLine1Txt'),
            text_or_empty(grp, 'irs:RecipientBusinessName/irs:BusinessNameLine2Txt'),
            text_or_empty(grp, 'irs:RecipientPersonNm'),
        )
        purpose = first_nonempty(
            text_or_empty(grp, 'irs:GrantOrContributionPurposeTxt'),
            text_or_empty(grp, 'irs:PurposeOfGrantTxt'),
        )
        match_basis = []
        if recipient_name and HIGHER_ED_PATTERN.search(recipient_name):
            match_basis.append('recipient_name_keyword')
        if purpose and PURPOSE_PATTERN.search(purpose):
            match_basis.append('purpose_keyword')

        if not match_basis:
            continue

        grants.append(
            {
                'grant_recipient_name': recipient_name,
                'recipient_relationship': text_or_empty(grp, 'irs:RecipientRelationshipTxt'),
                'recipient_foundation_status': text_or_empty(grp, 'irs:RecipientFoundationStatusTxt'),
                'recipient_city': first_nonempty(
                    text_or_empty(grp, 'irs:RecipientUSAddress/irs:CityNm'),
                    text_or_empty(grp, 'irs:RecipientForeignAddress/irs:CityNm'),
                ),
                'recipient_state': text_or_empty(grp, 'irs:RecipientUSAddress/irs:StateAbbreviationCd'),
                'recipient_country': text_or_empty(grp, 'irs:RecipientForeignAddress/irs:CountryCd'),
                'recipient_zip': text_or_empty(grp, 'irs:RecipientUSAddress/irs:ZIPCd'),
                'grant_amount_usd': text_or_empty(grp, 'irs:Amt'),
                'grant_purpose': purpose,
                'higher_ed_match_basis': ';'.join(match_basis),
            }
        )

    return grants


def main():
    by_batch = defaultdict(list)
    for year in YEARS:
        for row in fetch_index_rows(year):
            row['index_year'] = str(year)
            batch_id = (row.get('XML_BATCH_ID') or '').strip()
            if not batch_id:
                continue
            by_batch[batch_id].append(row)

    if not by_batch:
        raise RuntimeError('No index data loaded from IRS.')

    batches = sorted(by_batch.keys(), reverse=True)
    foundation_eins = set()
    records = []

    for batch in batches:
        if len(foundation_eins) >= TARGET_FOUNDATIONS:
            break

        year = batch.split('_')[0]
        zip_url = f"https://apps.irs.gov/pub/epostcard/990/xml/{year}/{batch}.zip"
        print(f"Processing batch {batch} ({len(foundation_eins)} foundations so far)", file=sys.stderr)

        try:
            zip_bytes = urllib.request.urlopen(zip_url, timeout=120).read()
        except Exception as e:
            print(f"  Failed to download {zip_url}: {e}", file=sys.stderr)
            continue

        row_map = {r['OBJECT_ID']: r for r in by_batch[batch]}

        try:
            zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
        except zipfile.BadZipFile:
            print(f"  Bad ZIP: {batch}", file=sys.stderr)
            continue

        for name in zf.namelist():
            if not name.endswith('_public.xml'):
                continue
            object_id = name.replace('_public.xml', '')
            row = row_map.get(object_id)
            if not row:
                continue

            try:
                xml_bytes = zf.read(name)
            except Exception:
                continue

            grants = parse_qualifying_grants(xml_bytes)
            if not grants:
                continue

            ein = (row.get('EIN') or '').strip()
            foundation_eins.add(ein)
            for g in grants:
                records.append(
                    {
                        'foundation_ein': ein,
                        'foundation_name': (row.get('TAXPAYER_NAME') or '').strip(),
                        'filing_tax_period': (row.get('TAX_PERIOD') or '').strip(),
                        'index_year': row.get('index_year', ''),
                        'submission_year': (row.get('SUB_DATE') or '').strip(),
                        'return_type': (row.get('RETURN_TYPE') or '').strip(),
                        'object_id': object_id,
                        'xml_batch_id': (row.get('XML_BATCH_ID') or '').strip(),
                        **g,
                    }
                )

            if len(foundation_eins) >= TARGET_FOUNDATIONS:
                break

    if not records:
        raise RuntimeError('No qualifying grant records found. Try loosening match patterns.')

    fields = [
        'foundation_ein',
        'foundation_name',
        'filing_tax_period',
        'index_year',
        'submission_year',
        'return_type',
        'object_id',
        'xml_batch_id',
        'grant_recipient_name',
        'recipient_relationship',
        'recipient_foundation_status',
        'recipient_city',
        'recipient_state',
        'recipient_country',
        'recipient_zip',
        'grant_amount_usd',
        'grant_purpose',
        'higher_ed_match_basis',
    ]

    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(records)

    print(f"Wrote {len(records)} grant rows from {len(foundation_eins)} foundations to {OUTPUT_CSV}")


if __name__ == '__main__':
    main()
