import csv
import json
import sys
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone

OPENALEX = 'https://api.openalex.org'
INSTITUTION_ID = 'I70983195'  # Syracuse University
SINCE_YEAR = 2016
MIN_WORKS_COUNT = 5
OUTPUT_CSV = 'faculty_intel.csv'
PER_PAGE = 200
MAX_RETRIES = 4
SLEEP_SECONDS = 0.2


def fetch_json(url: str):
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with urllib.request.urlopen(url, timeout=60) as resp:
                return json.load(resp)
        except Exception as e:
            last_err = e
            time.sleep(attempt * 0.75)
    raise RuntimeError(f'Failed after retries: {url} :: {last_err}')


def paginate(endpoint: str, filter_expr: str, select: str | None = None):
    cursor = '*'
    while cursor:
        params = {
            'filter': filter_expr,
            'per-page': str(PER_PAGE),
            'cursor': cursor,
        }
        if select:
            params['select'] = select
        url = f"{OPENALEX}/{endpoint}?{urllib.parse.urlencode(params)}"
        payload = fetch_json(url)
        results = payload.get('results', [])
        for row in results:
            yield row

        nxt = payload.get('meta', {}).get('next_cursor')
        if not nxt or nxt == cursor or not results:
            break
        cursor = nxt
        time.sleep(SLEEP_SECONDS)


def short_id(openalex_url: str) -> str:
    if not openalex_url:
        return ''
    return openalex_url.rstrip('/').split('/')[-1]


def top_topics(author: dict, limit: int = 5) -> str:
    topics = []
    for t in author.get('topics') or []:
        nm = (t.get('display_name') or '').strip()
        if nm:
            topics.append(nm)
    if not topics:
        for t in (author.get('x_concepts') or []):
            nm = (t.get('display_name') or '').strip()
            if nm:
                topics.append(nm)
    seen = set()
    ordered = []
    for t in topics:
        if t not in seen:
            seen.add(t)
            ordered.append(t)
        if len(ordered) >= limit:
            break
    return '; '.join(ordered)


def main():
    faculty = {}
    filter_expr = f'last_known_institutions.id:{INSTITUTION_ID},works_count:>{MIN_WORKS_COUNT}'
    print('Loading Syracuse-affiliated researchers from OpenAlex...', file=sys.stderr)
    for a in paginate(
        endpoint='authors',
        filter_expr=filter_expr,
        select='id,display_name,orcid,works_count,cited_by_count,summary_stats,topics,x_concepts,updated_date',
    ):
        aid = short_id(a.get('id', ''))
        if not aid:
            continue
        faculty[aid] = {
            'faculty_name': a.get('display_name', ''),
            'openalex_author_id': aid,
            'orcid': (a.get('orcid') or '').replace('https://orcid.org/', ''),
            'works_count': a.get('works_count', 0) or 0,
            'cited_by_count': a.get('cited_by_count', 0) or 0,
            'h_index': (a.get('summary_stats') or {}).get('h_index', ''),
            'i10_index': (a.get('summary_stats') or {}).get('i10_index', ''),
            'research_topics': top_topics(a),
            'openalex_updated_date': a.get('updated_date', ''),
        }

    if not faculty:
        raise RuntimeError('No researcher records found from OpenAlex.')

    print(f'Loaded {len(faculty)} researchers. Aggregating award/grant-linked works...', file=sys.stderr)

    works_filter = f'institutions.id:{INSTITUTION_ID},from_publication_date:{SINCE_YEAR}-01-01'
    grants_by_author = defaultdict(list)

    for w in paginate(endpoint='works', filter_expr=works_filter):
        awards = w.get('awards') or []
        if not awards:
            continue

        work_id = short_id(w.get('id', ''))
        title = (w.get('title') or '').replace('\n', ' ').strip()
        pub_year = w.get('publication_year', '')

        author_ids = []
        for au in (w.get('authorships') or []):
            aid = short_id((au.get('author') or {}).get('id', ''))
            if aid and aid in faculty:
                author_ids.append(aid)

        if not author_ids:
            continue

        award_bits = []
        for a in awards:
            funder = (a.get('funder_display_name') or '').strip()
            award_id = (a.get('funder_award_id') or '').strip()
            if funder or award_id:
                award_bits.append((funder, award_id))

        if not award_bits:
            continue

        for aid in set(author_ids):
            for funder, award_id in award_bits:
                grants_by_author[aid].append(
                    {
                        'funder': funder,
                        'award_id': award_id,
                        'work_id': work_id,
                        'title': title,
                        'publication_year': pub_year,
                    }
                )

    now = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    rows = []

    for aid, rec in faculty.items():
        entries = grants_by_author.get(aid, [])
        funder_counts = defaultdict(int)
        award_ids = set()
        sample_pubs = []

        for e in entries:
            if e['funder']:
                funder_counts[e['funder']] += 1
            if e['award_id']:
                award_ids.add(e['award_id'])
            if len(sample_pubs) < 8:
                sample_pubs.append(f"{e['publication_year']}: {e['title']} [{e['work_id']}]")

        top_funders = sorted(funder_counts.items(), key=lambda x: (-x[1], x[0]))[:10]

        rows.append(
            {
                **rec,
                'institution': 'Syracuse University',
                'award_or_grant_mentions_since_2016': len(entries),
                'top_funders_since_2016': '; '.join([f"{k} ({v})" for k, v in top_funders]),
                'sample_award_ids_since_2016': '; '.join(sorted(award_ids)[:20]),
                'sample_award_linked_publications': ' || '.join(sample_pubs),
                'grant_data_method': 'OpenAlex awards metadata linked to Syracuse-affiliated publications',
                'data_as_of_utc': now,
            }
        )

    rows.sort(key=lambda r: (-(r['award_or_grant_mentions_since_2016']), -(r['works_count']), r['faculty_name']))

    fieldnames = [
        'faculty_name',
        'institution',
        'openalex_author_id',
        'orcid',
        'works_count',
        'cited_by_count',
        'h_index',
        'i10_index',
        'research_topics',
        'award_or_grant_mentions_since_2016',
        'top_funders_since_2016',
        'sample_award_ids_since_2016',
        'sample_award_linked_publications',
        'grant_data_method',
        'openalex_updated_date',
        'data_as_of_utc',
    ]

    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f'Wrote {len(rows)} rows to {OUTPUT_CSV}', file=sys.stderr)


if __name__ == '__main__':
    main()
