"""Quick sanity check — scrape a few jobs and print them. No MCP / Claude needed.

Usage:
    uv run python example_search.py "python developer" "Berlin, Germany"
    uv run python example_search.py "data scientist" "remote"
"""

import sys

import pandas as pd
from jobspy import scrape_jobs


def val(row, key, fallback="—"):
    """Read a DataFrame cell, turning NaN/None into a readable fallback."""
    v = row.get(key)
    return v if pd.notna(v) else fallback

term = sys.argv[1] if len(sys.argv) > 1 else "python developer"
location = sys.argv[2] if len(sys.argv) > 2 else "Germany"

print(f"Searching Indeed for {term!r} in {location!r} ...\n")
df = scrape_jobs(
    site_name=["indeed"],
    search_term=term,
    location=location,
    results_wanted=10,
    country_indeed="germany",
    verbose=1,
)

count = 0 if df is None else len(df)
print(f"\nFound {count} jobs:\n")
if df is not None and count:
    for _, row in df.head(10).iterrows():
        print(f"- {val(row, 'title')}  @ {val(row, 'company')}  ({val(row, 'location')})")
        print(f"  {val(row, 'job_url')}")
