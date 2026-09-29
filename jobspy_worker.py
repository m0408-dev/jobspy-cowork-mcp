"""Isolated scraper process: parent enforces a deadline and can kill hung requests."""
import contextlib
import json
import sys

if __name__ == "__main__":
    for stream in (sys.stdin, sys.stdout, sys.stderr):
        stream.reconfigure(encoding='utf-8')
    arguments = json.load(sys.stdin)
    if arguments.get('site_name') == ['linkedin']:
        from linkedin_adapter import fetch_page
        sys.stdout.write(json.dumps(fetch_page(arguments), ensure_ascii=False))
        sys.exit(0)
    # Keep protocol stdout clean, including third-party startup prints.
    with contextlib.redirect_stdout(sys.stderr):
        from jobspy import scrape_jobs
        result = scrape_jobs(**arguments)
    sys.stdout.write("[]" if result is None else result.to_json(orient="records",date_format="iso"))
