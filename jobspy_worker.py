"""Isolated scraper process: parent enforces a deadline and can kill hung requests."""
import contextlib
import json
import sys

if __name__ == "__main__":
    arguments = json.load(sys.stdin)
    # Keep protocol stdout clean, including third-party startup prints.
    with contextlib.redirect_stdout(sys.stderr):
        from jobspy import scrape_jobs
        result = scrape_jobs(**arguments)
    sys.stdout.write("[]" if result is None else result.to_json(orient="records",date_format="iso"))
