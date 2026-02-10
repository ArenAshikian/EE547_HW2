#!/usr/bin/env python3

import json
import os
import time
import urllib.request
from datetime import datetime, timezone

def main():
    print(f"[{datetime.now(timezone.utc).isoformat()}] Fetcher starting", flush=True)

    input_file = "/shared/input/urls.txt"

    # Wait for input file
    while not os.path.exists(input_file):
        print(f"Waiting for {input_file}...", flush=True)
        time.sleep(2)

    # Read URLs (manual loop)
    urls = []
    with open(input_file, "r") as f:
        lines = f.readlines()

    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if line:
            urls.append(line)
        i += 1

    os.makedirs("/shared/raw", exist_ok=True)
    os.makedirs("/shared/status", exist_ok=True)

    results = []
    success_count = 0
    fail_count = 0

    index = 0
    while index < len(urls):
        url = urls[index]
        page_num = index + 1
        output_file = "/shared/raw/page_" + str(page_num) + ".html"

        try:
            print(f"Fetching {url}...", flush=True)
            response = urllib.request.urlopen(url, timeout=10)
            content = response.read()

            with open(output_file, "wb") as f:
                f.write(content)

            result = {}
            result["url"] = url
            result["file"] = "page_" + str(page_num) + ".html"
            result["size"] = len(content)
            result["status"] = "success"

            results.append(result)
            success_count += 1

        except Exception as e:
            result = {}
            result["url"] = url
            result["file"] = None
            result["error"] = str(e)
            result["status"] = "failed"

            results.append(result)
            fail_count += 1

        time.sleep(1)
        index += 1

    # Build status dictionary line-by-line
    status = {}
    status["timestamp"] = datetime.now(timezone.utc).isoformat()
    status["urls_processed"] = len(urls)
    status["successful"] = success_count
    status["failed"] = fail_count
    status["results"] = results

    with open("/shared/status/fetch_complete.json", "w") as f:
        json.dump(status, f, indent=2)

    print(f"[{datetime.now(timezone.utc).isoformat()}] Fetcher complete", flush=True)

if __name__ == "__main__":
    main()
