#!/usr/bin/env python3
import sys
import json
import time
import datetime
import os
import urllib.request


def now_utc_z():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def load_urls(path):
    urls = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line != "":
                urls.append(line)
    return urls


def make_dir(path):
    if os.path.exists(path):
        if os.path.isdir(path) == False:
            raise ValueError("Output path exists but is not a directory")
    else:
        os.makedirs(path)


def is_text(ctype):
    if ctype == "" or ctype == None:
        return False
    lower = ctype.lower()
    if lower.find("text") >= 0:
        return True
    return False


def count_words_bytes(body_bytes):
    text = body_bytes.decode("utf-8", errors="replace")

    in_word = False
    count = 0

    i = 0
    while i < len(text):
        ch = text[i]
        if ch.isalnum():
            if in_word == False:
                count = count + 1
                in_word = True
        else:
            in_word = False
        i = i + 1

    return count


def write_json_file(path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)


def log_error_line(path, url, message):
    with open(path, "a", encoding="utf-8") as f:
        f.write(now_utc_z() + " " + url + ": " + message + "\n")


def set_word_count_field(record, content_type, body):
    if is_text(content_type):
        if body != None:
            if len(body) > 0:
                record["word_count"] = count_words_bytes(body)
                return
    record["word_count"] = None


def fetch_url(url):
    record = {
        "url": url,
        "status_code": None,
        "response_time_ms": None,
        "content_length": None,
        "word_count": None,
        "timestamp": now_utc_z(),
        "error": None,
    }

    start = time.perf_counter()

    try:
        req = urllib.request.Request(url, method="GET")
        resp = urllib.request.urlopen(req, timeout=10)
        try:
            body = resp.read()

            record["status_code"] = resp.getcode()
            record["content_length"] = len(body)
            record["response_time_ms"] = (time.perf_counter() - start) * 1000.0

            ctype = resp.headers.get("Content-Type")
            set_word_count_field(record, ctype, body)

        finally:
            resp.close()

    except Exception as e:
        record["response_time_ms"] = (time.perf_counter() - start) * 1000.0

        if hasattr(e, "code"):
            try:
                record["status_code"] = int(e.code)
            except Exception:
                record["status_code"] = None

            body = b""
            try:
                if hasattr(e, "read"):
                    body = e.read()
            except Exception:
                body = b""

            record["content_length"] = len(body)

            ctype = None
            try:
                if hasattr(e, "headers"):
                    if e.headers != None:
                        ctype = e.headers.get("Content-Type")
            except Exception:
                ctype = None

            set_word_count_field(record, ctype, body)
            record["error"] = "HTTPError: " + str(record["status_code"])
        else:
            record["error"] = type(e).__name__ + ": " + str(e)

    return record


def summarize(records, start_ts, end_ts):
    total = len(records)
    ok = 0
    bad = 0
    total_ms = 0.0
    total_bytes = 0
    dist = {}

    i = 0
    while i < len(records):
        r = records[i]

        sc = r["status_code"]
        err = r["error"]

        if sc != None:
            key = str(sc)
            if key in dist:
                dist[key] = dist[key] + 1
            else:
                dist[key] = 1

        success = False
        if err == None:
            if sc != None:
                if sc >= 200:
                    if sc <= 299:
                        success = True

        if success:
            ok = ok + 1
        else:
            bad = bad + 1

        if r["response_time_ms"] != None:
            total_ms = total_ms + float(r["response_time_ms"])

        if r["content_length"] != None:
            total_bytes = total_bytes + int(r["content_length"])

        i = i + 1

    avg_ms = 0.0
    if total > 0:
        avg_ms = total_ms / total

    return {
        "total_urls": total,
        "successful_requests": ok,
        "failed_requests": bad,
        "average_response_time_ms": avg_ms,
        "total_bytes_downloaded": total_bytes,
        "status_code_distribution": dist,
        "processing_start": start_ts,
        "processing_end": end_ts,
    }


def main():
    if len(sys.argv) != 3:
        print("Usage: python fetch_and_process.py <input_urls_file> <output_dir>", file=sys.stderr)
        return 2

    input_file = sys.argv[1]
    out_dir = sys.argv[2]
    make_dir(out_dir)

    responses_path = os.path.join(out_dir, "responses.json")
    summary_path = os.path.join(out_dir, "summary.json")
    errors_path = os.path.join(out_dir, "errors.log")

    try:
        if os.path.exists(errors_path):
            os.remove(errors_path)
    except Exception:
        pass

    start_ts = now_utc_z()

    urls = load_urls(input_file)
    records = []

    j = 0
    while j < len(urls):
        url = urls[j]
        rec = fetch_url(url)
        records.append(rec)

        if rec["error"] != None:
            log_error_line(errors_path, url, rec["error"])

        j = j + 1

    end_ts = now_utc_z()

    write_json_file(responses_path, records)
    write_json_file(summary_path, summarize(records, start_ts, end_ts))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
