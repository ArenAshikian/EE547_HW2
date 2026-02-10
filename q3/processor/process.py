#!/usr/bin/env python3
import json
import os
import re
import sys
import time
from datetime import datetime, timezone

RAW_DIR = "/shared/raw"
PROCESSED_DIR = "/shared/processed"
STATUS_DIR = "/shared/status"

FETCH_DONE = STATUS_DIR + "/fetch_complete.json"
PROCESS_DONE = STATUS_DIR + "/process_complete.json"

WORD_RE = re.compile(r"\w+", re.UNICODE)
SENT_SPLIT_RE = re.compile(r"[.!?]+", re.UNICODE)

HREF_RE = re.compile(r'[Hh][Rr][Ee][Ff]=[\'"]?([^\'" >]+)')
SRC_RE  = re.compile(r'[Ss][Rr][Cc]=[\'"]?([^\'" >]+)')
P_TAG_RE = re.compile(r"<[Pp]\b")

TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")


def now_utc_iso():
    return datetime.now(timezone.utc).isoformat()


def read_text_file(path):
    f = open(path, "rb")
    data = f.read()
    f.close()

    try:
        return data.decode("utf-8")
    except Exception:
        return data.decode("latin-1", errors="replace")


def write_json(path, obj):
    f = open(path, "w", encoding="utf-8")
    json.dump(obj, f, indent=2, ensure_ascii=False)
    f.close()


def remove_tag_block(html, tag_name):
    low = html.lower()
    open_pat = "<" + tag_name
    close_pat = "</" + tag_name + ">"

    while True:
        start = low.find(open_pat)
        if start == -1:
            break

        end = low.find(close_pat, start)
        if end == -1:
            html = html[:start]
            break

        end = end + len(close_pat)
        html = html[:start] + " " + html[end:]
        low = html.lower()

    return html


def strip_html(html_content):
    html_content = remove_tag_block(html_content, "script")
    html_content = remove_tag_block(html_content, "style")

    links = HREF_RE.findall(html_content)
    images = SRC_RE.findall(html_content)

    text = TAG_RE.sub(" ", html_content)
    text = WS_RE.sub(" ", text).strip()

    return text, links, images


def count_paragraphs(html_content, extracted_text):
    p_count = len(P_TAG_RE.findall(html_content))
    if p_count > 0:
        return p_count
    if extracted_text.strip() != "":
        return 1
    return 0


def sentence_count(text):
    parts = SENT_SPLIT_RE.split(text)
    count = 0
    i = 0
    while i < len(parts):
        if parts[i].strip() != "":
            count = count + 1
        i = i + 1
    return count


def avg_word_length(words):
    if len(words) == 0:
        return 0.0
    total = 0
    i = 0
    while i < len(words):
        total = total + len(words[i])
        i = i + 1
    return float(total) / float(len(words))


def process_one_file(html_path):
    html = read_text_file(html_path)

    text, links, images = strip_html(html)

    words = WORD_RE.findall(text)
    wc = len(words)
    sc = sentence_count(text)
    pc = count_paragraphs(html, text)
    awl = avg_word_length(words)

    source_file = html_path.split("/")[-1]

    out = {}
    out["source_file"] = source_file
    out["text"] = text

    stats = {}
    stats["word_count"] = wc
    stats["sentence_count"] = sc
    stats["paragraph_count"] = pc
    stats["avg_word_length"] = awl
    out["statistics"] = stats

    out["links"] = links
    out["images"] = images
    out["processed_at"] = now_utc_iso()

    return out


def main():
    print("[" + now_utc_iso() + "] Processor starting", flush=True)

    while not os.path.exists(FETCH_DONE):
        print("Waiting for " + FETCH_DONE + "...", flush=True)
        time.sleep(2)

    os.makedirs(PROCESSED_DIR, exist_ok=True)
    os.makedirs(STATUS_DIR, exist_ok=True)

    if not os.path.exists(RAW_DIR):
        print("[" + now_utc_iso() + "] ERROR: Missing raw dir " + RAW_DIR, flush=True)

        status = {}
        status["timestamp"] = now_utc_iso()
        status["status"] = "failed"
        status["error"] = "Missing raw dir " + RAW_DIR
        status["files_processed"] = 0
        status["successful"] = 0
        status["failed"] = 0
        status["results"] = []

        write_json(PROCESS_DONE, status)
        return 1

    names = os.listdir(RAW_DIR)
    raw_files = []

    i = 0
    while i < len(names):
        name = names[i]
        if name.lower().endswith(".html"):
            raw_files.append(name)
        i = i + 1

    raw_files.sort()

    results = []
    ok = 0
    bad = 0

    i = 0
    while i < len(raw_files):
        fname = raw_files[i]
        in_path = RAW_DIR + "/" + fname

        base = fname
        if "." in fname:
            base = fname.rsplit(".", 1)[0]
        out_path = PROCESSED_DIR + "/" + base + ".json"

        try:
            print("Processing " + fname + "...", flush=True)
            obj = process_one_file(in_path)
            write_json(out_path, obj)

            r = {}
            r["source_file"] = fname
            r["output_file"] = out_path.split("/")[-1]
            r["status"] = "success"
            results.append(r)

            ok = ok + 1

        except Exception as e:
            r = {}
            r["source_file"] = fname
            r["output_file"] = None
            r["status"] = "failed"
            r["error"] = str(e)
            results.append(r)

            bad = bad + 1

        i = i + 1

    status = {}
    status["timestamp"] = now_utc_iso()
    status["files_processed"] = len(raw_files)
    status["successful"] = ok
    status["failed"] = bad
    status["results"] = results

    write_json(PROCESS_DONE, status)
    print("[" + now_utc_iso() + "] Processor complete", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
