#!/usr/bin/env python3

import json
import os
import re
import time
from datetime import datetime, timezone


PROCESSED_DIR = "/shared/processed"
STATUS_DIR = "/shared/status"
ANALYSIS_DIR = "/shared/analysis"

PROCESS_DONE = os.path.join(STATUS_DIR, "process_complete.json")
FINAL_REPORT = os.path.join(ANALYSIS_DIR, "final_report.json")

WORD_RE = re.compile(r"\w+", re.UNICODE)
SENT_SPLIT_RE = re.compile(r"[.!?]+", re.UNICODE)


def now_utc_iso():
    return datetime.now(timezone.utc).isoformat()


def write_json(path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)


def read_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def tokenize_words_lower(text):
    return WORD_RE.findall(text.lower())


def tokenize_words_original(text):
    return WORD_RE.findall(text)


def split_sentences(text):
    parts = SENT_SPLIT_RE.split(text)
    out = []
    i = 0
    while i < len(parts):
        s = parts[i].strip()
        if s != "":
            out.append(s)
        i += 1
    return out


def increment_count(d, key):
    if key in d:
        d[key] = d[key] + 1
    else:
        d[key] = 1


def build_ngrams(words, n):
    out = []
    i = 0
    while i + n <= len(words):
        out.append(" ".join(words[i:i + n]))
        i += 1
    return out


def jaccard_similarity(doc1_words, doc2_words):
    set1 = set(doc1_words)
    set2 = set(doc2_words)
    intersection = set1.intersection(set2)
    union = set1.union(set2)
    return float(len(intersection)) / float(len(union)) if len(union) > 0 else 0.0


def avg_word_length(words):
    if len(words) == 0:
        return 0.0
    total = 0
    i = 0
    while i < len(words):
        total += len(words[i])
        i += 1
    return float(total) / float(len(words))


def main():
    print(f"[{now_utc_iso()}] Analyzer starting", flush=True)

    while not os.path.exists(PROCESS_DONE):
        print(f"Waiting for {PROCESS_DONE}...", flush=True)
        time.sleep(2)

    os.makedirs(ANALYSIS_DIR, exist_ok=True)

    if not os.path.exists(PROCESSED_DIR):
        report = {
            "processing_timestamp": now_utc_iso(),
            "documents_processed": 0,
            "total_words": 0,
            "unique_words": 0,
            "top_100_words": [],
            "document_similarity": [],
            "top_bigrams": [],
            "top_trigrams": [],
            "readability": {
                "avg_sentence_length": 0.0,
                "avg_word_length": 0.0,
                "complexity_score": 0.0
            },
            "error": f"Missing processed dir {PROCESSED_DIR}"
        }
        write_json(FINAL_REPORT, report)
        return 1

    files = []
    for name in os.listdir(PROCESSED_DIR):
        if name.lower().endswith(".json"):
            files.append(name)
    files.sort()

    docs = []  
    global_freq = {}
    total_words = 0

    total_sentence_count = 0
    total_sentence_words = 0

    all_words_for_readability = [] 

    bigram_freq = {}
    trigram_freq = {}

    i = 0
    while i < len(files):
        path = os.path.join(PROCESSED_DIR, files[i])
        try:
            obj = read_json(path)
            text = obj.get("text", "")
        except Exception as e:
            print(f"Warning: could not read {files[i]}: {e}", flush=True)
            i += 1
            continue

        words = tokenize_words_lower(text)
        sents = split_sentences(text)

        j = 0
        while j < len(words):
            increment_count(global_freq, words[j])
            j += 1

        total_words += len(words)
        all_words_for_readability.extend(words)

        total_sentence_count += len(sents)
        j = 0
        while j < len(sents):
            total_sentence_words += len(tokenize_words_lower(sents[j]))
            j += 1

        bigrams = build_ngrams(words, 2)
        trigrams = build_ngrams(words, 3)

        j = 0
        while j < len(bigrams):
            increment_count(bigram_freq, bigrams[j])
            j += 1

        j = 0
        while j < len(trigrams):
            increment_count(trigram_freq, trigrams[j])
            j += 1

        docs.append({
            "file": files[i],
            "words": words
        })

        i += 1

    unique_words = len(global_freq)

    items = []
    for w in global_freq:
        items.append((w, global_freq[w]))
    items.sort(key=lambda x: (-x[1], x[0]))

    top_100_words = []
    k = 0
    while k < len(items) and k < 100:
        w = items[k][0]
        c = items[k][1]
        freq = float(c) / float(total_words) if total_words > 0 else 0.0
        top_100_words.append({
            "word": w,
            "count": c,
            "frequency": freq
        })
        k += 1

    sim_list = []
    i = 0
    while i < len(docs):
        j = i + 1
        while j < len(docs):
            sim = jaccard_similarity(docs[i]["words"], docs[j]["words"])
            sim_list.append({
                "doc1": docs[i]["file"],
                "doc2": docs[j]["file"],
                "similarity": sim
            })
            j += 1
        i += 1

    b_items = []
    for bg in bigram_freq:
        b_items.append((bg, bigram_freq[bg]))
    b_items.sort(key=lambda x: (-x[1], x[0]))

    t_items = []
    for tg in trigram_freq:
        t_items.append((tg, trigram_freq[tg]))
    t_items.sort(key=lambda x: (-x[1], x[0]))

    top_bigrams = []
    k = 0
    while k < len(b_items) and k < 50:
        top_bigrams.append({"bigram": b_items[k][0], "count": b_items[k][1]})
        k += 1

    top_trigrams = []
    k = 0
    while k < len(t_items) and k < 50:
        top_trigrams.append({"trigram": t_items[k][0], "count": t_items[k][1]})
        k += 1

    avg_sentence_length = (
        float(total_sentence_words) / float(total_sentence_count)
        if total_sentence_count > 0 else 0.0
    )
    avg_wlen = avg_word_length(all_words_for_readability)

    complexity_score = avg_sentence_length * avg_wlen

    report = {
        "processing_timestamp": now_utc_iso(),
        "documents_processed": len(docs),
        "total_words": total_words,
        "unique_words": unique_words,
        "top_100_words": top_100_words,
        "document_similarity": sim_list,
        "top_bigrams": top_bigrams,
        "top_trigrams": top_trigrams,
        "readability": {
            "avg_sentence_length": avg_sentence_length,
            "avg_word_length": avg_wlen,
            "complexity_score": complexity_score
        }
    }

    write_json(FINAL_REPORT, report)
    print(f"[{now_utc_iso()}] Analyzer complete", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

