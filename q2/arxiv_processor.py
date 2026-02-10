#!/usr/bin/env python3
import sys
import os
import json
import time
import datetime
import re
import urllib.request
import xml.etree.ElementTree as ET

from stopwords import STOPWORDS

ARXIV_API = "http://export.arxiv.org/api/query"

WORD_RE = re.compile(r"\w+", re.UNICODE)
SENT_SPLIT_RE = re.compile(r"[.!?]+")
HYPHEN_TERM_RE = re.compile(r"\b\w+(?:-\w+)+\b", re.UNICODE)

HAS_DIGIT_RE = re.compile(r"\d")
HAS_UPPER_RE = re.compile(r"[A-Z]")
HAS_LOWER_RE = re.compile(r"[a-z]")


def now_utc_z():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def make_dir(path):
    if os.path.exists(path):
        if os.path.isdir(path) == False:
            raise ValueError("Output path exists but is not a directory")
    else:
        os.makedirs(path)


def write_json(path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)


def log_line(path, message):
    with open(path, "a", encoding="utf-8") as f:
        f.write(now_utc_z() + " " + message + "\n")


def is_int_string(s):
    try:
        int(s)
        return True
    except Exception:
        return False


def url_encode_query_value(s):
    if s == None:
        return ""

    b = s.encode("utf-8")
    out = []
    i = 0

    while i < len(b):
        c = b[i]
        ch = chr(c)

        ok = False
        if 48 <= c <= 57:
            ok = True
        if 65 <= c <= 90:
            ok = True
        if 97 <= c <= 122:
            ok = True
        if ch in ["-", "_", ".", "~", ":"]:
            ok = True

        if ok:
            out.append(ch)
        else:
            out.append("%" + format(c, "02X"))

        i += 1

    return "".join(out)


def build_query_url(query, max_results):
    encoded = url_encode_query_value(query)
    return ARXIV_API + "?search_query=" + encoded + "&start=0&max_results=" + str(max_results)


def fetch_xml(url, log_path):
    attempts = 0

    while attempts < 3:
        attempts += 1

        try:
            req = urllib.request.Request(url, method="GET")
            req.add_header("User-Agent", "EE547-HW2 arxiv_processor")

            resp = urllib.request.urlopen(req, timeout=30)
            try:
                data = resp.read()
            finally:
                resp.close()

            return data

        except Exception as e:
            code = getattr(e, "code", None)
            if code == 429:
                log_line(
                    log_path,
                    "Received HTTP 429, waiting 3 seconds before retry (" + str(attempts) + "/3)"
                )
                time.sleep(3)
                if attempts >= 3:
                    raise
                continue

            raise

    raise RuntimeError("Failed to fetch from ArXiv after retries")


def normalize_space(s):
    if s == None:
        return ""
    return " ".join(s.split()).strip()


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


def tokenize_words_lower(text):
    return WORD_RE.findall(text.lower())


def tokenize_words_original(text):
    return WORD_RE.findall(text)


def avg_word_length(words):
    if len(words) == 0:
        return 0.0

    total_len = 0
    i = 0
    while i < len(words):
        total_len += len(words[i])
        i += 1

    return float(total_len) / float(len(words))


def increment_count(d, key):
    if key in d:
        d[key] = d[key] + 1
    else:
        d[key] = 1


def top_k_words_excluding_stopwords(words_lower, k):
    freq = {}

    i = 0
    while i < len(words_lower):
        w = words_lower[i]
        if w not in STOPWORDS:
            increment_count(freq, w)
        i += 1

    items = []
    for w in freq:
        items.append((w, freq[w]))
    items.sort(key=lambda x: (-x[1], x[0]))

    out = []
    i = 0
    while i < len(items) and i < k:
        out.append({"word": items[i][0], "frequency": items[i][1]})
        i += 1

    return out


def sentence_length_stats(sentences):
    if len(sentences) == 0:
        return (0, 0.0, 0, 0)

    total_words = 0
    longest = -1
    shortest = -1

    i = 0
    while i < len(sentences):
        wc = len(tokenize_words_lower(sentences[i]))
        total_words += wc

        if longest < 0:
            longest = wc
            shortest = wc
        else:
            if wc > longest:
                longest = wc
            if wc < shortest:
                shortest = wc

        i += 1

    avg_wps = float(total_words) / float(len(sentences))
    return (len(sentences), avg_wps, longest, shortest)


def analyze_abstract_stats(abstract):
    if abstract == None:
        abstract = ""

    words_lower = tokenize_words_lower(abstract)

    uniq = {}
    i = 0
    while i < len(words_lower):
        uniq[words_lower[i]] = 1
        i += 1

    sentences = split_sentences(abstract)
    sent_count, avg_wps, longest_sent_words, shortest_sent_words = sentence_length_stats(sentences)

    stats = {}
    stats["total_words"] = len(words_lower)
    stats["unique_words"] = len(uniq)
    stats["top_20_words"] = top_k_words_excluding_stopwords(words_lower, 20)
    stats["total_sentences"] = sent_count
    stats["avg_words_per_sentence"] = avg_wps
    stats["longest_sentence_words"] = longest_sent_words
    stats["shortest_sentence_words"] = shortest_sent_words
    stats["avg_word_length"] = avg_word_length(words_lower)

    return stats


def parse_feed(xml_bytes, log_path):
    ns = {"atom": "http://www.w3.org/2005/Atom"}

    root = ET.fromstring(xml_bytes)
    entries = root.findall("atom:entry", ns)

    papers = []

    i = 0
    while i < len(entries):
        entry = entries[i]

        try:
            id_elem = entry.find("atom:id", ns)
            title_elem = entry.find("atom:title", ns)
            abs_elem = entry.find("atom:summary", ns)
            pub_elem = entry.find("atom:published", ns)
            upd_elem = entry.find("atom:updated", ns)

            full_id = ""
            if id_elem != None and id_elem.text != None:
                full_id = normalize_space(id_elem.text)

            arxiv_id = ""
            if full_id != "":
                arxiv_id = full_id.split("/")[-1]

            title = ""
            if title_elem != None and title_elem.text != None:
                title = normalize_space(title_elem.text)

            abstract = ""
            if abs_elem != None and abs_elem.text != None:
                abstract = normalize_space(abs_elem.text)

            published = ""
            if pub_elem != None and pub_elem.text != None:
                published = normalize_space(pub_elem.text)

            updated = ""
            if upd_elem != None and upd_elem.text != None:
                updated = normalize_space(upd_elem.text)

            if arxiv_id == "" or title == "" or abstract == "" or published == "" or updated == "":
                log_line(log_path, "Warning: missing required fields, skipping entry")
                i += 1
                continue

            authors = []
            author_elems = entry.findall("atom:author", ns)

            j = 0
            while j < len(author_elems):
                name_elem = author_elems[j].find("atom:name", ns)
                if name_elem != None and name_elem.text != None:
                    name = normalize_space(name_elem.text)
                    if name != "":
                        authors.append(name)
                j += 1

            categories = []
            cat_elems = entry.findall("atom:category", ns)

            j = 0
            while j < len(cat_elems):
                if "term" in cat_elems[j].attrib:
                    term = normalize_space(cat_elems[j].attrib["term"])
                    if term != "":
                        categories.append(term)
                j += 1

            paper = {}
            paper["arxiv_id"] = arxiv_id
            paper["title"] = title
            paper["authors"] = authors
            paper["abstract"] = abstract
            paper["categories"] = categories
            paper["published"] = published
            paper["updated"] = updated
            paper["abstract_stats"] = analyze_abstract_stats(abstract)

            papers.append(paper)

        except Exception as e:
            log_line(
                log_path,
                "Invalid entry XML/content; skipping. Error: " + type(e).__name__ + ": " + str(e)
            )

        i += 1

    return papers


def token_all_letters_uppercase(tok):
    if tok == None or tok == "":
        return False
    if HAS_UPPER_RE.search(tok) == None:
        return False
    if HAS_LOWER_RE.search(tok) != None:
        return False
    return True


def build_corpus_analysis(papers, query):
    total_abstracts = len(papers)
    total_words = 0
    longest_abs = -1
    shortest_abs = -1

    global_unique = {}
    global_word_freq = {}
    global_doc_freq = {}

    uppercase_terms = {}
    numeric_terms = {}
    hyphenated_terms = {}

    category_dist = {}

    i = 0
    while i < len(papers):
        abs_text = papers[i]["abstract"]

        tokens = tokenize_words_lower(abs_text)
        wc = len(tokens)
        total_words += wc

        if longest_abs < 0:
            longest_abs = wc
            shortest_abs = wc
        else:
            if wc > longest_abs:
                longest_abs = wc
            if wc < shortest_abs:
                shortest_abs = wc

        doc_seen = {}

        j = 0
        while j < len(tokens):
            w = tokens[j]
            global_unique[w] = 1

            if w not in STOPWORDS:
                increment_count(global_word_freq, w)
                doc_seen[w] = 1

            j += 1

        for w in doc_seen:
            increment_count(global_doc_freq, w)

        orig_tokens = tokenize_words_original(abs_text)
        j = 0
        while j < len(orig_tokens):
            tok = orig_tokens[j]

            if token_all_letters_uppercase(tok):
                uppercase_terms[tok] = 1

            if HAS_DIGIT_RE.search(tok) != None:
                numeric_terms[tok] = 1

            j += 1

        hyps = HYPHEN_TERM_RE.findall(abs_text)
        j = 0
        while j < len(hyps):
            hyphenated_terms[hyps[j]] = 1
            j += 1

        cats = papers[i]["categories"]
        j = 0
        while j < len(cats):
            increment_count(category_dist, cats[j])
            j += 1

        i += 1

    if total_abstracts > 0:
        avg_abstract_len = float(total_words) / float(total_abstracts)
    else:
        avg_abstract_len = 0.0
        longest_abs = 0
        shortest_abs = 0

    items = []
    for w in global_word_freq:
        items.append((w, global_word_freq[w]))
    items.sort(key=lambda x: (-x[1], x[0]))

    top_50 = []
    k = 0
    while k < len(items) and k < 50:
        word = items[k][0]
        freq = items[k][1]
        docs = global_doc_freq.get(word, 0)
        top_50.append({"word": word, "frequency": freq, "documents": docs})
        k += 1

    uppercase_list = sorted(list(uppercase_terms.keys()))
    numeric_list = sorted(list(numeric_terms.keys()))
    hyphen_list = sorted(list(hyphenated_terms.keys()))

    out = {}
    out["query"] = query
    out["papers_processed"] = total_abstracts
    out["processing_timestamp"] = now_utc_z()

    out["corpus_stats"] = {
        "total_abstracts": total_abstracts,
        "total_words": total_words,
        "unique_words_global": len(global_unique),
        "avg_abstract_length": avg_abstract_len,
        "longest_abstract_words": longest_abs,
        "shortest_abstract_words": shortest_abs
    }

    out["top_50_words"] = top_50

    out["technical_terms"] = {
        "uppercase_terms": uppercase_list,
        "numeric_terms": numeric_list,
        "hyphenated_terms": hyphen_list
    }

    out["category_distribution"] = category_dist

    return out


def main():
    if len(sys.argv) != 4:
        print("Usage: python arxiv_processor.py <search_query> <max_results 1-100> <output_dir>", file=sys.stderr)
        return 2

    query = sys.argv[1]
    max_str = sys.argv[2]
    out_dir = sys.argv[3]

    if is_int_string(max_str) == False:
        print("Error: max_results must be an integer", file=sys.stderr)
        return 2

    max_results = int(max_str)
    if max_results < 1 or max_results > 100:
        print("Error: max_results must be between 1 and 100", file=sys.stderr)
        return 2

    make_dir(out_dir)

    papers_path = os.path.join(out_dir, "papers.json")
    corpus_path = os.path.join(out_dir, "corpus_analysis.json")
    log_path = os.path.join(out_dir, "processing.log")

    try:
        if os.path.exists(log_path):
            os.remove(log_path)
    except Exception:
        pass

    log_line(log_path, "Starting ArXiv query: " + query)

    url = build_query_url(query, max_results)
    start = time.perf_counter()

    try:
        xml_bytes = fetch_xml(url, log_path)
    except Exception as e:
        log_line(log_path, "Network error contacting ArXiv API: " + type(e).__name__ + ": " + str(e))
        return 1

    try:
        papers = parse_feed(xml_bytes, log_path)
        log_line(log_path, "Fetched " + str(len(papers)) + " results from ArXiv API")
    except Exception as e:
        log_line(log_path, "Invalid XML from ArXiv API: " + type(e).__name__ + ": " + str(e))
        papers = []

    i = 0
    while i < len(papers):
        log_line(log_path, "Processing paper: " + papers[i]["arxiv_id"])
        i += 1

    elapsed = time.perf_counter() - start
    log_line(
        log_path,
        "Completed processing: " + str(len(papers)) + " papers in " + ("%.2f" % elapsed) + " seconds"
    )

    write_json(papers_path, papers)
    write_json(corpus_path, build_corpus_analysis(papers, query))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
