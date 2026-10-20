#!/usr/bin/env python3
"""Checks the repository's Markdown: relative links and images exist, heading
anchors resolve (GitHub's slug rules), SVG files are well-formed XML, and
Mermaid blocks start with a known diagram type and have balanced brackets.

    python3 scripts/check-docs.py

Standard library only; exits 1 on any problem. External (http) links are not
fetched.
"""
import os
import re
import subprocess
import sys
import xml.dom.minidom

ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
errors = []


def tracked(ext):
    out = subprocess.run(["git", "ls-files", "-co", "--exclude-standard"], cwd=ROOT, capture_output=True, text=True, check=True).stdout
    return [f for f in out.split("\n") if f.endswith(ext) and os.path.exists(os.path.join(ROOT, f))]


def slug(heading):
    h = re.sub(r"`", "", heading.strip().lower())
    h = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", h)  # links keep their text
    h = re.sub(r"[^\w\- ]", "", h, flags=re.UNICODE)
    return h.replace(" ", "-")


def anchors(path):
    seen, out = {}, set()
    in_code = False
    with open(os.path.join(ROOT, path), encoding="utf-8") as f:
        for line in f:
            if line.startswith("```"):
                in_code = not in_code
            if in_code:
                continue
            m = re.match(r"^#{1,6}\s+(.*?)\s*#*\s*$", line)
            if m:
                s = slug(m.group(1))
                n = seen.get(s, 0)
                seen[s] = n + 1
                out.add(s if n == 0 else f"{s}-{n}")
    return out


anchor_cache = {}


def has_anchor(path, anchor):
    if path not in anchor_cache:
        anchor_cache[path] = anchors(path)
    return anchor in anchor_cache[path]


LINK = re.compile(r"!?\[[^\]]*\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
HTML_SRC = re.compile(r"""<(?:img|a)\b[^>]*?\b(?:src|href)=["']([^"']+)["']""")

for md in tracked(".md"):
    base = os.path.dirname(md)
    text = open(os.path.join(ROOT, md), encoding="utf-8").read()
    # drop fenced code so example links in shell snippets are not checked
    body = re.sub(r"```.*?```", "", text, flags=re.S)
    targets = LINK.findall(body) + HTML_SRC.findall(body)
    for t in targets:
        if re.match(r"^(https?:|mailto:|data:)", t):
            continue
        path, _, anchor = t.partition("#")
        if path == "":
            target = md
        else:
            target = os.path.normpath(os.path.join(base, path))
            if not os.path.exists(os.path.join(ROOT, target)):
                errors.append(f"{md}: broken link or image: {t}")
                continue
        if anchor and target.endswith(".md") and not has_anchor(target, anchor):
            errors.append(f"{md}: no heading for anchor: {t}")
    for i, block in enumerate(re.findall(r"```mermaid\n(.*?)```", text, flags=re.S), 1):
        first = block.strip().split("\n")[0].split()[0]
        if first not in ("flowchart", "graph", "sequenceDiagram", "stateDiagram-v2", "classDiagram", "erDiagram"):
            errors.append(f"{md}: mermaid block {i} starts with unknown diagram type {first!r}")
        for o, c in ("()", "[]", "{}"):
            if block.count(o) != block.count(c):
                errors.append(f"{md}: mermaid block {i}: unbalanced {o}{c}")

for svg in tracked(".svg"):
    try:
        xml.dom.minidom.parse(os.path.join(ROOT, svg))
    except Exception as e:  # noqa: BLE001
        errors.append(f"{svg}: not well-formed XML: {e}")

if errors:
    print("\n".join(errors))
    sys.exit(1)
print(f"docs ok: {len(tracked('.md'))} Markdown files, {len(tracked('.svg'))} SVG files")
