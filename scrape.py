#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scrape.py — Kaler Kantho opinion section scraper.
Fetches https://www.kalerkantho.com/online/opinion via FlareSolverr (or
direct requests as fallback), parses article cards, and merges results
into opinion.xml (RSS 2.0).
"""

import os
import re
import xml.etree.ElementTree as ET
from datetime import datetime

import requests
from bs4 import BeautifulSoup

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────
OPINION_URL       = "https://www.kalerkantho.com/online/opinion"
BASE_URL          = "https://www.kalerkantho.com"
OUTPUT_FILE       = "opinion.xml"
MAX_ITEMS         = 500

FLARE_URL         = os.environ.get("FLARE_URL", "")
FLARE_API_KEY     = os.environ.get("FLARE_API_KEY", "")
FLARE_SESSION     = os.environ.get("FLARE_SESSION", "")
FLARE_MAX_TIMEOUT = int(os.environ.get("FLARE_MAX_TIMEOUT", "60000"))
FLARE_WAIT_MS     = int(os.environ.get("FLARE_WAIT_MS", "5000"))

# ─────────────────────────────────────────
# BENGALI DATE PARSING
# ─────────────────────────────────────────
_BN_DIGITS = str.maketrans("০১২৩৪৫৬৭৮৯", "0123456789")
_BN_MONTHS = {
    "জানুয়ারি":  1,  "ফেব্রুয়ারি": 2,  "মার্চ":      3,
    "এপ্রিল":    4,  "মে":          5,  "জুন":        6,
    "জুলাই":     7,  "আগস্ট":       8,  "সেপ্টেম্বর": 9,
    "অক্টোবর":  10,  "নভেম্বর":    11,  "ডিসেম্বর":  12,
}

def parse_bn_date(text):
    """
    Parse Bengali date string into a naive datetime (treated as local/Dhaka time).

    Examples:
      '২১ আগস্ট, ২০২৬ ২১:৩২'  ->  datetime(2026, 8, 21, 21, 32)
      '১৬ আগস্ট, ২০২৬ ০৮:০৪'  ->  datetime(2026, 8, 16,  8,  4)
    """
    if not text:
        return None
    # Translate Bengali digits -> ASCII; month name letters are unaffected
    translated = text.strip().translate(_BN_DIGITS)
    # Result looks like: "21 আগস্ট, 2026 21:32"
    m = re.match(r"(\d+)\s+([^,\s]+),\s*(\d{4})\s+(\d{1,2}):(\d{2})", translated)
    if not m:
        return None
    day, month_bn, year, hour, minute = m.groups()
    month = _BN_MONTHS.get(month_bn)
    if not month:
        return None
    try:
        return datetime(int(year), month, int(day), int(hour), int(minute))
    except ValueError:
        return None


def date_from_url(url):
    """
    Extract a date-only datetime from a URL of the form
    /online/opinion/2026/08/19/1727793  ->  datetime(2026, 8, 19)
    Returns datetime.min on failure.
    """
    m = re.search(r"/(\d{4})/(\d{2})/(\d{2})/", url)
    if m:
        try:
            return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass
    return datetime.min


# ─────────────────────────────────────────
# FETCH
# ─────────────────────────────────────────
def _fetch_via_flare(url):
    """Send a request.get command to FlareSolverr and return the HTML."""
    effective_timeout = max(FLARE_MAX_TIMEOUT, FLARE_WAIT_MS + 15_000)

    payload = {
        "cmd":        "request.get",
        "url":        url,
        "maxTimeout": effective_timeout,
        "waitTime":   FLARE_WAIT_MS,   # idle ms after page load before capture
    }
    if FLARE_SESSION:
        payload["session"] = FLARE_SESSION

    headers = {"Content-Type": "application/json"}
    if FLARE_API_KEY:
        headers["X-Api-Key"] = FLARE_API_KEY

    resp = requests.post(
        FLARE_URL,
        json=payload,
        headers=headers,
        timeout=(10, effective_timeout // 1000 + 10),
    )
    resp.raise_for_status()
    data = resp.json()

    sol  = data.get("solution") or {}
    html = (
        sol.get("response")
        or sol.get("html")
        or data.get("response")
        or data.get("html")
    )
    if not html:
        raise RuntimeError(f"FlareSolverr returned no HTML for {url}: {data}")
    return html


def fetch_html(url):
    """Fetch page HTML. Uses FlareSolverr when FLARE_URL is set, else direct GET."""
    if FLARE_URL:
        return _fetch_via_flare(url)

    r = requests.get(
        url,
        timeout=(5, 30),
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        },
    )
    r.raise_for_status()
    return r.text


# ─────────────────────────────────────────
# PARSE
# ─────────────────────────────────────────
def parse_articles(html):
    """
    Parse opinion article cards from the KK opinion page.

    Card structure (div.row.position-relative):
      <img alt="TITLE" src="IMAGE_URL" ...>
      <h1|h3>TITLE</h1|h3>               <- fallback for title
      <p class="homeSubDesc">EXCERPT</p>  <- lead card only
      <div class="text-muted small ...">EXCERPT</div>
      <small class="text-muted">BENGALI DATE</small>
      <a class="stretched-link" href="/online/opinion/YYYY/MM/DD/ID"></a>
    """
    soup     = BeautifulSoup(html, "html.parser")
    articles = []
    seen     = set()

    cards = soup.find_all("div", class_=lambda c: c and "position-relative" in c)

    for card in cards:
        # link
        link_tag = card.find("a", class_="stretched-link")
        if not link_tag:
            continue
        href = link_tag.get("href", "").strip()
        if "/online/opinion/" not in href:
            continue
        full_url = (BASE_URL + href) if href.startswith("/") else href
        if full_url in seen:
            continue
        seen.add(full_url)

        # title: prefer img alt, fall back to heading text
        img   = card.find("img")
        title = (img.get("alt", "").strip() if img else "")
        if not title:
            for tag in ("h1", "h2", "h3"):
                h = card.find(tag)
                if h:
                    title = h.get_text(strip=True)
                    break

        # description
        desc = ""
        p    = card.find("p", class_="homeSubDesc")
        if p:
            desc = p.get_text(strip=True)
        else:
            d = card.find(
                "div",
                class_=lambda c: c and "text-muted" in c and "small" in c,
            )
            if d:
                desc = d.get_text(strip=True)

        # publication date
        pub_dt = None
        sm = card.find("small", class_="text-muted")
        if sm:
            pub_dt = parse_bn_date(sm.get_text(strip=True))
        if pub_dt is None:
            # Lead card has no <small>; extract date from URL
            pub_dt = date_from_url(href)

        # image
        img_src = (img.get("src", "").strip() if img else "")

        articles.append({
            "title":   title,
            "link":    full_url,
            "desc":    desc,
            "pub_dt":  pub_dt,
            "img_src": img_src,
        })

    return articles


# ─────────────────────────────────────────
# RSS HELPERS
# ─────────────────────────────────────────
def format_pubdate(dt):
    if not isinstance(dt, datetime) or dt == datetime.min:
        return "Thu, 01 Jan 1970 00:00:00 GMT"
    return dt.strftime("%a, %d %b %Y %H:%M:%S GMT")


def load_existing(path):
    """Load existing RSS file, or create a blank rss/channel skeleton."""
    if os.path.exists(path):
        try:
            return ET.parse(path).getroot()
        except Exception:
            pass
    root = ET.Element("rss", version="2.0")
    ET.SubElement(root, "channel")
    return root


def merge_articles(root, articles):
    """
    Merge newly scraped articles into the existing RSS channel.
    - New articles are inserted at the top.
    - Existing articles get their pubDate updated if we scraped a newer one
      (e.g. lead card has a precise time; a previous run may have only had
      the URL date with no time component).
    - Channel is capped at MAX_ITEMS, dropping oldest entries.
    """
    channel = root.find("channel")

    # Index existing items by link for O(1) lookup
    existing = {}
    for item in channel.findall("item"):
        lnk = (item.findtext("link") or "").strip()
        if lnk:
            existing[lnk] = item

    for art in articles:
        link = art["link"]

        if link in existing:
            # Update pubDate only if the new one is strictly better
            item   = existing[link]
            old_pd = item.findtext("pubDate") or ""
            try:
                old_dt = datetime.strptime(old_pd, "%a, %d %b %Y %H:%M:%S GMT")
            except ValueError:
                old_dt = datetime.min

            if art["pub_dt"] and art["pub_dt"] > old_dt:
                pd_el = item.find("pubDate")
                if pd_el is not None:
                    pd_el.text = format_pubdate(art["pub_dt"])
                # Re-insert at top to keep channel sorted by recency
                channel.remove(item)
                channel.insert(0, item)
        else:
            item = ET.Element("item")
            ET.SubElement(item, "title").text       = art["title"]
            ET.SubElement(item, "link").text        = link
            ET.SubElement(item, "description").text = art["desc"]
            ET.SubElement(item, "pubDate").text     = format_pubdate(art["pub_dt"])
            ET.SubElement(item, "guid", isPermaLink="true").text = link
            if art["img_src"]:
                enc = ET.SubElement(item, "enclosure")
                enc.set("url",    art["img_src"])
                enc.set("type",   "image/jpeg")
                enc.set("length", "0")
            channel.insert(0, item)
            existing[link] = item

    # Enforce cap — drop oldest (tail) items
    all_items = channel.findall("item")
    for extra in all_items[MAX_ITEMS:]:
        channel.remove(extra)


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
def main():
    print(f"Fetching {OPINION_URL} ...")
    html = fetch_html(OPINION_URL)

    articles = parse_articles(html)
    print(f"Parsed {len(articles)} opinion articles")

    root = load_existing(OUTPUT_FILE)
    merge_articles(root, articles)

    ET.ElementTree(root).write(OUTPUT_FILE, encoding="utf-8", xml_declaration=True)
    print(f"Saved -> {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
