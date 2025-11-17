import feedparser
import xml.etree.ElementTree as ET
import os
from datetime import datetime
import calendar
import email.utils
import json

SRC = "https://www.kalerkantho.com/rss.xml"
FILES = {
    "opinion": "opinion.xml",
    "world": "world.xml",
    "print_parts": ["daily_kalerkantho_part1.xml", "daily_kalerkantho_part2.xml"]
}

# -----------------------------
# Utility
# -----------------------------
def load_existing(path):
    root = ET.Element("rss", version="2.0")
    ET.SubElement(root, "channel")
    return root

def format_pubdate(dt):
    return dt.strftime("%a, %d %b %Y %H:%M:%S GMT")

def parse_struct_time(st):
    return datetime.utcfromtimestamp(calendar.timegm(st))

def get_entry_pubdt(entry):
    pp = getattr(entry, "published_parsed", None)
    if pp:
        try:
            return parse_struct_time(pp)
        except:
            pass
    ps = getattr(entry, "published", None)
    if ps:
        try:
            return email.utils.parsedate_to_datetime(ps).replace(tzinfo=None)
        except:
            pass
    return datetime.utcnow()

def get_item_pubdt(item):
    txt = item.findtext("pubDate")
    if not txt:
        return datetime.min
    try:
        return email.utils.parsedate_to_datetime(txt).replace(tzinfo=None)
    except:
        try:
            return datetime.strptime(txt, "%a, %d %b %Y %H:%M:%S GMT")
        except:
            return datetime.min

def merge_update_feed(root, entries):
    channel = root.find("channel")
    existing = {}

    for item in channel.findall("item"):
        link = item.findtext("link")
        if link:
            existing[link] = item

    for entry in entries:
        link = getattr(entry, "link", None) or getattr(entry, "id", None)
        if not link:
            continue
        link = link.strip()
        incoming_dt = get_entry_pubdt(entry)

        if link in existing:
            item = existing[link]
            if incoming_dt > get_item_pubdt(item):
                item.find("title").text = getattr(entry, "title", item.find("title").text)
                item.find("pubDate").text = getattr(entry, "published", format_pubdate(incoming_dt))
                item.find("guid").text = link
                channel.remove(item)
                channel.insert(0, item)
        else:
            item = ET.Element("item")
            ET.SubElement(item, "title").text = getattr(entry, "title", "")
            ET.SubElement(item, "link").text = link
            ET_Sub = ET.SubElement(item, "pubDate")
            ET_Sub.text = getattr(entry, "published", format_pubdate(incoming_dt))
            ET.SubElement(item, "guid", isPermaLink="false").text = link
            channel.insert(0, item)
            existing[link] = item

    # Cap at 500 items
    all_items = channel.findall("item")
    for extra in all_items[500:]:
        channel.remove(extra)

# -----------------------------
# Print edition logic
# -----------------------------
def normalize_link(link):
    if not link:
        return ""
    link = link.strip()
    link = link.split("?",1)[0].split("#",1)[0]
    return link.rstrip("/")

def add_items_print(entries, paths):
    seen = {}

    # Load existing items from both xmls
    for p in paths:
        if not os.path.exists(p):
            continue
        root = ET.parse(p).getroot()
        ch = root.find("channel")
        if not ch:
            continue
        for item in ch.findall("item"):
            link = (item.findtext("link") or "").strip()
            if not link:
                continue
            title = item.findtext("title") or ""
            pd_text = item.findtext("pubDate") or ""
            try:
                pd = email.utils.parsedate_to_datetime(pd_text).replace(tzinfo=None)
            except:
                try:
                    pd = datetime.strptime(pd_text, "%a, %d %b %Y %H:%M:%S GMT")
                except:
                    pd = datetime.min
            if link not in seen or pd > seen[link]["pubDate"]:
                seen[link] = {"title": title, "pubDate": pd}

    # Merge new entries
    for e in entries:
        raw = getattr(e, "link", None) or getattr(e, "id", None) or ""
        link = normalize_link(raw)
        if not link:
            continue
        pd = get_entry_pubdt(e)
        title = getattr(e, "title", "")
        if link not in seen or pd > seen[link]["pubDate"]:
            seen[link] = {"title": title, "pubDate": pd}

    # Sort newest first, cap 500
    items = sorted(
        [{"link": k, "title": v["title"], "pubDate": v["pubDate"]} for k,v in seen.items()],
        key=lambda x: x["pubDate"], reverse=True
    )[:500]

    # Split: first 100 strictly in part1, remaining in part2 (any count)
    part1 = items[:100]
    part2 = items[100:]

    def write_part(path, chunk):
        root = ET.Element("rss", version="2.0")
        ch = ET.SubElement(root, "channel")
        for it in chunk:
            item = ET.SubElement(ch, "item")
            ET.SubElement(item, "title").text = it["title"]
            ET.SubElement(item, "link").text = it["link"]
            ET.SubElement(item, "pubDate").text = format_pubdate(it["pubDate"])
            ET.SubElement(item, "guid", isPermaLink="false").text = it["link"]
        ET.ElementTree(root).write(path, encoding="utf-8", xml_declaration=True)

    write_part(paths[0], part1)
    write_part(paths[1], part2)

    # Remove extra old part files if any
    for j in range(2, len(paths)):
        if os.path.exists(paths[j]):
            os.remove(paths[j])

# -----------------------------
# MAIN
# -----------------------------
feed = feedparser.parse(SRC)

# Collect all links in print editions
op_print_links = set()
for p in FILES["print_parts"]:
    if os.path.exists(p):
        root_tmp = ET.parse(p).getroot()
        ch_tmp = root_tmp.find("channel")
        if ch_tmp:
            for it in ch_tmp.findall("item"):
                ln = it.findtext("link")
                if ln:
                    op_print_links.add(ln.strip())

# Opinion feed (exclude print edition links)
op_root = load_existing(FILES["opinion"])
op_entries = [
    e for e in feed.entries
    if any(x in (getattr(e, "link", "") or "") for x in ["/opinion/","/editorial/","/sub-editorial/"])
    and (getattr(e, "link", "") or "").strip() not in op_print_links
]
merge_update_feed(op_root, op_entries)
ET.ElementTree(op_root).write(FILES["opinion"], encoding="utf-8", xml_declaration=True)

# World feed (exclude print edition links)
wr_root = load_existing(FILES["world"])
wr_entries = [
    e for e in feed.entries
    if any(x in (getattr(e, "link", "") or "") for x in ["/world/","/deshe-deshe/"])
    and (getattr(e, "link", "") or "").strip() not in op_print_links
]
merge_update_feed(wr_root, wr_entries)
ET.ElementTree(wr_root).write(FILES["world"], encoding="utf-8", xml_declaration=True)

# Print edition
print_entries = [e for e in feed.entries if "/print-edition/" in (getattr(e,"link","") or "")]
add_items_print(print_entries, FILES["print_parts"])