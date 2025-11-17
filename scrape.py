import feedparser import xml.etree.ElementTree as ET import os from datetime import datetime import calendar import email.utils import json

SRC = "https://www.kalerkantho.com/rss.xml" FILES = { "opinion": "opinion.xml", "world": "world.xml", "print_parts": ["daily_kalerkantho_part1.xml", "daily_kalerkantho_part2.xml"] }

-----------------------------

Utility

-----------------------------

def load_existing(path): root = ET.Element("rss", version="2.0") ET.SubElement(root, "channel") return root

def format_pubdate(dt): return dt.strftime("%a, %d %b %Y %H:%M:%S GMT")

def parse_struct_time(st): return datetime.utcfromtimestamp(calendar.timegm(st))

def get_entry_pubdt(entry): pp = getattr(entry, "published_parsed", None) if pp: try: return parse_struct_time(pp) except: pass ps = getattr(entry, "published", None) if ps: try: return email.utils.parsedate_to_datetime(ps).replace(tzinfo=None) except: pass return datetime.utcnow()

def get_item_pubdt(item): txt = item.findtext("pubDate") if not txt: return datetime.min try: return email.utils.parsedate_to_datetime(txt).replace(tzinfo=None) except: try: return datetime.strptime(txt, "%a, %d %b %Y %H:%M:%S GMT") except: return datetime.min

def merge_update_feed(root, entries): channel = root.find("channel") existing = {}

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

# Cap to 500 (unchanged)
all_items = channel.findall("item")
for extra in all_items[500:]:
    channel.remove(extra)

-----------------------------

PRINT FEED LOGIC (strict 100 + overflow)

-----------------------------

def normalize_link(link): if not link: return "" link = link.strip() link = link.split("?",1)[0].split("#",1)[0] return link.rstrip("/")

def add_items_print(entries, paths): seen = {}

# merge new entries — no old-file loading; overwrite behavior enforced
for entry in entries:
    raw = getattr(entry, "link", None) or getattr(entry, "id", None) or ""
    link = normalize_link(raw)
    if not link:
        continue
    pd = get_entry_pubdt(entry)
    title = getattr(entry, "title", "")
    if link not in seen or pd > seen[link]["pubDate"]:
        seen[link] = {"title": title, "pubDate": pd}

# newest first
items = sorted(
    [{"link": k, "title": v["title"], "pubDate": v["pubDate"]} for k,v in seen.items()],
    key=lambda x: x["pubDate"], reverse=True
)

# split into first 100 and overflow
part1 = items[:100]
part2 = items[100:200]

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

# always overwrite
write_part(paths[0], part1)
write_part(paths[1], part2)

-----------------------------

MAIN

-----------------------------

feed = feedparser.parse(SRC)

opinion

op_root = load_existing(FILES["opinion"]) op_entries = [e for e in feed.entries if any(x in (getattr(e,"link","") or "") for x in ["/opinion/","/editorial/","/sub-editorial/"])] merge_update_feed(op_root, op_entries) ET.ElementTree(op_root).write(FILES["opinion"], encoding="utf-8", xml_declaration=True)

world

wr_root = load_existing(FILES["world"]) wr_entries = [e for e in feed.entries if any(x in (getattr(e,"link","") or "") for x in ["/world/","/deshe-deshe/"])] merge_update_feed(wr_root, wr_entries) ET.ElementTree(wr_root).write(FILES["world"], encoding="utf-8", xml_declaration=True)

print edition

print_entries = [e for e in feed.entries if "/print-edition/" in (getattr(e,"link","") or "")] add_items_print(print_entries, FILES["print_parts"])