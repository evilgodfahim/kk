# scrape.py
import feedparser
import xml.etree.ElementTree as ET
import os
from datetime import datetime
import calendar
import email.utils
import time

SRC = "https://www.kalerkantho.com/rss.xml"
FILES = {
    "opinion": "opinion.xml",
    "world": "world.xml",
    "print_parts": ["daily_kalerkantho_part1.xml", "daily_kalerkantho_part2.xml"]
}

def load_existing(path):
    if not os.path.exists(path):
        root = ET.Element("rss", version="2.0")
        ET.SubElement(root, "channel")
        return root
    return ET.parse(path).getroot()

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
    existing_map = {}
    for item in channel.findall("item"):
        link_text = item.findtext("link")
        if link_text:
            existing_map[link_text] = item

    for entry in entries:
        link = getattr(entry, "link", None) or getattr(entry, "id", None)
        if not link:
            continue
        link = link.strip()
        incoming_dt = get_entry_pubdt(entry)

        if link in existing_map:
            item = existing_map[link]
            existing_dt = get_item_pubdt(item)
            if incoming_dt > existing_dt:
                t = item.find("title")
                if t is None:
                    t = ET.SubElement(item, "title")
                t.text = getattr(entry, "title", t.text)

                pd = item.find("pubDate")
                if pd is None:
                    pd = ET.SubElement(item, "pubDate")
                pd.text = getattr(entry, "published", format_pubdate(incoming_dt))

                g = item.find("guid")
                if g is None:
                    g = ET.SubElement(item, "guid", isPermaLink="false")
                g.text = link

                channel.remove(item)
                channel.insert(0, item)
        else:
            item = ET.Element("item")
            ET.SubElement(item, "title").text = getattr(entry, "title", "")
            ET.SubElement(item, "link").text = link
            ET.SubElement(item, "pubDate").text = getattr(entry, "published", format_pubdate(incoming_dt))
            ET.SubElement(item, "guid", isPermaLink="false").text = link
            channel.insert(0, item)
            existing_map[link] = item

    all_items = channel.findall("item")
    if len(all_items) > 500:
        for extra in all_items[500:]:
            channel.remove(extra)

# ---------------------------
# FIXED PRINT EDITION LOGIC
# ---------------------------
def add_items_print(entries, paths):
    seen = {}  # link -> dict{title, pubDate}

    # load all existing parts into one map
    for p in paths:
        if not os.path.exists(p):
            continue
        root = ET.parse(p).getroot()
        channel = root.find("channel")
        if not channel:
            continue
        for item in channel.findall("item"):
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
            if link in seen:
                if pd > seen[link]["pubDate"]:
                    seen[link] = {"title": title, "pubDate": pd}
            else:
                seen[link] = {"title": title, "pubDate": pd}

    # merge new entries
    for entry in entries:
        link = (getattr(entry, "link", None) or getattr(entry, "id", None) or "").strip()
        if not link:
            continue
        pd = get_entry_pubdt(entry)
        title = getattr(entry, "title", "")
        if link in seen:
            if pd > seen[link]["pubDate"]:
                seen[link] = {"title": title, "pubDate": pd}
        else:
            seen[link] = {"title": title, "pubDate": pd}

    # sort newest first and cap 500
    items = sorted(
        [{"link": k, "title": v["title"], "pubDate": v["pubDate"]} for k, v in seen.items()],
        key=lambda x: x["pubDate"],
        reverse=True
    )[:500]

    # 100-sized chunks
    chunks = [items[i:i+100] for i in range(0, len(items), 100)]

    # write chunks back
    for i, chunk in enumerate(chunks):
        path = paths[i] if i < len(paths) else f"daily_kalerkantho_part{i+1}.xml"
        rss_root = ET.Element("rss", version="2.0")
        channel = ET.SubElement(rss_root, "channel")
        for it in chunk:
            item = ET.SubElement(channel, "item")
            ET.SubElement(item, "title").text = it["title"]
            ET.SubElement(item, "link").text = it["link"]
            ET.SubElement(item, "pubDate").text = format_pubdate(it["pubDate"])
            ET.SubElement(item, "guid", isPermaLink="false").text = it["link"]
        ET.ElementTree(rss_root).write(path, encoding="utf-8", xml_declaration=True)

    # remove extra old part files
    for j in range(len(chunks), len(paths)):
        if os.path.exists(paths[j]):
            os.remove(paths[j])

# Main
feed = feedparser.parse(SRC)

op_root = load_existing(FILES["opinion"])
op_entries = [e for e in feed.entries if any(x in ((getattr(e,"link",None) or getattr(e,"id",None) or "").strip()) for x in ["/opinion/","/editorial/","/sub-editorial/"])]
merge_update_feed(op_root, op_entries)
ET.ElementTree(op_root).write(FILES["opinion"], encoding="utf-8", xml_declaration=True)

wr_root = load_existing(FILES["world"])
wr_entries = [e for e in feed.entries if ("/world/" in ((getattr(e,"link",None) or getattr(e,"id",None) or "").strip()) or "/deshe-deshe/" in ((getattr(e,"link",None) or getattr(e,"id",None) or "").strip()))]
merge_update_feed(wr_root, wr_entries)
ET.ElementTree(wr_root).write(FILES["world"], encoding="utf-8", xml_declaration=True)

print_entries = [e for e in feed.entries if "/print-edition/" in ((getattr(e,"link",None) or getattr(e,"id",None) or "").strip())]
add_items_print(print_entries, FILES["print_parts"])