# scrape.py
import feedparser
import xml.etree.ElementTree as ET
import os
import glob
from datetime import datetime
import calendar
import email.utils

SRC = "https://www.kalerkantho.com/rss.xml"
FILES = {
    "opinion": "opinion.xml",
    "world": "world.xml",
}
PRINT_PREFIX = "daily_kalerkantho_part"
CHUNK_SIZE = 100
MAX_ITEMS = None  # None => no cap; set to int to cap total across parts

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
        except Exception:
            pass
    ps = getattr(entry, "published", None)
    if ps:
        try:
            return email.utils.parsedate_to_datetime(ps).replace(tzinfo=None)
        except Exception:
            pass
    return datetime.utcnow()

def get_item_pubdt(item):
    txt = item.findtext("pubDate")
    if not txt:
        return datetime.min
    try:
        return email.utils.parsedate_to_datetime(txt).replace(tzinfo=None)
    except Exception:
        try:
            return datetime.strptime(txt, "%a, %d %b %Y %H:%M:%S GMT")
        except Exception:
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
                pd_text = getattr(entry, "published", None)
                pd.text = pd_text if pd_text else format_pubdate(incoming_dt)

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
            pd_text = getattr(entry, "published", None)
            ET.SubElement(item, "pubDate").text = pd_text if pd_text else format_pubdate(incoming_dt)
            ET.SubElement(item, "guid", isPermaLink="false").text = link
            channel.insert(0, item)
            existing_map[link] = item

    all_items = channel.findall("item")
    if len(all_items) > 500:
        for extra in all_items[500:]:
            channel.remove(extra)

def process_print_feed(entries, prefix=PRINT_PREFIX, chunk_size=CHUNK_SIZE, max_items=MAX_ITEMS):
    # Collect existing items from any existing part files
    merged = {}  # link -> {"title":..., "pubDate": datetime, "guid":...}
    existing_files = sorted(glob.glob(f"{prefix}*.xml"))
    for path in existing_files:
        try:
            root = ET.parse(path).getroot()
        except Exception:
            continue
        channel = root.find("channel")
        if channel is None:
            continue
        for item in channel.findall("item"):
            link = item.findtext("link")
            if not link:
                continue
            link = link.strip()
            title = item.findtext("title") or ""
            pd_text = item.findtext("pubDate")
            pd = datetime.min
            if pd_text:
                try:
                    pd = email.utils.parsedate_to_datetime(pd_text).replace(tzinfo=None)
                except Exception:
                    try:
                        pd = datetime.strptime(pd_text, "%a, %d %b %Y %H:%M:%S GMT")
                    except Exception:
                        pd = datetime.min
            merged[link] = {"title": title, "pubDate": pd, "guid": link}

    # Incorporate incoming entries, update if newer
    for entry in entries:
        link = getattr(entry, "link", None) or getattr(entry, "id", None)
        if not link:
            continue
        link = link.strip()
        incoming_dt = get_entry_pubdt(entry)
        incoming_title = getattr(entry, "title", "") or ""
        if link in merged:
            if incoming_dt > merged[link]["pubDate"]:
                merged[link] = {"title": incoming_title, "pubDate": incoming_dt, "guid": link}
        else:
            merged[link] = {"title": incoming_title, "pubDate": incoming_dt, "guid": link}

    # Build sorted newest-first list
    items_list = sorted(
        ({"link": k, "title": v["title"], "pubDate": v["pubDate"]} for k, v in merged.items()),
        key=lambda x: x["pubDate"], reverse=True
    )

    # Apply max_items cap if set
    if isinstance(max_items, int) and max_items > 0:
        items_list = items_list[:max_items]

    # Split into chunks
    chunks = [items_list[i:i+chunk_size] for i in range(0, len(items_list), chunk_size)]

    # Write chunks to files (overwrite)
    written_paths = []
    for i, chunk in enumerate(chunks):
        path = f"{prefix}{i+1}.xml"
        rss_root = ET.Element("rss", version="2.0")
        channel = ET.SubElement(rss_root, "channel")
        for it in chunk:
            item = ET.SubElement(channel, "item")
            ET.SubElement(item, "title").text = it["title"]
            ET.SubElement(item, "link").text = it["link"]
            ET.SubElement(item, "pubDate").text = format_pubdate(it["pubDate"])
            ET.SubElement(item, "guid", isPermaLink="false").text = it["link"]
        ET.ElementTree(rss_root).write(path, encoding="utf-8", xml_declaration=True)
        written_paths.append(path)

    # Remove old part files that are no longer needed
    for path in existing_files:
        if path not in written_paths:
            try:
                os.remove(path)
            except Exception:
                pass

    return written_paths

# Main
feed = feedparser.parse(SRC)

# opinion (unchanged logic)
op_root = load_existing(FILES["opinion"])
op_entries = [
    e for e in feed.entries
    if any(x in ((getattr(e, "link", None) or getattr(e, "id", None) or "").strip()) for x in ["/opinion/", "/editorial/", "/sub-editorial/"])
]
merge_update_feed(op_root, op_entries)
ET.ElementTree(op_root).write(FILES["opinion"], encoding="utf-8", xml_declaration=True)

# world (unchanged logic)
wr_root = load_existing(FILES["world"])
wr_entries = [
    e for e in feed.entries
    if ("/world/" in ((getattr(e, "link", None) or getattr(e, "id", None) or "").strip()) or "/deshe-deshe/" in ((getattr(e, "link", None) or getattr(e, "id", None) or "").strip()))
]
merge_update_feed(wr_root, wr_entries)
ET.ElementTree(wr_root).write(FILES["world"], encoding="utf-8", xml_declaration=True)

# print: all /print-edition/ entries -> dynamic parts of CHUNK_SIZE
print_entries = [e for e in feed.entries if "/print-edition/" in ((getattr(e, "link", None) or getattr(e, "id", None) or "").strip())]
written = process_print_feed(print_entries, prefix=PRINT_PREFIX, chunk_size=CHUNK_SIZE, max_items=MAX_ITEMS)
print("Written print parts:", written)