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
    # print handled as parts
    "print_parts": ["daily_kalerkantho_part1.xml", "daily_kalerkantho_part2.xml"]
}

def load_existing(path):
    if not os.path.exists(path):
        root = ET.Element("rss", version="2.0")
        ET.SubElement(root, "channel")
        return root
    return ET.parse(path).getroot()

def format_pubdate(dt):
    # RFC-822 style GMT string
    return dt.strftime("%a, %d %b %Y %H:%M:%S GMT")

def parse_struct_time(st):
    # st is time.struct_time (UTC) from feedparser.published_parsed
    return datetime.utcfromtimestamp(calendar.timegm(st))

def get_entry_pubdt(entry):
    # Prefer published_parsed, then published string, else now
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
            # fallback parse common format
            return datetime.strptime(txt, "%a, %d %b %Y %H:%M:%S GMT")
        except Exception:
            return datetime.min

def merge_update_feed(root, entries):
    """
    Update existing XML (opinion/world). Behavior:
      - If incoming link not present => insert new item at top.
      - If incoming link present and incoming pubDate > existing pubDate => update title/pubDate/guid and move to top.
      - Otherwise skip.
      - Keep max 500 items.
    """
    channel = root.find("channel")
    # map link -> item element
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
                # update fields
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

                # move to top
                channel.remove(item)
                channel.insert(0, item)
            # else: existing is newer or same -> do nothing
        else:
            # create new item and insert at top
            item = ET.Element("item")
            ET.SubElement(item, "title").text = getattr(entry, "title", "")
            ET.SubElement(item, "link").text = link
            ET.SubElement(item, "pubDate").text = getattr(entry, "published", format_pubdate(incoming_dt))
            ET.SubElement(item, "guid", isPermaLink="false").text = link
            channel.insert(0, item)
            existing_map[link] = item

    # enforce max 500
    all_items = channel.findall("item")
    if len(all_items) > 500:
        for extra in all_items[500:]:
            channel.remove(extra)

def add_items_print(entries, paths):
    """
    Merge incoming print entries with existing part files, avoid duplicates across parts,
    update entries when incoming pubDate is newer, then re-chunk newest-first into 100-item parts.
    Total items capped at 500 across all parts.
    """
    # build map from existing part files
    merged = {}  # link -> dict{title, pubDate(datetime), guid}
    for path in paths:
        if not os.path.exists(path):
            continue
        root = ET.parse(path).getroot()
        channel = root.find("channel")
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

    # incorporate incoming entries, updating if newer
    for entry in entries:
        link = getattr(entry, "link", None) or getattr(entry, "id", None)
        if not link:
            continue
        link = link.strip()
        incoming_dt = get_entry_pubdt(entry)
        incoming_title = getattr(entry, "title", "")
        if link in merged:
            if incoming_dt > merged[link]["pubDate"]:
                merged[link] = {"title": incoming_title, "pubDate": incoming_dt, "guid": link}
        else:
            merged[link] = {"title": incoming_title, "pubDate": incoming_dt, "guid": link}

    # create list sorted newest-first
    items_list = sorted(
        [{"link": k, "title": v["title"], "pubDate": v["pubDate"]} for k, v in merged.items()],
        key=lambda x: x["pubDate"], reverse=True
    )

    # cap total to 500
    items_list = items_list[:500]

    # split into 100-sized chunks
    chunks = [items_list[i:i+100] for i in range(0, len(items_list), 100)]

    # write chunks to files (overwrite)
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

    # if previously there were more part files than current chunks, remove excess files
    for j in range(len(chunks), len(paths)):
        if os.path.exists(paths[j]):
            try:
                os.remove(paths[j])
            except Exception:
                pass

# Main
feed = feedparser.parse(SRC)

# opinion
op_root = load_existing(FILES["opinion"])
op_entries = [e for e in feed.entries if any(x in ((getattr(e,"link",None) or getattr(e,"id",None) or "").strip()) for x in ["/opinion/","/editorial/","/sub-editorial/"])]
merge_update_feed(op_root, op_entries)
ET.ElementTree(op_root).write(FILES["opinion"], encoding="utf-8", xml_declaration=True)

# world
wr_root = load_existing(FILES["world"])
wr_entries = [e for e in feed.entries if ("/world/" in ((getattr(e,"link",None) or getattr(e,"id",None) or "").strip()) or "/deshe-deshe/" in ((getattr(e,"link",None) or getattr(e,"id",None) or "").strip()))]
merge_update_feed(wr_root, wr_entries)
ET.ElementTree(wr_root).write(FILES["world"], encoding="utf-8", xml_declaration=True)

# print (parts) - only print-related logic changed/handled here
print_entries = [e for e in feed.entries if "/print-edition/" in ((getattr(e,"link",None) or getattr(e,"id",None) or "").strip())]
add_items_print(print_entries, FILES["print_parts"])