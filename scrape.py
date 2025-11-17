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
PRINT_TRACKER = "print_articles_tracker.json"

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
# Print edition logic with tracker
# -----------------------------
def normalize_link(link):
    if not link:
        return ""
    link = link.strip()
    link = link.split("?",1)[0].split("#",1)[0]
    return link.rstrip("/")

def load_tracker():
    """Load the tracker JSON that remembers which file each article belongs to"""
    if os.path.exists(PRINT_TRACKER):
        try:
            with open(PRINT_TRACKER, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            pass
    return {}

def save_tracker(tracker):
    """Save the tracker JSON"""
    with open(PRINT_TRACKER, 'w', encoding='utf-8') as f:
        json.dump(tracker, f, ensure_ascii=False, indent=2)

def add_items_print(entries, paths):
    # Load tracker - format: {link: {"file_index": 0 or 1, "title": "...", "pubDate": "..."}}
    tracker = load_tracker()
    
    # Load existing items from both XMLs and update tracker
    for idx, p in enumerate(paths):
        if not os.path.exists(p):
            continue
        root = ET.parse(p).getroot()
        ch = root.find("channel")
        if not ch:
            continue
        for item in ch.findall("item"):
            link = normalize_link(item.findtext("link") or "")
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
            
            # If link already in tracker, keep its original file assignment
            if link not in tracker:
                tracker[link] = {
                    "file_index": idx,
                    "title": title,
                    "pubDate": pd.isoformat()
                }
            else:
                # Update title and pubDate if newer, but keep file_index
                try:
                    existing_pd = datetime.fromisoformat(tracker[link]["pubDate"])
                    if pd > existing_pd:
                        tracker[link]["title"] = title
                        tracker[link]["pubDate"] = pd.isoformat()
                except:
                    pass

    # Process new entries from feed
    new_articles = []
    for e in entries:
        raw = getattr(e, "link", None) or getattr(e, "id", None) or ""
        link = normalize_link(raw)
        if not link:
            continue
        
        # Only add if NOT already in tracker (truly new article)
        if link not in tracker:
            pd = get_entry_pubdt(e)
            title = getattr(e, "title", "")
            new_articles.append({
                "link": link,
                "title": title,
                "pubDate": pd
            })
    
    # Sort new articles by date (newest first)
    new_articles.sort(key=lambda x: x["pubDate"], reverse=True)
    
    # Assign new articles to files
    # Count current items in each file
    file_counts = [0, 0]
    for link, data in tracker.items():
        file_idx = data.get("file_index", 0)
        if file_idx < len(file_counts):
            file_counts[file_idx] += 1
    
    # Add new articles - fill part1 first (up to 100), rest go to part2
    for article in new_articles:
        if file_counts[0] < 100:
            target_file = 0
        else:
            target_file = 1
        
        tracker[article["link"]] = {
            "file_index": target_file,
            "title": article["title"],
            "pubDate": article["pubDate"].isoformat()
        }
        file_counts[target_file] += 1
    
    # Build items for each file from tracker
    file_items = [[], []]
    for link, data in tracker.items():
        file_idx = data.get("file_index", 0)
        if file_idx < len(file_items):
            try:
                pd = datetime.fromisoformat(data["pubDate"])
            except:
                pd = datetime.min
            file_items[file_idx].append({
                "link": link,
                "title": data["title"],
                "pubDate": pd
            })
    
    # Sort each file's items by date (newest first) and cap at 500 total
    for items in file_items:
        items.sort(key=lambda x: x["pubDate"], reverse=True)
    
    # Apply 500 item cap across both files (remove oldest)
    all_tracked = []
    for idx, items in enumerate(file_items):
        for item in items:
            all_tracked.append((item, idx))
    
    all_tracked.sort(key=lambda x: x[0]["pubDate"], reverse=True)
    
    if len(all_tracked) > 500:
        # Remove oldest items from tracker
        for item_data, file_idx in all_tracked[500:]:
            link = item_data["link"]
            if link in tracker:
                del tracker[link]
        
        # Rebuild file_items after cleanup
        file_items = [[], []]
        for link, data in tracker.items():
            file_idx = data.get("file_index", 0)
            if file_idx < len(file_items):
                try:
                    pd = datetime.fromisoformat(data["pubDate"])
                except:
                    pd = datetime.min
                file_items[file_idx].append({
                    "link": link,
                    "title": data["title"],
                    "pubDate": pd
                })
        
        for items in file_items:
            items.sort(key=lambda x: x["pubDate"], reverse=True)
    
    # Write files
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
    
    for idx, path in enumerate(paths):
        write_part(path, file_items[idx])
    
    # Save tracker
    save_tracker(tracker)
    
    # Remove extra old part files if any
    for j in range(2, 10):  # Check up to 10 potential old files
        old_path = f"daily_kalerkantho_part{j+1}.xml"
        if os.path.exists(old_path):
            os.remove(old_path)

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