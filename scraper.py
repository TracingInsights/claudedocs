"""FIA F1 Document Scraper

Scrapes the FIA website for Formula 1 decision documents (PDFs)
and saves them in a structured directory: year/grand-prix/document-name.pdf

Uses asyncio + aiohttp for parallel AJAX discovery and PDF downloads.
"""

import asyncio
import json
import logging
import os
import re
import argparse
from pathlib import Path
from urllib.parse import urljoin

import aiofiles
import aiohttp
from bs4 import BeautifulSoup

BASE_URL = "https://www.fia.com"
CHAMPIONSHIP_PATH = "/documents/championships/fia-formula-one-world-championship-14"
DOCUMENTS_URL = f"{BASE_URL}{CHAMPIONSHIP_PATH}"
AJAX_URL = f"{BASE_URL}/decision-document-list/ajax/"

# Concurrency limits
MAX_AJAX_CONCURRENT = 10
MAX_DOWNLOAD_CONCURRENT = 15

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
}
AJAX_HEADERS = {
    **HEADERS,
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


def slugify(text: str) -> str:
    """Convert text to a filesystem-safe slug."""
    text = text.strip().lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text.strip("-")


def _extract_pdf_links(soup: BeautifulSoup) -> list[dict]:
    """Extract PDF document info from a BeautifulSoup fragment."""
    documents = []
    for link in soup.find_all("a", href=lambda h: h and h.endswith(".pdf")):
        raw_text = link.get_text(strip=True)
        title = re.sub(r"^Doc\s+\d+\s*[-–]\s*", "", raw_text)
        title = re.sub(r"Published\s+on.*$", "", title, flags=re.IGNORECASE).strip()
        if not title:
            title = Path(link["href"]).stem

        doc_num_match = re.match(r"Doc\s+(\d+)", raw_text)
        doc_num = int(doc_num_match.group(1)) if doc_num_match else 0

        documents.append({
            "title": title,
            "url": urljoin(BASE_URL, link["href"]),
            "filename": slugify(title) + ".pdf",
            "doc_number": doc_num,
        })
    return documents


def discover_seasons(soup: BeautifulSoup) -> list[dict]:
    """Extract season options from the facet dropdown."""
    seasons = []
    for option in soup.find_all("option"):
        value = option.get("value", "")
        text = option.get_text(strip=True)
        match = re.match(r"SEASON\s+(\d{4})", text, re.IGNORECASE)
        if match and CHAMPIONSHIP_PATH in value:
            seasons.append({
                "year": int(match.group(1)),
                "url": urljoin(BASE_URL, value),
            })
    return sorted(seasons, key=lambda s: s["year"], reverse=True)


def discover_events_on_page(soup: BeautifulSoup, year: int) -> list[dict]:
    """Extract events from a season page."""
    events = []
    for ew in soup.find_all(class_="event-wrapper"):
        title_div = ew.find(class_=lambda c: c and "title" in str(c).lower())
        name = title_div.get_text(strip=True) if title_div else None
        if not name:
            continue

        event_id = None
        ajax_link = ew.find("a", href=lambda h: h and "decision-document-list" in h)
        if ajax_link:
            m = re.search(r"/(\d+)$", ajax_link["href"])
            if m:
                event_id = m.group(1)

        inline_docs = _extract_pdf_links(ew)

        events.append({
            "name": name,
            "id": event_id,
            "year": year,
            "inline_docs": inline_docs,
        })
    return events


async def fetch_text(session: aiohttp.ClientSession, url: str, headers: dict | None = None, retries: int = 3) -> str | None:
    """Fetch URL text with retries."""
    for attempt in range(retries):
        try:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                resp.raise_for_status()
                return await resp.text()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            log.warning("Attempt %d failed for %s: %s", attempt + 1, url, e)
            if attempt < retries - 1:
                await asyncio.sleep(1)
    return None


async def fetch_event_docs(session: aiohttp.ClientSession, sem: asyncio.Semaphore, event: dict) -> list[dict]:
    """Load documents for a single event via AJAX (with semaphore)."""
    if event["inline_docs"]:
        return event["inline_docs"]

    if not event["id"]:
        return []

    async with sem:
        text = await fetch_text(session, f"{AJAX_URL}{event['id']}", headers=AJAX_HEADERS)

    if not text:
        return []

    try:
        data = json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return []

    documents = []
    for item in data:
        if item.get("command") == "insert" and "data" in item:
            soup = BeautifulSoup(item["data"], "html.parser")
            documents.extend(_extract_pdf_links(soup))
    return documents


async def download_pdf(session: aiohttp.ClientSession, sem: asyncio.Semaphore, url: str, dest: Path) -> bool:
    """Download a PDF file concurrently. Returns True if newly downloaded."""
    if dest.exists():
        return False

    dest.parent.mkdir(parents=True, exist_ok=True)

    try:
        async with sem:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=120)) as resp:
                resp.raise_for_status()
                content = await resp.read()

        async with aiofiles.open(dest, "wb") as f:
            await f.write(content)

        log.info("Downloaded: %s (%d KB)", dest, len(content) // 1024)
        return True
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        log.error("Failed to download %s: %s", url, e)
        if dest.exists():
            dest.unlink()
        return False


def load_manifest(path: Path) -> dict:
    if path.exists():
        return json.loads(path.read_text())
    return {}


def save_manifest(path: Path, manifest: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True))


async def scrape(output_dir: str = "documents", year_filter: int | None = None) -> int:
    """Main scrape entrypoint. Returns count of newly downloaded documents."""
    output = Path(output_dir)
    manifest_path = output / "manifest.json"
    manifest = load_manifest(manifest_path)

    connector = aiohttp.TCPConnector(limit=30, limit_per_host=15)
    async with aiohttp.ClientSession(headers=HEADERS, connector=connector) as session:

        # 1. Fetch main page and discover seasons
        log.info("Fetching main documents page...")
        main_html = await fetch_text(session, DOCUMENTS_URL)
        if not main_html:
            log.error("Failed to fetch main documents page")
            return 0

        main_soup = BeautifulSoup(main_html, "html.parser")
        seasons = discover_seasons(main_soup)
        log.info("Found seasons: %s", [s["year"] for s in seasons])

        if year_filter:
            seasons = [s for s in seasons if s["year"] == year_filter]
            if not seasons:
                log.error("Season %d not found", year_filter)
                return 0

        # 2. Fetch all season pages in parallel
        log.info("Fetching %d season pages in parallel...", len(seasons))
        season_htmls = await asyncio.gather(
            *(fetch_text(session, s["url"]) for s in seasons)
        )

        # 3. Parse events from all seasons
        all_events = []
        for season, html in zip(seasons, season_htmls):
            if not html:
                log.warning("Failed to fetch season %d, skipping", season["year"])
                continue
            soup = BeautifulSoup(html, "html.parser")
            events = discover_events_on_page(soup, season["year"])
            log.info("Season %d: %d events", season["year"], len(events))
            all_events.extend(events)

        # 4. Load all event documents via AJAX in parallel
        ajax_sem = asyncio.Semaphore(MAX_AJAX_CONCURRENT)
        events_needing_ajax = [e for e in all_events if not e["inline_docs"] and e["id"]]
        log.info("Loading documents for %d events via AJAX (concurrency=%d)...",
                 len(events_needing_ajax), MAX_AJAX_CONCURRENT)

        ajax_results = await asyncio.gather(
            *(fetch_event_docs(session, ajax_sem, e) for e in events_needing_ajax)
        )

        # Merge AJAX results back into events
        ajax_idx = 0
        for event in all_events:
            if not event["inline_docs"] and event["id"]:
                event["documents"] = ajax_results[ajax_idx]
                ajax_idx += 1
            else:
                event["documents"] = event["inline_docs"]

        # 5. Build download queue (skip already-downloaded)
        download_queue: list[tuple[str, Path, dict]] = []
        assigned_paths: set[Path] = set()
        for event in all_events:
            gp_slug = slugify(event["name"])
            year = event["year"]

            for doc in event.get("documents", []):
                if doc["url"] in manifest:
                    continue

                dest = output / str(year) / gp_slug / doc["filename"]
                # Deduplicate filenames within the same queue + existing files
                if dest in assigned_paths or dest.exists():
                    base = dest.stem
                    suffix = 1
                    candidate = dest.with_name(f"{base}-{suffix}.pdf")
                    while candidate in assigned_paths or candidate.exists():
                        suffix += 1
                        candidate = dest.with_name(f"{base}-{suffix}.pdf")
                    dest = candidate

                assigned_paths.add(dest)
                download_queue.append((doc["url"], dest, {
                    "year": year,
                    "event": event["name"],
                    "title": doc["title"],
                }))

        log.info("Download queue: %d new documents", len(download_queue))

        if not download_queue:
            log.info("No new documents to download.")
            return 0

        # 6. Download all PDFs in parallel
        dl_sem = asyncio.Semaphore(MAX_DOWNLOAD_CONCURRENT)
        results = await asyncio.gather(
            *(download_pdf(session, dl_sem, url, dest) for url, dest, _ in download_queue)
        )

        # 7. Update manifest with successful downloads
        total_new = 0
        for (url, dest, meta), downloaded in zip(download_queue, results):
            if downloaded:
                total_new += 1
                manifest[url] = {**meta, "path": str(dest)}

        save_manifest(manifest_path, manifest)

    log.info("Done! Downloaded %d new documents.", total_new)
    return total_new


def main():
    parser = argparse.ArgumentParser(description="FIA F1 Document Scraper")
    parser.add_argument(
        "--output-dir",
        default="documents",
        help="Base directory for saving PDFs (default: documents)",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=None,
        help="Only scrape a specific year",
    )
    args = parser.parse_args()

    new_count = asyncio.run(scrape(output_dir=args.output_dir, year_filter=args.year))

    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"new_documents={new_count}\n")


if __name__ == "__main__":
    main()
