"""FIA F1 Document Scraper

Scrapes the FIA website for Formula 1 decision documents:
  - Decision documents  → year/grand-prix/document-name.pdf

Uses curl_cffi for browser-impersonated requests (bypasses TLS fingerprint checks)
and asyncio for parallel discovery and PDF downloads.
"""

import asyncio
import json
import logging
import os
import re
from pathlib import Path
from urllib.parse import urljoin

import aiofiles
from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession

BASE_URL = "https://www.fia.com"
CHAMPIONSHIP_PATH = "/documents/championships/fia-formula-one-world-championship-14"
DOCUMENTS_URL = f"{BASE_URL}{CHAMPIONSHIP_PATH}"
AJAX_URL = f"{BASE_URL}/decision-document-list/ajax/"

MAX_AJAX_CONCURRENT = 10
MAX_DOWNLOAD_CONCURRENT = 15

AJAX_EXTRA_HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared utilities
# ---------------------------------------------------------------------------

def slugify(text: str) -> str:
    text = text.strip().lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text.strip("-")


def _extract_pdf_links(soup: BeautifulSoup) -> list[dict]:
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


async def fetch_text(
    session: AsyncSession,
    url: str,
    extra_headers: dict | None = None,
    retries: int = 3,
) -> str | None:
    """GET a URL, returning text or None after retries."""
    for attempt in range(retries):
        try:
            resp = await session.get(url, headers=extra_headers or {}, timeout=30)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            log.warning("Attempt %d failed for %s: %s", attempt + 1, url, e)
            if attempt < retries - 1:
                await asyncio.sleep(1)
    return None


async def download_pdf(
    session: AsyncSession,
    sem: asyncio.Semaphore,
    url: str,
    dest: Path,
) -> bool:
    """Download a single PDF. Returns True if newly written."""
    if dest.exists():
        return False

    dest.parent.mkdir(parents=True, exist_ok=True)

    try:
        async with sem:
            resp = await session.get(url, timeout=120)
            resp.raise_for_status()
            content = resp.content

        async with aiofiles.open(dest, "wb") as f:
            await f.write(content)

        log.info("Downloaded: %s (%d KB)", dest, len(content) // 1024)
        return True
    except Exception as e:
        log.error("Failed to download %s: %s", url, e)
        if dest.exists():
            dest.unlink()
        return False


def load_manifest(path: Path) -> dict:
    if path.exists():
        return json.loads(path.read_text())
    return {}


def save_manifest(path: Path, manifest: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True))


def _unique_dest(dest: Path, assigned: set[Path]) -> Path:
    if dest not in assigned and not dest.exists():
        return dest
    base, suffix = dest.stem, 1
    while True:
        candidate = dest.with_name(f"{base}-{suffix}.pdf")
        if candidate not in assigned and not candidate.exists():
            return candidate
        suffix += 1


# ---------------------------------------------------------------------------
# Decision-documents scraping
# ---------------------------------------------------------------------------

def discover_seasons(soup: BeautifulSoup) -> list[dict]:
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


async def fetch_event_docs(
    session: AsyncSession,
    sem: asyncio.Semaphore,
    event: dict,
) -> list[dict]:
    if event["inline_docs"]:
        return event["inline_docs"]
    if not event["id"]:
        return []

    async with sem:
        text = await fetch_text(session, f"{AJAX_URL}{event['id']}", extra_headers=AJAX_EXTRA_HEADERS)

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


# ---------------------------------------------------------------------------
# Main scrape orchestrator
# ---------------------------------------------------------------------------

async def scrape(
    output_dir: str = "documents",
    year_filter: int | None = None,
) -> int:
    output = Path(output_dir)
    manifest_path = output / "manifest.json"
    manifest = load_manifest(manifest_path)

    async with AsyncSession(impersonate="chrome120") as session:

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

        log.info("Fetching %d season pages in parallel...", len(seasons))
        season_htmls = await asyncio.gather(
            *(fetch_text(session, s["url"]) for s in seasons)
        )

        all_events: list[dict] = []
        for season, html in zip(seasons, season_htmls):
            if not html:
                log.warning("Failed to fetch season %d, skipping", season["year"])
                continue
            soup = BeautifulSoup(html, "html.parser")
            events = discover_events_on_page(soup, season["year"])
            log.info("Season %d: %d decision-doc events", season["year"], len(events))
            all_events.extend(events)

        ajax_sem = asyncio.Semaphore(MAX_AJAX_CONCURRENT)
        events_needing_ajax = [e for e in all_events if not e["inline_docs"] and e["id"]]
        log.info(
            "Loading decision docs for %d events via AJAX (concurrency=%d)...",
            len(events_needing_ajax),
            MAX_AJAX_CONCURRENT,
        )
        ajax_results = await asyncio.gather(
            *(fetch_event_docs(session, ajax_sem, e) for e in events_needing_ajax)
        )

        ajax_idx = 0
        for event in all_events:
            if not event["inline_docs"] and event["id"]:
                event["documents"] = ajax_results[ajax_idx]
                ajax_idx += 1
            else:
                event["documents"] = event["inline_docs"]

        download_queue: list[tuple[str, Path, dict]] = []
        assigned_paths: set[Path] = set()

        for event in all_events:
            gp_slug = slugify(event["name"])
            year = event["year"]
            for doc in event.get("documents", []):
                if doc["url"] in manifest:
                    continue
                dest = _unique_dest(
                    output / str(year) / gp_slug / doc["filename"],
                    assigned_paths,
                )
                assigned_paths.add(dest)
                download_queue.append((
                    doc["url"],
                    dest,
                    {"year": year, "event": event["name"], "title": doc["title"], "source": "decision"},
                ))

        log.info("Download queue: %d new documents", len(download_queue))
        if not download_queue:
            log.info("Nothing new to download.")
            return 0

        dl_sem = asyncio.Semaphore(MAX_DOWNLOAD_CONCURRENT)
        results = await asyncio.gather(
            *(download_pdf(session, dl_sem, url, dest) for url, dest, _ in download_queue)
        )

        total_new = 0
        for (url, dest, meta), downloaded in zip(download_queue, results):
            if downloaded:
                total_new += 1
                manifest[url] = {**meta, "path": str(dest)}

        save_manifest(manifest_path, manifest)

    log.info("Done! Downloaded %d new documents.", total_new)
    return total_new


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    import sys

    output_dir = "documents"
    year_filter = None

    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == "--output-dir" and i + 1 < len(args):
            output_dir = args[i + 1]
            i += 2
        elif args[i] == "--year" and i + 1 < len(args):
            year_filter = int(args[i + 1])
            i += 2
        else:
            i += 1

    new_count = asyncio.run(scrape(output_dir=output_dir, year_filter=year_filter))

    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"new_documents={new_count}\n")


if __name__ == "__main__":
    main()