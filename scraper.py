"""FIA F1 Document Scraper

Scrapes the FIA website for Formula 1:
  - Decision documents  → year/grand-prix/document-name.pdf
  - Event timing info   → year/grand-prix/session/document-name.pdf

Uses asyncio + aiohttp for parallel discovery and PDF downloads.
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

# Event timing info lives under a completely separate URL tree.
EVENTS_PATH = "/events/fia-formula-one-world-championship"
EVENTS_BASE_URL = f"{BASE_URL}{EVENTS_PATH}"

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

# Matches standard F1 session names, used to identify section headings
# on timing info pages.
_SESSION_RE = re.compile(
    r"(practice\s*\d+|qualifying|sprint[\s-]qualifying|sprint\s*race|sprint|race"
    r"|pre[-\s]season\s*test(?:ing)?|shakedown)",
    re.IGNORECASE,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared utilities
# ---------------------------------------------------------------------------

def slugify(text: str) -> str:
    """Convert text to a filesystem-safe slug."""
    text = text.strip().lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text.strip("-")


def _extract_pdf_links(soup: BeautifulSoup) -> list[dict]:
    """Return PDF document records from a BeautifulSoup fragment."""
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
    session: aiohttp.ClientSession,
    url: str,
    headers: dict | None = None,
    retries: int = 3,
) -> str | None:
    """GET a URL, returning text or None after retries."""
    for attempt in range(retries):
        try:
            async with session.get(
                url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                resp.raise_for_status()
                return await resp.text()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            log.warning("Attempt %d failed for %s: %s", attempt + 1, url, e)
            if attempt < retries - 1:
                await asyncio.sleep(1)
    return None


async def download_pdf(
    session: aiohttp.ClientSession,
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


def save_manifest(path: Path, manifest: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True))


def _unique_dest(dest: Path, assigned: set[Path]) -> Path:
    """Append a numeric suffix if dest collides with an existing path."""
    if dest not in assigned and not dest.exists():
        return dest
    base, suffix = dest.stem, 1
    while True:
        candidate = dest.with_name(f"{base}-{suffix}.pdf")
        if candidate not in assigned and not candidate.exists():
            return candidate
        suffix += 1


# ---------------------------------------------------------------------------
# Decision-documents scraping (original logic, unchanged in structure)
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
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    event: dict,
) -> list[dict]:
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


# ---------------------------------------------------------------------------
# Event timing-info scraping (new)
# ---------------------------------------------------------------------------

def _discover_timing_events_on_season_page(soup: BeautifulSoup, year: int) -> list[dict]:
    """
    Find per-GP event slugs from the season events listing page at:
      /events/fia-formula-one-world-championship/season-{year}

    The FIA site links to individual GP pages like:
      /events/fia-formula-one-world-championship/season-{year}/grand-prix-australia
    We derive the timing-info URL by appending /eventtiming-information.
    """
    pattern = re.compile(
        rf"{re.escape(EVENTS_PATH)}/season-{year}/([^/\"'\s]+?)/?(?:[\"'\s]|$)"
    )
    seen: set[str] = set()
    events = []

    for a in soup.find_all("a", href=True):
        m = pattern.search(a["href"])
        if not m:
            continue
        slug = m.group(1).rstrip("/")
        # Skip the season index itself and non-GP slugs (e.g. "teams", "drivers")
        if slug in seen or not slug or "/" in slug:
            continue
        seen.add(slug)
        base = f"{EVENTS_BASE_URL}/season-{year}/{slug}"
        events.append({
            "slug": slug,
            "year": year,
            "timing_url": f"{base}/eventtiming-information",
        })

    return events


def _extract_sessions_from_timing_page(soup: BeautifulSoup) -> list[dict]:
    """
    Parse session sections out of a timing-info page.

    The FIA website is not consistent across years, so we try four strategies
    in order, stopping as soon as any produces results.

    Strategy A — Heading-delimited sections (h2/h3/h4 whose text names a
                  session; PDFs are collected from siblings until the next
                  same-level heading).
    Strategy B — Containers with a data-session / data-title attribute.
    Strategy C — Tab panels: elements with role="tabpanel" or class "tab-pane",
                  each preceded by a labelled tab.
    Strategy D — Fallback: all PDFs on the page under a single
                 "uncategorized" session.
    """

    # --- Strategy A: heading-delimited sections ---
    sessions: list[dict] = []
    heading_tags = soup.find_all(re.compile(r"^h[2-4]$"))
    for heading in heading_tags:
        label = heading.get_text(strip=True)
        if not _SESSION_RE.search(label):
            continue
        docs: list[dict] = []
        for sibling in heading.find_next_siblings():
            # Stop at the next heading of same or higher level
            if sibling.name and re.match(r"^h[1-4]$", sibling.name):
                break
            docs.extend(_extract_pdf_links(BeautifulSoup(str(sibling), "html.parser")))
        if docs:
            sessions.append({"session_slug": slugify(label), "documents": docs})

    if sessions:
        return sessions

    # --- Strategy B: data-session / data-title attributes ---
    for container in soup.find_all(
        lambda tag: tag.has_attr("data-session") or tag.has_attr("data-title")
    ):
        label = container.get("data-session") or container.get("data-title", "")
        if not _SESSION_RE.search(label):
            continue
        docs = _extract_pdf_links(container)
        if docs:
            sessions.append({"session_slug": slugify(label), "documents": docs})

    if sessions:
        return sessions

    # --- Strategy C: tab panels ---
    # Collect tab labels first (aria-controls or href="#panel-id")
    tab_labels: dict[str, str] = {}
    for tab in soup.find_all(attrs={"role": "tab"}):
        label = tab.get_text(strip=True)
        panel_id = tab.get("aria-controls") or tab.get("href", "").lstrip("#")
        if panel_id and label:
            tab_labels[panel_id] = label

    for panel in soup.find_all(attrs={"role": "tabpanel"}):
        panel_id = panel.get("id", "")
        label = tab_labels.get(panel_id, panel_id)
        docs = _extract_pdf_links(panel)
        if docs:
            session_slug = slugify(label) if label else "session"
            sessions.append({"session_slug": session_slug, "documents": docs})

    # Also try class="tab-pane" if role attributes are absent
    if not sessions:
        for panel in soup.find_all(class_="tab-pane"):
            panel_id = panel.get("id", "")
            label = tab_labels.get(panel_id, panel_id)
            docs = _extract_pdf_links(panel)
            if docs:
                sessions.append({
                    "session_slug": slugify(label) if label else "session",
                    "documents": docs,
                })

    if sessions:
        return sessions

    # --- Strategy D: fallback — dump everything into uncategorized ---
    all_docs = _extract_pdf_links(soup)
    if all_docs:
        sessions.append({"session_slug": "uncategorized", "documents": all_docs})

    return sessions


async def fetch_timing_event_docs(
    http: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    event: dict,
) -> list[dict]:
    """
    Fetch a single GP's timing-info page and return a flat list of doc records,
    each carrying year / event_slug / session_slug for path construction.
    """
    async with sem:
        html = await fetch_text(http, event["timing_url"])

    if not html:
        log.debug("No timing page (or 404) for %s", event["timing_url"])
        return []

    soup = BeautifulSoup(html, "html.parser")
    sessions = _extract_sessions_from_timing_page(soup)

    if not sessions:
        log.debug("No timing documents found at %s", event["timing_url"])
        return []

    results = []
    for sess in sessions:
        for doc in sess["documents"]:
            results.append({
                **doc,
                "year": event["year"],
                "event_slug": event["slug"],
                "session_slug": sess["session_slug"],
                "source": "timing",
            })
    log.info(
        "Timing %s/%s: %d sessions, %d docs",
        event["year"],
        event["slug"],
        len(sessions),
        len(results),
    )
    return results


# ---------------------------------------------------------------------------
# Main scrape orchestrator
# ---------------------------------------------------------------------------

async def scrape(
    output_dir: str = "documents",
    year_filter: int | None = None,
    include_timing: bool = True,
) -> int:
    """
    Scrape decision documents and (optionally) event timing info.
    Returns the count of newly downloaded PDFs.
    """
    output = Path(output_dir)
    manifest_path = output / "manifest.json"
    manifest = load_manifest(manifest_path)

    connector = aiohttp.TCPConnector(limit=30, limit_per_host=15)
    async with aiohttp.ClientSession(headers=HEADERS, connector=connector) as session:

        # ------------------------------------------------------------------ #
        # 1. Decision documents                                               #
        # ------------------------------------------------------------------ #
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

        # Build decision-doc download queue
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

        # ------------------------------------------------------------------ #
        # 2. Event timing info                                                #
        # ------------------------------------------------------------------ #
        if include_timing:
            # Discover GP slugs from the events section of the FIA site.
            # This is a separate URL tree from the documents page.
            years_to_scan = list({s["year"] for s in seasons})
            season_events_urls = [
                f"{EVENTS_BASE_URL}/season-{y}" for y in years_to_scan
            ]
            log.info("Fetching %d season events pages for timing discovery...", len(season_events_urls))
            season_events_htmls = await asyncio.gather(
                *(fetch_text(session, url) for url in season_events_urls)
            )

            timing_events: list[dict] = []
            for year, html in zip(years_to_scan, season_events_htmls):
                if not html:
                    log.warning("Failed to fetch events page for season %d", year)
                    continue
                soup = BeautifulSoup(html, "html.parser")
                found = _discover_timing_events_on_season_page(soup, year)
                log.info("Season %d: %d timing events discovered", year, len(found))
                timing_events.extend(found)

            # Fetch all timing pages in parallel (reuse ajax_sem for pacing)
            log.info(
                "Fetching timing info for %d events (concurrency=%d)...",
                len(timing_events),
                MAX_AJAX_CONCURRENT,
            )
            timing_results = await asyncio.gather(
                *(fetch_timing_event_docs(session, ajax_sem, e) for e in timing_events)
            )

            for docs in timing_results:
                for doc in docs:
                    if doc["url"] in manifest:
                        continue
                    dest = _unique_dest(
                        output
                        / str(doc["year"])
                        / doc["event_slug"]
                        / doc["session_slug"]
                        / doc["filename"],
                        assigned_paths,
                    )
                    assigned_paths.add(dest)
                    download_queue.append((
                        doc["url"],
                        dest,
                        {
                            "year": doc["year"],
                            "event": doc["event_slug"],
                            "session": doc["session_slug"],
                            "title": doc["title"],
                            "source": "timing",
                        },
                    ))

        # ------------------------------------------------------------------ #
        # 3. Download everything                                              #
        # ------------------------------------------------------------------ #
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
    parser.add_argument(
        "--no-timing",
        action="store_true",
        help="Skip event timing-info documents",
    )
    args = parser.parse_args()

    new_count = asyncio.run(
        scrape(
            output_dir=args.output_dir,
            year_filter=args.year,
            include_timing=not args.no_timing,
        )
    )

    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"new_documents={new_count}\n")


if __name__ == "__main__":
    main()