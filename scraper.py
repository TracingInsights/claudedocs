"""FIA F1 Document Scraper

Scrapes the FIA website for Formula 1 decision documents (PDFs)
and saves them in a structured directory: year/grand-prix/document-name.pdf

The FIA site uses Drupal with AJAX-loaded event document lists.
The main page shows events for the current/selected season, but only the
first event's documents are inline — the rest require AJAX calls.
"""

import os
import re
import json
import time
import logging
import argparse
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.fia.com"
CHAMPIONSHIP_PATH = "/documents/championships/fia-formula-one-world-championship-14"
DOCUMENTS_URL = f"{BASE_URL}{CHAMPIONSHIP_PATH}"
AJAX_URL = f"{BASE_URL}/decision-document-list/ajax/"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


def create_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
    })
    return session


SESSION = create_session()


def slugify(text: str) -> str:
    """Convert text to a filesystem-safe slug."""
    text = text.strip().lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text.strip("-")


def fetch(url: str, retries: int = 3, **kwargs) -> requests.Response | None:
    """Fetch a URL with retries."""
    for attempt in range(retries):
        try:
            resp = SESSION.get(url, timeout=30, **kwargs)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            log.warning("Attempt %d failed for %s: %s", attempt + 1, url, e)
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return None


def get_soup(url: str) -> BeautifulSoup | None:
    resp = fetch(url)
    if resp:
        return BeautifulSoup(resp.text, "html.parser")
    return None


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
    """Extract events from a season page.

    Returns events with their ID (for AJAX loading) and any inline PDF links
    already present on the page (the first/latest event is pre-expanded).
    """
    events = []
    event_wrappers = soup.find_all(class_="event-wrapper")

    for ew in event_wrappers:
        # Get event name from the title div
        title_div = ew.find(class_=lambda c: c and "title" in str(c).lower())
        name = title_div.get_text(strip=True) if title_div else None
        if not name:
            continue

        # Get event ID from the AJAX link (collapsed events have this)
        event_id = None
        ajax_link = ew.find("a", href=lambda h: h and "decision-document-list" in h)
        if ajax_link:
            m = re.search(r"/(\d+)$", ajax_link["href"])
            if m:
                event_id = m.group(1)

        # Collect any inline PDF links (first event is pre-expanded)
        inline_docs = _extract_pdf_links(ew)

        events.append({
            "name": name,
            "id": event_id,
            "year": year,
            "inline_docs": inline_docs,
        })

    return events


def load_event_documents(event_id: str) -> list[dict]:
    """Load documents for an event via the AJAX endpoint."""
    resp = fetch(
        f"{AJAX_URL}{event_id}",
        headers={
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
        },
    )
    if not resp:
        return []

    try:
        data = resp.json()
    except (json.JSONDecodeError, ValueError):
        return []

    documents = []
    for item in data:
        if item.get("command") == "insert" and "data" in item:
            html_soup = BeautifulSoup(item["data"], "html.parser")
            documents.extend(_extract_pdf_links(html_soup))

    return documents


def _extract_pdf_links(soup: BeautifulSoup) -> list[dict]:
    """Extract PDF document info from a BeautifulSoup fragment."""
    documents = []
    for link in soup.find_all("a", href=lambda h: h and h.endswith(".pdf")):
        raw_text = link.get_text(strip=True)

        # Parse title: remove "Doc N - " prefix and "Published on..." suffix
        title = re.sub(r"^Doc\s+\d+\s*[-–]\s*", "", raw_text)
        title = re.sub(r"Published\s+on.*$", "", title, flags=re.IGNORECASE).strip()
        if not title:
            title = Path(link["href"]).stem

        # Extract doc number for sorting
        doc_num_match = re.match(r"Doc\s+(\d+)", raw_text)
        doc_num = int(doc_num_match.group(1)) if doc_num_match else 0

        pdf_url = urljoin(BASE_URL, link["href"])

        documents.append({
            "title": title,
            "url": pdf_url,
            "filename": slugify(title) + ".pdf",
            "doc_number": doc_num,
        })

    return documents


def download_pdf(url: str, dest: Path) -> bool:
    """Download a PDF file. Returns True if newly downloaded."""
    if dest.exists():
        return False

    dest.parent.mkdir(parents=True, exist_ok=True)

    try:
        resp = SESSION.get(url, timeout=60, stream=True)
        resp.raise_for_status()

        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)

        log.info("Downloaded: %s", dest)
        return True
    except requests.RequestException as e:
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


def scrape(output_dir: str = "documents", year_filter: int | None = None) -> int:
    """Main scrape entrypoint. Returns count of newly downloaded documents."""
    output = Path(output_dir)
    manifest_path = output / "manifest.json"
    manifest = load_manifest(manifest_path)

    log.info("Fetching main documents page...")
    main_soup = get_soup(DOCUMENTS_URL)
    if not main_soup:
        log.error("Failed to fetch main documents page")
        return 0

    # Discover available seasons
    seasons = discover_seasons(main_soup)
    log.info("Found seasons: %s", [s["year"] for s in seasons])

    if year_filter:
        seasons = [s for s in seasons if s["year"] == year_filter]
        if not seasons:
            log.error("Season %d not found", year_filter)
            return 0

    total_new = 0

    for season in seasons:
        year = season["year"]
        log.info("=== Season %d ===", year)

        season_soup = get_soup(season["url"])
        if not season_soup:
            log.warning("Failed to fetch season %d page, skipping", year)
            continue

        events = discover_events_on_page(season_soup, year)
        log.info("Found %d events for %d", len(events), year)

        for event in events:
            gp_slug = slugify(event["name"])
            log.info("  Processing: %s", event["name"])

            # Use inline docs if available, otherwise load via AJAX
            if event["inline_docs"]:
                documents = event["inline_docs"]
                log.info("  %d documents (inline)", len(documents))
            elif event["id"]:
                documents = load_event_documents(event["id"])
                log.info("  %d documents (AJAX)", len(documents))
                time.sleep(0.5)  # Be polite between AJAX calls
            else:
                log.warning("  No documents and no event ID, skipping")
                continue

            for doc in documents:
                if doc["url"] in manifest:
                    continue

                dest = output / str(year) / gp_slug / doc["filename"]

                # Handle filename collisions
                if dest.exists():
                    base = dest.stem
                    suffix = 1
                    while dest.exists():
                        dest = dest.with_name(f"{base}-{suffix}.pdf")
                        suffix += 1

                downloaded = download_pdf(doc["url"], dest)
                if downloaded:
                    total_new += 1
                    manifest[doc["url"]] = {
                        "year": year,
                        "event": event["name"],
                        "title": doc["title"],
                        "path": str(dest),
                    }
                    # Save after each download so progress isn't lost
                    save_manifest(manifest_path, manifest)

                time.sleep(0.3)

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

    new_count = scrape(output_dir=args.output_dir, year_filter=args.year)

    # Set GitHub Actions output if running in CI
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"new_documents={new_count}\n")


if __name__ == "__main__":
    main()
