# FIA F1 Document Scraper

You can browse these FIA F1 Decision Documents at https://tracinginsights.com/ or f1tel.com. Also able to download these as pdf/images on website.


Automatically scrapes Formula 1 decision documents (PDFs) from the [FIA website](https://www.fia.com/documents/championships/fia-formula-one-world-championship-14) and stores them in this repository.

## Directory Structure

```
documents/
├── 2026/
│   ├── chinese-grand-prix/
│   │   ├── championship-points.pdf
│   │   ├── final-starting-grid.pdf
│   │   └── ...
│   ├── australian-grand-prix/
│   │   └── ...
│   └── ...
├── 2025/
│   └── ...
└── manifest.json
```

## How It Works

- A GitHub Action runs every **3 hours** to check for new documents
- Uses `asyncio` + `aiohttp` for parallel AJAX discovery and PDF downloads (15 concurrent downloads)
- All events within a season are discovered in parallel via AJAX
- A `manifest.json` tracks all downloaded URLs to avoid duplicates

## Manual Usage

```bash
# Install dependencies
uv sync

# Scrape all available seasons
uv run python scraper.py

# Scrape a specific year
uv run python scraper.py --year 2025

# Custom output directory
uv run python scraper.py --output-dir my-docs
```

## Manual Trigger

You can manually trigger the scraper from the **Actions** tab > **Scrape FIA F1 Documents** > **Run workflow**.
