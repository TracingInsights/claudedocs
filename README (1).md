# FIA F1 Document Scraper

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
- The scraper visits the FIA decision documents page, discovers all events and their PDFs
- New PDFs are downloaded and committed to the repository automatically
- A `manifest.json` file tracks all downloaded documents to avoid duplicates

## Manual Usage

```bash
pip install -r requirements.txt

# Scrape all available seasons
python scraper.py

# Scrape a specific year
python scraper.py --year 2025

# Custom output directory
python scraper.py --output-dir my-docs
```

## Manual Trigger

You can manually trigger the scraper from the **Actions** tab → **Scrape FIA F1 Documents** → **Run workflow**.
