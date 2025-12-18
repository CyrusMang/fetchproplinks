# Copilot Instructions for fetchproplinks

## Project Overview
Property data scraper for 28hse.com that extracts rental/sale listings and stores them in MongoDB. Two-stage pipeline: link extraction → detail fetching.

## Architecture

### Two-Script Pipeline
1. **`1_extract.py`**: Entry point that calls `n28hse.extract()` to scrape listings
2. **`2_fetch_details.py`**: Legacy test script (do not use for production)

The main extraction logic uses **dual-browser strategy**:
- Browser 1: Navigates pagination and collects listing links
- Browser 2: Fetches individual property details in parallel
- Both run simultaneously to optimize scraping speed

### Key Components
- **`extracters/n28hse.py`**: Core scraper with `extract_rent()` and `extract_sell()` functions
- **`models/prop.py`**: MongoDB ORM-like wrapper for property documents in `props` collection
- **`artifacts/28hse_links.csv`**: CSV append-log of scraped property links (downstream usage TBD)

## Critical Patterns

### Anti-Bot Strategy
Uses `undetected_chromedriver` with specific configuration:
```python
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
driver = uc.Chrome(options=options, use_subprocess=True, version_main=143)
```
Random delays (2-10s) between requests to avoid detection.

### Data Deduplication
Properties tracked by composite ID: `cid = "28hse-{prop_id}"`. Updates skip records refreshed within 3 days (`updated_at` timestamp check).

### Selector Patterns
- Phone/WhatsApp clicks: `[attr="phone"]`, `[attr="whatsapp"]`
- Pagination: `.pagination [attr1="{}"]` with page number
- Breadcrumb location: `ol.breadcrumb a span[itemprop="name"]` (skip first 2 items)

### MongoDB Integration
Connection string from `.env` file (`MONGODB_CONNECTION_STRING`). Database name is hardcoded as `prop_main`. The `Prop` model uses class methods (`Prop.get_by_id()`, `Prop.create()`) not instance initialization.

## Environment Setup
Required `.env` variables:
- `MONGODB_CONNECTION_STRING`: MongoDB connection URI
- `ARTIFACTS_FOLDER`: Output directory for CSV files (default: `artifacts`)

## Running the Scraper
```bash
python 1_extract.py  # Full extraction pipeline
```

No test suite or build commands exist. The project is a direct-execution Python script.

## Common Modifications

### Adding New Property Sources
The project is designed to support multiple property listing sites. To add a new source:
1. Create new extractor in `extracters/` following `n28hse.py` pattern
2. Implement `extract()` function with dual-browser setup for parallel scraping
3. Add corresponding CSV file in `artifacts/` for link tracking
4. Call from main entry point (e.g., `1_extract.py`)

### Changing Scrape Frequency
Modify the 3-day cache check in `extract_details()`:
```python
if now - prop.data['updated_at'] < 3 * 24 * 60 * 60:  # Change duration here
```

### Adjusting Cloudflare Wait Time
Random delay range in `extract_details()`:
```python
random_number = random.randint(2, 10)  # Adjust min/max seconds
```
