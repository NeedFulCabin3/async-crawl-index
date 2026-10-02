# async-crawl-index
> Asynchronous recursive web crawler and vector-free search indexing engine for Python.

## Overview / Value Proposition
Scraping and indexing online web content at scale presents distinct operational challenges: unthrottled fetch loops trigger IP bans, synchronous HTTP calls create severe I/O bottlenecks, and heavy database reliance adds unnecessary infrastructure overhead for lightweight lookup tools. 

`async-crawl-index` resolves these friction points by providing an integrated concurrent crawler and in-memory TF-IDF search index. It evaluates `robots.txt` compliance dynamically, enforces non-blocking rate limits per domain, strips noisy markup, and scores search terms using smoothed inverse document frequency calculations—all within a clean Python asynchronous event loop.

## Under the Hood / How It Works
1. **Asynchronous Traversal**: An `asyncio.Queue` manages target URLs initialized from seed inputs. Worker tasks consume items concurrently up to a configurable `asyncio.Semaphore` limit.
2. **Domain Governance**: Before fetching target payloads, `RobotsAndRateLimiter` queries `robots.txt` via `urllib.robotparser.RobotFileParser`. Requests are throttled per domain using non-blocking delta checks against loop execution timestamps.
3. **HTML Parsing & Extraction**: Markup is fetched with `aiohttp.ClientSession`. `BeautifulSoup` decomposes non-content elements (`<script>`, `<style>`, `<nav>`, `<footer>`), extracts plain text, and converts relative anchors (`<a href>`) to absolute URLs using `urllib.parse.urljoin`.
4. **Inverted Indexing & TF-IDF Scoring**: Content is tokenized, stripped of standard English stop words, and mapped in an in-memory dictionary (`term -> {doc_id: term_frequency}`). Search queries calculate score weights via smoothed Inverse Document Frequency:

$$\text{IDF}(t) = \ln\left(1 + \frac{N}{1 + \vert{}\left\{d \in D : t \in d\right\}\vert{}}\right)$$

Document matches are ordered by normalized term frequency multiplied by IDF.

## Key Features
- **Concurrent Request Pipeline**: Leverages `aiohttp` non-blocking sockets bound by `asyncio.Semaphore`.
- **Dynamic Throttle Control**: Automatic per-domain sleep delay prevents server overload and reduces HTTP 429 penalties.
- **Strict Parsing Sanitization**: Strips structural boilerplate tags before tokenization.
- **Stop-Word Tokenizer**: Native token filtering optimizes index size and search accuracy.
- **Zero-Database Vector Scoring**: Runs TF-IDF mathematical relevance queries entirely in memory.

## Tech Stack & Core Dependencies Breakdown
- **Python Target**: Python 3.10+ (utilizes modern type hinting syntax `dict[str, int]`).
- **Standard Library**: `asyncio` (concurrency), `urllib.parse` (URL resolution), `urllib.robotparser` (robots validation), `re` & `math` (tokenization & IDF log metrics), `collections.defaultdict` (index building).
- **aiohttp**: Essential for non-blocking HTTP GET requests within the event loop.
- **beautifulsoup4**: Fast HTML DOM traversal, tag decomposition, and link extraction.

## Environment & Web-Based Quick Start

### 1. Launch via GitHub Codespaces
Click **Code** -> **Codespaces** -> **Create codespace on main** inside the repository page to boot a pre-configured cloud environment directly in your browser.

### 2. Local Environment Setup
```bash
# Clone and enter directory
git clone [https://github.com/](https://github.com/)<your-username>/async-crawl-index.git
cd async-crawl-index

# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```
### 3. Execution Command

Run the primary script to launch the crawl pipeline and output TF-IDF query scores:

```bash
python main.py
```

## Repository Structure

```bash
async-crawl-index/
├── .github/
│   └── workflows/
│       └── ci.yml             # Automated CI workflow for syntax and lint verification
├── .gitignore                 # Python standard git ignore rules
├── LICENSE                    # MIT License file
├── README.md                  # Project architecture and developer guide
├── main.py                    # Core application script containing crawler and indexer
└── requirements.txt           # Project third-party dependencies
```

## Roadmap

**[ ] Type Stub Hardening:** Add explicit mypy static typing annotations across all index dictionaries and crawler data structures.

**[ ] Storage Persistence:** Add an optional SQLite / DuckDB persistence engine to dump and load state snapshots between crawler runs.

**[ ] Async I/O Migration for HTML Parsing:** Integrate lxml with process pool executors to offload heavy BeautifulSoup string parsing from the primary event loop thread.