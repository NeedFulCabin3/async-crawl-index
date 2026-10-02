import asyncio
import math
import re
from collections import defaultdict
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser
import aiohttp
from bs4 import BeautifulSoup


class RobotsAndRateLimiter:
    """Manages robots.txt rules and per-domain rate limiting."""

    def __init__(self, user_agent: str = "PySearchCrawler/1.0"):
        self.user_agent = user_agent
        self.parsers: dict[str, RobotFileParser] = {}
        self.last_accessed: dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def can_fetch(
        self, session: aiohttp.ClientSession, url: str
    ) -> bool:
        parsed = urlparse(url)
        domain = f"{parsed.scheme}://{parsed.netloc}"

        async with self._lock:
            if domain not in self.parsers:
                parser = RobotFileParser()
                robots_url = urljoin(domain, "/robots.txt")
                try:
                    async with session.get(
                        robots_url, timeout=5
                    ) as resp:
                        if resp.status == 200:
                            content = await resp.text()
                            parser.parse(content.splitlines())
                        else:
                            parser.allow_all = True
                except Exception:
                    parser.allow_all = True
                self.parsers[domain] = parser

        return self.parsers[domain].can_fetch(self.user_agent, url)

    async def throttle(self, url: str, delay: float = 0.5):
        domain = urlparse(url).netloc
        async with self._lock:
            now = asyncio.get_event_loop().time()
            elapsed = now - self.last_accessed.get(domain, 0)
            if elapsed < delay:
                await asyncio.sleep(delay - elapsed)
            self.last_accessed[domain] = asyncio.get_event_loop().time()


class InvertedIndex:
    """In-memory inverted index with TF-IDF calculation."""

    STOPWORDS = {
        "a", "about", "above", "after", "again", "against", "all", "am", "an",
        "and", "any", "are", "aren't", "as", "at", "be", "because", "been",
        "before", "being", "below", "between", "both", "but", "by", "can",
        "did", "do", "does", "doing", "don", "down", "during", "each", "few",
        "for", "from", "further", "had", "has", "have", "having", "he", "her",
        "here", "hers", "herself", "him", "himself", "his", "how", "i", "if",
        "in", "into", "is", "it", "its", "itself", "just", "me", "more", "most",
        "my", "myself", "no", "nor", "not", "of", "off", "on", "once", "only",
        "or", "other", "our", "ours", "ourselves", "out", "over", "own", "s",
        "same", "she", "should", "so", "some", "such", "than", "that", "the",
        "their", "theirs", "them", "themselves", "then", "there", "these",
        "they", "this", "those", "through", "to", "too", "under", "until",
        "up", "very", "was", "we", "were", "what", "when", "where", "which",
        "while", "who", "whom", "why", "with", "you", "your", "yours"
    }

    def __init__(self):
        # term -> {doc_id: term_frequency}
        self.index: dict[str, dict[str, int]] = defaultdict(
            lambda: defaultdict(int)
        )
        self.doc_lengths: dict[str, int] = defaultdict(int)
        self.documents: dict[str, str] = {}  # doc_id -> raw text preview

    def _tokenize(self, text: str) -> list[str]:
        words = re.findall(r"\b[a-zA-Z0-9]+\b", text.lower())
        return [w for w in words if w not in self.STOPWORDS and len(w) > 1]

    def add_document(self, doc_id: str, text: str):
        tokens = self._tokenize(text)
        if not tokens:
            return

        self.documents[doc_id] = text[:200] + "..."
        self.doc_lengths[doc_id] = len(tokens)

        for token in tokens:
            self.index[token][doc_id] += 1

    def search(self, query: str, top_k: int = 5) -> list[tuple[str, float]]:
        query_tokens = self._tokenize(query)
        if not query_tokens:
            return []

        total_docs = len(self.doc_lengths)
        if total_docs == 0:
            return []

        scores: dict[str, float] = defaultdict(float)

        for token in query_tokens:
            if token not in self.index:
                continue

            posting_list = self.index[token]
            doc_freq = len(posting_list)
            # Inverse Document Frequency with smoothing
            idf = math.log(1 + (total_docs / (1 + doc_freq)))

            for doc_id, tf_count in posting_list.items():
                # Normalized Term Frequency
                tf = tf_count / self.doc_lengths[doc_id]
                scores[doc_id] += tf * idf

        return sorted(scores.items(), key=lambda x: x[1], reverse=True)[:top_k]


class WebCrawler:
    """Concurrent, polite web crawler using asyncio and aiohttp."""

    def __init__(
        self,
        seed_urls: list[str],
        max_pages: int = 50,
        concurrency: int = 10,
    ):
        self.seed_urls = seed_urls
        self.max_pages = max_pages
        self.semaphore = asyncio.Semaphore(concurrency)
        self.visited: set[str] = set()
        self.queue: asyncio.Queue[str] = asyncio.Queue()
        self.limiter = RobotsAndRateLimiter()
        self.index = InvertedIndex()

    def _extract_links(self, html: str, base_url: str) -> list[str]:
        soup = BeautifulSoup(html, "html.parser")
        links = []
        for anchor in soup.find_all("a", href=True):
            href = anchor["href"]
            absolute_url = urljoin(base_url, href)
            parsed = urlparse(absolute_url)
            # Retain HTTP/HTTPS protocols and strip anchor fragments
            if parsed.scheme in ("http", "https"):
                clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
                links.append(clean_url)
        return links

    def _extract_text(self, html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")
        for element in soup(["script", "style", "nav", "footer", "header"]):
            element.decompose()
        return soup.get_text(separator=" ", strip=True)

    async def _fetch_and_parse(
        self, session: aiohttp.ClientSession, url: str
    ):
        async with self.semaphore:
            if len(self.visited) >= self.max_pages:
                return

            if not await self.limiter.can_fetch(session, url):
                return

            await self.limiter.throttle(url)

            try:
                headers = {"User-Agent": self.limiter.user_agent}
                async with session.get(
                    url, headers=headers, timeout=10
                ) as response:
                    if (
                        response.status != 200
                        or "text/html" not in response.headers.get("Content-Type", "")
                    ):
                        return

                    html = await response.text()
                    text = self._extract_text(html)

                    # Update Index & Tracking
                    self.index.add_document(url, text)
                    self.visited.add(url)

                    # Extract new links for traversal
                    for link in self._extract_links(html, url):
                        if (
                            link not in self.visited
                            and len(self.visited) < self.max_pages
                        ):
                            await self.queue.put(link)

            except Exception:
                # Silently drop failed requests to keep pipeline resilient
                pass

    async def run(self):
        for seed in self.seed_urls:
            await self.queue.put(seed)

        async with aiohttp.ClientSession() as session:
            workers = []
            while not self.queue.empty() and len(self.visited) < self.max_pages:
                url = await self.queue.get()
                if url in self.visited:
                    self.queue.task_done()
                    continue

                task = asyncio.create_task(self._fetch_and_parse(session, url))
                workers.append(task)
                self.queue.task_done()

                # Clean up finished tasks periodically
                if len(workers) >= 20:
                    done, pending = await asyncio.wait(
                        workers, return_when=asyncio.FIRST_COMPLETED
                    )
                    workers = list(pending)

            if workers:
                await asyncio.gather(*workers, return_exceptions=True)


# --- Execution Example ---
async def main():
    seed_urls = [
        "https://docs.python.org/3/",
    ]

    print("--- Starting Asynchronous Web Crawler ---")
    crawler = WebCrawler(seed_urls=seed_urls, max_pages=15, concurrency=5)
    await crawler.run()

    print(f"\nCrawling complete. Indexed {len(crawler.visited)} pages.")

    # Execute Search Queries against the inverted index
    queries = ["python documentation", "asyncio loop performance", "modules"]
    
    print("\n--- Executing TF-IDF Search Queries ---")
    for query in queries:
        print(f"\nQuery: '{query}'")
        results = crawler.index.search(query, top_k=3)
        if not results:
            print("  No matching documents found.")
            continue
        for rank, (doc_id, score) in enumerate(results, start=1):
            print(f"  [{rank}] Score: {score:.4f} | URL: {doc_id}")

if __name__ == "__main__":
    asyncio.run(main())