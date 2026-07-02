import argparse
import asyncio
import hashlib
import logging
import os
import random
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse

import httpx
import tiktoken
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
from openai import AsyncAzureOpenAI, AsyncOpenAI, NotFoundError

try:
	from playwright.async_api import TimeoutError as PlaywrightTimeoutError
	from playwright.async_api import async_playwright
except Exception:  # pragma: no cover
	PlaywrightTimeoutError = Exception
	async_playwright = None


load_dotenv()

MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "prop_main")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_API_ENDPOINT = os.getenv("OPENAI_API_ENDPOINT")
OPENAI_API_VERSION = os.getenv("OPENAI_API_VERSION")
OPENAI_EMBEDDING_MODEL = os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small")
OPENAI_EMBEDDING_DEPLOYMENT = os.getenv("OPENAI_EMBEDDING_DEPLOYMENT")

DEFAULT_CATEGORY = "GENERAL"
ALLOWED_CATEGORIES = {"PROCESS_LAW", "DISTRICT_ESTATE", "GENERAL"}

USER_AGENTS = [
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]


@dataclass
class KnowledgeDoc:
	source_url: str
	title: str
	category: str
	content: str
	metadata: dict


class Scraper:
	def __init__(
		self,
		timeout_seconds: int = 30,
		max_retries: int = 3,
		min_delay_seconds: float = 1.0,
		max_delay_seconds: float = 2.0,
	) -> None:
		self.timeout_seconds = timeout_seconds
		self.max_retries = max_retries
		self.min_delay_seconds = min_delay_seconds
		self.max_delay_seconds = max_delay_seconds

	async def fetch_static(self, url: str) -> str:
		for attempt in range(1, self.max_retries + 1):
			headers = {"User-Agent": random.choice(USER_AGENTS)}
			try:
				async with httpx.AsyncClient(timeout=self.timeout_seconds, follow_redirects=True) as client:
					response = await client.get(url, headers=headers)
					response.raise_for_status()
				await asyncio.sleep(random.uniform(self.min_delay_seconds, self.max_delay_seconds))
				return response.text
			except Exception as exc:
				logging.warning(
					"Static fetch failed for %s (attempt %s/%s): %s",
					url,
					attempt,
					self.max_retries,
					exc,
				)
				if attempt == self.max_retries:
					raise
				await asyncio.sleep(min(2 * attempt, 6))
		raise RuntimeError(f"Failed to fetch static URL: {url}")

	async def fetch_dynamic(self, url: str) -> str:
		if async_playwright is None:
			raise RuntimeError(
				"Playwright is not available. Install it via requirements and run: playwright install chromium"
			)

		for attempt in range(1, self.max_retries + 1):
			try:
				async with async_playwright() as p:
					browser = await p.chromium.launch(headless=True)
					context = await browser.new_context(user_agent=random.choice(USER_AGENTS))
					page = await context.new_page()
					await page.goto(url, wait_until="networkidle", timeout=self.timeout_seconds * 1000)
					html = await page.content()
					await browser.close()
				await asyncio.sleep(random.uniform(self.min_delay_seconds, self.max_delay_seconds))
				return html
			except PlaywrightTimeoutError as exc:
				logging.warning(
					"Dynamic fetch timeout for %s (attempt %s/%s): %s",
					url,
					attempt,
					self.max_retries,
					exc,
				)
				if attempt == self.max_retries:
					raise
				await asyncio.sleep(min(2 * attempt, 6))
			except Exception as exc:
				logging.warning(
					"Dynamic fetch failed for %s (attempt %s/%s): %s",
					url,
					attempt,
					self.max_retries,
					exc,
				)
				if attempt == self.max_retries:
					raise
				await asyncio.sleep(min(2 * attempt, 6))

		raise RuntimeError(f"Failed to fetch dynamic URL: {url}")

	async def fetch(self, url: str, prefer_dynamic: bool = False) -> str:
		if prefer_dynamic:
			try:
				return await self.fetch_dynamic(url)
			except Exception:
				logging.info("Falling back to static fetch for %s", url)
				return await self.fetch_static(url)

		return await self.fetch_static(url)

	@staticmethod
	def _normalize_text_block(text: str) -> str:
		text = re.sub(r"\s+", " ", text).strip()
		return text

	@staticmethod
	def _extract_table_text(target: BeautifulSoup) -> list[str]:
		table_blocks: list[str] = []
		for table_index, table in enumerate(target.select("table"), start=1):
			rows = table.select("tr")
			if not rows:
				continue

			header_cells = rows[0].select("th, td")
			headers = [Scraper._normalize_text_block(cell.get_text(" ", strip=True)) for cell in header_cells]
			has_header_row = bool(rows[0].select("th"))
			formatted_rows: list[str] = []

			data_rows = rows[1:] if has_header_row else rows
			for row in data_rows:
				cells = [Scraper._normalize_text_block(cell.get_text(" ", strip=True)) for cell in row.select("th, td")]
				cells = [cell for cell in cells if cell]
				if not cells:
					continue

				if has_header_row and len(headers) == len(cells):
					pairs = [f"{header}: {value}" for header, value in zip(headers, cells) if header and value]
					formatted_rows.append("; ".join(pairs))
				else:
					formatted_rows.append(" | ".join(cells))

			if not formatted_rows:
				continue

			table_title = f"Table {table_index}"
			if has_header_row and any(headers):
				table_title = f"Table {table_index}: {' | '.join([header for header in headers if header])}"
			table_blocks.append(f"{table_title}\n" + "\n".join(formatted_rows))

		return table_blocks

	@staticmethod
	def clean_html(html: str) -> tuple[str, str]:
		soup = BeautifulSoup(html, "lxml")

		# Remove known non-content regions and noisy tags before text extraction.
		for tag in soup(["script", "style", "noscript", "svg", "canvas", "form", "iframe"]):
			tag.decompose()

		for node in soup.select(
			"header, footer, nav, aside, .sidebar, .menu, .breadcrumb, .ads, .advertisement, .cookie, .popup"
		):
			node.decompose()

		title = ""
		if soup.title and soup.title.string:
			title = soup.title.string.strip()

		main_candidates = soup.select("main, article, .article, .post, #content, .content")
		target = main_candidates[0] if main_candidates else soup.body or soup
		table_blocks = Scraper._extract_table_text(target)
		for table in target.select("table"):
			table.decompose()

		raw_text = target.get_text(separator="\n", strip=True)
		text = re.sub(r"\n{2,}", "\n\n", raw_text)
		text = re.sub(r"[ \t]{2,}", " ", text).strip()
		if table_blocks:
			text = f"{text}\n\nStructured tables\n" + "\n\n".join(table_blocks)
		return title, text


class RecursiveTokenChunker:
	def __init__(
		self,
		model_encoding: str = "cl100k_base",
		target_tokens: int = 500,
		overlap_tokens: int = 80,
	) -> None:
		self.encoder = tiktoken.get_encoding(model_encoding)
		self.target_tokens = target_tokens
		self.overlap_tokens = overlap_tokens
		self.separators = ["\n\n", "\n", ". ", "? ", "! ", "; ", ", ", " "]

	def token_count(self, text: str) -> int:
		return len(self.encoder.encode(text))

	def _split_recursive(self, text: str, separator_index: int = 0) -> list[str]:
		text = text.strip()
		if not text:
			return []
		if self.token_count(text) <= self.target_tokens:
			return [text]
		if separator_index >= len(self.separators):
			return self._force_split(text)

		sep = self.separators[separator_index]
		parts = text.split(sep)
		if len(parts) == 1:
			return self._split_recursive(text, separator_index + 1)

		chunks: list[str] = []
		current = ""
		for part in parts:
			candidate = f"{current}{sep}{part}".strip() if current else part
			if self.token_count(candidate) <= self.target_tokens:
				current = candidate
				continue

			if current:
				chunks.extend(self._split_recursive(current, separator_index + 1))
			current = part

		if current:
			chunks.extend(self._split_recursive(current, separator_index + 1))

		return chunks

	def _force_split(self, text: str) -> list[str]:
		tokens = self.encoder.encode(text)
		chunks: list[str] = []
		step = max(1, self.target_tokens - self.overlap_tokens)
		for start in range(0, len(tokens), step):
			end = start + self.target_tokens
			token_slice = tokens[start:end]
			if not token_slice:
				continue
			chunks.append(self.encoder.decode(token_slice).strip())
			if end >= len(tokens):
				break
		return [c for c in chunks if c]

	def chunk(self, text: str) -> list[str]:
		base_chunks = [c for c in self._split_recursive(text) if c]
		if not base_chunks:
			return []

		# Add overlap by carrying trailing tokens from previous chunk.
		out: list[str] = []
		for idx, chunk in enumerate(base_chunks):
			if idx == 0:
				out.append(chunk)
				continue
			prev_tokens = self.encoder.encode(out[-1])
			overlap_slice = prev_tokens[-self.overlap_tokens :] if self.overlap_tokens > 0 else []
			overlap_text = self.encoder.decode(overlap_slice).strip() if overlap_slice else ""
			merged = f"{overlap_text}\n\n{chunk}".strip() if overlap_text else chunk

			if self.token_count(merged) > self.target_tokens + self.overlap_tokens:
				trimmed_tokens = self.encoder.encode(merged)[: self.target_tokens + self.overlap_tokens]
				merged = self.encoder.decode(trimmed_tokens).strip()
			out.append(merged)

		return out


class KnowledgePipeline:
	def __init__(
		self,
		mongo_uri: str,
		openai_api_key: str,
		mongo_db: str = "prop_main",
		collection_name: str = "knowledge_base",
		embed_model: str = OPENAI_EMBEDDING_MODEL,
		embedding_dim: int = 1536,
		embedding_batch_size: int = 64,
		mongo_batch_size: int = 100,
	) -> None:
		self.mongo_uri = mongo_uri
		self.mongo_db = mongo_db
		self.collection_name = collection_name
		self.embed_model = embed_model
		self.embedding_dim = embedding_dim
		self.embedding_batch_size = embedding_batch_size
		self.mongo_batch_size = mongo_batch_size
		self.uses_azure = bool(OPENAI_API_ENDPOINT and OPENAI_API_VERSION)
		self.azure_deployment = OPENAI_EMBEDDING_DEPLOYMENT or self.embed_model

		self.mongo_client = AsyncIOMotorClient(self.mongo_uri)
		self.db = self.mongo_client[self.mongo_db]
		self.collection = self.db[self.collection_name]
		if self.uses_azure:
			self.openai_client = AsyncAzureOpenAI(
				azure_endpoint=OPENAI_API_ENDPOINT,
				api_key=openai_api_key,
				api_version=OPENAI_API_VERSION,
			)
			logging.info(
				"Using Azure OpenAI embeddings endpoint %s with deployment %s",
				OPENAI_API_ENDPOINT,
				self.azure_deployment,
			)
		else:
			self.openai_client = AsyncOpenAI(api_key=openai_api_key)
			logging.info("Using public OpenAI embeddings endpoint with model %s", self.embed_model)

	async def ensure_indexes(self) -> None:
		await self.collection.create_index("chunk_id", unique=True)
		await self.collection.create_index("source_url")
		await self.collection.create_index("category")
		await self.collection.create_index("metadata.scraped_at")

	async def generate_embeddings(self, texts: list[str]) -> list[list[float]]:
		vectors: list[list[float]] = []
		for i in range(0, len(texts), self.embedding_batch_size):
			batch = texts[i : i + self.embedding_batch_size]
			try:
				resp = await self.openai_client.embeddings.create(
					model=self.azure_deployment if self.uses_azure else self.embed_model,
					input=batch,
				)
			except NotFoundError as exc:
				if self.uses_azure:
					raise RuntimeError(
						"Azure OpenAI embedding deployment was not found. Set OPENAI_EMBEDDING_DEPLOYMENT "
						"to your Azure deployment name for the embedding model."
					) from exc
				raise
			batch_vectors = [item.embedding for item in resp.data]
			for vec in batch_vectors:
				if len(vec) != self.embedding_dim:
					raise ValueError(
						f"Unexpected embedding dimension {len(vec)} for model {self.embed_model}; expected {self.embedding_dim}"
					)
			vectors.extend(batch_vectors)
		return vectors

	async def upsert_chunks(self, rows: list[dict]) -> None:
		if not rows:
			return

		for i in range(0, len(rows), self.mongo_batch_size):
			batch = rows[i : i + self.mongo_batch_size]
			tasks = []
			for row in batch:
				update = {
					"$set": {
						"source_url": row["source_url"],
						"title": row["title"],
						"category": row["category"],
						"chunk_id": row["chunk_id"],
						"content": row["content"],
						"embedding": row["embedding"],
						"metadata": row["metadata"],
					}
				}
				tasks.append(
					self.collection.find_one_and_update(
						{"chunk_id": row["chunk_id"]},
						update,
						upsert=True,
						return_document=False,
					)
				)
			await asyncio.gather(*tasks)

	async def close(self) -> None:
		self.mongo_client.close()
		await self.openai_client.close()


def infer_category(url: str, title: str, content: str) -> str:
	haystack = f"{url}\n{title}\n{content[:2000]}".lower()
	process_terms = [
		"stamp duty",
		"ssd",
		"bsd",
		"mortgage",
		"legal",
		"tenancy agreement",
		"rental",
		"conveyancing",
		"buying process",
	]
	district_terms = [
		"district",
		"estate",
		"neighborhood",
		"mtr",
		"school net",
		"pet-friendly",
		"tseung kwan o",
		"sai kung",
	]

	if any(term in haystack for term in process_terms):
		return "PROCESS_LAW"
	if any(term in haystack for term in district_terms):
		return "DISTRICT_ESTATE"
	return DEFAULT_CATEGORY


def build_chunk_rows(
	source_url: str,
	title: str,
	category: str,
	chunks: Iterable[str],
	districts: list[str] | None,
	estates: list[str] | None,
) -> list[dict]:
	clean_title = title or source_url
	doc_hash = hashlib.sha256(f"{source_url}|{clean_title}".encode("utf-8")).hexdigest()[:16]
	now = datetime.now(timezone.utc)

	rows: list[dict] = []
	for idx, chunk in enumerate(chunks):
		if not chunk.strip():
			continue
		chunk_id = f"{doc_hash}_{idx:04d}"
		rows.append(
			{
				"source_url": source_url,
				"title": clean_title,
				"category": category if category in ALLOWED_CATEGORIES else DEFAULT_CATEGORY,
				"chunk_id": chunk_id,
				"content": chunk.strip(),
				"metadata": {
					"districts": districts or [],
					"estates": estates or [],
					"scraped_at": now,
				},
			}
		)
	return rows


def load_urls_from_file(file_path: str) -> list[str]:
	path = Path(file_path)
	if not path.exists():
		raise FileNotFoundError(f"URL file not found: {file_path}")

	urls: list[str] = []
	with path.open("r", encoding="utf-8") as f:
		for raw_line in f:
			line = raw_line.strip()
			if not line or line.startswith("#"):
				continue
			if "#" in line:
				line = line.split("#", 1)[0].strip()
			if not line:
				continue

			parsed = urlparse(line)
			if parsed.scheme not in {"http", "https"} or not parsed.netloc:
				logging.warning("Skip invalid URL in file: %s", line)
				continue
			urls.append(line)

	return urls


def dedupe_preserve_order(items: list[str]) -> list[str]:
	return list(dict.fromkeys(items))


def chunk_list(items: list[str], size: int) -> list[list[str]]:
	if size <= 0:
		return [items]
	return [items[i : i + size] for i in range(0, len(items), size)]


async def process_urls(
	urls: list[str],
	prefer_dynamic: bool,
	category_override: str | None,
	districts: list[str] | None,
	estates: list[str] | None,
) -> None:
	if not MONGODB_CONNECTION_STRING:
		raise RuntimeError("Missing MONGODB_CONNECTION_STRING in environment.")
	if not OPENAI_API_KEY:
		raise RuntimeError("Missing OPENAI_API_KEY in environment.")
	if bool(OPENAI_API_ENDPOINT) != bool(OPENAI_API_VERSION):
		raise RuntimeError("OPENAI_API_ENDPOINT and OPENAI_API_VERSION must be set together for Azure OpenAI.")
	if category_override and category_override not in ALLOWED_CATEGORIES:
		raise ValueError(f"Invalid category {category_override}. Must be one of {sorted(ALLOWED_CATEGORIES)}")

	scraper = Scraper()
	chunker = RecursiveTokenChunker(target_tokens=500, overlap_tokens=80)
	pipeline = KnowledgePipeline(
		mongo_uri=MONGODB_CONNECTION_STRING,
		openai_api_key=OPENAI_API_KEY,
		mongo_db=MONGODB_DATABASE,
		collection_name="knowledge_base",
	)

	await pipeline.ensure_indexes()

	total_rows: list[dict] = []
	failed_urls: list[str] = []
	for url in urls:
		logging.info("Scraping %s", url)
		try:
			html = await scraper.fetch(url, prefer_dynamic=prefer_dynamic)
		except Exception as exc:
			failed_urls.append(url)
			logging.warning("Skipping %s after fetch failure: %s", url, exc)
			continue
		title, cleaned_text = scraper.clean_html(html)
		if not cleaned_text:
			logging.warning("No cleaned text extracted for %s", url)
			continue

		chunks = chunker.chunk(cleaned_text)
		if not chunks:
			logging.warning("No chunks generated for %s", url)
			continue

		category = category_override or infer_category(url, title, cleaned_text)
		rows = build_chunk_rows(
			source_url=url,
			title=title,
			category=category,
			chunks=chunks,
			districts=districts,
			estates=estates,
		)
		total_rows.extend(rows)
		logging.info("Prepared %s chunks for %s", len(rows), url)

	if not total_rows:
		logging.info("No rows to embed/upsert.")
		await pipeline.close()
		return

	texts = [row["content"] for row in total_rows]
	embeddings = await pipeline.generate_embeddings(texts)
	for idx, embedding in enumerate(embeddings):
		total_rows[idx]["embedding"] = embedding

	await pipeline.upsert_chunks(total_rows)
	logging.info("Upserted %s knowledge chunks into %s.%s", len(total_rows), MONGODB_DATABASE, "knowledge_base")
	if failed_urls:
		logging.warning("Skipped %s URLs due to fetch failures.", len(failed_urls))
	await pipeline.close()


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(description="Scrape HK property knowledge pages and store chunk embeddings in MongoDB.")
	parser.add_argument("--urls", nargs="+", default=[], help="One or more source URLs to scrape.")
	parser.add_argument(
		"--url-file",
		default="static/knowledge_seed_urls.txt",
		help="Path to newline-delimited seed URL file. Lines starting with # are ignored.",
	)
	parser.add_argument(
		"--url-batch-size",
		type=int,
		default=0,
		help="Optional number of URLs per processing batch (0 means process all at once).",
	)
	parser.add_argument(
		"--category",
		choices=sorted(ALLOWED_CATEGORIES),
		default=None,
		help="Optional fixed category override for all URLs.",
	)
	parser.add_argument(
		"--districts",
		nargs="*",
		default=[],
		help="Optional district tags to apply to all chunks from the run.",
	)
	parser.add_argument(
		"--estates",
		nargs="*",
		default=[],
		help="Optional estate tags to apply to all chunks from the run.",
	)
	parser.add_argument(
		"--prefer-dynamic",
		action="store_true",
		help="Try Playwright first, then fallback to static requests+BS4.",
	)
	parser.add_argument(
		"--log-level",
		default="INFO",
		choices=["DEBUG", "INFO", "WARNING", "ERROR"],
		help="Log verbosity.",
	)
	return parser.parse_args()


def main() -> None:
	args = parse_args()
	logging.basicConfig(
		level=getattr(logging, args.log_level),
		format="%(asctime)s %(levelname)s %(message)s",
	)
	input_urls = list(args.urls)
	if args.url_file:
		input_urls.extend(load_urls_from_file(args.url_file))

	input_urls = dedupe_preserve_order(input_urls)
	if not input_urls:
		raise ValueError("No URLs provided. Use --urls and/or --url-file.")

	url_batches = chunk_list(input_urls, args.url_batch_size)
	for batch_index, batch_urls in enumerate(url_batches, start=1):
		logging.info(
			"Running URL batch %s/%s with %s URLs",
			batch_index,
			len(url_batches),
			len(batch_urls),
		)
		asyncio.run(
			process_urls(
				urls=batch_urls,
				prefer_dynamic=args.prefer_dynamic,
				category_override=args.category,
				districts=args.districts,
				estates=args.estates,
			)
		)


if __name__ == "__main__":
	main()


"""
MongoDB Atlas Vector Search Index (knowledge_base)

Create a Vector Search index on collection: knowledge_base

{
  "fields": [
	{
	  "type": "vector",
	  "path": "embedding",
	  "numDimensions": 1536,
	  "similarity": "cosine"
	},
	{
	  "type": "filter",
	  "path": "category"
	},
	{
	  "type": "filter",
	  "path": "metadata.districts"
	},
	{
	  "type": "filter",
	  "path": "metadata.estates"
	}
  ]
}
"""
