import argparse
import asyncio
import hashlib
import json
import logging
import os
import random
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import parse_qsl, urlencode, urlparse, urlsplit, urlunsplit

import httpx
import tiktoken
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
from openai import AsyncAzureOpenAI, AsyncOpenAI, NotFoundError
import trafilatura

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
SERPER_API_KEY = os.getenv("SERPER_API_KEY")

DEFAULT_CATEGORY = "GENERAL"
ALLOWED_CATEGORIES = {"PROCESS_LAW", "DISTRICT_ESTATE", "GENERAL"}

DEFAULT_NOISE_URL_PATTERNS = [
	r"/tag/",
	r"/tags/",
	r"/category/",
	r"/author/",
	r"/feed/?$",
	r"/wp-json",
	r"/privacy",
	r"/terms",
	r"/cookie",
	r"/login",
	r"/signup",
	r"/register",
	r"/cart",
	r"/checkout",
	r"facebook\\.com",
	r"instagram\\.com",
	r"x\\.com",
	r"twitter\\.com",
	r"youtube\\.com",
	r"tiktok\\.com",
]

NOISE_LINE_PATTERNS = [
	r"^menu$",
	r"^navigation$",
	r"^main menu$",
	r"^skip to content$",
	r"^cookie( policy| settings)?$",
	r"^privacy policy$",
	r"^terms( of service)?$",
	r"^all rights reserved",
	r"^copyright",
	r"^subscribe$",
	r"^newsletter$",
	r"^back to top$",
	r"^follow us$",
	r"^share( this)?$",
	r"^home$",
	r"^about$",
	r"^contact$",
	r"^登入$",
	r"^註冊$",
	r"^主頁$",
	r"^首頁$",
	r"^返回頂部$",
	r"^私隱政策$",
	r"^服務條款$",
	r"^版權所有$",
	r"^更多$",
	r"^下一頁$",
	r"^上一頁$",
	r"^廣告$",
	r"^贊助內容$",
  f"即日起",
  f"可享",
  f"優惠",
  f"立即登記",
  f"立即報名",
  f"立即申請",
  f"立即預約",
  f"立即下載",
  f"立即訂閱",
  f"立即購買",
  f"立即註冊",
  f"立即登入",
  f"立即參加",
  f"MoneyHero",
  f"推廣",
  f"限時優惠",
  f"優惠碼",
  f"迎新禮品",
  f"送總值",
  f"Zurich",
  f"WeLend",
  f"蘇黎世",
]

NAV_KEYWORDS = [
	"menu",
	"navigation",
	"cookie",
	"privacy",
	"terms",
	"subscribe",
	"follow",
	"share",
	"login",
	"signup",
	"register",
	"home",
	"contact",
	"關於",
	"聯絡",
	"登入",
	"註冊",
	"主頁",
	"首頁",
	"私隱",
	"條款",
	"版權",
	"返回",
	"廣告",
	"贊助",
]

USER_AGENTS = [
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

DEFAULT_RENT_RELATED_KEYWORDS = [
	"香港 租樓",
	"香港 租房",
	"香港 租屋",
	"香港 租盤",
	"香港 租樓 流程",
	"香港 租樓 注意事項",
	"香港 租樓 攻略",
	"香港 租樓 新手",
	"香港 租樓 合約",
	"香港 租樓 租約",
	"香港 租樓 打厘印",
	"香港 租樓 印花稅",
	"香港 租樓 佣金",
	"香港 租樓 按金",
	"香港 租樓 水電煤",
	"香港 租樓 管理費",
	"香港 租樓 差餉",
	"香港 租樓 地租",
	"香港 租樓 短租",
	"香港 租樓 長租",
	"香港 租樓 住宅",
	"香港 租樓 劏房",
	"香港 租樓 開放式",
	"香港 租樓 一房",
	"香港 租樓 兩房",
	"香港 租樓 三房",
	"香港 租樓 連傢俬",
	"香港 租樓 連電器",
	"香港 租樓 免佣",
	"香港 租樓 業主盤",
	"香港 租樓 代理盤",
	"香港 租樓 筍盤",
	"香港 租樓 平租",
	"香港 租樓 豪宅",
	"香港 租樓 港島",
	"香港 租樓 九龍",
	"香港 租樓 新界",
	"香港 租樓 地鐵沿線",
	"香港 租樓 校網",
	"香港 租樓 寵物",
	"香港 租樓 村屋",
	"香港 租樓 唐樓",
	"香港 租樓 私樓",
	"香港 租樓 公屋",
	"香港 租樓 居屋",
	"香港 租樓 凶宅",
	"香港 租樓 風險",
	"香港 租樓 驗樓",
	"香港 租樓 睇樓",
	"香港 租樓 議價",
	"香港 租樓 續租",
	"香港 租樓 終止租約",
	"香港 租樓 退按金",
	"香港 租樓 常見陷阱",
	"香港 租樓 常見問題",
	"Hong Kong rent apartment",
	"Hong Kong tenancy agreement",
	"Hong Kong renting guide",
	"Hong Kong landlord tenant rules",
	"Hong Kong lease stamp duty",
	"Hong Kong serviced apartment rent",
	"香港 樓市 租務",
	"香港 住宅 租金 走勢",
	"香港 區域 租金 比較",
	"香港 地區 租金",
	"香港 屋苑 租金",
	"香港 租樓 網站",
	"香港 租樓 平台",
	"租樓 合約 樣本 香港",
	"租客 權益 香港",
	"業主 租客 爭議 香港",
]


@dataclass
class KnowledgeDoc:
	source_url: str
	title: str
	category: str
	content: str
	metadata: dict


class SerperClient:
	BASE_URL = "https://google.serper.dev/search"

	def __init__(self, api_key: str, timeout_seconds: int = 20) -> None:
		self.api_key = api_key
		self.timeout_seconds = timeout_seconds

	async def search(
		self,
		query: str,
		num: int = 10,
		gl: str = "hk",
		hl: str = "zh-tw",
		location: str = "Hong Kong",
	) -> list[str]:
		headers = {"X-API-KEY": self.api_key, "Content-Type": "application/json"}
		payload = {
			"q": query,
			"num": num,
			"gl": gl,
			"hl": hl,
			"location": location,
		}

		async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
			response = await client.post(self.BASE_URL, headers=headers, json=payload)
			response.raise_for_status()
			data = response.json()

		links: list[str] = []
		for row in data.get("organic", []):
			link = row.get("link")
			if not link:
				continue
			parsed = urlparse(link)
			if parsed.scheme in {"http", "https"} and parsed.netloc:
				links.append(link)
		return links


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
		title = ""
		try:
			title_soup = BeautifulSoup(html, "html.parser")
			if title_soup.title and title_soup.title.string:
				title = title_soup.title.string.strip()
		except Exception:
			pass

		# Prefer Trafilatura to reduce page chrome noise (menus, footers, ads).
		trafilatura_text = ""
		try:
			trafilatura_text = trafilatura.extract(
				html,
				output_format="markdown",
				include_tables=True,
				include_links=False,
				include_images=False,
				include_comments=False,
				favor_precision=True,
				deduplicate=True,
			) or ""
		except Exception as exc:
			logging.debug("Trafilatura extraction failed: %s", exc)

		if trafilatura_text.strip():
			cleaned = re.sub(r"\n{2,}", "\n\n", trafilatura_text)
			cleaned = re.sub(r"[ \t]{2,}", " ", cleaned).strip()
			return title, cleaned

		# Some pages contain broken namespace-like attributes that can crash the lxml builder.
		# Try lxml first for speed/quality, then fallback to stdlib parser for resilience.
		soup = None
		parse_errors: list[str] = []
		for parser in ("lxml", "html.parser"):
			try:
				soup = BeautifulSoup(html, parser)
				break
			except Exception as exc:
				parse_errors.append(f"{parser}: {exc}")

		if soup is None:
			raise RuntimeError(f"Unable to parse HTML with available parsers ({'; '.join(parse_errors)})")

		# Remove known non-content regions and noisy tags before text extraction.
		for tag in soup(["script", "style", "noscript", "svg", "canvas", "form", "iframe"]):
			tag.decompose()

		for node in soup.select(
			"header, footer, nav, aside, .sidebar, .menu, .breadcrumb, .ads, .advertisement, .cookie, .popup"
		):
			node.decompose()

		if not title and soup.title and soup.title.string:
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
		self.separators = ["\n## ", "\n### ", "\n# ", "\n\n", "\n", ". ", "? ", "! ", "; ", ", ", " "]

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


def load_keywords_from_file(file_path: str) -> list[str]:
	path = Path(file_path)
	if not path.exists():
		raise FileNotFoundError(f"Keyword file not found: {file_path}")

	keywords: list[str] = []
	with path.open("r", encoding="utf-8") as f:
		for raw_line in f:
			line = raw_line.strip()
			if not line or line.startswith("#"):
				continue
			if "#" in line:
				line = line.split("#", 1)[0].strip()
			if line:
				keywords.append(line)
	return keywords


def _normalize_line(line: str) -> str:
	line = re.sub(r"\s+", " ", line).strip()
	return line


def _strip_tracking_params_from_url(url: str) -> str:
	try:
		split = urlsplit(url)
		pairs = parse_qsl(split.query, keep_blank_values=True)
		clean_pairs = []
		for key, value in pairs:
			key_l = key.lower()
			if key_l.startswith("utm_"):
				continue
			if key_l in {"fbclid", "gclid", "msclkid", "igshid", "mc_cid", "mc_eid"}:
				continue
			clean_pairs.append((key, value))

		clean_query = urlencode(clean_pairs, doseq=True)
		clean_split = split._replace(query=clean_query)
		return urlunsplit(clean_split)
	except Exception:
		return url


def _strip_tracking_params_in_text(text: str) -> str:
	def repl(match: re.Match[str]) -> str:
		url = match.group(0)
		trimmed = _strip_tracking_params_from_url(url)
		return trimmed

	# Match plain URLs in text. Avoid trailing punctuation in common cases.
	return re.sub(r"https?://[^\s)\]>\"']+", repl, text)


def text_sanitizer(text: str) -> str:
	if not text:
		return ""

	# Remove markdown image syntax: ![alt](url)
	text = re.sub(r"!\[.*?\]\(.*?\)", "", text)

	# Remove markdown links but keep the anchor text: [text](url) -> text
	text = re.sub(r"\[(.*?)\]\(.*?\)", r"\1", text)

	# Remove javascript pseudo-links that sometimes leak from bad markup.
	text = re.sub(r"javascript:\s*void\(0\)", "", text, flags=re.IGNORECASE)

	# Remove leftover naked javascript pseudo-links in markdown-like wrappers.
	text = re.sub(r"\(\s*javascript:\s*void\(0\)\s*\)", "", text, flags=re.IGNORECASE)

	# Strip tracking params from any plain URLs that remain in text.
	text = _strip_tracking_params_in_text(text)

	# Normalize common HTML entities frequently left in extracted content.
	text = text.replace("&nbsp;", " ").replace("&amp;", "&").replace("&middot;", "·")

	# Collapse excessive blank lines while keeping readable paragraphs.
	text = re.sub(r"\n{3,}", "\n\n", text)

	return text.strip()


def denoise_text(text: str) -> str:
	lines = [_normalize_line(line) for line in text.splitlines()]
	lines = [line for line in lines if line]
	if not lines:
		return ""

	cleaned_lines: list[str] = []
	for line in lines:
		line_l = line.lower()
		if any(re.search(pattern, line_l) for pattern in NOISE_LINE_PATTERNS):
			continue
		if len(line) <= 2:
			continue
		cleaned_lines.append(line)

	# Remove exact duplicate lines while preserving order.
	cleaned_lines = dedupe_preserve_order(cleaned_lines)

	out = "\n".join(cleaned_lines)
	out = re.sub(r"\n{3,}", "\n\n", out).strip()
	return out


def assess_text_quality(
	text: str,
	min_chars: int,
	min_lines: int,
	min_unique_line_ratio: float,
	max_nav_keyword_line_ratio: float,
) -> tuple[bool, str]:
	if not text.strip():
		return False, "empty text"

	lines = [_normalize_line(line) for line in text.splitlines() if _normalize_line(line)]
	if len(lines) < max(1, min_lines):
		return False, f"too few lines ({len(lines)} < {min_lines})"

	char_count = len(text)
	if char_count < max(1, min_chars):
		return False, f"too short ({char_count} < {min_chars} chars)"

	unique_ratio = len(set(lines)) / max(1, len(lines))
	if unique_ratio < min_unique_line_ratio:
		return False, f"low unique-line ratio ({unique_ratio:.2f} < {min_unique_line_ratio:.2f})"

	nav_lines = 0
	for line in lines:
		line_l = line.lower()
		if any(keyword in line_l for keyword in NAV_KEYWORDS):
			nav_lines += 1
	nav_ratio = nav_lines / max(1, len(lines))
	if nav_ratio > max_nav_keyword_line_ratio:
		return False, f"too many navigation/noise lines ({nav_ratio:.2f} > {max_nav_keyword_line_ratio:.2f})"

	return True, "ok"


def should_skip_url(url: str, patterns: list[str]) -> tuple[bool, str]:
	if not patterns:
		return False, ""
	for pattern in patterns:
		if re.search(pattern, url, flags=re.IGNORECASE):
			return True, pattern
	return False, ""


def dedupe_preserve_order(items: list[str]) -> list[str]:
	return list(dict.fromkeys(items))


def chunk_list(items: list[str], size: int) -> list[list[str]]:
	if size <= 0:
		return [items]
	return [items[i : i + size] for i in range(0, len(items), size)]


def _serper_cache_key(
	keywords: list[str],
	num_per_query: int,
	gl: str,
	hl: str,
	location: str,
	max_queries: int,
) -> str:
	payload = {
		"keywords": keywords,
		"num_per_query": num_per_query,
		"gl": gl,
		"hl": hl,
		"location": location,
		"max_queries": max_queries,
	}
	normalized = json.dumps(payload, ensure_ascii=False, sort_keys=True)
	return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _load_serper_cache(cache_file: str) -> dict:
	path = Path(cache_file)
	if not path.exists():
		return {"version": 1, "entries": {}}

	try:
		with path.open("r", encoding="utf-8") as f:
			data = json.load(f)
		if not isinstance(data, dict):
			return {"version": 1, "entries": {}}
		entries = data.get("entries")
		if not isinstance(entries, dict):
			data["entries"] = {}
		if "version" not in data:
			data["version"] = 1
		return data
	except Exception as exc:
		logging.warning("Failed to read Serper cache file %s: %s", cache_file, exc)
		return {"version": 1, "entries": {}}


def _save_serper_cache(cache_file: str, cache: dict) -> None:
	path = Path(cache_file)
	path.parent.mkdir(parents=True, exist_ok=True)
	temp_path = path.with_suffix(path.suffix + ".tmp")
	with temp_path.open("w", encoding="utf-8") as f:
		json.dump(cache, f, ensure_ascii=False, indent=2)
	temp_path.replace(path)


def _try_get_cached_serper_urls(
	cache_file: str,
	cache_key: str,
	ttl_minutes: int,
) -> tuple[list[str] | None, int | None]:
	cache = _load_serper_cache(cache_file)
	entries = cache.get("entries", {})
	entry = entries.get(cache_key)
	if not isinstance(entry, dict):
		return None, None

	created_at_raw = entry.get("created_at")
	urls = entry.get("urls")
	if not isinstance(created_at_raw, str) or not isinstance(urls, list):
		return None, None

	try:
		created_at = datetime.fromisoformat(created_at_raw)
	except Exception:
		return None, None

	if created_at.tzinfo is None:
		created_at = created_at.replace(tzinfo=timezone.utc)

	now = datetime.now(timezone.utc)
	age_seconds = max(0, int((now - created_at).total_seconds()))
	if ttl_minutes > 0 and age_seconds > ttl_minutes * 60:
		return None, age_seconds

	clean_urls = [u for u in urls if isinstance(u, str) and u.strip()]
	return dedupe_preserve_order(clean_urls), age_seconds


def _update_serper_cache(
	cache_file: str,
	cache_key: str,
	keywords: list[str],
	num_per_query: int,
	gl: str,
	hl: str,
	location: str,
	max_queries: int,
	urls: list[str],
	max_entries: int = 50,
) -> None:
	cache = _load_serper_cache(cache_file)
	entries = cache.setdefault("entries", {})
	now = datetime.now(timezone.utc).isoformat()
	entries[cache_key] = {
		"created_at": now,
		"keywords": keywords,
		"num_per_query": num_per_query,
		"gl": gl,
		"hl": hl,
		"location": location,
		"max_queries": max_queries,
		"urls": dedupe_preserve_order(urls),
	}

	# Keep cache bounded by removing oldest entries first.
	if len(entries) > max_entries:
		sorted_keys = sorted(
			entries.keys(),
			key=lambda key: entries.get(key, {}).get("created_at", ""),
		)
		for key in sorted_keys[: len(entries) - max_entries]:
			entries.pop(key, None)

	_save_serper_cache(cache_file, cache)


async def collect_urls_from_serper(
	keywords: list[str],
	num_per_query: int,
	gl: str,
	hl: str,
	location: str,
	sleep_seconds: float,
	max_queries: int,
) -> list[str]:
	if not SERPER_API_KEY:
		raise RuntimeError("Missing SERPER_API_KEY in environment.")

	queries = dedupe_preserve_order([q.strip() for q in keywords if q.strip()])
	if max_queries > 0:
		queries = queries[:max_queries]

	if not queries:
		return []

	client = SerperClient(api_key=SERPER_API_KEY)
	out_urls: list[str] = []
	for idx, query in enumerate(queries, start=1):
		logging.info("Serper query %s/%s: %s", idx, len(queries), query)
		try:
			links = await client.search(
				query=query,
				num=num_per_query,
				gl=gl,
				hl=hl,
				location=location,
			)
		except Exception as exc:
			logging.warning("Serper query failed (%s): %s", query, exc)
			continue

		if links:
			logging.info("Serper returned %s links for query: %s", len(links), query)
			out_urls.extend(links)

		if sleep_seconds > 0:
			await asyncio.sleep(sleep_seconds)

	return dedupe_preserve_order(out_urls)


async def process_urls(
	urls: list[str],
	prefer_dynamic: bool,
	category_override: str | None,
	districts: list[str] | None,
	estates: list[str] | None,
	min_content_chars: int,
	min_content_lines: int,
	min_unique_line_ratio: float,
	max_nav_keyword_line_ratio: float,
	exclude_url_patterns: list[str],
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

	failed_urls: list[str] = []
	skipped_noise_urls: list[str] = []
	total_upserted_chunks = 0
	processed_urls = 0
	for url in urls:
		skip_url, skip_pattern = should_skip_url(url, exclude_url_patterns)
		if skip_url:
			skipped_noise_urls.append(url)
			logging.info("Skipping %s due to URL noise pattern: %s", url, skip_pattern)
			continue

		logging.info("Scraping %s", url)
		try:
			html = await scraper.fetch(url, prefer_dynamic=prefer_dynamic)
		except Exception as exc:
			failed_urls.append(url)
			logging.warning("Skipping %s after fetch failure: %s", url, exc)
			continue
		try:
			title, cleaned_text = scraper.clean_html(html)
		except Exception as exc:
			failed_urls.append(url)
			logging.warning("Skipping %s after clean_html failure: %s", url, exc)
			continue

		cleaned_text = text_sanitizer(cleaned_text)
		cleaned_text = denoise_text(cleaned_text)
		if not cleaned_text:
			logging.warning("No cleaned text extracted for %s", url)
			continue

		is_good_quality, quality_reason = assess_text_quality(
			text=cleaned_text,
			min_chars=min_content_chars,
			min_lines=min_content_lines,
			min_unique_line_ratio=min_unique_line_ratio,
			max_nav_keyword_line_ratio=max_nav_keyword_line_ratio,
		)
		if not is_good_quality:
			logging.info("Skipping %s due to low content quality: %s", url, quality_reason)
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
		logging.info("Prepared %s chunks for %s", len(rows), url)

		try:
			texts = [row["content"] for row in rows]
			embeddings = await pipeline.generate_embeddings(texts)
			for idx, embedding in enumerate(embeddings):
				rows[idx]["embedding"] = embedding
			await pipeline.upsert_chunks(rows)
		except Exception as exc:
			failed_urls.append(url)
			logging.warning("Skipping %s after embedding/upsert failure: %s", url, exc)
			continue

		processed_urls += 1
		total_upserted_chunks += len(rows)
		logging.info("Persisted %s chunks for %s", len(rows), url)

	if total_upserted_chunks == 0:
		logging.info("No rows were persisted.")
	else:
		logging.info(
			"Upserted %s knowledge chunks from %s URLs into %s.%s",
			total_upserted_chunks,
			processed_urls,
			MONGODB_DATABASE,
			"knowledge_base",
		)
	if failed_urls:
		logging.warning("Skipped %s URLs due to processing failures.", len(failed_urls))
	if skipped_noise_urls:
		logging.info("Skipped %s URLs due to URL noise filters.", len(skipped_noise_urls))
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
	parser.add_argument(
		"--use-serper",
		action="store_true",
		help="Use Serper keyword search to discover related URLs and merge with --urls/--url-file.",
	)
	parser.add_argument(
		"--serper-keywords",
		nargs="*",
		default=[],
		help="Extra Serper search keywords.",
	)
	parser.add_argument(
		"--serper-keyword-file",
		default=None,
		help="Optional newline-delimited keyword file for Serper searches.",
	)
	parser.add_argument(
		"--serper-no-default-keywords",
		action="store_true",
		help="Disable built-in rent-related default keyword set.",
	)
	parser.add_argument(
		"--serper-num-per-query",
		type=int,
		default=10,
		help="Number of search results to request per Serper query.",
	)
	parser.add_argument(
		"--serper-gl",
		default="hk",
		help="Serper gl (geo) parameter.",
	)
	parser.add_argument(
		"--serper-hl",
		default="zh-tw",
		help="Serper hl (language) parameter.",
	)
	parser.add_argument(
		"--serper-location",
		default="Hong Kong",
		help="Serper location parameter.",
	)
	parser.add_argument(
		"--serper-sleep-seconds",
		type=float,
		default=0.5,
		help="Delay between Serper queries to reduce burst traffic.",
	)
	parser.add_argument(
		"--serper-max-queries",
		type=int,
		default=0,
		help="Optional cap on number of Serper keywords to execute (0 means all).",
	)
	parser.add_argument(
		"--serper-cache-file",
		default="artifacts/knowledge/serper_search_cache.json",
		help="Cache file for Serper discovered URLs.",
	)
	parser.add_argument(
		"--serper-cache-ttl-minutes",
		type=int,
		default=120,
		help="Reuse cached Serper URLs if cache age is within this TTL (minutes). 0 means always stale.",
	)
	parser.add_argument(
		"--serper-refresh",
		action="store_true",
		help="Ignore Serper URL cache and force fresh Serper searches.",
	)
	parser.add_argument(
		"--min-content-chars",
		type=int,
		default=700,
		help="Minimum cleaned content length (characters) required before embedding.",
	)
	parser.add_argument(
		"--min-content-lines",
		type=int,
		default=8,
		help="Minimum cleaned content lines required before embedding.",
	)
	parser.add_argument(
		"--min-unique-line-ratio",
		type=float,
		default=0.55,
		help="Minimum ratio of unique lines to total lines after cleaning.",
	)
	parser.add_argument(
		"--max-nav-keyword-line-ratio",
		type=float,
		default=0.35,
		help="Maximum ratio of lines containing navigation/noise keywords.",
	)
	parser.add_argument(
		"--exclude-url-patterns",
		nargs="*",
		default=[],
		help="Additional regex patterns; matching URLs will be skipped.",
	)
	parser.add_argument(
		"--disable-default-noise-url-filters",
		action="store_true",
		help="Disable built-in noisy URL pattern filters.",
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

	if args.use_serper:
		keywords: list[str] = []
		if not args.serper_no_default_keywords:
			keywords.extend(DEFAULT_RENT_RELATED_KEYWORDS)
		keywords.extend(args.serper_keywords)
		if args.serper_keyword_file:
			keywords.extend(load_keywords_from_file(args.serper_keyword_file))

		keywords = dedupe_preserve_order(keywords)
		if not keywords:
			raise ValueError("Serper was enabled but no keywords were provided.")

		final_max_queries = max(0, args.serper_max_queries)
		if final_max_queries > 0:
			keywords_for_key = keywords[:final_max_queries]
		else:
			keywords_for_key = keywords

		cache_key = _serper_cache_key(
			keywords=keywords_for_key,
			num_per_query=max(1, args.serper_num_per_query),
			gl=args.serper_gl,
			hl=args.serper_hl,
			location=args.serper_location,
			max_queries=final_max_queries,
		)

		serper_urls: list[str] = []
		if not args.serper_refresh:
			cached_urls, cache_age_seconds = _try_get_cached_serper_urls(
				cache_file=args.serper_cache_file,
				cache_key=cache_key,
				ttl_minutes=max(0, args.serper_cache_ttl_minutes),
			)
			if cached_urls is not None:
				serper_urls = cached_urls
				age_text = f"{cache_age_seconds}s" if cache_age_seconds is not None else "unknown"
				logging.info(
					"Using %s cached Serper URLs from %s (age=%s)",
					len(serper_urls),
					args.serper_cache_file,
					age_text,
				)

		if not serper_urls:
			serper_urls = asyncio.run(
				collect_urls_from_serper(
					keywords=keywords,
					num_per_query=max(1, args.serper_num_per_query),
					gl=args.serper_gl,
					hl=args.serper_hl,
					location=args.serper_location,
					sleep_seconds=max(0.0, args.serper_sleep_seconds),
					max_queries=final_max_queries,
				)
			)
			_update_serper_cache(
				cache_file=args.serper_cache_file,
				cache_key=cache_key,
				keywords=keywords_for_key,
				num_per_query=max(1, args.serper_num_per_query),
				gl=args.serper_gl,
				hl=args.serper_hl,
				location=args.serper_location,
				max_queries=final_max_queries,
				urls=serper_urls,
			)
			logging.info("Saved %s Serper URLs into cache %s", len(serper_urls), args.serper_cache_file)
		logging.info("Collected %s URLs from Serper", len(serper_urls))
		input_urls.extend(serper_urls)

	input_urls = dedupe_preserve_order(input_urls)
	if not input_urls:
		raise ValueError("No URLs provided. Use --urls and/or --url-file.")

	url_batches = chunk_list(input_urls, args.url_batch_size)
	compiled_exclude_patterns: list[str] = []
	if not args.disable_default_noise_url_filters:
		compiled_exclude_patterns.extend(DEFAULT_NOISE_URL_PATTERNS)
	compiled_exclude_patterns.extend(args.exclude_url_patterns)
	compiled_exclude_patterns = dedupe_preserve_order(compiled_exclude_patterns)

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
				min_content_chars=max(1, args.min_content_chars),
				min_content_lines=max(1, args.min_content_lines),
				min_unique_line_ratio=min(1.0, max(0.0, args.min_unique_line_ratio)),
				max_nav_keyword_line_ratio=min(1.0, max(0.0, args.max_nav_keyword_line_ratio)),
				exclude_url_patterns=compiled_exclude_patterns,
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
