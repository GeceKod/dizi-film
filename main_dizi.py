from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html import unescape
from pathlib import Path
from threading import Lock, RLock, local
from typing import Any
from urllib.parse import parse_qs, urljoin, urlparse

import hashlib
import json
import logging
import os
import re
import shutil
import time

from bs4 import BeautifulSoup
from curl_cffi import requests
from seleniumbase import SB
import tmdbsimple as tmdb


DEFAULT_DESCRIPTION = "Açıklama yok."
DEFAULT_PLATFORM = "Platform Dışı"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)
CHALLENGE_MARKERS = (
    "just a moment",
    "cf-chl",
    "cf-browser-verification",
    "/cdn-cgi/challenge-platform/",
    "challenges.cloudflare.com/turnstile",
    "verify you are human",
)
TMDB_TITLE_CLEAN_RE = re.compile(
    r"\b(izle|full|hd|türkçe dublaj|turkce dublaj|altyazılı|altyazili|1080p|720p)\b",
    flags=re.IGNORECASE,
)
EPISODE_URL_RE = re.compile(r"-(\d+)-sezon-(\d+)-bolum", flags=re.IGNORECASE)
BACKGROUND_URL_RE = re.compile(r"url\(['\"]?(.*?)['\"]?\)", flags=re.IGNORECASE)
IFRAME_URL_RE = re.compile(
    r"""(?P<url>(?:(?:https?:)?//|/)[^"'<>\\\s]*(?:iframe\.php|embed|player)[^"'<>\\\s]*)""",
    flags=re.IGNORECASE,
)
IFRAME_SKIP_MARKERS = (
    "google-analytics.com",
    "googleads",
    "doubleclick.net",
    "facebook.com",
    "instagram.com",
    "twitter.com",
    "x.com",
    "yandex",
    "youtube.com",
    "youtu.be",
)
IFRAME_SKIP_EXTENSIONS = (
    ".js",
    ".css",
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".webp",
    ".svg",
    ".ico",
    ".woff",
    ".woff2",
    ".ttf",
    ".json",
    ".xml",
    ".txt",
)
TMDB_CACHE_VERSION = 3
TMDB_TV_VIDEO_LANGUAGE_FALLBACK = "tr,en-US,en,null"
TMDB_TRAILER_TYPE_PRIORITY = {
    "trailer": 0,
    "teaser": 1,
}
TMDB_TRAILER_LANGUAGE_PRIORITY = {
    "tr": 0,
    "en": 1,
    "": 2,
}
IMDB_TITLE_URL_TEMPLATE = "https://www.imdb.com/title/{imdb_id}/"
IMDB_VIDEO_PAGE_URL_TEMPLATE = "https://www.imdb.com/video/{video_id}/"
IMDB_TRAILER_LINK_RE = re.compile(
    r'href="(?P<href>/video/(?:vi\d+)/[^"]*)"[^>]*>.*?Play trailer(?: with sound)?',
    flags=re.IGNORECASE | re.DOTALL,
)
IMDB_TRAILER_ARIA_LINK_RE = re.compile(
    r'href="(?P<href>/video/(?:vi\d+)/[^"]*)"[^>]*aria-label="[^"]*trailer[^"]*"',
    flags=re.IGNORECASE | re.DOTALL,
)
IMDB_VIDEO_ID_RE = re.compile(r"/video/(?P<video_id>vi\d+)/", flags=re.IGNORECASE)

save_lock = Lock()
thread_local = local()
logger = logging.getLogger("dizi_sync")


@dataclass(frozen=True)
class AppConfig:
    base_domain: str
    data_file: Path
    legacy_data_file: Path
    state_file: Path
    log_file: Path
    backup_file: Path
    http_timeout: int
    http_retries: int
    http_retry_sleep: float
    selenium_wait_seconds: int
    selenium_headless: bool
    max_list_pages: int
    session_ttl: timedelta
    list_page_workers: int
    episode_workers: int
    checkpoint_item_interval: int
    checkpoint_time_seconds: int
    failed_retry_passes: int
    failed_retry_wait_seconds: float
    tmdb_hit_ttl: timedelta
    tmdb_miss_ttl: timedelta
    tmdb_error_ttl: timedelta
    browser_impersonation: str
    tmdb_api_key: str


@dataclass(frozen=True)
class SeriesListItem:
    url: str
    title: str
    poster: str


@dataclass
class FetchPayload:
    url: str
    status_code: int | None
    text: str = ""
    error: str = ""
    final_url: str = ""

    @property
    def challenge(self) -> bool:
        return is_cloudflare_challenge(self.text)

    def soup(self) -> BeautifulSoup | None:
        if not self.text:
            return None
        return BeautifulSoup(self.text, "html.parser")


@dataclass(frozen=True)
class PageFetchResult:
    page: int
    items: list[SeriesListItem]
    error: str = ""
    session_invalid: bool = False


@dataclass(frozen=True)
class EpisodeFetchResult:
    url: str
    episode: dict[str, Any] | None
    error: str = ""
    session_invalid: bool = False


@dataclass(frozen=True)
class SeriesProcessResult:
    status: str


def load_config() -> AppConfig:
    data_file = Path(os.getenv("DIZI_DATA_FILE", "diziler.json"))
    return AppConfig(
        base_domain=os.getenv("DIZI_BASE_DOMAIN", "https://dizipal.im").rstrip("/"),
        data_file=data_file,
        legacy_data_file=Path(os.getenv("DIZI_LEGACY_DATA_FILE", "diziler_full.json")),
        state_file=Path(os.getenv("DIZI_STATE_FILE", "diziler_state.json")),
        log_file=Path(os.getenv("DIZI_LOG_FILE", "logs/dizi_sync.log")),
        backup_file=Path(f"{data_file}.bak"),
        http_timeout=int(os.getenv("DIZI_HTTP_TIMEOUT", "20")),
        http_retries=int(os.getenv("DIZI_HTTP_RETRIES", "3")),
        http_retry_sleep=float(os.getenv("DIZI_HTTP_RETRY_SLEEP", "1.5")),
        selenium_wait_seconds=int(os.getenv("DIZI_SELENIUM_WAIT", "18")),
        selenium_headless=os.getenv("DIZI_SELENIUM_HEADLESS", "0") == "1",
        max_list_pages=max(0, int(os.getenv("DIZI_MAX_LIST_PAGES", "0"))),
        session_ttl=timedelta(hours=int(os.getenv("DIZI_SESSION_TTL_HOURS", "12"))),
        list_page_workers=max(1, min(4, int(os.getenv("DIZI_LIST_PAGE_WORKERS", "4")))),
        episode_workers=max(1, min(4, int(os.getenv("DIZI_EPISODE_WORKERS", "4")))),
        checkpoint_item_interval=max(1, int(os.getenv("DIZI_CHECKPOINT_ITEMS", "10"))),
        checkpoint_time_seconds=max(10, int(os.getenv("DIZI_CHECKPOINT_SECONDS", "60"))),
        failed_retry_passes=max(0, int(os.getenv("DIZI_FAILED_RETRY_PASSES", "1"))),
        failed_retry_wait_seconds=max(0.0, float(os.getenv("DIZI_FAILED_RETRY_WAIT_SECONDS", "5"))),
        tmdb_hit_ttl=timedelta(days=int(os.getenv("DIZI_TMDB_HIT_TTL_DAYS", "30"))),
        tmdb_miss_ttl=timedelta(days=int(os.getenv("DIZI_TMDB_MISS_TTL_DAYS", "7"))),
        tmdb_error_ttl=timedelta(hours=int(os.getenv("DIZI_TMDB_ERROR_TTL_HOURS", "6"))),
        browser_impersonation=os.getenv("DIZI_IMPERSONATE", "chrome110"),
        tmdb_api_key=os.getenv("TMDB_API_KEY", "48ce82f1de91232f542660e99a9d1336"),
    )


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat()


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def is_within_ttl(value: str | None, ttl: timedelta) -> bool:
    parsed = parse_iso_datetime(value)
    if parsed is None:
        return False
    return utc_now() - parsed <= ttl


def configure_logging(log_file: Path) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)


def default_state() -> dict[str, Any]:
    return {
        "version": 1,
        "session": {},
        "series": {},
        "tmdb_cache": {},
        "imdb_cache": {},
        "run": {},
    }


def normalize_state(raw_state: Any) -> dict[str, Any]:
    normalized = default_state()
    if not isinstance(raw_state, dict):
        return normalized

    if isinstance(raw_state.get("version"), int):
        normalized["version"] = raw_state["version"]

    for key in ("session", "series", "tmdb_cache", "imdb_cache", "run"):
        value = raw_state.get(key)
        if isinstance(value, dict):
            normalized[key] = value

    return normalized


def atomic_write_json(path: Path, payload: Any, backup_path: Path | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")

    try:
        with temp_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)

        with temp_path.open("r", encoding="utf-8") as handle:
            json.load(handle)

        if backup_path and path.exists():
            shutil.copy2(path, backup_path)

        os.replace(temp_path, path)
    finally:
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)


def save_state(path: Path, state: dict[str, Any]) -> None:
    atomic_write_json(path, state)


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return default_state()

    try:
        with path.open("r", encoding="utf-8") as handle:
            return normalize_state(json.load(handle))
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("State dosyasi okunamadi, sifirdan baslanacak: %s", exc)
        return default_state()


def load_json_list(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    if not isinstance(payload, list):
        raise ValueError(f"{path} liste formatinda degil.")

    return payload


def migrate_legacy_data_if_needed(config: AppConfig) -> None:
    if config.data_file.exists() or not config.legacy_data_file.exists():
        return

    legacy_payload = load_json_list(config.legacy_data_file)
    atomic_write_json(config.data_file, legacy_payload)
    logger.info(
        "Kanonik dizi dosyasi olusturuldu: %s -> %s",
        config.legacy_data_file.name,
        config.data_file.name,
    )


def load_series_database(config: AppConfig) -> list[dict[str, Any]]:
    migrate_legacy_data_if_needed(config)

    if not config.data_file.exists():
        return []

    try:
        return load_json_list(config.data_file)
    except (json.JSONDecodeError, OSError, ValueError) as exc:
        logger.error("Ana JSON okunamadi: %s", exc)
        if config.backup_file.exists():
            backup_payload = load_json_list(config.backup_file)
            atomic_write_json(config.data_file, backup_payload)
            logger.warning("Bozuk ana dosya yedekten geri yuklendi: %s", config.backup_file.name)
            return backup_payload
        raise RuntimeError(
            f"{config.data_file} bozuk ve gecerli bir {config.backup_file.name} bulunamadi."
        ) from exc


def get_http_session() -> requests.Session:
    session = getattr(thread_local, "session", None)
    if session is None:
        session = requests.Session()
        thread_local.session = session
    return session


def is_cloudflare_challenge(html: str) -> bool:
    lowered = html.lower()
    if any(marker in lowered for marker in CHALLENGE_MARKERS):
        return True
    if "attention required" in lowered and "cloudflare" in lowered:
        return True
    return False


def build_headers(user_agent: str | None, config: AppConfig) -> dict[str, str]:
    return {
        "User-Agent": user_agent or DEFAULT_USER_AGENT,
        "Referer": config.base_domain,
        "Accept-Language": "tr-TR,tr;q=0.9,en;q=0.8",
    }


def fetch_html(url: str, cookies: dict[str, str], user_agent: str, config: AppConfig) -> FetchPayload:
    last_error = ""
    for attempt in range(1, config.http_retries + 1):
        try:
            response = get_http_session().get(
                url,
                cookies=cookies or None,
                headers=build_headers(user_agent, config),
                impersonate=config.browser_impersonation,
                timeout=config.http_timeout,
                allow_redirects=True,
            )
            text = response.text or ""
            return FetchPayload(
                url=url,
                status_code=response.status_code,
                text=text,
                final_url=str(response.url),
            )
        except Exception as exc:
            last_error = str(exc)
            if attempt < config.http_retries:
                time.sleep(config.http_retry_sleep * attempt)

    return FetchPayload(url=url, status_code=None, error=last_error)


def normalize_site_url(url: str | None, base_domain: str) -> str:
    if not url:
        return ""
    return urljoin(base_domain + "/", url)


def unique_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return result


def detect_total_pages(soup: BeautifulSoup) -> int:
    total_pages = 1
    for anchor in soup.find_all("a", href=True):
        match = re.search(r"/page/(\d+)/", anchor["href"])
        if match:
            total_pages = max(total_pages, int(match.group(1)))
    return total_pages


def extract_series_list_items(soup: BeautifulSoup, base_domain: str) -> list[SeriesListItem]:
    items: list[SeriesListItem] = []
    for item in soup.find_all("div", class_="post-item"):
        anchor = item.find("a", href=True)
        if not anchor:
            continue

        title = anchor.get("title", "").strip()
        url = normalize_site_url(anchor.get("href", ""), base_domain)
        image = item.find("img")
        poster = ""
        if image:
            poster = normalize_site_url(image.get("data-src") or image.get("src"), base_domain)

        if url and title:
            items.append(SeriesListItem(url=url, title=title, poster=poster))
    return items


def extract_cover_image(soup: BeautifulSoup) -> str:
    head = soup.find("div", id="head")
    if not head:
        return ""
    style = head.get("style", "")
    match = BACKGROUND_URL_RE.search(style)
    return match.group(1).strip() if match else ""


def extract_platform_and_added_date(soup: BeautifulSoup) -> tuple[str, str]:
    platform = DEFAULT_PLATFORM
    added_date = ""

    platform_label = soup.find("span", string=lambda value: value and "Platform" in value.strip())
    if platform_label:
        container = platform_label.find_parent("div")
        anchor = container.find("a") if container else None
        if anchor:
            platform = anchor.get_text(strip=True)
        elif container:
            raw_text = container.get_text(" ", strip=True)
            platform = raw_text.replace("Platform", "", 1).strip() or DEFAULT_PLATFORM

    upload_icon = soup.find("img", src=lambda src: src and "Upload.svg" in src)
    if upload_icon and upload_icon.parent:
        added_date = upload_icon.parent.get_text(" ", strip=True).replace("Eklenme Tarihi", "", 1).strip()

    return platform or DEFAULT_PLATFORM, added_date


def extract_episode_links(soup: BeautifulSoup, base_domain: str) -> list[str]:
    links = [
        normalize_site_url(anchor.get("href"), base_domain)
        for anchor in soup.find_all("a", href=True)
        if "/bolum/" in anchor.get("href", "")
    ]
    return unique_preserve_order(links)


def extract_season_urls(soup: BeautifulSoup, base_domain: str) -> list[str]:
    season_urls = [
        normalize_site_url(anchor.get("href"), base_domain)
        for anchor in soup.find_all("a", href=True)
        if "?sezon=" in anchor.get("href", "")
    ]
    return unique_preserve_order(season_urls)


def parse_series_detail_soup(soup: BeautifulSoup, base_domain: str) -> dict[str, Any]:
    platform, added_date = extract_platform_and_added_date(soup)
    return {
        "platform": platform,
        "added_date": added_date,
        "cover_image": extract_cover_image(soup),
        "episode_links": extract_episode_links(soup, base_domain),
        "season_urls": extract_season_urls(soup, base_domain),
    }


def parse_series_list_html(html: str, base_domain: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    return {
        "items": extract_series_list_items(soup, base_domain),
        "total_pages": detect_total_pages(soup),
    }


def extract_series_items_from_html(html: str, base_domain: str) -> list[SeriesListItem]:
    if not html:
        return []
    return extract_series_list_items(BeautifulSoup(html, "html.parser"), base_domain)


def parse_series_detail_html(html: str, base_domain: str) -> dict[str, Any]:
    return parse_series_detail_soup(BeautifulSoup(html, "html.parser"), base_domain)


def normalize_iframe_candidate(url: str | None, base_domain: str) -> str:
    if not url:
        return ""

    cleaned = unescape(url.strip()).replace("\\/", "/")
    if cleaned.startswith("//"):
        cleaned = f"https:{cleaned}"

    normalized = normalize_site_url(cleaned, base_domain)
    if not normalized:
        return ""

    lowered = normalized.lower()
    parsed = urlparse(normalized)
    if any(marker in lowered for marker in IFRAME_SKIP_MARKERS):
        return ""
    if any(parsed.path.lower().endswith(ext) for ext in IFRAME_SKIP_EXTENSIONS):
        return ""
    return normalized


def is_likely_iframe_candidate(url: str) -> bool:
    lowered = url.lower()
    parsed = urlparse(url)
    path = parsed.path.lower()
    if "iframe.php" in lowered:
        return True
    if "/embed/" in path or "embed" in lowered:
        return True
    if "player" in lowered and not path.endswith(".js"):
        return True
    if path.endswith(".php") and ("v=" in lowered or "id=" in lowered):
        return True
    return False


def score_iframe_candidate(url: str) -> tuple[int, int]:
    lowered = url.lower()
    score = 0
    if "iframe.php" in lowered:
        score += 100
    if "/embed/" in lowered or "embed" in lowered:
        score += 60
    if "player" in lowered:
        score += 40
    if lowered.startswith("https://"):
        score += 10
    if "?" in lowered:
        score += 5
    return score, len(url)


def pick_best_iframe_candidate(candidates: list[str]) -> str:
    unique_candidates = unique_preserve_order(candidates)
    filtered = [candidate for candidate in unique_candidates if is_likely_iframe_candidate(candidate)]
    if not filtered:
        return ""
    filtered.sort(key=score_iframe_candidate, reverse=True)
    return filtered[0]


def extract_iframe_candidates_from_soup(soup: BeautifulSoup, base_domain: str) -> list[str]:
    candidates: list[str] = []

    for iframe in soup.find_all("iframe", src=True):
        candidate = normalize_iframe_candidate(iframe.get("src"), base_domain)
        if candidate:
            candidates.append(candidate)

    for attr_name in ("data-src", "data-url", "data-href"):
        for element in soup.find_all(attrs={attr_name: True}):
            candidate = normalize_iframe_candidate(element.get(attr_name), base_domain)
            if candidate:
                candidates.append(candidate)

    html_text = str(soup)
    for match in IFRAME_URL_RE.finditer(html_text):
        candidate = normalize_iframe_candidate(match.group("url"), base_domain)
        if candidate:
            candidates.append(candidate)

    return unique_preserve_order(candidates)


def extract_iframe_url_from_html(html: str, base_domain: str) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    return pick_best_iframe_candidate(extract_iframe_candidates_from_soup(soup, base_domain))


def extract_iframe_url(soup: BeautifulSoup, base_domain: str) -> str:
    return pick_best_iframe_candidate(extract_iframe_candidates_from_soup(soup, base_domain))


def open_browser_target(sb: Any, target_url: str, wait_seconds: int) -> None:
    reconnect_time = max(3, min(6, wait_seconds // 4 or 3))
    if hasattr(sb, "uc_open_with_reconnect"):
        try:
            sb.uc_open_with_reconnect(target_url, reconnect_time)
            return
        except Exception:
            pass
    sb.open(target_url)


def apply_browser_cookies(
    sb: Any,
    base_domain: str,
    wait_seconds: int,
    cookies: dict[str, str] | None,
) -> None:
    if not cookies:
        return

    open_browser_target(sb, base_domain, wait_seconds)
    cookie_domain = urlparse(base_domain).hostname or ""
    for name, value in cookies.items():
        cookie_payload = {
            "name": name,
            "value": value,
            "path": "/",
        }
        if cookie_domain:
            cookie_payload["domain"] = cookie_domain

        try:
            sb.driver.add_cookie(cookie_payload)
        except Exception:
            cookie_payload.pop("domain", None)
            try:
                sb.driver.add_cookie(cookie_payload)
            except Exception:
                continue


def resolve_iframe_url_in_browser(
    sb: Any,
    page_url: str,
    base_domain: str,
    wait_seconds: int,
) -> str:
    open_browser_target(sb, page_url, wait_seconds)
    deadline = time.time() + wait_seconds
    last_html = ""

    while time.time() < deadline:
        current_url = ""
        try:
            current_url = getattr(sb.driver, "current_url", "") or ""
        except Exception:
            current_url = ""

        normalized_current = normalize_iframe_candidate(current_url, base_domain)
        if normalized_current and is_likely_iframe_candidate(normalized_current):
            return normalized_current

        try:
            last_html = sb.get_page_source() or ""
        except Exception:
            last_html = ""

        iframe_url = extract_iframe_url_from_html(last_html, base_domain)
        if iframe_url:
            return iframe_url

        time.sleep(1)

    return extract_iframe_url_from_html(last_html, base_domain)


def resolve_iframe_urls_with_browser(
    urls: list[str],
    *,
    base_domain: str,
    wait_seconds: int,
    headless: bool,
    log_context: str,
    cookies: dict[str, str] | None = None,
    log: logging.Logger | None = None,
) -> dict[str, str]:
    resolved: dict[str, str] = {}
    unique_urls = unique_preserve_order(urls)
    if not unique_urls:
        return resolved

    active_logger = log or logger
    active_logger.info(
        "Selenium iframe fallback basliyor (%s): %s sayfa",
        log_context,
        len(unique_urls),
    )

    with SB(uc=not headless, headless=headless) as sb:
        apply_browser_cookies(sb, base_domain, wait_seconds, cookies)
        for index, url in enumerate(unique_urls, start=1):
            try:
                iframe_url = resolve_iframe_url_in_browser(sb, url, base_domain, wait_seconds)
            except Exception as exc:
                active_logger.warning(
                    "Selenium iframe fallback hatasi (%s %s/%s): %s -> %s",
                    log_context,
                    index,
                    len(unique_urls),
                    url,
                    exc,
                )
                continue

            if iframe_url:
                resolved[url] = iframe_url
                active_logger.info(
                    "Selenium iframe bulundu (%s %s/%s): %s",
                    log_context,
                    index,
                    len(unique_urls),
                    url,
                )
            else:
                active_logger.warning(
                    "Selenium iframe bulunamadi (%s %s/%s): %s",
                    log_context,
                    index,
                    len(unique_urls),
                    url,
                )

    return resolved


def parse_episode_numbers(url: str) -> tuple[int, int]:
    match = EPISODE_URL_RE.search(url.lower())
    if not match:
        return 0, 0
    return int(match.group(1)), int(match.group(2))


def parse_season_number_from_url(url: str) -> int:
    try:
        parsed = urlparse(url)
        value = parse_qs(parsed.query).get("sezon", [])
        return int(value[0]) if value else 0
    except (TypeError, ValueError):
        return 0


def build_episode_record(
    series_title: str,
    episode_url: str,
    video_url: str,
    existing: dict[str, Any] | None = None,
) -> dict[str, Any]:
    season_no, episode_no = parse_episode_numbers(episode_url)
    payload = dict(existing or {})
    payload["url"] = episode_url
    payload["videoUrl"] = video_url or payload.get("videoUrl", "")
    if season_no and episode_no:
        payload["title"] = f"{series_title} {season_no}. Sezon {episode_no}. Bölüm"
        payload["episode_number"] = f"{episode_no}. Bölüm"
    else:
        payload["title"] = series_title
        payload["episode_number"] = payload.get("episode_number", "1. Bölüm") or "1. Bölüm"
    payload["thumbnail"] = payload.get("thumbnail", "")
    return payload


def make_episode_fingerprint(episode_links: list[str]) -> str:
    joined = "\n".join(sorted(episode_links))
    return hashlib.sha1(joined.encode("utf-8")).hexdigest()


def is_meaningful_value(field: str, value: Any) -> bool:
    if field == "description":
        return value not in ("", None, DEFAULT_DESCRIPTION)
    if isinstance(value, list):
        return bool(value)
    return value not in ("", None)


def record_needs_refresh(record: dict[str, Any] | None) -> bool:
    if not record:
        return True
    return any(
        (
            not record.get("episodes"),
            not record.get("poster"),
            not record.get("imdb_id"),
            not record.get("trailer"),
            record.get("description") in ("", None, DEFAULT_DESCRIPTION),
        )
    )


def build_series_state_entry(
    title: str,
    episode_links: list[str],
    episode_failures: list[str],
    error: str = "",
) -> dict[str, Any]:
    payload = {
        "title": title,
        "episode_count": len(episode_links),
        "episode_fingerprint": make_episode_fingerprint(episode_links),
        "last_seen_at": iso_now(),
    }
    if episode_failures:
        payload["episode_failures"] = episode_failures[:100]
    if error:
        payload["last_error"] = error
        payload["last_error_at"] = iso_now()
    else:
        payload["last_success_at"] = iso_now()
    return payload


def normalize_tmdb_title(title: str) -> str:
    cleaned = TMDB_TITLE_CLEAN_RE.sub(" ", title.casefold())
    cleaned = re.sub(r"[^\w\s]", " ", cleaned, flags=re.UNICODE)
    return re.sub(r"\s+", " ", cleaned, flags=re.UNICODE).strip()


def cache_entry_is_fresh(entry: dict[str, Any], config: AppConfig) -> bool:
    status = entry.get("status")
    cached_at = entry.get("cached_at")
    if status == "hit":
        return is_within_ttl(cached_at, config.tmdb_hit_ttl)
    if status == "miss":
        return is_within_ttl(cached_at, config.tmdb_miss_ttl)
    if status == "error":
        return is_within_ttl(cached_at, config.tmdb_error_ttl)
    return False


def cache_entry_is_current(entry: dict[str, Any]) -> bool:
    try:
        return int(entry.get("version", 1)) == TMDB_CACHE_VERSION
    except (TypeError, ValueError):
        return False


def close_imdb_browser() -> None:
    context = getattr(thread_local, "imdb_browser_context", None)
    thread_local.imdb_browser_context = None
    thread_local.imdb_browser = None
    thread_local.imdb_browser_headless = None

    if context is None:
        return

    try:
        context.__exit__(None, None, None)
    except Exception:
        return


def imdb_browser_is_alive(browser: Any) -> bool:
    if browser is None:
        return False

    driver = getattr(browser, "driver", browser)
    session_id = getattr(driver, "session_id", None)
    if not session_id:
        return False

    try:
        driver.window_handles
        return True
    except Exception:
        return False


def get_imdb_browser(headless: bool) -> Any:
    browser = getattr(thread_local, "imdb_browser", None)
    browser_headless = getattr(thread_local, "imdb_browser_headless", None)
    if browser is not None and browser_headless == headless and imdb_browser_is_alive(browser):
        return browser

    close_imdb_browser()
    context = SB(uc=True, headless=headless)
    browser = context.__enter__()
    thread_local.imdb_browser_context = context
    thread_local.imdb_browser = browser
    thread_local.imdb_browser_headless = headless
    return browser


def normalize_imdb_trailer_url(candidate: str) -> str:
    if not candidate:
        return ""

    unescaped = unescape(candidate)
    match = IMDB_VIDEO_ID_RE.search(unescaped)
    if not match:
        return ""
    return IMDB_VIDEO_PAGE_URL_TEMPLATE.format(video_id=match.group("video_id"))


def extract_imdb_trailer_url(html: str) -> str:
    if not html:
        return ""

    match = IMDB_TRAILER_LINK_RE.search(html)
    if match:
        return normalize_imdb_trailer_url(match.group("href"))

    match = IMDB_TRAILER_ARIA_LINK_RE.search(html)
    if match:
        return normalize_imdb_trailer_url(match.group("href"))

    return ""


def fetch_imdb_trailer_url(imdb_id: str, config: AppConfig) -> str:
    normalized_imdb_id = imdb_id.strip()
    if not normalized_imdb_id:
        return ""

    wait_seconds = max(3, min(6, config.selenium_wait_seconds // 3 or 3))
    target_url = IMDB_TITLE_URL_TEMPLATE.format(imdb_id=normalized_imdb_id)
    reconnect_time = max(2, min(4, wait_seconds // 2 or 2))
    attempts = 2

    for attempt in range(attempts):
        sb = get_imdb_browser(config.selenium_headless)
        try:
            if hasattr(sb, "uc_open_with_reconnect"):
                sb.uc_open_with_reconnect(target_url, reconnect_time)
            else:
                sb.open(target_url)

            deadline = time.time() + wait_seconds
            last_html = ""
            while time.time() < deadline:
                try:
                    last_html = sb.get_page_source() or ""
                except Exception:
                    last_html = ""
                    close_imdb_browser()
                    break

                trailer_url = extract_imdb_trailer_url(last_html)
                if trailer_url:
                    return trailer_url

                time.sleep(0.5)
            else:
                return extract_imdb_trailer_url(last_html)
        except Exception:
            close_imdb_browser()
            if attempt + 1 == attempts:
                raise

    return ""


def get_imdb_trailer_data(
    imdb_id: str,
    state: dict[str, Any],
    config: AppConfig,
) -> dict[str, Any] | None:
    normalized_imdb_id = imdb_id.strip()
    if not normalized_imdb_id:
        return {}

    cache = state.setdefault("imdb_cache", {})
    cache_key = normalized_imdb_id.casefold()
    cached_entry = cache.get(cache_key)
    if isinstance(cached_entry, dict) and cache_entry_is_fresh(cached_entry, config):
        status = cached_entry.get("status")
        if status == "hit":
            return dict(cached_entry.get("data", {}))
        if status == "miss":
            return {}
        return None

    try:
        trailer_url = fetch_imdb_trailer_url(normalized_imdb_id, config)
        if trailer_url:
            payload = {"trailer": trailer_url}
            cache[cache_key] = {
                "status": "hit",
                "cached_at": iso_now(),
                "data": payload,
            }
            return dict(payload)

        cache[cache_key] = {
            "status": "miss",
            "cached_at": iso_now(),
            "data": {},
        }
        return {}
    except Exception as exc:
        logger.warning("IMDb hatasi (%s): %s", normalized_imdb_id, exc)
        cache[cache_key] = {
            "status": "error",
            "cached_at": iso_now(),
            "message": str(exc)[:500],
        }
        close_imdb_browser()
        return None


def extract_tmdb_trailer_url(videos: list[dict[str, Any]]) -> str:
    candidates: list[dict[str, Any]] = []
    for video in videos:
        if video.get("site") != "YouTube" or not video.get("key"):
            continue
        video_type = str(video.get("type", "")).strip().casefold()
        if video_type not in TMDB_TRAILER_TYPE_PRIORITY:
            continue
        candidates.append(video)

    if not candidates:
        return ""

    def sort_key(video: dict[str, Any]) -> tuple[int, int, int, int, str]:
        video_type = str(video.get("type", "")).strip().casefold()
        language = str(video.get("iso_639_1") or "").strip().casefold()
        size = video.get("size")
        normalized_size = size if isinstance(size, int) else 0
        return (
            TMDB_TRAILER_TYPE_PRIORITY.get(video_type, len(TMDB_TRAILER_TYPE_PRIORITY)),
            TMDB_TRAILER_LANGUAGE_PRIORITY.get(language, len(TMDB_TRAILER_LANGUAGE_PRIORITY)),
            0 if video.get("official") else 1,
            -normalized_size,
            str(video.get("published_at") or ""),
        )

    selected = sorted(candidates, key=sort_key)[0]
    return f"https://www.youtube.com/watch?v={selected['key']}"


def fetch_tmdb_series_trailer(tv: Any, info: dict[str, Any]) -> str:
    trailer_url = extract_tmdb_trailer_url(info.get("videos", {}).get("results", []))
    if trailer_url:
        return trailer_url

    try:
        video_payload = tv.videos(
            language="tr",
            include_video_language=TMDB_TV_VIDEO_LANGUAGE_FALLBACK,
        )
    except Exception as exc:
        logger.warning("TMDB dizi fragmanlari alinamadi (%s): %s", tv.id, exc)
        return ""

    return extract_tmdb_trailer_url(video_payload.get("results", []))


def build_tmdb_payload(
    info: dict[str, Any],
    episode_images: dict[str, str],
    trailer_url: str,
) -> dict[str, Any]:
    credits = info.get("credits", {}).get("cast", [])
    external_ids = info.get("external_ids", {})
    vote_average = info.get("vote_average", 0.0)

    return {
        "imdb": str(round(vote_average, 1)) if vote_average else "0.0",
        "imdb_id": external_ids.get("imdb_id", ""),
        "year": str(info.get("first_air_date", ""))[:4] if info.get("first_air_date") else "",
        "genres": [genre["name"] for genre in info.get("genres", []) if genre.get("name")],
        "description": info.get("overview") or DEFAULT_DESCRIPTION,
        "poster": (
            f"https://image.tmdb.org/t/p/w500{info.get('poster_path')}"
            if info.get("poster_path")
            else ""
        ),
        "cover_image": (
            f"https://image.tmdb.org/t/p/original{info.get('backdrop_path')}"
            if info.get("backdrop_path")
            else ""
        ),
        "cast": [person["name"] for person in credits[:12] if person.get("name")],
        "trailer": trailer_url,
        "episode_images": episode_images,
    }


def fetch_tmdb_episode_images(tv_id: int, info: dict[str, Any]) -> dict[str, str]:
    episode_images: dict[str, str] = {}
    for season in info.get("seasons", []):
        season_number = season.get("season_number")
        if season_number in (None, 0):
            continue
        try:
            season_info = tmdb.TV_Seasons(tv_id, season_number).info(language="tr")
        except Exception as exc:
            logger.warning("TMDB sezon gorseli alinamadi (tv=%s sezon=%s): %s", tv_id, season_number, exc)
            continue

        for episode in season_info.get("episodes", []):
            episode_number = episode.get("episode_number")
            still_path = episode.get("still_path")
            if season_number and episode_number and still_path:
                episode_images[f"{season_number}_{episode_number}"] = (
                    f"https://image.tmdb.org/t/p/w500{still_path}"
                )
    return episode_images


def get_tmdb_series_data(
    title: str,
    state: dict[str, Any],
    config: AppConfig,
    imdb_id: str = "",
) -> dict[str, Any] | None:
    normalized_imdb_id = imdb_id.strip()
    cache_key = normalize_tmdb_title(title)
    if normalized_imdb_id:
        cache_key = f"{cache_key}|{normalized_imdb_id.casefold()}"
    cache = state.setdefault("tmdb_cache", {})
    cached_entry = cache.get(cache_key)

    if (
        isinstance(cached_entry, dict)
        and cache_entry_is_current(cached_entry)
        and cache_entry_is_fresh(cached_entry, config)
    ):
        status = cached_entry.get("status")
        if status == "hit":
            return dict(cached_entry.get("data", {}))
        if status == "miss":
            return {}
        return None

    search = tmdb.Search()
    queries = [title]
    normalized_query = re.sub(r"\s+", " ", TMDB_TITLE_CLEAN_RE.sub(" ", title)).strip()
    if normalized_query and normalized_query != title:
        queries.append(normalized_query)

    try:
        search_result: dict[str, Any] = {}
        tv_id = 0
        if normalized_imdb_id:
            try:
                find_result = tmdb.Find(normalized_imdb_id).info(external_source="imdb_id")
                tv_results = find_result.get("tv_results", [])
                if tv_results:
                    tv_id = int(tv_results[0]["id"])
            except Exception as exc:
                logger.warning("TMDB IMDb dizi aramasi hatasi (%s / %s): %s", title, normalized_imdb_id, exc)

        if not tv_id:
            for query in queries:
                search_result = search.tv(query=query)
                if search_result.get("results"):
                    break

        if not tv_id and not search_result.get("results"):
            imdb_payload = get_imdb_trailer_data(normalized_imdb_id, state, config)
            if isinstance(imdb_payload, dict) and imdb_payload.get("trailer"):
                cache[cache_key] = {
                    "version": TMDB_CACHE_VERSION,
                    "status": "hit",
                    "cached_at": iso_now(),
                    "data": imdb_payload,
                }
                return dict(imdb_payload)

            cache[cache_key] = {
                "version": TMDB_CACHE_VERSION,
                "status": "miss",
                "cached_at": iso_now(),
                "data": {},
            }
            return {}

        if not tv_id:
            tv_id = search_result["results"][0]["id"]
        tv = tmdb.TV(tv_id)
        info = tv.info(language="tr", append_to_response="videos,credits,external_ids")
        episode_images = fetch_tmdb_episode_images(tv_id, info)
        payload = build_tmdb_payload(
            info,
            episode_images,
            fetch_tmdb_series_trailer(tv, info),
        )
        fallback_imdb_id = normalized_imdb_id or str(payload.get("imdb_id", "")).strip()
        if not payload.get("trailer") and fallback_imdb_id:
            imdb_payload = get_imdb_trailer_data(fallback_imdb_id, state, config)
            if isinstance(imdb_payload, dict) and imdb_payload.get("trailer"):
                payload["trailer"] = imdb_payload["trailer"]
        cache[cache_key] = {
            "version": TMDB_CACHE_VERSION,
            "status": "hit",
            "cached_at": iso_now(),
            "data": payload,
        }
        return dict(payload)
    except Exception as exc:
        logger.warning("TMDB hatasi (%s): %s", title, exc)
        cache[cache_key] = {
            "version": TMDB_CACHE_VERSION,
            "status": "error",
            "cached_at": iso_now(),
            "message": str(exc)[:500],
        }
        return None


def apply_episode_images(episodes: list[dict[str, Any]], episode_images: dict[str, str]) -> list[dict[str, Any]]:
    for episode in episodes:
        season_no, episode_no = parse_episode_numbers(episode.get("url", ""))
        cache_key = f"{season_no}_{episode_no}"
        if episode_images.get(cache_key):
            episode["thumbnail"] = episode_images[cache_key]
        else:
            episode["thumbnail"] = episode.get("thumbnail", "")
    return episodes


def finalize_series_record(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "dizi",
        "title": record.get("title", ""),
        "url": record.get("url", ""),
        "poster": record.get("poster", ""),
        "cover_image": record.get("cover_image", ""),
        "platform": record.get("platform", DEFAULT_PLATFORM) or DEFAULT_PLATFORM,
        "added_date": record.get("added_date", ""),
        "episodes": record.get("episodes", []),
        "description": record.get("description", DEFAULT_DESCRIPTION) or DEFAULT_DESCRIPTION,
        "imdb": record.get("imdb", "0.0") or "0.0",
        "imdb_id": record.get("imdb_id", ""),
        "year": record.get("year", ""),
        "genres": record.get("genres", []),
        "cast": record.get("cast", []),
        "trailer": record.get("trailer", ""),
    }


def merge_series_record(
    existing: dict[str, Any] | None,
    item: SeriesListItem,
    site_payload: dict[str, Any],
    episodes: list[dict[str, Any]],
    tmdb_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    merged = dict(existing or {})
    merged["type"] = "dizi"
    merged["title"] = item.title
    merged["url"] = item.url

    if item.poster and not is_meaningful_value("poster", merged.get("poster")):
        merged["poster"] = item.poster

    for field in ("cover_image", "platform", "added_date"):
        if is_meaningful_value(field, site_payload.get(field)):
            merged[field] = site_payload[field]

    if episodes:
        merged["episodes"] = episodes
    else:
        merged["episodes"] = merged.get("episodes", [])

    if tmdb_payload is not None:
        for field in ("imdb", "imdb_id", "year", "genres", "description", "poster", "cover_image", "cast", "trailer"):
            if is_meaningful_value(field, tmdb_payload.get(field)):
                merged[field] = tmdb_payload[field]

    if not is_meaningful_value("poster", merged.get("poster")) and item.poster:
        merged["poster"] = item.poster

    if not is_meaningful_value("cover_image", merged.get("cover_image")) and site_payload.get("cover_image"):
        merged["cover_image"] = site_payload["cover_image"]

    if not is_meaningful_value("platform", merged.get("platform")):
        merged["platform"] = DEFAULT_PLATFORM

    if not is_meaningful_value("description", merged.get("description")):
        merged["description"] = DEFAULT_DESCRIPTION

    merged.setdefault("imdb", "0.0")
    merged.setdefault("imdb_id", "")
    merged.setdefault("year", "")
    merged.setdefault("genres", [])
    merged.setdefault("cast", [])
    merged.setdefault("trailer", "")
    return finalize_series_record(merged)


def episodes_are_equal(left: list[dict[str, Any]], right: list[dict[str, Any]]) -> bool:
    return json.dumps(left, ensure_ascii=False, sort_keys=True) == json.dumps(
        right,
        ensure_ascii=False,
        sort_keys=True,
    )


def series_records_equal(left: dict[str, Any], right: dict[str, Any]) -> bool:
    comparable_fields = (
        "type",
        "title",
        "url",
        "poster",
        "cover_image",
        "platform",
        "added_date",
        "description",
        "imdb",
        "imdb_id",
        "year",
        "genres",
        "cast",
        "trailer",
    )
    for field in comparable_fields:
        if left.get(field) != right.get(field):
            return False
    return episodes_are_equal(left.get("episodes", []), right.get("episodes", []))


def needs_session_refresh(payload: FetchPayload, expect_series_cards: bool, config: AppConfig) -> bool:
    if payload.status_code in (403, 503):
        return True
    if payload.challenge:
        return True
    if payload.status_code != 200 or not payload.text:
        return False
    if expect_series_cards:
        soup = payload.soup()
        return not bool(soup and extract_series_list_items(soup, config.base_domain))
    return False


def save_bootstrap_debug_artifacts(
    config: AppConfig,
    page_html: str,
    metadata: dict[str, Any],
    screenshot_bytes: bytes | None = None,
) -> tuple[Path, Path, Path | None]:
    debug_dir = config.log_file.parent / "bootstrap_debug"
    debug_dir.mkdir(parents=True, exist_ok=True)
    stamp = utc_now().strftime("%Y%m%d_%H%M%S")

    html_path = debug_dir / f"bootstrap_failure_{stamp}.html"
    meta_path = debug_dir / f"bootstrap_failure_{stamp}.json"
    screenshot_path = debug_dir / f"bootstrap_failure_{stamp}.png" if screenshot_bytes else None

    html_path.write_text(page_html or "", encoding="utf-8")
    meta_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
    if screenshot_path and screenshot_bytes:
        screenshot_path.write_bytes(screenshot_bytes)

    return html_path, meta_path, screenshot_path


class SessionContext:
    def __init__(self, config: AppConfig, state: dict[str, Any]) -> None:
        self.config = config
        self.state = state
        self.cookies: dict[str, str] = {}
        self.user_agent = DEFAULT_USER_AGENT
        self.page1_html = ""

    def ensure(self) -> None:
        session_state = self.state.get("session", {})
        cached_cookies = session_state.get("cookies") if isinstance(session_state, dict) else None
        cached_user_agent = session_state.get("user_agent") if isinstance(session_state, dict) else None
        cached_at = session_state.get("captured_at") if isinstance(session_state, dict) else None

        if (
            isinstance(cached_cookies, dict)
            and cached_cookies
            and isinstance(cached_user_agent, str)
            and cached_user_agent
            and is_within_ttl(cached_at, self.config.session_ttl)
        ):
            payload = fetch_html(
                f"{self.config.base_domain}/diziler/",
                cached_cookies,
                cached_user_agent,
                self.config,
            )
            if (
                payload.status_code == 200
                and payload.text
                and not needs_session_refresh(payload, expect_series_cards=True, config=self.config)
            ):
                self.cookies = cached_cookies
                self.user_agent = cached_user_agent
                self.page1_html = payload.text
                logger.info("Kayitli cookie/UA oturumu yeniden kullanildi.")
                return

        self.refresh("gecerli bir kayitli oturum bulunamadi")

    def refresh(self, reason: str) -> None:
        logger.info("Oturum yenileniyor: %s", reason)
        cookies, user_agent, page1_html = bootstrap_session(self.config)
        self.cookies = cookies
        self.user_agent = user_agent
        self.page1_html = page1_html
        self.state["session"] = {
            "cookies": cookies,
            "user_agent": user_agent,
            "captured_at": iso_now(),
        }
        save_state(self.config.state_file, self.state)


def bootstrap_session(config: AppConfig) -> tuple[dict[str, str], str, str]:
    logger.info("SeleniumBase ile otomatik Cloudflare oturumu alinacak.")
    with SB(uc=True, headless=config.selenium_headless) as sb:
        target_url = f"{config.base_domain}/diziler/"
        reconnect_time = max(3, min(6, config.selenium_wait_seconds // 4 or 3))

        def open_target() -> None:
            if hasattr(sb, "uc_open_with_reconnect"):
                sb.uc_open_with_reconnect(target_url, reconnect_time)
            else:
                sb.open(target_url)
            if not config.selenium_headless:
                try:
                    sb.maximize_window()
                except Exception:
                    try:
                        sb.driver.maximize_window()
                    except Exception:
                        pass

        open_target()
        deadline = time.time() + config.selenium_wait_seconds
        page_html = ""
        items: list[SeriesListItem] = []
        captcha_attempted = False
        retried_open = False

        while time.time() < deadline:
            try:
                page_html = sb.get_page_source()
            except Exception:
                page_html = ""

            items = extract_series_items_from_html(page_html, config.base_domain)
            if items and not is_cloudflare_challenge(page_html):
                break

            challenge_detected = is_cloudflare_challenge(page_html)
            if challenge_detected and not captcha_attempted and hasattr(sb, "uc_gui_click_captcha"):
                try:
                    sb.uc_gui_click_captcha()
                    captcha_attempted = True
                    time.sleep(2)
                    continue
                except Exception as exc:
                    logger.warning("Captcha tiklama denemesi basarisiz: %s", exc)
                    captcha_attempted = True

            if not retried_open and time.time() + 4 < deadline:
                try:
                    open_target()
                    retried_open = True
                    time.sleep(2)
                    continue
                except Exception as exc:
                    logger.warning("Selenium oturumu yeniden acilamadi: %s", exc)
                    retried_open = True

            time.sleep(2)

        cookies = {cookie["name"]: cookie["value"] for cookie in sb.get_cookies()}
        user_agent = sb.get_user_agent()

        if items and not is_cloudflare_challenge(page_html):
            logger.info("Cloudflare oturumu basariyla alindi. Ilk sayfa: %s kayıt.", len(items))
            return cookies, user_agent, page_html

        http_payload = fetch_html(target_url, cookies, user_agent, config)
        if (
            http_payload.status_code == 200
            and http_payload.text
            and not is_cloudflare_challenge(http_payload.text)
        ):
            http_items = extract_series_items_from_html(http_payload.text, config.base_domain)
            if http_items:
                logger.info(
                    "Cloudflare oturumu HTTP dogrulamasi ile kabul edildi. Ilk sayfa: %s kayıt.",
                    len(http_items),
                )
                return cookies, user_agent, http_payload.text

        screenshot_bytes = None
        try:
            screenshot_bytes = sb.driver.get_screenshot_as_png()
        except Exception:
            screenshot_bytes = None

        metadata = {
            "base_domain": config.base_domain,
            "current_url": getattr(sb.driver, "current_url", ""),
            "title": getattr(sb.driver, "title", ""),
            "page_length": len(page_html or ""),
            "browser_item_count": len(items),
            "browser_challenge_detected": is_cloudflare_challenge(page_html),
            "http_status_code": http_payload.status_code,
            "http_item_count": len(extract_series_items_from_html(http_payload.text, config.base_domain))
            if http_payload.text
            else 0,
            "http_challenge_detected": is_cloudflare_challenge(http_payload.text),
            "http_error": http_payload.error,
        }
        html_path, meta_path, screenshot_path = save_bootstrap_debug_artifacts(
            config,
            page_html or http_payload.text,
            metadata,
            screenshot_bytes=screenshot_bytes,
        )
        screenshot_hint = f", PNG: {screenshot_path}" if screenshot_path else ""
        raise RuntimeError(
            "Cloudflare asamasi otomatik olarak gecilemedi veya liste bos dondu. "
            f"Debug HTML: {html_path}, meta: {meta_path}{screenshot_hint}"
        )


def fetch_with_reauth(
    url: str,
    session_ctx: SessionContext,
    expect_series_cards: bool = False,
) -> FetchPayload:
    payload = fetch_html(url, session_ctx.cookies, session_ctx.user_agent, session_ctx.config)
    if needs_session_refresh(payload, expect_series_cards=expect_series_cards, config=session_ctx.config):
        session_ctx.refresh(f"yeniden dogrulama gerekli: {url}")
        payload = fetch_html(url, session_ctx.cookies, session_ctx.user_agent, session_ctx.config)
    return payload


def fetch_list_page(
    page_number: int,
    cookies: dict[str, str],
    user_agent: str,
    config: AppConfig,
) -> PageFetchResult:
    url = f"{config.base_domain}/diziler/page/{page_number}/"
    payload = fetch_html(url, cookies, user_agent, config)
    if payload.status_code == 404:
        return PageFetchResult(page=page_number, items=[])

    session_invalid = needs_session_refresh(payload, expect_series_cards=True, config=config)
    if payload.status_code != 200 or not payload.text or session_invalid:
        return PageFetchResult(
            page=page_number,
            items=[],
            error=payload.error or f"HTTP {payload.status_code}",
            session_invalid=session_invalid,
        )

    soup = payload.soup()
    items = extract_series_list_items(soup, config.base_domain) if soup else []
    return PageFetchResult(page=page_number, items=items)


def gather_all_series_items(session_ctx: SessionContext) -> tuple[int, list[SeriesListItem]]:
    first_page = parse_series_list_html(session_ctx.page1_html, session_ctx.config.base_domain)
    items_by_page: dict[int, list[SeriesListItem]] = {1: first_page["items"]}
    total_pages = first_page["total_pages"]

    if not items_by_page[1]:
        session_ctx.refresh("ilk sayfa kartlari bos geldi")
        first_page = parse_series_list_html(session_ctx.page1_html, session_ctx.config.base_domain)
        items_by_page[1] = first_page["items"]
        total_pages = first_page["total_pages"]

    if not items_by_page[1]:
        raise RuntimeError("Ilk sayfada hic dizi karti bulunamadi.")

    if session_ctx.config.max_list_pages > 0:
        total_pages = min(total_pages, session_ctx.config.max_list_pages)

    logger.info("Toplam %s liste sayfasi tespit edildi.", total_pages)
    invalid_pages: list[int] = []

    with ThreadPoolExecutor(max_workers=session_ctx.config.list_page_workers) as executor:
        futures = {
            executor.submit(
                fetch_list_page,
                page,
                session_ctx.cookies,
                session_ctx.user_agent,
                session_ctx.config,
            ): page
            for page in range(2, total_pages + 1)
        }

        for future in as_completed(futures):
            page = futures[future]
            try:
                result = future.result()
            except Exception as exc:
                logger.error("Liste sayfasi alinamadi (%s): %s", page, exc)
                invalid_pages.append(page)
                continue

            items_by_page[result.page] = result.items
            if result.session_invalid:
                invalid_pages.append(result.page)
            elif result.error:
                logger.warning("Liste sayfasi eksik kaldi (%s): %s", result.page, result.error)

    if invalid_pages:
        session_ctx.refresh(f"{len(set(invalid_pages))} liste sayfasi yeniden istenecek")
        for page in sorted(set(invalid_pages)):
            result = fetch_list_page(page, session_ctx.cookies, session_ctx.user_agent, session_ctx.config)
            items_by_page[result.page] = result.items
            if result.session_invalid or result.error:
                logger.error("Liste sayfasi kalici olarak alinamadi (%s): %s", page, result.error)

    ordered_items: list[SeriesListItem] = []
    seen: set[str] = set()
    for page in range(1, total_pages + 1):
        for item in items_by_page.get(page, []):
            if item.url not in seen:
                seen.add(item.url)
                ordered_items.append(item)

    logger.info("Benzersiz dizi sayisi: %s", len(ordered_items))
    return total_pages, ordered_items


def fetch_series_catalog(series_url: str, session_ctx: SessionContext) -> tuple[dict[str, Any] | None, list[str]]:
    detail_payload = fetch_with_reauth(series_url, session_ctx)
    if detail_payload.status_code != 200 or not detail_payload.text:
        return None, [f"Detay sayfasi alinamadi: {detail_payload.error or detail_payload.status_code}"]

    detail_soup = detail_payload.soup()
    if detail_soup is None:
        return None, ["Detay HTML parse edilemedi."]

    payload = parse_series_detail_soup(detail_soup, session_ctx.config.base_domain)
    episode_links = list(payload["episode_links"])
    failures: list[str] = []

    base_seasons = {season for season, _ in map(parse_episode_numbers, episode_links) if season}

    for season_url in payload["season_urls"]:
        season_number = parse_season_number_from_url(season_url)
        if season_number and season_number in base_seasons:
            continue

        season_payload = fetch_with_reauth(season_url, session_ctx)
        if season_payload.status_code != 200 or not season_payload.text:
            failures.append(f"{season_url} -> {season_payload.error or season_payload.status_code}")
            continue

        season_soup = season_payload.soup()
        if season_soup is None:
            failures.append(f"{season_url} -> HTML parse hatasi")
            continue

        episode_links.extend(extract_episode_links(season_soup, session_ctx.config.base_domain))

    payload["episode_links"] = unique_preserve_order(episode_links)
    return payload, failures


def fetch_episode_iframe(
    episode_url: str,
    series_title: str,
    cookies: dict[str, str],
    user_agent: str,
    config: AppConfig,
) -> EpisodeFetchResult:
    payload = fetch_html(episode_url, cookies, user_agent, config)
    session_invalid = needs_session_refresh(payload, expect_series_cards=False, config=config)
    if payload.status_code != 200 or not payload.text or session_invalid:
        return EpisodeFetchResult(
            url=episode_url,
            episode=None,
            error=payload.error or f"HTTP {payload.status_code}",
            session_invalid=session_invalid,
        )

    soup = payload.soup()
    if soup is None:
        return EpisodeFetchResult(url=episode_url, episode=None, error="Episode HTML parse edilemedi.")

    iframe_url = extract_iframe_url(soup, config.base_domain)
    if not iframe_url:
        return EpisodeFetchResult(url=episode_url, episode=None, error="Iframe kaynagi bulunamadi.")

    return EpisodeFetchResult(
        url=episode_url,
        episode=build_episode_record(series_title, episode_url, iframe_url),
    )


def fetch_missing_episodes(
    episode_urls: list[str],
    series_title: str,
    session_ctx: SessionContext,
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    fetched: dict[str, dict[str, Any]] = {}
    failures: list[str] = []
    retry_urls: list[str] = []
    iframe_missing_urls: list[str] = []

    if not episode_urls:
        return fetched, failures

    with ThreadPoolExecutor(max_workers=session_ctx.config.episode_workers) as executor:
        futures = {
            executor.submit(
                fetch_episode_iframe,
                url,
                series_title,
                session_ctx.cookies,
                session_ctx.user_agent,
                session_ctx.config,
            ): url
            for url in episode_urls
        }

        for future in as_completed(futures):
            try:
                result = future.result()
            except Exception as exc:
                failures.append(f"{futures[future]} -> {exc}")
                continue

            if result.episode:
                fetched[result.url] = result.episode
            elif result.session_invalid:
                retry_urls.append(result.url)
            elif result.error == "Iframe kaynagi bulunamadi.":
                iframe_missing_urls.append(result.url)
            else:
                failures.append(f"{result.url} -> {result.error}")

    if retry_urls:
        session_ctx.refresh(f"{len(retry_urls)} bolum sayfasi yeniden istenecek")
        for url in retry_urls:
            result = fetch_episode_iframe(
                url,
                series_title,
                session_ctx.cookies,
                session_ctx.user_agent,
                session_ctx.config,
            )
            if result.episode:
                fetched[result.url] = result.episode
            elif result.error == "Iframe kaynagi bulunamadi.":
                iframe_missing_urls.append(url)
            else:
                failures.append(f"{url} -> {result.error or 'yeniden denemede basarisiz'}")

    if iframe_missing_urls:
        resolved_iframes = resolve_iframe_urls_with_browser(
            iframe_missing_urls,
            base_domain=session_ctx.config.base_domain,
            wait_seconds=session_ctx.config.selenium_wait_seconds,
            headless=session_ctx.config.selenium_headless,
            log_context="bolum",
            cookies=session_ctx.cookies,
            log=logger,
        )

        unresolved_iframes: list[str] = []
        for url in unique_preserve_order(iframe_missing_urls):
            iframe_url = resolved_iframes.get(url, "")
            if iframe_url:
                fetched[url] = build_episode_record(series_title, url, iframe_url)
            else:
                unresolved_iframes.append(url)

        failures.extend(f"{url} -> Selenium iframe kaynagi bulunamadi." for url in unresolved_iframes)

    return fetched, failures


def build_candidate_episode_links(current_links: list[str], existing_links: list[str]) -> list[str]:
    return unique_preserve_order(current_links + existing_links)


def sort_episode_records(episodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        episodes,
        key=lambda episode: (*parse_episode_numbers(episode.get("url", "")), episode.get("url", "")),
    )


def merge_episode_records(
    existing_episodes: list[dict[str, Any]],
    fetched_episodes: dict[str, dict[str, Any]],
    candidate_links: list[str],
    series_title: str,
) -> tuple[list[dict[str, Any]], list[str]]:
    existing_map = {episode.get("url"): dict(episode) for episode in existing_episodes if episode.get("url")}
    merged: list[dict[str, Any]] = []
    failures: list[str] = []

    for link in candidate_links:
        existing_episode = existing_map.get(link)
        fetched_episode = fetched_episodes.get(link)
        if fetched_episode:
            merged.append(build_episode_record(series_title, link, fetched_episode["videoUrl"], existing_episode))
            continue

        if existing_episode and existing_episode.get("videoUrl"):
            merged.append(build_episode_record(series_title, link, existing_episode["videoUrl"], existing_episode))
            continue

        failures.append(link)

    return sort_episode_records(merged), failures


def persist_checkpoint(
    config: AppConfig,
    all_series: list[dict[str, Any]],
    state: dict[str, Any],
    run_stats: dict[str, Any],
) -> None:
    with save_lock:
        atomic_write_json(config.data_file, all_series, backup_path=config.backup_file)
        run_state = state.setdefault("run", {})
        run_state.update(run_stats)
        run_state["last_checkpoint_at"] = iso_now()
        save_state(config.state_file, state)


def apply_series_result_counts(
    updated_count: int,
    skipped_count: int,
    failed_count: int,
    status: str,
) -> tuple[int, int, int]:
    if status == "updated":
        return updated_count + 1, skipped_count, failed_count
    if status == "skipped":
        return updated_count, skipped_count + 1, failed_count
    if status == "failed":
        return updated_count, skipped_count, failed_count + 1
    raise ValueError(f"Bilinmeyen islem durumu: {status}")


def reconcile_retry_result_counts(
    updated_count: int,
    skipped_count: int,
    failed_count: int,
    status: str,
) -> tuple[int, int, int]:
    if status == "failed":
        return updated_count, skipped_count, failed_count
    updated_count, skipped_count, _ = apply_series_result_counts(
        updated_count,
        skipped_count,
        0,
        status,
    )
    return updated_count, skipped_count, max(0, failed_count - 1)


def maybe_persist_running_checkpoint(
    config: AppConfig,
    all_series: list[dict[str, Any]],
    state: dict[str, Any],
    run_state: dict[str, Any],
    updated_count: int,
    skipped_count: int,
    failed_count: int,
    changes_since_checkpoint: int,
    last_checkpoint_at: float,
) -> tuple[int, float]:
    should_checkpoint = (
        changes_since_checkpoint >= config.checkpoint_item_interval
        or time.monotonic() - last_checkpoint_at >= config.checkpoint_time_seconds
    )
    if not should_checkpoint:
        return changes_since_checkpoint, last_checkpoint_at

    persist_checkpoint(
        config,
        all_series,
        state,
        {
            "status": "running",
            "last_started_at": run_state.get("last_started_at"),
            "updated_count": updated_count,
            "skipped_count": skipped_count,
            "failed_count": failed_count,
        },
    )
    return 0, time.monotonic()


def process_series_item(
    item: SeriesListItem,
    session_ctx: SessionContext,
    config: AppConfig,
    state: dict[str, Any],
    all_series: list[dict[str, Any]],
    url_map: dict[str, int],
) -> SeriesProcessResult:
    existing_index = url_map.get(item.url)
    existing_record = all_series[existing_index] if existing_index is not None else None

    try:
        site_payload, catalog_failures = fetch_series_catalog(item.url, session_ctx)
    except Exception as exc:
        state.setdefault("series", {})[item.url] = build_series_state_entry(
            item.title,
            [],
            [],
            error=str(exc),
        )
        logger.exception("Dizi katalogu alinamadi: %s", item.title)
        return SeriesProcessResult(status="failed")

    if site_payload is None:
        state.setdefault("series", {})[item.url] = build_series_state_entry(
            item.title,
            [],
            catalog_failures,
            error="Detay sayfasi alinamadi.",
        )
        logger.error("Detay sayfasi alinamadi: %s", item.title)
        return SeriesProcessResult(status="failed")

    current_links = site_payload["episode_links"]
    existing_episodes = (existing_record or {}).get("episodes", [])
    existing_links = [episode.get("url", "") for episode in existing_episodes if episode.get("url")]
    candidate_links = build_candidate_episode_links(current_links, existing_links)

    existing_episode_set = set(existing_links)
    current_episode_set = set(current_links)
    links_to_fetch = [
        link
        for link in current_links
        if link not in existing_episode_set
        or not any(
            episode.get("url") == link and episode.get("videoUrl")
            for episode in existing_episodes
        )
    ]

    should_process = (
        existing_record is None
        or record_needs_refresh(existing_record)
        or current_episode_set != existing_episode_set
        or bool(links_to_fetch)
    )

    if not should_process:
        state.setdefault("series", {})[item.url] = build_series_state_entry(
            item.title,
            current_links,
            catalog_failures,
        )
        return SeriesProcessResult(status="skipped")

    fetched_episodes, episode_fetch_failures = fetch_missing_episodes(
        links_to_fetch,
        item.title,
        session_ctx,
    )

    merged_episodes, episode_merge_failures = merge_episode_records(
        existing_episodes,
        fetched_episodes,
        candidate_links,
        item.title,
    )

    tmdb_payload = get_tmdb_series_data(
        item.title,
        state,
        config,
        imdb_id=(existing_record or {}).get("imdb_id", ""),
    )
    episode_images = {}
    if isinstance(tmdb_payload, dict):
        episode_images = tmdb_payload.pop("episode_images", {})
    merged_episodes = apply_episode_images(merged_episodes, episode_images)

    if existing_record is None and not merged_episodes:
        state.setdefault("series", {})[item.url] = build_series_state_entry(
            item.title,
            current_links,
            catalog_failures + episode_fetch_failures + episode_merge_failures,
            error="Yeni dizi icin hic gecerli bolum alinmadi.",
        )
        logger.error("Yeni dizi atlandi, gecerli bolum bulunamadi: %s", item.title)
        return SeriesProcessResult(status="failed")

    final_record = merge_series_record(
        existing=existing_record,
        item=item,
        site_payload=site_payload,
        episodes=merged_episodes,
        tmdb_payload=tmdb_payload,
    )

    if existing_record and series_records_equal(existing_record, final_record):
        state.setdefault("series", {})[item.url] = build_series_state_entry(
            item.title,
            current_links,
            catalog_failures + episode_fetch_failures + episode_merge_failures,
        )
        return SeriesProcessResult(status="skipped")

    if existing_index is None:
        all_series.append(final_record)
        url_map[item.url] = len(all_series) - 1
        logger.info("Yeni dizi eklendi: %s (%s bolum)", item.title, len(merged_episodes))
    else:
        all_series[existing_index] = final_record
        logger.info("Dizi guncellendi: %s (%s bolum)", item.title, len(merged_episodes))

    state.setdefault("series", {})[item.url] = build_series_state_entry(
        item.title,
        current_links,
        catalog_failures + episode_fetch_failures + episode_merge_failures,
    )
    return SeriesProcessResult(status="updated")


def main_legacy() -> None:
    config = load_config()
    configure_logging(config.log_file)
    tmdb.API_KEY = config.tmdb_api_key

    logger.info("Dizi senkronizasyonu basliyor. Hedef dosya: %s", config.data_file.name)

    state = load_state(config.state_file)
    run_state = state.setdefault("run", {})
    run_state.update(
        {
            "status": "running",
            "last_started_at": iso_now(),
            "updated_count": 0,
            "skipped_count": 0,
            "failed_count": 0,
        }
    )
    save_state(config.state_file, state)

    session_ctx = SessionContext(config, state)
    session_ctx.ensure()

    all_series = load_series_database(config)
    url_map = {entry.get("url"): index for index, entry in enumerate(all_series) if entry.get("url")}

    total_pages, items = gather_all_series_items(session_ctx)
    logger.info("%s sayfa tarandi, %s benzersiz dizi işlenecek.", total_pages, len(items))

    changes_since_checkpoint = 0
    last_checkpoint_at = time.monotonic()
    updated_count = 0
    skipped_count = 0
    failed_count = 0

    for index, item in enumerate(items, start=1):
        logger.info("[%s/%s] %s", index, len(items), item.title)
        existing_index = url_map.get(item.url)
        existing_record = all_series[existing_index] if existing_index is not None else None

        try:
            site_payload, catalog_failures = fetch_series_catalog(item.url, session_ctx)
        except Exception as exc:
            failed_count += 1
            state.setdefault("series", {})[item.url] = build_series_state_entry(
                item.title,
                [],
                [],
                error=str(exc),
            )
            logger.exception("Dizi katalogu alinamadi: %s", item.title)
            continue

        if site_payload is None:
            failed_count += 1
            state.setdefault("series", {})[item.url] = build_series_state_entry(
                item.title,
                [],
                catalog_failures,
                error="Detay sayfasi alinamadi.",
            )
            logger.error("Detay sayfasi alınamadı: %s", item.title)
            continue

        current_links = site_payload["episode_links"]
        existing_episodes = (existing_record or {}).get("episodes", [])
        existing_links = [episode.get("url", "") for episode in existing_episodes if episode.get("url")]
        candidate_links = build_candidate_episode_links(current_links, existing_links)

        existing_episode_set = set(existing_links)
        current_episode_set = set(current_links)
        links_to_fetch = [
            link
            for link in current_links
            if link not in existing_episode_set
            or not any(
                episode.get("url") == link and episode.get("videoUrl")
                for episode in existing_episodes
            )
        ]

        should_process = (
            existing_record is None
            or record_needs_refresh(existing_record)
            or current_episode_set != existing_episode_set
            or bool(links_to_fetch)
        )

        if not should_process:
            skipped_count += 1
            state.setdefault("series", {})[item.url] = build_series_state_entry(
                item.title,
                current_links,
                catalog_failures,
            )
            continue

        fetched_episodes, episode_fetch_failures = fetch_missing_episodes(
            links_to_fetch,
            item.title,
            session_ctx,
        )

        merged_episodes, episode_merge_failures = merge_episode_records(
            existing_episodes,
            fetched_episodes,
            candidate_links,
            item.title,
        )

        tmdb_payload = get_tmdb_series_data(
            item.title,
            state,
            config,
            imdb_id=(existing_record or {}).get("imdb_id", ""),
        )
        episode_images = {}
        if isinstance(tmdb_payload, dict):
            episode_images = tmdb_payload.pop("episode_images", {})
        merged_episodes = apply_episode_images(merged_episodes, episode_images)

        if existing_record is None and not merged_episodes:
            failed_count += 1
            state.setdefault("series", {})[item.url] = build_series_state_entry(
                item.title,
                current_links,
                catalog_failures + episode_fetch_failures + episode_merge_failures,
                error="Yeni dizi icin hic gecerli bolum alinmadi.",
            )
            logger.error("Yeni dizi atlandi, gecerli bolum bulunamadi: %s", item.title)
            continue

        final_record = merge_series_record(
            existing=existing_record,
            item=item,
            site_payload=site_payload,
            episodes=merged_episodes,
            tmdb_payload=tmdb_payload,
        )

        if existing_record and series_records_equal(existing_record, final_record):
            skipped_count += 1
            state.setdefault("series", {})[item.url] = build_series_state_entry(
                item.title,
                current_links,
                catalog_failures + episode_fetch_failures + episode_merge_failures,
            )
            continue

        if existing_index is None:
            all_series.append(final_record)
            url_map[item.url] = len(all_series) - 1
            logger.info("Yeni dizi eklendi: %s (%s bolum)", item.title, len(merged_episodes))
        else:
            all_series[existing_index] = final_record
            logger.info("Dizi guncellendi: %s (%s bolum)", item.title, len(merged_episodes))

        updated_count += 1
        changes_since_checkpoint += 1

        state.setdefault("series", {})[item.url] = build_series_state_entry(
            item.title,
            current_links,
            catalog_failures + episode_fetch_failures + episode_merge_failures,
        )

        should_checkpoint = (
            changes_since_checkpoint >= config.checkpoint_item_interval
            or time.monotonic() - last_checkpoint_at >= config.checkpoint_time_seconds
        )
        if should_checkpoint:
            persist_checkpoint(
                config,
                all_series,
                state,
                {
                    "status": "running",
                    "last_started_at": run_state.get("last_started_at"),
                    "updated_count": updated_count,
                    "skipped_count": skipped_count,
                    "failed_count": failed_count,
                },
            )
            changes_since_checkpoint = 0
            last_checkpoint_at = time.monotonic()

    persist_checkpoint(
        config,
        all_series,
        state,
        {
            "status": "success",
            "last_started_at": run_state.get("last_started_at"),
            "last_completed_at": iso_now(),
            "updated_count": updated_count,
            "skipped_count": skipped_count,
            "failed_count": failed_count,
        },
    )

    logger.info(
        "Islem tamamlandi. Toplam kayit: %s | Guncellenen: %s | Atlanan: %s | Hata: %s",
        len(all_series),
        updated_count,
        skipped_count,
        failed_count,
    )


def main() -> None:
    config = load_config()
    configure_logging(config.log_file)
    tmdb.API_KEY = config.tmdb_api_key

    logger.info("Dizi senkronizasyonu basliyor. Hedef dosya: %s", config.data_file.name)

    state = load_state(config.state_file)
    run_state = state.setdefault("run", {})
    run_state.update(
        {
            "status": "running",
            "last_started_at": iso_now(),
            "updated_count": 0,
            "skipped_count": 0,
            "failed_count": 0,
        }
    )
    save_state(config.state_file, state)

    session_ctx = SessionContext(config, state)
    session_ctx.ensure()

    all_series = load_series_database(config)
    url_map = {entry.get("url"): index for index, entry in enumerate(all_series) if entry.get("url")}

    total_pages, items = gather_all_series_items(session_ctx)
    logger.info("%s sayfa tarandi, %s benzersiz dizi islenecek.", total_pages, len(items))

    changes_since_checkpoint = 0
    last_checkpoint_at = time.monotonic()
    updated_count = 0
    skipped_count = 0
    failed_count = 0
    retry_candidates: list[SeriesListItem] = []

    for index, item in enumerate(items, start=1):
        logger.info("[%s/%s] %s", index, len(items), item.title)
        result = process_series_item(item, session_ctx, config, state, all_series, url_map)
        updated_count, skipped_count, failed_count = apply_series_result_counts(
            updated_count,
            skipped_count,
            failed_count,
            result.status,
        )
        if result.status == "failed":
            retry_candidates.append(item)
            continue

        if result.status == "updated":
            changes_since_checkpoint += 1
            changes_since_checkpoint, last_checkpoint_at = maybe_persist_running_checkpoint(
                config,
                all_series,
                state,
                run_state,
                updated_count,
                skipped_count,
                failed_count,
                changes_since_checkpoint,
                last_checkpoint_at,
            )

    remaining_retry_candidates = retry_candidates
    for retry_pass in range(1, config.failed_retry_passes + 1):
        if not remaining_retry_candidates:
            break

        logger.info(
            "Hata alan %s dizi icin retry turu %s/%s basliyor.",
            len(remaining_retry_candidates),
            retry_pass,
            config.failed_retry_passes,
        )
        if config.failed_retry_wait_seconds > 0:
            time.sleep(config.failed_retry_wait_seconds)

        next_retry_candidates: list[SeriesListItem] = []
        for retry_index, item in enumerate(remaining_retry_candidates, start=1):
            logger.info(
                "[Retry %s/%s - Tur %s/%s] %s",
                retry_index,
                len(remaining_retry_candidates),
                retry_pass,
                config.failed_retry_passes,
                item.title,
            )
            result = process_series_item(item, session_ctx, config, state, all_series, url_map)
            if result.status == "failed":
                next_retry_candidates.append(item)
                continue

            updated_count, skipped_count, failed_count = reconcile_retry_result_counts(
                updated_count,
                skipped_count,
                failed_count,
                result.status,
            )
            if result.status == "updated":
                changes_since_checkpoint += 1
                changes_since_checkpoint, last_checkpoint_at = maybe_persist_running_checkpoint(
                    config,
                    all_series,
                    state,
                    run_state,
                    updated_count,
                    skipped_count,
                    failed_count,
                    changes_since_checkpoint,
                    last_checkpoint_at,
                )

        logger.info(
            "Retry turu %s tamamlandi. Kurtarilan: %s | Kalan hata: %s",
            retry_pass,
            len(remaining_retry_candidates) - len(next_retry_candidates),
            len(next_retry_candidates),
        )
        remaining_retry_candidates = next_retry_candidates

    persist_checkpoint(
        config,
        all_series,
        state,
        {
            "status": "success",
            "last_started_at": run_state.get("last_started_at"),
            "last_completed_at": iso_now(),
            "updated_count": updated_count,
            "skipped_count": skipped_count,
            "failed_count": failed_count,
        },
    )

    logger.info(
        "Islem tamamlandi. Toplam kayit: %s | Guncellenen: %s | Atlanan: %s | Hata: %s",
        len(all_series),
        updated_count,
        skipped_count,
        failed_count,
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        if logger.handlers:
            logger.exception("Beklenmeyen hata ile cikildi: %s", exc)
        raise
    finally:
        close_imdb_browser()
