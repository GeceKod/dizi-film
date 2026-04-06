from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Lock
from typing import Any

import json
import hashlib
import logging
import os
import re
import time
from urllib.parse import parse_qs, urlparse

from bs4 import BeautifulSoup
from seleniumbase import SB
import tmdbsimple as tmdb

from main_dizi import (
    DEFAULT_USER_AGENT,
    FetchPayload,
    TMDB_TITLE_CLEAN_RE,
    atomic_write_json,
    cache_entry_is_fresh,
    detect_total_pages,
    extract_iframe_url,
    fetch_html,
    is_cloudflare_challenge,
    is_within_ttl,
    iso_now,
    load_json_list,
    normalize_site_url,
    resolve_iframe_urls_with_browser,
    save_bootstrap_debug_artifacts,
)
from title_localization import resolve_turkish_title


DEFAULT_DESCRIPTION = "Aciklama yok."
DEFAULT_PLATFORM = "Platform Disi"
TR_MONTHS = ("", "Ocak", "Subat", "Mart", "Nisan", "Mayis", "Haziran", "Temmuz", "Agustos", "Eylul", "Ekim", "Kasim", "Aralik")

save_lock = Lock()
logger = logging.getLogger("film_sync")


@dataclass(frozen=True)
class AppConfig:
    base_domain: str
    data_file: Path
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
class MovieListItem:
    url: str
    title: str
    poster: str


@dataclass(frozen=True)
class PageFetchResult:
    page: int
    items: list[MovieListItem]
    error: str = ""
    session_invalid: bool = False


@dataclass(frozen=True)
class MovieProcessResult:
    status: str


def load_config() -> AppConfig:
    data_file = Path(os.getenv("FILM_DATA_FILE", "movies.json"))
    return AppConfig(
        base_domain=os.getenv("FILM_BASE_DOMAIN", "https://dizipal.im").rstrip("/"),
        data_file=data_file,
        state_file=Path(os.getenv("FILM_STATE_FILE", "movies_state.json")),
        log_file=Path(os.getenv("FILM_LOG_FILE", "logs/film_sync.log")),
        backup_file=Path(f"{data_file}.bak"),
        http_timeout=int(os.getenv("FILM_HTTP_TIMEOUT", "20")),
        http_retries=int(os.getenv("FILM_HTTP_RETRIES", "3")),
        http_retry_sleep=float(os.getenv("FILM_HTTP_RETRY_SLEEP", "1.5")),
        selenium_wait_seconds=int(os.getenv("FILM_SELENIUM_WAIT", "18")),
        selenium_headless=os.getenv("FILM_SELENIUM_HEADLESS", "0") == "1",
        max_list_pages=max(0, int(os.getenv("FILM_MAX_LIST_PAGES", "0"))),
        session_ttl=timedelta(hours=int(os.getenv("FILM_SESSION_TTL_HOURS", "12"))),
        list_page_workers=max(1, min(4, int(os.getenv("FILM_LIST_PAGE_WORKERS", "4")))),
        checkpoint_item_interval=max(1, int(os.getenv("FILM_CHECKPOINT_ITEMS", "10"))),
        checkpoint_time_seconds=max(10, int(os.getenv("FILM_CHECKPOINT_SECONDS", "60"))),
        failed_retry_passes=max(0, int(os.getenv("FILM_FAILED_RETRY_PASSES", "1"))),
        failed_retry_wait_seconds=max(0.0, float(os.getenv("FILM_FAILED_RETRY_WAIT_SECONDS", "5"))),
        tmdb_hit_ttl=timedelta(days=int(os.getenv("FILM_TMDB_HIT_TTL_DAYS", "30"))),
        tmdb_miss_ttl=timedelta(days=int(os.getenv("FILM_TMDB_MISS_TTL_DAYS", "7"))),
        tmdb_error_ttl=timedelta(hours=int(os.getenv("FILM_TMDB_ERROR_TTL_HOURS", "6"))),
        browser_impersonation=os.getenv("FILM_IMPERSONATE", "chrome110"),
        tmdb_api_key=os.getenv("TMDB_API_KEY", "48ce82f1de91232f542660e99a9d1336"),
    )


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


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
        "movies": {},
        "tmdb_cache": {},
        "run": {},
    }


def normalize_state(raw_state: Any) -> dict[str, Any]:
    normalized = default_state()
    if not isinstance(raw_state, dict):
        return normalized
    for key in ("version",):
        if isinstance(raw_state.get(key), int):
            normalized[key] = raw_state[key]
    for key in ("session", "movies", "tmdb_cache", "run"):
        if isinstance(raw_state.get(key), dict):
            normalized[key] = raw_state[key]
    return normalized


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


def load_movie_database(config: AppConfig) -> list[dict[str, Any]]:
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


def extract_movie_list_items(soup: BeautifulSoup, base_domain: str) -> list[MovieListItem]:
    items: list[MovieListItem] = []
    for item in soup.find_all("div", class_="post-item"):
        anchor = item.find("a", href=True)
        if not anchor:
            continue
        title = anchor.get("title", "").strip()
        url = normalize_site_url(anchor.get("href", ""), base_domain)
        image = item.find("img")
        poster = normalize_site_url(image.get("data-src") or image.get("src"), base_domain) if image else ""
        if url and title:
            items.append(MovieListItem(url=url, title=title, poster=poster))
    return items


def parse_movie_list_html(html: str, base_domain: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    return {"items": extract_movie_list_items(soup, base_domain), "total_pages": detect_total_pages(soup)}


def extract_movie_items_from_html(html: str, base_domain: str) -> list[MovieListItem]:
    if not html:
        return []
    return extract_movie_list_items(BeautifulSoup(html, "html.parser"), base_domain)


def format_published_date(raw_value: str) -> str:
    try:
        parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
    except ValueError:
        return raw_value[:10]
    return f"{parsed.day} {TR_MONTHS[parsed.month]}, {parsed.year}"


def extract_movie_video_url(soup: BeautifulSoup, base_domain: str) -> str:
    base_host = (urlparse(base_domain).hostname or "").lower()
    candidates: list[str] = []

    def is_rejected_candidate(candidate_url: str) -> bool:
        lowered = candidate_url.lower()
        return "/wp-json/oembed/" in lowered or ("oembed" in lowered and "format=xml" in lowered)

    def score_candidate(candidate_url: str) -> tuple[int, int]:
        lowered = candidate_url.lower()
        parsed = urlparse(candidate_url)
        path = parsed.path.lower()
        score = 0
        if "/embed-" in path and path.endswith(".html"):
            score += 220
        if "iframe.php" in lowered:
            score += 200
        if "/embed/" in path or "embed" in lowered:
            score += 120
        if "player" in lowered:
            score += 80
        if parsed.hostname and parsed.hostname.lower() != base_host:
            score += 40
        if "youtube.com/embed" in lowered or "youtu.be/" in lowered:
            score -= 160
        return score, -len(candidate_url)

    for iframe in soup.find_all("iframe", src=True):
        candidate = normalize_site_url(iframe.get("src"), base_domain)
        if candidate and not is_rejected_candidate(candidate):
            candidates.append(candidate)

    if candidates:
        candidates.sort(key=score_candidate, reverse=True)
        return candidates[0]

    fallback = extract_iframe_url(soup, base_domain)
    if fallback and not is_rejected_candidate(fallback):
        return fallback
    return ""


def normalize_movie_video_candidate(
    movie_url: str,
    candidate_url: str | None,
    base_domain: str,
) -> tuple[str, bool]:
    normalized_movie_url = normalize_site_url(movie_url, base_domain)
    normalized_candidate = normalize_site_url(candidate_url or "", base_domain)
    if not normalized_candidate:
        return "", True

    lowered = normalized_candidate.lower()
    parsed = urlparse(normalized_candidate)
    is_oembed_endpoint = (
        "/wp-json/oembed/" in parsed.path.lower()
        or ("oembed" in lowered and "format=xml" in lowered)
    )

    if is_oembed_endpoint:
        target_candidates = parse_qs(parsed.query).get("url", [])
        target_url = normalize_site_url(target_candidates[0], base_domain) if target_candidates else ""
        return target_url or normalized_movie_url, True

    return normalized_candidate, normalized_candidate == normalized_movie_url


def extract_movie_added_date(soup: BeautifulSoup) -> str:
    published = soup.find("meta", property="article:published_time")
    if published and published.get("content"):
        return format_published_date(published["content"])
    upload_icon = soup.find("img", src=lambda src: src and "Upload.svg" in src)
    if upload_icon and upload_icon.parent:
        return upload_icon.parent.get_text(" ", strip=True).replace("Eklenme Tarihi", "", 1).strip()
    return ""


def extract_movie_cover_image(soup: BeautifulSoup, base_domain: str) -> str:
    og_image = soup.find("meta", property="og:image")
    if og_image and og_image.get("content"):
        return normalize_site_url(og_image["content"], base_domain)
    return ""


def parse_movie_detail_soup(soup: BeautifulSoup, base_domain: str) -> dict[str, Any]:
    return {
        "videoUrl": extract_movie_video_url(soup, base_domain),
        "added_date": extract_movie_added_date(soup),
        "cover_image": extract_movie_cover_image(soup, base_domain),
    }


def parse_movie_detail_html(html: str, base_domain: str) -> dict[str, Any]:
    return parse_movie_detail_soup(BeautifulSoup(html, "html.parser"), base_domain)


def make_movie_fingerprint(payload: dict[str, Any] | None) -> str:
    video_url = (payload or {}).get("videoUrl", "")
    added_date = (payload or {}).get("added_date", "")
    joined = "\n".join(part for part in (video_url, added_date) if part)
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
            not record.get("videoUrl"),
            not record.get("poster"),
            not record.get("imdb_id"),
            record.get("description") in ("", None, DEFAULT_DESCRIPTION),
        )
    )


def build_movie_state_entry(
    title: str,
    site_payload: dict[str, Any] | None,
    detail_failures: list[str],
    error: str = "",
) -> dict[str, Any]:
    payload = {
        "title": title,
        "video_url_present": bool((site_payload or {}).get("videoUrl")),
        "content_fingerprint": make_movie_fingerprint(site_payload),
        "added_date": (site_payload or {}).get("added_date", ""),
        "last_seen_at": iso_now(),
    }
    if detail_failures:
        payload["detail_failures"] = detail_failures[:100]
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


def extract_tmdb_platform(provider_payload: dict[str, Any]) -> str:
    region_payload = provider_payload.get("results", {}).get("TR", {})
    for key in ("flatrate", "rent", "buy"):
        providers = region_payload.get(key, [])
        if providers:
            name = providers[0].get("provider_name", "").strip()
            if name:
                return name
    return DEFAULT_PLATFORM


def build_tmdb_movie_payload(info: dict[str, Any], platform: str) -> dict[str, Any]:
    videos = info.get("videos", {}).get("results", [])
    credits = info.get("credits", {})
    cast = credits.get("cast", [])
    crew = credits.get("crew", [])
    external_ids = info.get("external_ids", {})
    vote_average = info.get("vote_average", 0.0)
    release_date = info.get("release_date", "")
    return {
        "title": info.get("title") or "",
        "original_title": info.get("original_title") or "",
        "description": info.get("overview") or DEFAULT_DESCRIPTION,
        "imdb": str(round(vote_average, 1)) if vote_average else "0.0",
        "imdb_id": external_ids.get("imdb_id", ""),
        "year": str(release_date)[:4] if release_date else "",
        "genres": [genre["name"] for genre in info.get("genres", []) if genre.get("name")],
        "cast": [person["name"] for person in cast[:12] if person.get("name")],
        "director": next(
            (person["name"] for person in crew if person.get("job") == "Director" and person.get("name")),
            "",
        ),
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
        "trailer": next(
            (
                f"https://www.youtube.com/watch?v={video['key']}"
                for video in videos
                if video.get("site") == "YouTube"
                and "trailer" in video.get("type", "").lower()
                and video.get("key")
            ),
            "",
        ),
        "platform": platform or DEFAULT_PLATFORM,
    }


def get_tmdb_movie_data(title: str, state: dict[str, Any], config: AppConfig) -> dict[str, Any] | None:
    cache_key = normalize_tmdb_title(title)
    cache = state.setdefault("tmdb_cache", {})
    cached_entry = cache.get(cache_key)
    if isinstance(cached_entry, dict) and cache_entry_is_fresh(cached_entry, config):
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
        for query in queries:
            search_result = search.movie(query=query)
            if search_result.get("results"):
                break

        if not search_result.get("results"):
            cache[cache_key] = {"status": "miss", "cached_at": iso_now(), "data": {}}
            return {}

        movie_id = search_result["results"][0]["id"]
        movie = tmdb.Movies(movie_id)
        info = movie.info(language="tr", append_to_response="videos,credits,external_ids")
        provider_payload: dict[str, Any] = {}
        try:
            provider_payload = movie.watch_providers()
        except Exception as exc:
            logger.warning("TMDB platform bilgisi alinamadi (%s): %s", title, exc)

        payload = build_tmdb_movie_payload(info, extract_tmdb_platform(provider_payload))
        cache[cache_key] = {"status": "hit", "cached_at": iso_now(), "data": payload}
        return dict(payload)
    except Exception as exc:
        logger.warning("TMDB hatasi (%s): %s", title, exc)
        cache[cache_key] = {"status": "error", "cached_at": iso_now(), "message": str(exc)[:500]}
        return None


def finalize_movie_record(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "film",
        "title": record.get("title", ""),
        "url": record.get("url", ""),
        "videoUrl": record.get("videoUrl", ""),
        "added_date": record.get("added_date", ""),
        "description": record.get("description", DEFAULT_DESCRIPTION) or DEFAULT_DESCRIPTION,
        "imdb": record.get("imdb", "0.0") or "0.0",
        "imdb_id": record.get("imdb_id", ""),
        "year": record.get("year", ""),
        "genres": record.get("genres", []),
        "cast": record.get("cast", []),
        "director": record.get("director", ""),
        "poster": record.get("poster", ""),
        "cover_image": record.get("cover_image", ""),
        "trailer": record.get("trailer", ""),
        "platform": record.get("platform", DEFAULT_PLATFORM) or DEFAULT_PLATFORM,
    }


def merge_movie_record(
    existing: dict[str, Any] | None,
    item: MovieListItem,
    site_payload: dict[str, Any],
    tmdb_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    merged = dict(existing or {})
    previous_title = str(merged.get("title", "") or "")
    merged["type"] = "film"
    merged["url"] = item.url
    if item.poster and not is_meaningful_value("poster", merged.get("poster")):
        merged["poster"] = item.poster

    for field in ("videoUrl", "added_date"):
        if is_meaningful_value(field, site_payload.get(field)):
            merged[field] = site_payload[field]

    if site_payload.get("cover_image") and not is_meaningful_value("cover_image", merged.get("cover_image")):
        merged["cover_image"] = site_payload["cover_image"]

    if tmdb_payload is not None:
        for field in (
            "description",
            "imdb",
            "imdb_id",
            "year",
            "genres",
            "cast",
            "director",
            "poster",
            "cover_image",
            "trailer",
            "platform",
        ):
            if is_meaningful_value(field, tmdb_payload.get(field)):
                merged[field] = tmdb_payload[field]

    if not is_meaningful_value("poster", merged.get("poster")) and item.poster:
        merged["poster"] = item.poster
    if not is_meaningful_value("cover_image", merged.get("cover_image")) and site_payload.get("cover_image"):
        merged["cover_image"] = site_payload["cover_image"]
    if not is_meaningful_value("description", merged.get("description")):
        merged["description"] = DEFAULT_DESCRIPTION
    if not is_meaningful_value("platform", merged.get("platform")):
        merged["platform"] = DEFAULT_PLATFORM

    merged["title"] = resolve_turkish_title(
        current_title=previous_title,
        official_title=(tmdb_payload or {}).get("title", ""),
        original_title=(tmdb_payload or {}).get("original_title", ""),
        fallback_title=item.title,
    )

    merged.setdefault("videoUrl", "")
    merged.setdefault("added_date", "")
    merged.setdefault("imdb", "0.0")
    merged.setdefault("imdb_id", "")
    merged.setdefault("year", "")
    merged.setdefault("genres", [])
    merged.setdefault("cast", [])
    merged.setdefault("director", "")
    merged.setdefault("poster", "")
    merged.setdefault("cover_image", "")
    merged.setdefault("trailer", "")
    return finalize_movie_record(merged)


def movie_records_equal(left: dict[str, Any], right: dict[str, Any]) -> bool:
    comparable_fields = (
        "type",
        "title",
        "url",
        "videoUrl",
        "added_date",
        "description",
        "imdb",
        "imdb_id",
        "year",
        "genres",
        "cast",
        "director",
        "poster",
        "cover_image",
        "trailer",
        "platform",
    )
    for field in comparable_fields:
        if left.get(field) != right.get(field):
            return False
    return True


def needs_session_refresh(payload: FetchPayload, expect_movie_cards: bool, config: AppConfig) -> bool:
    if payload.status_code in (403, 503):
        return True
    if payload.challenge:
        return True
    if payload.status_code != 200 or not payload.text:
        return False
    if expect_movie_cards:
        soup = payload.soup()
        return not bool(soup and extract_movie_list_items(soup, config.base_domain))
    return False


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
                f"{self.config.base_domain}/filmler/",
                cached_cookies,
                cached_user_agent,
                self.config,
            )
            if (
                payload.status_code == 200
                and payload.text
                and not needs_session_refresh(payload, expect_movie_cards=True, config=self.config)
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
        target_url = f"{config.base_domain}/filmler/"
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
        items: list[MovieListItem] = []
        captcha_attempted = False
        retried_open = False

        while time.time() < deadline:
            try:
                page_html = sb.get_page_source()
            except Exception:
                page_html = ""

            items = extract_movie_items_from_html(page_html, config.base_domain)
            if items and not is_cloudflare_challenge(page_html):
                break

            if is_cloudflare_challenge(page_html) and not captcha_attempted and hasattr(sb, "uc_gui_click_captcha"):
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
            logger.info("Cloudflare oturumu basariyla alindi. Ilk sayfa: %s kayit.", len(items))
            return cookies, user_agent, page_html

        http_payload = fetch_html(target_url, cookies, user_agent, config)
        if (
            http_payload.status_code == 200
            and http_payload.text
            and not is_cloudflare_challenge(http_payload.text)
        ):
            http_items = extract_movie_items_from_html(http_payload.text, config.base_domain)
            if http_items:
                logger.info(
                    "Cloudflare oturumu HTTP dogrulamasi ile kabul edildi. Ilk sayfa: %s kayit.",
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
            "http_item_count": len(extract_movie_items_from_html(http_payload.text, config.base_domain))
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


def fetch_with_reauth(url: str, session_ctx: SessionContext, expect_movie_cards: bool = False) -> FetchPayload:
    payload = fetch_html(url, session_ctx.cookies, session_ctx.user_agent, session_ctx.config)
    if needs_session_refresh(payload, expect_movie_cards=expect_movie_cards, config=session_ctx.config):
        session_ctx.refresh(f"yeniden dogrulama gerekli: {url}")
        payload = fetch_html(url, session_ctx.cookies, session_ctx.user_agent, session_ctx.config)
    return payload


def fetch_list_page(page_number: int, cookies: dict[str, str], user_agent: str, config: AppConfig) -> PageFetchResult:
    url = f"{config.base_domain}/filmler/page/{page_number}/"
    payload = fetch_html(url, cookies, user_agent, config)
    if payload.status_code == 404:
        return PageFetchResult(page=page_number, items=[])
    session_invalid = needs_session_refresh(payload, expect_movie_cards=True, config=config)
    if payload.status_code != 200 or not payload.text or session_invalid:
        return PageFetchResult(
            page=page_number,
            items=[],
            error=payload.error or f"HTTP {payload.status_code}",
            session_invalid=session_invalid,
        )
    soup = payload.soup()
    items = extract_movie_list_items(soup, config.base_domain) if soup else []
    return PageFetchResult(page=page_number, items=items)


def gather_all_movie_items(session_ctx: SessionContext) -> tuple[int, list[MovieListItem]]:
    first_page = parse_movie_list_html(session_ctx.page1_html, session_ctx.config.base_domain)
    items_by_page: dict[int, list[MovieListItem]] = {1: first_page["items"]}
    total_pages = first_page["total_pages"]
    if not items_by_page[1]:
        session_ctx.refresh("ilk sayfa kartlari bos geldi")
        first_page = parse_movie_list_html(session_ctx.page1_html, session_ctx.config.base_domain)
        items_by_page[1] = first_page["items"]
        total_pages = first_page["total_pages"]
    if not items_by_page[1]:
        raise RuntimeError("Ilk sayfada hic film karti bulunamadi.")

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

    ordered_items: list[MovieListItem] = []
    seen: set[str] = set()
    for page in range(1, total_pages + 1):
        for item in items_by_page.get(page, []):
            if item.url not in seen:
                seen.add(item.url)
                ordered_items.append(item)

    logger.info("Benzersiz film sayisi: %s", len(ordered_items))
    return total_pages, ordered_items


def fetch_movie_detail(movie_url: str, session_ctx: SessionContext) -> tuple[dict[str, Any] | None, list[str]]:
    detail_payload = fetch_with_reauth(movie_url, session_ctx)
    if detail_payload.status_code != 200 or not detail_payload.text:
        return None, [f"Detay sayfasi alinamadi: {detail_payload.error or detail_payload.status_code}"]
    detail_soup = detail_payload.soup()
    if detail_soup is None:
        return None, ["Detay HTML parse edilemedi."]
    payload = parse_movie_detail_soup(detail_soup, session_ctx.config.base_domain)
    payload["videoUrl"], should_try_browser = normalize_movie_video_candidate(
        movie_url,
        payload.get("videoUrl", ""),
        session_ctx.config.base_domain,
    )
    failures: list[str] = []
    if not payload.get("videoUrl") or should_try_browser:
        resolved_iframes = resolve_iframe_urls_with_browser(
            [movie_url],
            base_domain=session_ctx.config.base_domain,
            wait_seconds=session_ctx.config.selenium_wait_seconds,
            headless=session_ctx.config.selenium_headless,
            log_context="film",
            cookies=session_ctx.cookies,
            log=logger,
        )
        resolved_video_url, _ = normalize_movie_video_candidate(
            movie_url,
            resolved_iframes.get(movie_url, ""),
            session_ctx.config.base_domain,
        )
        if resolved_video_url:
            payload["videoUrl"] = resolved_video_url
        if not payload.get("videoUrl"):
            failures.append("Iframe kaynagi bulunamadi.")
    return payload, failures


def persist_checkpoint(
    config: AppConfig,
    all_movies: list[dict[str, Any]],
    state: dict[str, Any],
    run_stats: dict[str, Any],
) -> None:
    with save_lock:
        atomic_write_json(config.data_file, all_movies, backup_path=config.backup_file)
        run_state = state.setdefault("run", {})
        run_state.update(run_stats)
        run_state["last_checkpoint_at"] = iso_now()
        save_state(config.state_file, state)


def apply_process_result_counts(
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
    updated_count, skipped_count, _ = apply_process_result_counts(updated_count, skipped_count, 0, status)
    return updated_count, skipped_count, max(0, failed_count - 1)


def maybe_persist_running_checkpoint(
    config: AppConfig,
    all_movies: list[dict[str, Any]],
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
        all_movies,
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


def process_movie_item(
    item: MovieListItem,
    session_ctx: SessionContext,
    config: AppConfig,
    state: dict[str, Any],
    all_movies: list[dict[str, Any]],
    url_map: dict[str, int],
) -> MovieProcessResult:
    existing_index = url_map.get(item.url)
    existing_record = all_movies[existing_index] if existing_index is not None else None

    try:
        site_payload, detail_failures = fetch_movie_detail(item.url, session_ctx)
    except Exception as exc:
        state.setdefault("movies", {})[item.url] = build_movie_state_entry(item.title, None, [], error=str(exc))
        logger.exception("Film katalogu alinamadi: %s", item.title)
        return MovieProcessResult(status="failed")

    if site_payload is None:
        state.setdefault("movies", {})[item.url] = build_movie_state_entry(
            item.title,
            None,
            detail_failures,
            error="Detay sayfasi alinamadi.",
        )
        logger.error("Detay sayfasi alinamadi: %s", item.title)
        return MovieProcessResult(status="failed")

    existing_video_url = (existing_record or {}).get("videoUrl", "")
    if not site_payload.get("videoUrl") and not existing_video_url:
        state.setdefault("movies", {})[item.url] = build_movie_state_entry(
            item.title,
            site_payload,
            detail_failures,
            error="Film iframe kaynagi bulunamadi.",
        )
        logger.error("Film iframe kaynagi bulunamadi: %s", item.title)
        return MovieProcessResult(status="failed")

    should_process = (
        existing_record is None
        or record_needs_refresh(existing_record)
        or (site_payload.get("videoUrl") and site_payload["videoUrl"] != existing_video_url)
        or (
            site_payload.get("added_date")
            and site_payload["added_date"] != (existing_record or {}).get("added_date", "")
        )
    )
    if not should_process:
        state.setdefault("movies", {})[item.url] = build_movie_state_entry(item.title, site_payload, detail_failures)
        return MovieProcessResult(status="skipped")

    tmdb_payload = get_tmdb_movie_data(item.title, state, config)
    final_record = merge_movie_record(existing_record, item, site_payload, tmdb_payload)
    if existing_record and movie_records_equal(existing_record, final_record):
        state.setdefault("movies", {})[item.url] = build_movie_state_entry(item.title, site_payload, detail_failures)
        return MovieProcessResult(status="skipped")

    if existing_index is None:
        all_movies.append(final_record)
        url_map[item.url] = len(all_movies) - 1
        logger.info("Yeni film eklendi: %s", item.title)
    else:
        all_movies[existing_index] = final_record
        logger.info("Film guncellendi: %s", item.title)

    state.setdefault("movies", {})[item.url] = build_movie_state_entry(item.title, site_payload, detail_failures)
    return MovieProcessResult(status="updated")


def main() -> None:
    config = load_config()
    configure_logging(config.log_file)
    tmdb.API_KEY = config.tmdb_api_key

    logger.info("Film senkronizasyonu basliyor. Hedef dosya: %s", config.data_file.name)

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

    all_movies = load_movie_database(config)
    url_map = {entry.get("url"): index for index, entry in enumerate(all_movies) if entry.get("url")}

    total_pages, items = gather_all_movie_items(session_ctx)
    logger.info("%s sayfa tarandi, %s benzersiz film islenecek.", total_pages, len(items))

    changes_since_checkpoint = 0
    last_checkpoint_at = time.monotonic()
    updated_count = 0
    skipped_count = 0
    failed_count = 0
    retry_candidates: list[MovieListItem] = []

    for index, item in enumerate(items, start=1):
        logger.info("[%s/%s] %s", index, len(items), item.title)
        result = process_movie_item(item, session_ctx, config, state, all_movies, url_map)
        updated_count, skipped_count, failed_count = apply_process_result_counts(
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
                all_movies,
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
            "Hata alan %s film icin retry turu %s/%s basliyor.",
            len(remaining_retry_candidates),
            retry_pass,
            config.failed_retry_passes,
        )
        if config.failed_retry_wait_seconds > 0:
            time.sleep(config.failed_retry_wait_seconds)

        next_retry_candidates: list[MovieListItem] = []
        for retry_index, item in enumerate(remaining_retry_candidates, start=1):
            logger.info(
                "[Retry %s/%s - Tur %s/%s] %s",
                retry_index,
                len(remaining_retry_candidates),
                retry_pass,
                config.failed_retry_passes,
                item.title,
            )
            result = process_movie_item(item, session_ctx, config, state, all_movies, url_map)
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
                    all_movies,
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
        all_movies,
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
        len(all_movies),
        updated_count,
        skipped_count,
        failed_count,
    )


if __name__ == "__main__":
    main()
