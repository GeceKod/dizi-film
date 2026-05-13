from __future__ import annotations

import os
import re
from pathlib import Path
from urllib.parse import urlparse


DEFAULT_BASE_DOMAIN = "https://dizipal.im"
DEFAULT_DOMAIN_FILE = Path("dizipal_domain.txt")
IMDB_ID_RE = re.compile(r"^tt\d+$", flags=re.IGNORECASE)


def normalize_base_domain(value: str | None, fallback: str = DEFAULT_BASE_DOMAIN) -> str:
    cleaned = (value or "").strip()
    if not cleaned:
        cleaned = fallback
    if "://" not in cleaned:
        cleaned = f"https://{cleaned}"
    return cleaned.rstrip("/")


def load_base_domain(env_var: str | None = None) -> str:
    if env_var and os.getenv(env_var):
        return normalize_base_domain(os.getenv(env_var))

    if os.getenv("DIZIPAL_BASE_DOMAIN"):
        return normalize_base_domain(os.getenv("DIZIPAL_BASE_DOMAIN"))

    domain_file = Path(os.getenv("DIZIPAL_DOMAIN_FILE", str(DEFAULT_DOMAIN_FILE)))
    if domain_file.exists():
        for line in domain_file.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if stripped and not stripped.startswith("#"):
                return normalize_base_domain(stripped)

    return DEFAULT_BASE_DOMAIN


def is_dizipal_host(hostname: str | None) -> bool:
    if not hostname:
        return False
    normalized = hostname.lower().strip(".")
    return normalized.startswith("dizipal.") or normalized.startswith("www.dizipal.")


def is_dizipal_url(url: str | None) -> bool:
    if not url:
        return False
    return is_dizipal_host(urlparse(url).hostname)


def extract_dizipal_imdb_slug(url: str | None) -> str:
    if not url or not is_dizipal_url(url):
        return ""
    slug = urlparse(url).path.strip("/").split("/")[-1]
    return slug if IMDB_ID_RE.fullmatch(slug or "") else ""


def imdb_title_url(imdb_id: str) -> str:
    return f"https://www.imdb.com/title/{imdb_id}/"


def vidmody_video_url(imdb_id: str) -> str:
    return f"https://vidmody.com/vs/{imdb_id}"


def replace_dizipal_host(url: str, base_domain: str) -> str:
    parsed = urlparse(url)
    base = urlparse(base_domain)
    if not parsed.hostname or not base.hostname or not is_dizipal_host(parsed.hostname):
        return url
    return parsed._replace(
        scheme=base.scheme or parsed.scheme or "https",
        netloc=base.netloc,
    ).geturl()
