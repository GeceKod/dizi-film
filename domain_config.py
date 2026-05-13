from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlparse


DEFAULT_BASE_DOMAIN = "https://dizipal.im"
DEFAULT_DOMAIN_FILE = Path("dizipal_domain.txt")


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


def replace_dizipal_host(url: str, base_domain: str) -> str:
    parsed = urlparse(url)
    base = urlparse(base_domain)
    if not parsed.hostname or not base.hostname or not is_dizipal_host(parsed.hostname):
        return url
    return parsed._replace(
        scheme=base.scheme or parsed.scheme or "https",
        netloc=base.netloc,
    ).geturl()
