from __future__ import annotations

import re
import threading
import time
import unicodedata

from deep_translator import GoogleTranslator


_TURKISH_CHARS = set("çÇğĞıİöÖşŞüÜ")
_TITLE_TOKEN_RE = re.compile(r"[0-9A-Za-zÇĞİÖŞÜçğıöşü']+")
_WS_RE = re.compile(r"\s+")
_thread_local = threading.local()
_translation_cache: dict[str, str] = {}
_cache_lock = threading.Lock()

_COMMON_TURKISH_TOKENS = {
    "aile",
    "akşam",
    "anne",
    "arkadaş",
    "arkadaşım",
    "aşk",
    "ay",
    "ay'a",
    "baba",
    "başlangıç",
    "beni",
    "bölüm",
    "büyük",
    "çıkış",
    "çılgın",
    "çocuk",
    "da",
    "de",
    "deli",
    "dünya",
    "ev",
    "fırtına",
    "gece",
    "gibi",
    "gölge",
    "görev",
    "gün",
    "güzel",
    "hayalet",
    "hikayesi",
    "hikâyesi",
    "için",
    "ile",
    "iyi",
    "kara",
    "katil",
    "kız",
    "koca",
    "koru",
    "krallar",
    "mahzen",
    "merhamet",
    "nasıl",
    "no",
    "nokta",
    "ol",
    "ölüm",
    "ölümcül",
    "oyun",
    "prenses",
    "sakallıoğlu",
    "savaş",
    "sev",
    "sezon",
    "son",
    "sığınak",
    "takip",
    "tuzak",
    "uçur",
    "ve",
    "yok",
    "yol",
    "yolculuk",
    "ziyaretçiler",
}


def _clean_title(value: str | None) -> str:
    return _WS_RE.sub(" ", (value or "").strip())


def normalize_title_for_compare(value: str | None) -> str:
    cleaned = _clean_title(value)
    if not cleaned:
        return ""
    normalized = unicodedata.normalize("NFKC", cleaned).casefold()
    return _WS_RE.sub(" ", normalized).strip()


def _get_translator() -> GoogleTranslator:
    translator = getattr(_thread_local, "translator", None)
    if translator is None:
        translator = GoogleTranslator(source="auto", target="tr")
        _thread_local.translator = translator
    return translator


def quick_has_turkish_markers(title: str | None) -> bool:
    cleaned = _clean_title(title)
    if not cleaned:
        return False

    if any(char in _TURKISH_CHARS for char in cleaned):
        return True

    tokens = [token.casefold() for token in _TITLE_TOKEN_RE.findall(cleaned)]
    return any(token in _COMMON_TURKISH_TOKENS for token in tokens)


def translate_title_to_turkish(title: str | None) -> str:
    cleaned = _clean_title(title)
    if not cleaned:
        return ""

    cache_key = normalize_title_for_compare(cleaned)
    with _cache_lock:
        cached = _translation_cache.get(cache_key)
    if cached is not None:
        return cached

    translated = cleaned
    for attempt in range(4):
        try:
            candidate = _get_translator().translate(cleaned)
            translated = _clean_title(candidate) or cleaned
            break
        except Exception:
            if attempt == 3:
                translated = cleaned
                break
            time.sleep(2**attempt)

    with _cache_lock:
        _translation_cache[cache_key] = translated
    return translated


def title_is_effectively_turkish(title: str | None) -> bool:
    cleaned = _clean_title(title)
    if not cleaned:
        return False
    if quick_has_turkish_markers(cleaned):
        return True
    translated = translate_title_to_turkish(cleaned)
    return normalize_title_for_compare(translated) == normalize_title_for_compare(cleaned)


def resolve_turkish_title(
    current_title: str | None,
    official_title: str | None = None,
    original_title: str | None = None,
    fallback_title: str | None = None,
) -> str:
    current = _clean_title(current_title)
    fallback = _clean_title(fallback_title)
    official = _clean_title(official_title)
    original = _clean_title(original_title)

    for candidate in (current, fallback):
        if title_is_effectively_turkish(candidate):
            return candidate

    if official and normalize_title_for_compare(official) != normalize_title_for_compare(original):
        return official

    for candidate in (current, fallback, original, official):
        translated = translate_title_to_turkish(candidate)
        if translated and normalize_title_for_compare(translated) != normalize_title_for_compare(candidate):
            return translated

    return current or fallback or official or original
