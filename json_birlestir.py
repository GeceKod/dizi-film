from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse


DIZI_DOSYASI = Path(os.getenv("DIZI_DATA_FILE", "diziler.json"))
FILM_DOSYASI = Path(os.getenv("FILM_DATA_FILE", "movies.json"))
CIKTI_DOSYASI = Path(os.getenv("CIKTI_DOSYASI", "dizipal.json"))
BASE_DOMAIN = os.getenv("DIZIPAL_BASE_DOMAIN", "https://dizipal.im").rstrip("/")


def replace_file(source: Path, target: Path) -> None:
    try:
        os.replace(source, target)
        return
    except PermissionError:
        backup_path = target.with_suffix(target.suffix + ".replacebak")

    if backup_path.exists():
        backup_path.unlink()
    os.replace(target, backup_path)
    try:
        os.replace(source, target)
    except Exception:
        os.replace(backup_path, target)
        raise
    finally:
        backup_path.unlink(missing_ok=True)


def atomic_write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    try:
        with temp_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
        with temp_path.open("r", encoding="utf-8") as handle:
            json.load(handle)
        replace_file(temp_path, path)
    finally:
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)


def load_json_list(path: Path) -> list[dict]:
    if not path.exists():
        print(f"Uyari: Dosya bulunamadi: {path}")
        return []
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except json.JSONDecodeError as exc:
        print(f"Hata: {path} bozuk JSON. Satir {exc.lineno}, sutun {exc.colno}: {exc.msg}")
        return []
    except OSError as exc:
        print(f"Hata: {path} okunamadi: {exc}")
        return []
    if not isinstance(payload, list):
        print(f"Uyari: {path} liste formatinda degil.")
        return []
    return payload


def is_dizipal_host(hostname: str) -> bool:
    normalized = hostname.lower().strip(".")
    return normalized.startswith("dizipal.") or normalized.startswith("www.dizipal.")


def normalize_site_url(url: str | None, base_domain: str = BASE_DOMAIN) -> str:
    if not url:
        return ""
    normalized = urljoin(base_domain + "/", str(url))
    parsed = urlparse(normalized)
    base = urlparse(base_domain)
    if parsed.hostname and base.hostname and is_dizipal_host(parsed.hostname):
        normalized = parsed._replace(
            scheme=base.scheme or parsed.scheme or "https",
            netloc=base.netloc,
        ).geturl()
    return normalized


def normalize_direct_video_urls(record: dict[str, Any]) -> dict[str, Any]:
    payload = dict(record)
    content_url = normalize_site_url(payload.get("url", ""))
    if content_url:
        payload["url"] = content_url

    if payload.get("type") == "film" and content_url:
        payload["videoUrl"] = content_url

    episodes: list[Any] = []
    for episode in payload.get("episodes", []):
        if not isinstance(episode, dict):
            episodes.append(episode)
            continue
        episode_payload = dict(episode)
        episode_url = normalize_site_url(episode_payload.get("url", ""))
        if episode_url:
            episode_payload["url"] = episode_url
            episode_payload["videoUrl"] = episode_url
        episodes.append(episode_payload)

    if episodes:
        payload["episodes"] = episodes
    return payload


def make_record_key(record: dict[str, Any], fallback_index: int) -> tuple[str, str]:
    content_type = str(record.get("type", "") or "")
    imdb_id = str(record.get("imdb_id", "") or "").strip()
    url = normalize_site_url(str(record.get("url", "") or "").strip())
    title = str(record.get("title", "") or "").strip().casefold()

    if imdb_id:
        return content_type, f"imdb:{imdb_id}"
    if url:
        return content_type, f"url:{url}"
    if title:
        return content_type, f"title:{title}"
    return content_type, f"index:{fallback_index}"


def merge_lists(existing: list[dict], incoming: list[dict]) -> tuple[list[dict], int, int]:
    merged: list[dict] = []
    index_by_key: dict[tuple[str, str], int] = {}
    replaced = 0
    added = 0

    for idx, record in enumerate(existing):
        key = make_record_key(record, idx)
        if key in index_by_key:
            merged[index_by_key[key]] = record
            continue
        index_by_key[key] = len(merged)
        merged.append(record)

    for idx, record in enumerate(incoming, start=len(existing)):
        key = make_record_key(record, idx)
        existing_index = index_by_key.get(key)
        if existing_index is not None:
            merged[existing_index] = record
            replaced += 1
        else:
            index_by_key[key] = len(merged)
            merged.append(record)
            added += 1

    return merged, replaced, added


def main() -> None:
    print("JSON dosyalari birlestiriliyor...")
    print("-" * 40)

    diziler = [normalize_direct_video_urls(record) for record in load_json_list(DIZI_DOSYASI)]
    filmler = [normalize_direct_video_urls(record) for record in load_json_list(FILM_DOSYASI)]
    mevcut_cikti = [normalize_direct_video_urls(record) for record in load_json_list(CIKTI_DOSYASI)]
    gelen_liste = diziler + filmler
    toplam_liste, replaced, added = merge_lists(mevcut_cikti, gelen_liste)

    if not toplam_liste:
        print("Uyari: Birlestirilecek gecerli veri bulunamadi.")
        return

    try:
        atomic_write_json(CIKTI_DOSYASI, toplam_liste)
    except OSError as exc:
        print(f"Hata: {CIKTI_DOSYASI} yazilamadi: {exc}")
        return

    print("-" * 40)
    print("Islem basarili.")
    print(f"Toplam {len(toplam_liste)} icerik {CIKTI_DOSYASI.name} dosyasina kaydedildi.")
    print(f"Dizi sayisi: {len(diziler)}")
    print(f"Film sayisi: {len(filmler)}")
    print(f"Mevcut kayitlardan guncellenen: {replaced}")
    print(f"Yeni eklenen kayit: {added}")


if __name__ == "__main__":
    main()
