from __future__ import annotations

import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

from title_localization import resolve_turkish_title


TMDB_API_KEY = os.getenv("TMDB_API_KEY", "48ce82f1de91232f542660e99a9d1336")
TMDB_LANGUAGE = os.getenv("TMDB_LANGUAGE", "tr-TR")
MAX_WORKERS = max(1, int(os.getenv("TMDB_TITLE_WORKERS", "6")))
REQUEST_TIMEOUT = int(os.getenv("TMDB_REQUEST_TIMEOUT", "30"))
TARGET_FILES = (
    Path("github_data/movies.json"),
    Path("github_data/dizipal.json"),
)

_thread_local = threading.local()


def atomic_write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    try:
        with temp_path.open("w", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
        with temp_path.open("r", encoding="utf-8") as handle:
            json.load(handle)
        os.replace(temp_path, path)
    finally:
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)


def load_json_list(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, list):
        raise ValueError(f"{path} liste formatinda degil.")
    return payload


def iter_film_records(payload: list[dict]):
    for item in payload:
        if isinstance(item, dict) and item.get("type") == "film":
            yield item


def get_session() -> requests.Session:
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = requests.Session()
        session.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": "dizi-film-title-updater/1.0",
            }
        )
        _thread_local.session = session
    return session


def fetch_tmdb_title_data(imdb_id: str) -> dict[str, str]:
    session = get_session()
    url = f"https://api.themoviedb.org/3/find/{imdb_id}"
    params = {
        "api_key": TMDB_API_KEY,
        "external_source": "imdb_id",
        "language": TMDB_LANGUAGE,
    }

    for attempt in range(4):
        try:
            response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                wait_seconds = int(retry_after) if retry_after and retry_after.isdigit() else 2 ** attempt
                time.sleep(max(1, wait_seconds))
                continue
            response.raise_for_status()
            payload = response.json()
            movie_results = payload.get("movie_results") or []
            if not movie_results:
                return {}
            movie = movie_results[0]
            return {
                "title": (movie.get("title") or "").strip(),
                "original_title": (movie.get("original_title") or "").strip(),
            }
        except requests.RequestException:
            if attempt == 3:
                return {}
            time.sleep(2 ** attempt)
    return {}


def build_tmdb_map(imdb_ids: set[str]) -> tuple[dict[str, dict[str, str]], int]:
    title_map: dict[str, dict[str, str]] = {}
    missing = 0
    completed = 0
    total = len(imdb_ids)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {executor.submit(fetch_tmdb_title_data, imdb_id): imdb_id for imdb_id in sorted(imdb_ids)}
        for future in as_completed(future_map):
            imdb_id = future_map[future]
            title_data = future.result()
            if title_data:
                title_map[imdb_id] = title_data
            else:
                missing += 1

            completed += 1
            if completed % 500 == 0 or completed == total:
                print(f"Ilerleme: {completed}/{total} IMDb kaydi sorgulandi.")

    return title_map, missing


def build_resolution_map(requests_to_resolve: set[tuple[str, str, str]]) -> dict[tuple[str, str, str], str]:
    resolved: dict[tuple[str, str, str], str] = {}
    completed = 0
    total = len(requests_to_resolve)
    if not total:
        return resolved

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {
            executor.submit(resolve_turkish_title, current_title, official_title, original_title): (
                current_title,
                official_title,
                original_title,
            )
            for current_title, official_title, original_title in requests_to_resolve
        }
        for future in as_completed(future_map):
            request_key = future_map[future]
            resolved[request_key] = future.result()
            completed += 1
            if completed % 1000 == 0 or completed == total:
                print(f"Ilerleme: {completed}/{total} baslik cozuldu.")

    return resolved


def main() -> None:
    payload_by_path = {path: load_json_list(path) for path in TARGET_FILES}

    imdb_ids = {
        imdb_id
        for payload in payload_by_path.values()
        for item in iter_film_records(payload)
        for imdb_id in [item.get("imdb_id", "").strip()]
        if imdb_id
    }

    if imdb_ids:
        print(f"Toplam {len(imdb_ids)} benzersiz film IMDb kaydi bulundu.")
        title_map, missing = build_tmdb_map(imdb_ids)
    else:
        print("IMDb kimligi olan film kaydi bulunamadi; yalnizca mevcut basliklar uzerinden islem yapilacak.")
        title_map, missing = {}, 0

    resolution_requests: set[tuple[str, str, str]] = set()
    for payload in payload_by_path.values():
        for item in iter_film_records(payload):
            imdb_id = item.get("imdb_id", "").strip()
            tmdb_data = title_map.get(imdb_id, {})
            resolution_requests.add(
                (
                    (item.get("title") or "").strip(),
                    tmdb_data.get("title", ""),
                    tmdb_data.get("original_title", ""),
                )
            )

    print(f"Toplam {len(resolution_requests)} benzersiz baslik kurali cozuluyor.")
    resolved_map = build_resolution_map(resolution_requests)

    total_updates = 0
    for path, payload in payload_by_path.items():
        file_updates = 0
        for item in iter_film_records(payload):
            imdb_id = item.get("imdb_id", "").strip()
            tmdb_data = title_map.get(imdb_id, {})
            request_key = (
                (item.get("title") or "").strip(),
                tmdb_data.get("title", ""),
                tmdb_data.get("original_title", ""),
            )
            title = resolved_map.get(request_key, item.get("title", ""))
            if title and item.get("title") != title:
                item["title"] = title
                file_updates += 1

        atomic_write_json(path, payload)
        total_updates += file_updates
        print(f"{path}: {file_updates} baslik guncellendi.")

    print(f"Toplam guncellenen baslik sayisi: {total_updates}")
    print(f"TMDb'de Turkce baslik bulunamayan kayit sayisi: {missing}")


if __name__ == "__main__":
    main()
