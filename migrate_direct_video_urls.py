from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

from domain_config import (
    extract_dizipal_imdb_slug,
    imdb_title_url,
    is_dizipal_url,
    load_base_domain,
    replace_dizipal_host,
    vidmody_video_url,
)

BASE_DOMAIN = load_base_domain()
DATA_FILES = (
    Path("github_data/movies.json"),
    Path("github_data/diziler.json"),
    Path("github_data/dizipal.json"),
)


def normalize_site_url(url: str | None, base_domain: str = BASE_DOMAIN) -> str:
    if not url:
        return ""
    return replace_dizipal_host(urljoin(base_domain + "/", str(url)), base_domain)


def normalize_record(record: dict[str, Any]) -> tuple[dict[str, Any], int]:
    changed = 0
    normalized = dict(record)
    content_url = normalize_site_url(normalized.get("url", ""))
    imdb_slug = extract_dizipal_imdb_slug(content_url)
    if normalized.get("type") == "film" and imdb_slug:
        desired_url = imdb_title_url(imdb_slug)
        desired_video = vidmody_video_url(imdb_slug)
        if normalized.get("url") != desired_url:
            normalized["url"] = desired_url
            changed += 1
        if normalized.get("videoUrl") != desired_video:
            normalized["videoUrl"] = desired_video
            changed += 1
        return normalized, changed

    if content_url and normalized.get("url") != content_url:
        normalized["url"] = content_url
        changed += 1

    video_url = str(normalized.get("videoUrl", "") or "")
    should_use_direct_url = (
        content_url
        and is_dizipal_url(content_url)
        and (not video_url or "embed" in video_url.lower() or "iframe.php" in video_url.lower() or is_dizipal_url(video_url))
    )
    if normalized.get("type") == "film" and should_use_direct_url and normalized.get("videoUrl") != content_url:
        normalized["videoUrl"] = content_url
        changed += 1

    episodes: list[Any] = []
    for episode in normalized.get("episodes", []):
        if not isinstance(episode, dict):
            episodes.append(episode)
            continue

        episode_payload = dict(episode)
        episode_url = normalize_site_url(episode_payload.get("url", ""))
        if episode_url and episode_payload.get("url") != episode_url:
            episode_payload["url"] = episode_url
            changed += 1
        if episode_url and episode_payload.get("videoUrl") != episode_url:
            episode_payload["videoUrl"] = episode_url
            changed += 1
        episodes.append(episode_payload)

    if episodes:
        normalized["episodes"] = episodes
    return normalized, changed


def migrate_file(path: Path) -> tuple[int, int]:
    if not path.exists():
        return 0, 0

    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    if not isinstance(payload, list):
        raise ValueError(f"{path} liste formatinda degil.")

    migrated: list[dict[str, Any]] = []
    changed_fields = 0
    for record in payload:
        if not isinstance(record, dict):
            migrated.append(record)
            continue
        normalized, changes = normalize_record(record)
        migrated.append(normalized)
        changed_fields += changes

    if changed_fields:
        temp_path = path.with_suffix(path.suffix + ".tmp")
        with temp_path.open("w", encoding="utf-8") as handle:
            json.dump(migrated, handle, ensure_ascii=False, indent=2)
        with temp_path.open("r", encoding="utf-8") as handle:
            json.load(handle)
        os.replace(temp_path, path)

    return len(migrated), changed_fields


def main() -> None:
    total_changes = 0
    for path in DATA_FILES:
        record_count, changed_fields = migrate_file(path)
        total_changes += changed_fields
        print(f"{path}: {record_count} kayit, {changed_fields} alan guncellendi")
    print(f"Toplam guncellenen alan: {total_changes}")


if __name__ == "__main__":
    main()
