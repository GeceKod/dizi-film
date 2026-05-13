from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from domain_config import is_dizipal_host, load_base_domain, replace_dizipal_host


DEFAULT_DATA_FILES = (
    Path("github_data/movies.json"),
    Path("github_data/diziler.json"),
    Path("github_data/dizipal.json"),
    Path("movies.json"),
    Path("diziler.json"),
    Path("dizipal.json"),
)


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
    temp_path = path.with_suffix(path.suffix + ".tmp")
    try:
        with temp_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
        with temp_path.open("r", encoding="utf-8") as handle:
            json.load(handle)
        replace_file(temp_path, path)
    finally:
        temp_path.unlink(missing_ok=True)


def update_url_string(value: str, base_domain: str) -> tuple[str, int]:
    parsed = urlparse(value)
    if not parsed.hostname or not is_dizipal_host(parsed.hostname):
        return value, 0
    updated = replace_dizipal_host(value, base_domain)
    return updated, int(updated != value)


def update_value_domains(value: Any, base_domain: str) -> tuple[Any, int]:
    if isinstance(value, str):
        return update_url_string(value, base_domain)
    if isinstance(value, list):
        updated_items = []
        changes = 0
        for item in value:
            updated_item, item_changes = update_value_domains(item, base_domain)
            updated_items.append(updated_item)
            changes += item_changes
        return updated_items, changes
    if isinstance(value, dict):
        updated_dict: dict[str, Any] = {}
        changes = 0
        for key, item in value.items():
            updated_item, item_changes = update_value_domains(item, base_domain)
            updated_dict[key] = updated_item
            changes += item_changes
        return updated_dict, changes
    return value, 0


def normalize_direct_video_fields(record: dict[str, Any]) -> int:
    changes = 0
    content_url = record.get("url", "")
    if record.get("type") == "film" and content_url and record.get("videoUrl") != content_url:
        record["videoUrl"] = content_url
        changes += 1

    for episode in record.get("episodes", []) or []:
        if not isinstance(episode, dict):
            continue
        episode_url = episode.get("url", "")
        if episode_url and episode.get("videoUrl") != episode_url:
            episode["videoUrl"] = episode_url
            changes += 1
    return changes


def update_json_file(path: Path, base_domain: str, dry_run: bool) -> tuple[int, int]:
    if not path.exists():
        return 0, 0

    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    updated_payload, changes = update_value_domains(payload, base_domain)

    if isinstance(updated_payload, list):
        for record in updated_payload:
            if isinstance(record, dict):
                changes += normalize_direct_video_fields(record)

    if changes and not dry_run:
        atomic_write_json(path, updated_payload)

    record_count = len(updated_payload) if isinstance(updated_payload, list) else 1
    return record_count, changes


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="dizipal_domain.txt dosyasindaki domaine gore JSON URL hostlarini gunceller.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Dosyalari yazmadan kac alan degisecek gosterir.",
    )
    parser.add_argument(
        "files",
        nargs="*",
        type=Path,
        help="Varsayilan dosyalar yerine guncellenecek JSON dosyalari.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    base_domain = load_base_domain()
    files = tuple(args.files) if args.files else DEFAULT_DATA_FILES

    print(f"Hedef domain: {base_domain}")
    total_changes = 0
    for path in files:
        record_count, changes = update_json_file(path, base_domain, args.dry_run)
        if not path.exists():
            print(f"{path}: bulunamadi, atlandi")
            continue
        total_changes += changes
        action = "degisecek" if args.dry_run else "guncellendi"
        print(f"{path}: {record_count} kayit, {changes} alan {action}")

    if args.dry_run:
        print(f"Toplam degisecek alan: {total_changes}")
    else:
        print(f"Toplam guncellenen alan: {total_changes}")


if __name__ == "__main__":
    main()
