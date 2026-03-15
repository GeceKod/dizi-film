from __future__ import annotations

import json
import os
from pathlib import Path


DIZI_DOSYASI = Path(os.getenv("DIZI_DATA_FILE", "diziler.json"))
FILM_DOSYASI = Path(os.getenv("FILM_DATA_FILE", "movies.json"))
CIKTI_DOSYASI = Path(os.getenv("CIKTI_DOSYASI", "dizipal.json"))


def atomic_write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    try:
        with temp_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
        with temp_path.open("r", encoding="utf-8") as handle:
            json.load(handle)
        os.replace(temp_path, path)
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


def main() -> None:
    print("JSON dosyalari birlestiriliyor...")
    print("-" * 40)

    diziler = load_json_list(DIZI_DOSYASI)
    filmler = load_json_list(FILM_DOSYASI)
    toplam_liste = diziler + filmler

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


if __name__ == "__main__":
    main()
