# Dizi_Film

Bu repo iki farkli calisma modunu ayni kod tabaninda tasir:

- Lokal calisma: varsayilan davranis tum sayfalari tarar.
- GitHub Actions calismasi: env degiskenleri ile sadece ilk 2 sayfayi tarar.

## Lokal calistirma

- Sadece dizi: `BASLAT_DIZI.bat`
- Sadece film: `BASLAT_FILM.bat`
- Uctan uca senkron: `BASLAT_TUM_SENKRON.bat`

## GitHub tarafi

- Workflow her 30 dakikada bir tetiklenir.
- `github_data/` altina kucuk veri seti uretir.
- Dizi ve film scraper'lari GitHub ortaminda sadece ilk 2 sayfayi tarar.
- Selenium tarafi GitHub'da `xvfb` sanal ekran uzerinde 1920x1080 calisir.

## Onemli not

Kok dizindeki `diziler.json`, `movies.json` ve `dizipal.json` lokal tam veri dosyalaridir; repo'ya dahil edilmez. GitHub tarafinda commitlenen ciktilar `github_data/` altindadir.
