@echo off
title Gunes TV Dizi Botu - Calistirici
color 0A

echo ========================================================
echo   TEMIZLIK YAPILIYOR...
echo   (Acik kalan Chrome ve Driver'lar kapatiliyor)
echo ========================================================
echo.

taskkill /F /IM chrome.exe /T >nul 2>&1
taskkill /F /IM chromedriver.exe /T >nul 2>&1

echo [OK] Temizlik tamamlandi.
echo.
echo ========================================================
echo   ORTAM KONTROL EDILIYOR...
echo ========================================================

python -c "import seleniumbase, curl_cffi, bs4, requests, tmdbsimple" >nul 2>&1
if errorlevel 1 (
    echo [HATA] Gerekli Python kutuphaneleri eksik.
    echo [INFO] Tek seferlik kurulum icin su komutu calistirin:
    echo        python -m pip install seleniumbase curl_cffi beautifulsoup4 requests tmdbsimple
    echo.
    pause
    exit /b 1
)

where chromedriver >nul 2>&1
if errorlevel 1 (
    echo [UYARI] chromedriver PATH uzerinde bulunamadi.
    echo [INFO] Gerekirse tek seferlik su komutu calistirin:
    echo        python -m seleniumbase install chromedriver
    echo [INFO] SeleniumBase yine de yerel driver yonetimi ile devam etmeyi deneyebilir.
    echo.
)

echo [OK] Hazir. Bot baslatiliyor...
echo.

python -X utf8 main_dizi.py
set EXIT_CODE=%ERRORLEVEL%

echo.
echo ========================================================
echo   ISLEM TAMAMLANDI.
echo ========================================================
echo Cikis kodu: %EXIT_CODE%
pause
exit /b %EXIT_CODE%
