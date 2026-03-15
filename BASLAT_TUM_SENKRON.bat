@echo off
title Gunes TV Tam Senkron - Calistirici
color 0E

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

echo [1/3] Dizi senkronu baslatiliyor...
python -X utf8 main_dizi.py
set EXIT_CODE=%ERRORLEVEL%
if not "%EXIT_CODE%"=="0" goto :end

echo.
echo [2/3] Film senkronu baslatiliyor...
python -X utf8 main_film.py
set EXIT_CODE=%ERRORLEVEL%
if not "%EXIT_CODE%"=="0" goto :end

echo.
echo [3/3] Birlestirme baslatiliyor...
python -X utf8 json_birlestir.py
set EXIT_CODE=%ERRORLEVEL%

:end
echo.
echo ========================================================
echo   ISLEM TAMAMLANDI.
echo ========================================================
echo Cikis kodu: %EXIT_CODE%
pause
exit /b %EXIT_CODE%
