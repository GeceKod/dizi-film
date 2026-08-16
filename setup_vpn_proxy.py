import os
import subprocess
import sys
import time

WG_CONFIG = """[Interface]
PrivateKey = Ypkcs0S9LcpFWJTUt/JoyCEQCLgon0YsB5OlQnsiD9c=
Address = 172.16.82.2/24
Table = off

[Peer]
PublicKey = hovIs5/4QATCJ34/njAuKGTUvrEJGvRMyrqzH9tb4TU=
Endpoint = 95.70.160.57:51820
AllowedIPs = 0.0.0.0/0
PersistentKeepalive = 25
"""

def run_cmd(cmd, check=True):
    """Komutu ekrana basıp çalıştırır; çıktıyı döndürür."""
    print(f"[*] Calistiriliyor: {cmd if isinstance(cmd, str) else ' '.join(cmd)}")
    res = subprocess.run(
        cmd,
        shell=isinstance(cmd, str),
        check=check,
        capture_output=True,
        text=True,
    )
    if res.stdout:
        print(res.stdout.strip())
    if res.stderr:
        print(res.stderr.strip())
    return res


def main():
    print("=" * 50)
    print("[*] Keenetic WireGuard VPN & Split-Tunnel Proxy")
    print("=" * 50)

    # -----------------------------------------------------------------
    # 1️⃣ Sistem ayarları
    # -----------------------------------------------------------------
    run_cmd("sysctl -w net.ipv4.conf.all.rp_filter=0", check=False)
    run_cmd("sysctl -w net.ipv4.conf.default.rp_filter=0", check=False)
    run_cmd("sysctl -w net.ipv4.ip_forward=1", check=False)

    # -----------------------------------------------------------------
    # 2️⃣ WireGuard kurulumu
    # -----------------------------------------------------------------
    os.makedirs("/etc/wireguard", exist_ok=True)
    with open("/etc/wireguard/wg0.conf", "w", encoding="utf-8") as f:
        f.write(WG_CONFIG)
    os.chmod("/etc/wireguard/wg0.conf", 0o600)

    # 2a. Mevcut tüneli kapatıp yeniden aç
    run_cmd(["wg-quick", "down", "wg0"], check=False)
    run_cmd(["wg-quick", "up", "wg0"], check=False)
    run_cmd("sysctl -w net.ipv4.conf.wg0.rp_filter=0", check=False)

    time.sleep(2)
    print("=== WireGuard Arayuz Durumu ===")
    run_cmd(["wg", "show"], check=False)

    # -----------------------------------------------------------------
    # 3️⃣ Modeme ping testi (opsiyonel)
    # -----------------------------------------------------------------
    print("=== Modem Ping Testi (172.16.82.1) ===")
    run_cmd("ping -c 2 -W 2 -I wg0 172.16.82.1", check=False)

    # -----------------------------------------------------------------
    # 4️⃣ Policy‑routing (split‑tunnel) – tabloyu oluştur ve kural ekle
    # -----------------------------------------------------------------
    print("=== Policy Routing (Split‑Tunnel) ===")
    # `ip route add … table 200` komutu tabloyu otomatik yaratır
    run_cmd("ip route add default dev wg0 table 200", check=False)
    run_cmd("ip rule add from 172.16.82.2 table 200", check=False)

    # -----------------------------------------------------------------
    # 5️⃣ Proxy başlatma
    # -----------------------------------------------------------------
    print("[*] Port 8888 temizleniyor ve proxy_server.py (wg0 / 172.16.82.2) başlatılıyor...")
    run_cmd("fuser -k 8888/tcp || true", check=False)
    run_cmd("pkill -f proxy_server.py || true", check=False)

    # Ortam değişkenleri
    env = os.environ.copy()
    env["PROXY_IFACE"] = "wg0"
    env["PROXY_BIND_IP"] = "172.16.82.2"

    # -----------------------------------------------------------------
    # DNS resolver (opsiyonel, yazma hatasını sessizce yoksay)
    # -----------------------------------------------------------------
    try:
        with open("/etc/resolv.conf", "a", encoding="utf-8") as resolv:
            resolv.write("\nnameserver 8.8.8.8\nnameserver 1.1.1.1\n")
    except Exception as e:
        print(f"[!] resolv.conf yazilirken hata (yoksayılıyor): {e}")

    # Proxy log dosyası
    proxy_log_path = "/tmp/proxy.log"
    proxy_log = open(proxy_log_path, "w")

    # Proxy sürecini arka planda başlat
    proc = subprocess.Popen(
        [sys.executable, "proxy_server.py"],
        stdout=proxy_log,
        stderr=proxy_log,
        env=env,
    )
    time.sleep(3)                     # proxy’nin tam başlaması için bekle
    if proc.poll() is not None:
        print("[!] proxy_server.py hemen çöktü – logu aşağıda gösteriyorum")
        try:
            with open(proxy_log_path) as f:
                print(f.read())
        except Exception as e:
            print(f"Log okunamadı: {e}")

    # Kontrol: 8888 portu dinleniyor mu?
    run_cmd("ss -ltnp | grep 8888 || true", check=False)

    # -----------------------------------------------------------------
    # 6️⃣ Ev‑IP çıkış testi (curl -v ile daha detaylı)
    # -----------------------------------------------------------------
    print("[*] Proxy üzerinden ev interneti çıkış testi yapılıyor...")
    test_res = run_cmd(
        "curl -v -s --max-time 15 -x http://127.0.0.1:8888 https://ipinfo.io/json",
        check=False,
    )
    print("=" * 50)
    print("=== Ev Interneti (Static IP) Çıkış Bilgisi ===")
    # stderr (verbose) de olası hata mesajları bulunur
    print(test_res.stderr or test_res.stdout or "IP bilgisi alinamadi")
    print("=" * 50)


if __name__ == "__main__":
    main()
