import os
import subprocess
import time
import sys

WG_CONFIG = """[Interface]
PrivateKey = Ypkcs0S9LcpFWJTUt/JoyCEQCLgon0YsB5OlQnsiD9c=
Address = 172.16.82.2/32
Table = off

[Peer]
PublicKey = hovIs5/4QATCJ34/njAuKGTUvrEJGvRMyrqzH9tb4TU=
Endpoint = 95.70.160.57:51820
AllowedIPs = 0.0.0.0/0
PersistentKeepalive = 25
"""

def run_cmd(cmd, check=True):
    print(f"[*] Calistiriliyor: {' '.join(cmd) if isinstance(cmd, list) else cmd}")
    return subprocess.run(cmd, shell=isinstance(cmd, str), check=check, capture_output=True, text=True)

def main():
    print("==================================================")
    print("[*] WireGuard VPN & Split-Tunnel Proxy Baslatiliyor")
    print("==================================================")

    # 1. WireGuard dosyasini olustur
    os.makedirs("/etc/wireguard", exist_ok=True)
    with open("/etc/wireguard/wg0.conf", "w", encoding="utf-8") as f:
        f.write(WG_CONFIG)
    os.chmod("/etc/wireguard/wg0.conf", 0o600)

    # 2. WireGuard tünelini ac
    run_cmd(["wg-quick", "down", "wg0"], check=False)
    wg_up = run_cmd(["wg-quick", "up", "wg0"], check=False)
    print(wg_up.stdout or wg_up.stderr)

    time.sleep(2)
    show_res = run_cmd(["wg", "show"], check=False)
    print("=== WireGuard Arayuz Durumu ===")
    print(show_res.stdout or show_res.stderr)

    # 3. Policy Routing (Split Tunnel) yapilandir
    # Runner baglantisi kopmasin diye sadece 172.16.82.2 kaynakli paketler wg0'a gitsin
    run_cmd("ip route flush table 200", check=False)
    run_cmd("ip rule del from 172.16.82.2 table 200", check=False)
    run_cmd("ip route add default dev wg0 table 200", check=False)
    run_cmd("ip rule add from 172.16.82.2 table 200", check=False)

    # 4. Tinyproxy'yi 172.16.82.2 uzerinden cikis yapacak sekilde ayarla
    print("[*] Tinyproxy yapilandiriliyor...")
    run_cmd(["pkill", "tinyproxy"], check=False)
    os.makedirs("/etc/tinyproxy", exist_ok=True)
    tiny_conf = """User tinyproxy
Group tinyproxy
Port 8888
Listen 127.0.0.1
Timeout 600
DefaultErrorFile "/usr/share/tinyproxy/default.html"
StatFile "/usr/share/tinyproxy/stats.html"
LogFile "/var/log/tinyproxy/tinyproxy.log"
LogLevel Info
PidFile "/run/tinyproxy/tinyproxy.pid"
MaxClients 100
MinSpareServers 5
MaxSpareServers 20
StartServers 10
MaxRequestsPerChild 0
Allow 127.0.0.1
ViaProxyName "tinyproxy"
Bind 172.16.82.2
"""
    with open("/etc/tinyproxy/tinyproxy.conf", "w", encoding="utf-8") as f:
        f.write(tiny_conf)

    run_cmd(["systemctl", "restart", "tinyproxy"], check=False)
    subprocess.Popen(["tinyproxy", "-c", "/etc/tinyproxy/tinyproxy.conf"])
    time.sleep(3)

    # 5. Ev IP'si Cikis Testi
    print("[*] Proxy uzerinden cikis testi yapiliyor...")
    test_res = run_cmd("curl -s --max-time 15 -x http://127.0.0.1:8888 https://ipinfo.io/json", check=False)
    print("==================================================")
    print("=== Ev Interneti (Static IP) Cikis Bilgisi ===")
    print(test_res.stdout or test_res.stderr or "IP bilgisi alinamadi")
    print("==================================================")

if __name__ == "__main__":
    main()
