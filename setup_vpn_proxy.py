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
    print(f"[*] Calistiriliyor: {' '.join(cmd) if isinstance(cmd, list) else cmd}")
    res = subprocess.run(cmd, shell=isinstance(cmd, str), check=check, capture_output=True, text=True)
    if res.stdout:
        print(res.stdout.strip())
    if res.stderr:
        print(res.stderr.strip())
    return res


def main():
    print("==================================================")
    print("[*] WireGuard VPN & Split-Tunnel Proxy Baslatiliyor")
    print("==================================================")

    # 1. Linux sysctl ayarlarini yap (Reverse path filtering kapat)
    run_cmd("sysctl -w net.ipv4.conf.all.rp_filter=0", check=False)
    run_cmd("sysctl -w net.ipv4.conf.default.rp_filter=0", check=False)
    run_cmd("sysctl -w net.ipv4.ip_forward=1", check=False)

    # 2. WireGuard dosyasini olustur
    os.makedirs("/etc/wireguard", exist_ok=True)
    with open("/etc/wireguard/wg0.conf", "w", encoding="utf-8") as f:
        f.write(WG_CONFIG)
    os.chmod("/etc/wireguard/wg0.conf", 0o600)

    # 3. WireGuard tünelini ac
    run_cmd(["wg-quick", "down", "wg0"], check=False)
    run_cmd(["wg-quick", "up", "wg0"], check=False)
    run_cmd("sysctl -w net.ipv4.conf.wg0.rp_filter=0", check=False)

    time.sleep(2)
    print("=== WireGuard Arayuz Durumu ===")
    run_cmd(["wg", "show"], check=False)

    # 4. Modeme Ping Testi
    print("=== Modem Ping Testi (172.16.82.1) ===")
    run_cmd("ping -c 2 -W 2 -I wg0 172.16.82.1", check=False)

    # 5. Policy Routing (Split Tunnel) yapilandir
    run_cmd("ip route flush table 200", check=False)
    run_cmd("ip rule del from 172.16.82.2 table 200", check=False)
    run_cmd("ip route add default dev wg0 table 200", check=False)
    run_cmd("ip rule add from 172.16.82.2 table 200", check=False)

    print("=== IP Rules ve Route Table 200 ===")
    run_cmd("ip rule show", check=False)
    run_cmd("ip route show table 200", check=False)

    # 6. Dogrudan wg0 arayuzunden Curl Testi
    print("=== Dogrudan wg0 Curl Testi ===")
    run_cmd("curl -s --max-time 10 --interface wg0 https://ipinfo.io/json", check=False)

    # 7. Proxy Sunucusunu Root Yetkisiyle Arka Planda Baslat
    print("[*] proxy_server.py arka planda baslatiliyor...")
    proxy_log = open("/tmp/proxy.log", "w")
    subprocess.Popen([sys.executable, "proxy_server.py"], stdout=proxy_log, stderr=proxy_log)
    time.sleep(2)

    # 8. Ev IP'si Proxy Cikis Testi
    print("[*] Proxy uzerinden ev interneti cikis testi yapiliyor...")
    test_res = run_cmd("curl -s --max-time 15 -x http://127.0.0.1:8888 https://ipinfo.io/json", check=False)
    
    print("=== Proxy Loglari (/tmp/proxy.log) ===")
    if os.path.exists("/tmp/proxy.log"):
        with open("/tmp/proxy.log", "r") as f:
            print(f.read())

    print("==================================================")
    print("=== Ev Interneti (Static IP) Cikis Bilgisi ===")
    print(test_res.stdout or test_res.stderr or "IP bilgisi alinamadi")
    print("==================================================")


if __name__ == "__main__":
    main()
