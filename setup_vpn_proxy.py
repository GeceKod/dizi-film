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
    print("[*] Keenetic WireGuard VPN & Split-Tunnel Proxy")
    print("==================================================")

    # 1. Linux sysctl ayarlarini yap
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

    # 6. Port 8888 temizligi ve Proxy baslatma
    print("[*] Port 8888 temizleniyor ve proxy_server.py (wg0 / 172.16.82.2) baslatiliyor...")
    run_cmd("fuser -k 8888/tcp || true", check=False)
    run_cmd("pkill -f proxy_server.py || true", check=False)
    time.sleep(1)

    env = os.environ.copy()
    env["PROXY_IFACE"] = "wg0"
    env["PROXY_BIND_IP"] = "172.16.82.2"

    # Write DNS resolver for tunnel
    try:
        with open("/etc/resolv.conf", "w") as resolv:
            resolv.write("nameserver 8.8.8.8\n")
    except Exception as e:
        print(f"[!] resolv.conf error: {e}")

    proxy_log = open("/tmp/proxy.log", "w")
    subprocess.Popen([sys.executable, "proxy_server.py"], stdout=proxy_log, stderr=proxy_log, env=env)
    time.sleep(5)

    # 7. Ev IP'si Cikis Testi
    print("[*] Proxy uzerinden ev interneti cikis testi yapiliyor...")
    test_res = run_cmd("curl -s --max-time 15 -x http://127.0.0.1:8888 https://ipinfo.io/json", check=False)

    print("==================================================")
    print("=== Ev Interneti (Static IP) Cikis Bilgisi ===")
    print(test_res.stdout or test_res.stderr or "IP bilgisi alinamadi")
    print("==================================================")


if __name__ == "__main__":
    main()
