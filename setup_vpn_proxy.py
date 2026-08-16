import os
import subprocess
import time
import sys

def main():
    user = os.getenv("SSTP_USER", "").strip()
    password = os.getenv("SSTP_PASS", "").strip()

    print(f"[*] Secret Kontrolu: User uzunluk={len(user)}, Pass uzunluk={len(password)}")
    if not user or not password:
        print("[!] HATA: SSTP_USER veya SSTP_PASS environment degiskeni bos!")
        sys.exit(1)

    print("[*] Peer dosyasi olusturuluyor (/etc/ppp/peers/keenetic)...")
    os.makedirs("/etc/ppp/peers", exist_ok=True)
    peer_config = f"""pty "sstpc --nolaunchpppd --log-level 2 --log-lineno --tls-ext --cert-warn oktay2617.keenetic.link:443"
user "{user}"
password "{password}"
require-mschap-v2
noauth
noipdefault
usepeerdns
nodefaultroute
persist
maxfail 5
debug
dump
logfile /tmp/sstp.log
"""
    with open("/etc/ppp/peers/keenetic", "w", encoding="utf-8") as f:
        f.write(peer_config)
    os.chmod("/etc/ppp/peers/keenetic", 0o600)

    print("[*] SSTP VPN baslatiliyor (pppd call keenetic)...")
    subprocess.Popen(["pppd", "call", "keenetic"])

    print("[*] ppp0 tünel arayuzu bekleniyor...")
    ppp_ip = None
    for i in range(1, 31):
        try:
            out = subprocess.check_output(["ip", "-4", "addr", "show", "ppp0"], text=True, stderr=subprocess.DEVNULL)
            for line in out.splitlines():
                line = line.strip()
                if line.startswith("inet "):
                    ppp_ip = line.split()[1].split("/")[0]
                    break
        except Exception:
            pass

        if ppp_ip:
            print(f"[+] SSTP VPN Baglantisi Basarili! Tunel IP: {ppp_ip}")
            break
        print(f"[*] Baglanti bekleniyor ({i}/30)...")
        time.sleep(2)

    if not ppp_ip:
        print("[!] SSTP baglantisi zaman asimina ugradi. /tmp/sstp.log icerigi:")
        if os.path.exists("/tmp/sstp.log"):
            with open("/tmp/sstp.log", "r", encoding="utf-8", errors="ignore") as f:
                print(f.read())
        sys.exit(1)

    print("[*] Tinyproxy yapilandiriliyor...")
    subprocess.run(["pkill", "tinyproxy"], check=False)
    os.makedirs("/etc/tinyproxy", exist_ok=True)
    tiny_conf = f"""User tinyproxy
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
Bind {ppp_ip}
"""
    with open("/etc/tinyproxy/tinyproxy.conf", "w", encoding="utf-8") as f:
        f.write(tiny_conf)

    subprocess.run(["systemctl", "restart", "tinyproxy"], check=False)
    subprocess.Popen(["tinyproxy", "-c", "/etc/tinyproxy/tinyproxy.conf"])
    time.sleep(3)

    print("[*] Proxy uzerinden cikis testi yapiliyor...")
    test_res = subprocess.run(
        ["curl", "-s", "--max-time", "15", "-x", "http://127.0.0.1:8888", "https://ipinfo.io/json"],
        capture_output=True,
        text=True
    )
    print("=== Ev Interneti Cikis IP Bilgisi ===")
    print(test_res.stdout or test_res.stderr or "IP bilgisi alinamadi")

if __name__ == "__main__":
    main()
