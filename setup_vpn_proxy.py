import os
import subprocess
import sys
import time


def run_cmd(cmd, check=True):
    print(f"[*] Calistiriliyor: {' '.join(cmd) if isinstance(cmd, list) else cmd}")
    res = subprocess.run(cmd, shell=isinstance(cmd, str), check=check, capture_output=True, text=True)
    if res.stdout:
        print(res.stdout.strip())
    if res.stderr:
        print(res.stderr.strip())
    return res


def get_interface_ip(ifname):
    try:
        out = subprocess.check_output(["ip", "-4", "addr", "show", ifname], text=True, stderr=subprocess.DEVNULL)
        for line in out.splitlines():
            line = line.strip()
            if line.startswith("inet "):
                return line.split()[1].split("/")[0]
    except Exception:
        pass
    return None


def main():
    print("==================================================")
    print("[*] Keenetic SSTP VPN & Split-Tunnel Proxy")
    print("==================================================")

    # 1. Linux sysctl ayarlarini yap
    run_cmd("sysctl -w net.ipv4.conf.all.rp_filter=0", check=False)
    run_cmd("sysctl -w net.ipv4.conf.default.rp_filter=0", check=False)
    run_cmd("sysctl -w net.ipv4.ip_forward=1", check=False)

    # 2. Hosts dosyasina SNI icin domain-IP eslesmesi ekle
    with open("/etc/hosts", "a") as f:
        f.write("\n95.70.160.57 oktay2617.keenetic.link\n")

    user = os.getenv("SSTP_USER", "github").strip()
    password = os.getenv("SSTP_PASS", "").strip()

    if not user or not password:
        print("[!] HATA: SSTP_USER veya SSTP_PASS bulunamadi!")
        sys.exit(1)

    # Onceki pppd ve sstpc kaliplarini temizle
    run_cmd("pkill -9 pppd || true", check=False)
    run_cmd("pkill -9 sstpc || true", check=False)
    time.sleep(1)

    print("[*] SSTP VPN baslatiliyor (oktay2617.keenetic.link:443)...")
    sstp_cmd = [
        "sstpc",
        "--log-level", "2",
        "--log-lineno",
        "--tls-ext",
        "--cert-warn",
        "--user", user,
        "--password", password,
        "oktay2617.keenetic.link:443",
        "--",
        "require-mschap-v2",
        "refuse-pap",
        "refuse-eap",
        "refuse-mschap",
        "noauth",
        "noipdefault",
        "nodefaultroute",
        "usepeerdns",
        "name", user,
        "logfile", "/tmp/sstp.log",
        "debug",
        "dump",
    ]

    subprocess.Popen(sstp_cmd)

    ppp_ip = None
    for i in range(1, 30):
        ppp_ip = get_interface_ip("ppp0")
        if ppp_ip:
            print(f"[+] SSTP VPN Baglantisi Basarili! ppp0 IP: {ppp_ip}")
            break
        print(f"[*] SSTP bekleniyor ({i}/30)...")
        time.sleep(2)

    if not ppp_ip:
        print("[!] SSTP baglantisi kurulamadi. /tmp/sstp.log icerigi:")
        if os.path.exists("/tmp/sstp.log"):
            with open("/tmp/sstp.log", "r") as f:
                print(f.read())
        sys.exit(1)

    run_cmd("sysctl -w net.ipv4.conf.ppp0.rp_filter=0", check=False)

    # 3. Policy Routing (Split Tunnel) yapilandir
    run_cmd("ip route flush table 200", check=False)
    run_cmd(f"ip rule del from {ppp_ip} table 200", check=False)
    run_cmd("ip route add default dev ppp0 table 200", check=False)
    run_cmd(f"ip rule add from {ppp_ip} table 200", check=False)

    # 4. Port 8888 temizligi ve Proxy baslatma
    print(f"[*] Port 8888 temizleniyor ve proxy_server.py (ppp0 / {ppp_ip}) baslatiliyor...")
    run_cmd("fuser -k 8888/tcp || true", check=False)
    run_cmd("pkill -f proxy_server.py || true", check=False)
    time.sleep(1)

    env = os.environ.copy()
    env["PROXY_IFACE"] = "ppp0"
    env["PROXY_BIND_IP"] = ppp_ip

    proxy_log = open("/tmp/proxy.log", "w")
    subprocess.Popen([sys.executable, "proxy_server.py"], stdout=proxy_log, stderr=proxy_log, env=env)
    time.sleep(2)

    # 5. Ev IP'si Cikis Testi
    print("[*] Proxy uzerinden ev interneti cikis testi yapiliyor...")
    test_res = run_cmd("curl -s --max-time 15 -x http://127.0.0.1:8888 https://ipinfo.io/json", check=False)

    print("==================================================")
    print("=== Ev Interneti (Static IP) Cikis Bilgisi ===")
    print(test_res.stdout or test_res.stderr or "IP bilgisi alinamadi")
    print("==================================================")


if __name__ == "__main__":
    main()
