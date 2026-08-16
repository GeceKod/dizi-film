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


def connect_sstp(user, password, host="95.70.160.57:443"):
    print(f"[*] SSTP VPN yapilandiriliyor ({host})...")
    os.makedirs("/etc/ppp/peers", exist_ok=True)
    peer_config = f"""pty "sstpc --nolaunchpppd --log-level 2 --log-lineno --tls-ext --cert-warn {host}"
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

    print("[*] SSTP baglantisi baslatiliyor (pppd call keenetic)...")
    subprocess.Popen(["pppd", "call", "keenetic"])

    for i in range(1, 20):
        ip = get_interface_ip("ppp0")
        if ip:
            print(f"[+] SSTP baglantisi basarili! ppp0 IP: {ip}")
            return "ppp0", ip
        time.sleep(2)
    return None, None


def main():
    print("==================================================")
    print("[*] Keenetic VPN & Split-Tunnel Proxy Baslatiliyor")
    print("==================================================")

    # 1. Linux sysctl ayarlarini yap
    run_cmd("sysctl -w net.ipv4.conf.all.rp_filter=0", check=False)
    run_cmd("sysctl -w net.ipv4.conf.default.rp_filter=0", check=False)
    run_cmd("sysctl -w net.ipv4.ip_forward=1", check=False)

    user = os.getenv("SSTP_USER", "github").strip()
    password = os.getenv("SSTP_PASS", "").strip()

    iface = None
    bind_ip = None

    if user and password:
        iface, bind_ip = connect_sstp(user, password, host="95.70.160.57:443")
        if not iface:
            print("[!] 95.70.160.57:443 ile baglanti alinamadi, KeenDNS deneniyor...")
            iface, bind_ip = connect_sstp(user, password, host="oktay2617.keenetic.link:443")

    if not iface:
        print("[!] SSTP baglantisi kurulamadi. /tmp/sstp.log icerigi:")
        if os.path.exists("/tmp/sstp.log"):
            with open("/tmp/sstp.log", "r") as f:
                print(f.read())
        sys.exit(1)

    run_cmd(f"sysctl -w net.ipv4.conf.{iface}.rp_filter=0", check=False)

    # 2. Policy Routing (Split Tunnel) yapilandir
    run_cmd("ip route flush table 200", check=False)
    run_cmd(f"ip rule del from {bind_ip} table 200", check=False)
    run_cmd(f"ip route add default dev {iface} table 200", check=False)
    run_cmd(f"ip rule add from {bind_ip} table 200", check=False)

    # 3. Port 8888 temizligi ve Proxy baslatma
    print(f"[*] Port 8888 temizleniyor ve proxy_server.py ({iface} / {bind_ip}) baslatiliyor...")
    run_cmd("fuser -k 8888/tcp || true", check=False)
    run_cmd("pkill -f proxy_server.py || true", check=False)
    run_cmd("pkill -f tinyproxy || true", check=False)
    time.sleep(1)

    env = os.environ.copy()
    env["PROXY_IFACE"] = iface
    env["PROXY_BIND_IP"] = bind_ip

    proxy_log = open("/tmp/proxy.log", "w")
    subprocess.Popen([sys.executable, "proxy_server.py"], stdout=proxy_log, stderr=proxy_log, env=env)
    time.sleep(2)

    # 4. Ev IP'si Cikis Testi
    print("[*] Proxy uzerinden ev interneti cikis testi yapiliyor...")
    test_res = run_cmd("curl -s --max-time 15 -x http://127.0.0.1:8888 https://ipinfo.io/json", check=False)

    print("==================================================")
    print("=== Ev Interneti (Static IP) Cikis Bilgisi ===")
    print(test_res.stdout or test_res.stderr or "IP bilgisi alinamadi")
    print("==================================================")


if __name__ == "__main__":
    main()
