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
    proxy_log = open("/tmp/proxy.log", "w")
    subprocess.Popen([sys.executable, "proxy_server.py"], stdout=proxy_log, stderr=proxy_log, env=env)
    time.sleep(2)
    # 7. Ev IP'si Cikis Testi
    print("[*] Proxy uzerinden ev interneti cikis testi yapiliyor...")
    test_res = run_cmd("curl -s --max-time 15 -x http://127.0.0.1:8888 https://ipinfo.io/json", check=False)
    print("==================================================")
    print("=== Ev Interneti (Static IP) Cikis Bilgisi ===")
    print(test_res.stdout or test_res.stderr or "IP bilgisi alinamadi")
    print("==================================================")
if __name__ == "__main__":
    main()
