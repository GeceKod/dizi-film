import os
import select
import socket
import sys
import threading
from urllib.parse import urlparse

BIND_IP = "172.16.82.2"
PROXY_HOST = "127.0.0.1"
PROXY_PORT = 8888
SO_BINDTODEVICE = 25


def log(msg):
    sys.stderr.write(f"[PROXY] {msg}\n")
    sys.stderr.flush()


def forward(src, dst):
    try:
        while True:
            r, _, _ = select.select([src], [], [], 60)
            if not r:
                break
            data = src.recv(8192)
            if not data:
                break
            dst.sendall(data)
    except Exception:
        pass
    finally:
        try:
            src.close()
        except Exception:
            pass
        try:
            dst.close()
        except Exception:
            pass


def create_bound_socket():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.setsockopt(socket.SOL_SOCKET, SO_BINDTODEVICE, b"wg0\0")
    except Exception as e:
        log(f"SO_BINDTODEVICE hatasi: {e}")
    try:
        sock.bind((BIND_IP, 0))
    except Exception as e:
        log(f"bind hatasi ({BIND_IP}): {e}")
    sock.settimeout(15)
    return sock


def handle_client(client_sock):
    try:
        req_data = client_sock.recv(8192)
        if not req_data:
            client_sock.close()
            return

        lines = req_data.split(b"\r\n")
        first_line = lines[0].decode("latin1", errors="ignore")
        parts = first_line.split(" ")
        if len(parts) < 2:
            client_sock.close()
            return

        method = parts[0].upper()
        target = parts[1]
        log(f"Istek geldi: {method} {target}")

        if method == "CONNECT":
            # HTTPS Tunnel
            if ":" in target:
                host, port_str = target.split(":", 1)
                port = int(port_str)
            else:
                host, port = target, 443

            log(f"Hedefe baglaniliyor (CONNECT): {host}:{port} dev wg0...")
            remote_sock = create_bound_socket()
            remote_sock.connect((host, port))
            log(f"Hedefe baglandi: {host}:{port}")
            client_sock.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")

            t1 = threading.Thread(target=forward, args=(client_sock, remote_sock), daemon=True)
            t2 = threading.Thread(target=forward, args=(remote_sock, client_sock), daemon=True)
            t1.start()
            t2.start()
            t1.join()
            t2.join()
        else:
            # Plain HTTP
            parsed = urlparse(target if target.startswith("http") else f"http://{target}")
            host = parsed.hostname
            port = parsed.port or 80
            path = parsed.path or "/"
            if parsed.query:
                path += f"?{parsed.query}"

            log(f"Hedefe baglaniliyor (HTTP): {host}:{port} dev wg0...")
            remote_sock = create_bound_socket()
            remote_sock.connect((host, port))
            log(f"Hedefe baglandi: {host}:{port}")

            modified_req = f"{method} {path} HTTP/1.1\r\n".encode("latin1") + b"\r\n".join(lines[1:])
            remote_sock.sendall(modified_req)

            t1 = threading.Thread(target=forward, args=(client_sock, remote_sock), daemon=True)
            t2 = threading.Thread(target=forward, args=(remote_sock, client_sock), daemon=True)
            t1.start()
            t2.start()
            t1.join()
            t2.join()
    except Exception as exc:
        log(f"Baglanti hatasi: {exc}")
        try:
            client_sock.close()
        except Exception:
            pass


def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((PROXY_HOST, PROXY_PORT))
    server.listen(128)
    log(f"Proxy Server aktif: {PROXY_HOST}:{PROXY_PORT} -> wg0 ({BIND_IP})")
    while True:
        client_sock, _ = server.accept()
        t = threading.Thread(target=handle_client, args=(client_sock,), daemon=True)
        t.start()


if __name__ == "__main__":
    main()
