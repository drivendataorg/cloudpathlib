from datetime import datetime
from functools import partial
from http.server import HTTPServer, SimpleHTTPRequestHandler
import os
from pathlib import Path
import shutil
import ssl
import threading
import time
from urllib.request import urlopen

from pytest import fixture


utilities_dir = Path(__file__).parent / "utilities"


class TestHTTPRequestHandler(SimpleHTTPRequestHandler):
    """Also allows PUT and DELETE requests for testing."""

    def do_PUT(self):
        length = int(self.headers["Content-Length"])
        path = Path(self.translate_path(self.path))

        if path.is_dir():
            path.mkdir(parents=True, exist_ok=True)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)

        with path.open("wb") as f:
            f.write(self.rfile.read(length))

        now = datetime.now().timestamp()
        os.utime(path, (now, now))

        self.send_response(201)
        self.end_headers()

    def do_DELETE(self):
        path = Path(self.translate_path(self.path))

        try:
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()
            self.send_response(204)
        except FileNotFoundError:
            self.send_response(404)

        self.end_headers()


def _http_server(
    root_dir, port, hostname="localhost", use_ssl=False, certfile=None, keyfile=None, threaded=True
):
    root_dir.mkdir(exist_ok=True)

    scheme = "http" if not use_ssl else "https"

    def start_server():
        handler = partial(TestHTTPRequestHandler, directory=str(root_dir))
        httpd = HTTPServer((hostname, port), handler)

        if use_ssl:
            if not certfile or not keyfile:
                raise ValueError("certfile and keyfile must be provided if `ssl=True`")

            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(certfile=certfile, keyfile=keyfile)
            context.check_hostname = False
            httpd.socket = context.wrap_socket(httpd.socket, server_side=True)

        httpd.serve_forever()

    if threaded:
        server_thread = threading.Thread(target=start_server, daemon=True)
        server_thread.start()

    else:
        start_server()

    # Wait for the server to start
    for _ in range(10):
        try:
            if use_ssl:
                req_context = ssl.SSLContext()
                req_context.check_hostname = False
                req_context.verify_mode = ssl.CERT_NONE
            else:
                req_context = None

            urlopen(f"{scheme}://{hostname}:{port}", context=req_context)

            break
        except Exception:
            time.sleep(0.1)

    return f"{scheme}://{hostname}:{port}", server_thread


@fixture(scope="module")
def http_server(tmp_path_factory, worker_id):
    port = 9077 + (
        int(worker_id.lstrip("gw")) if worker_id != "master" else 0
    )  # don't collide if tests running in parallel with multiple servers

    server_dir = tmp_path_factory.mktemp("server_files").resolve()

    host, server_thread = _http_server(server_dir, port)

    yield host, server_dir

    server_thread.join(0)

    if server_dir.exists():
        shutil.rmtree(server_dir)


@fixture(scope="module")
def https_server(tmp_path_factory, worker_id):
    port = 4443 + (
        int(worker_id.lstrip("gw")) if worker_id != "master" else 0
    )  # don't collide if tests running in parallel with multiple servers

    server_dir = tmp_path_factory.mktemp("server_files").resolve()

    host, server_thread = _http_server(
        server_dir,
        port,
        use_ssl=True,
        certfile=utilities_dir / "insecure-test.pem",
        keyfile=utilities_dir / "insecure-test.key",
    )

    yield host, server_dir

    server_thread.join(0)

    if server_dir.exists():
        shutil.rmtree(server_dir)
