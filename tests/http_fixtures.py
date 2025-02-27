from datetime import datetime
from functools import partial
from http.server import HTTPServer, SimpleHTTPRequestHandler
import os
from pathlib import Path
import random
import shutil
import ssl
import threading
import time
from urllib.request import urlopen

from pytest import fixture
from tenacity import retry, stop_after_attempt, wait_fixed

from .utils import _sync_filesystem

utilities_dir = Path(__file__).parent / "utilities"


class TestHTTPRequestHandler(SimpleHTTPRequestHandler):
    """Also allows PUT and DELETE requests for testing."""

    @retry(stop=stop_after_attempt(5), wait=wait_fixed(0.1))
    def do_PUT(self):
        length = int(self.headers["Content-Length"])
        path = Path(self.translate_path(self.path))

        if path.is_dir():
            path.mkdir(parents=True, exist_ok=True)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)

        _sync_filesystem()

        with path.open("wb") as f:
            f.write(self.rfile.read(length))

            # Ensure the file is flushed and synced to disk before returning
            # The perf hit is ok here since this is a test server
            f.flush()
            os.fsync(f.fileno())

        now = datetime.now().timestamp()
        os.utime(path, (now, now))

        self.send_response(201)
        self.end_headers()

    @retry(stop=stop_after_attempt(5), wait=wait_fixed(0.1))
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

    @retry(stop=stop_after_attempt(5), wait=wait_fixed(0.1))
    def do_POST(self):
        # roundtrip any posted JSON data for testing
        content_length = int(self.headers["Content-Length"])
        post_data = self.rfile.read(content_length)
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.send_header("Content-Length", self.headers["Content-Length"])
        self.end_headers()
        self.wfile.write(post_data)

    @retry(stop=stop_after_attempt(5), wait=wait_fixed(0.1))
    def do_GET(self):
        super().do_GET()

    @retry(stop=stop_after_attempt(5), wait=wait_fixed(0.1))
    def do_HEAD(self):
        super().do_HEAD()


def _http_server(
    root_dir, port, hostname="localhost", use_ssl=False, certfile=None, keyfile=None, threaded=True
):
    root_dir.mkdir(exist_ok=True)

    scheme = "http" if not use_ssl else "https"

    def start_server():
        handler = partial(TestHTTPRequestHandler, directory=str(root_dir))

        try:
            httpd = HTTPServer((hostname, port), handler)
        except OSError as e:
            if e.errno == 48:
                httpd = HTTPServer(
                    (hostname, port + random.randint(0, 10000)), handler
                )  # somtimes the same worker collides before port is released; retry
            else:
                raise e

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

    max_attempts = 100
    wait_time = 0.2

    for attempt in range(max_attempts):
        try:
            if use_ssl:
                req_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                req_context.check_hostname = False
                req_context.verify_mode = ssl.CERT_NONE
            else:
                req_context = None

            with urlopen(
                f"{scheme}://{hostname}:{port}", context=req_context, timeout=1.0
            ) as response:
                if response.status == 200:
                    break
        except Exception:
            if attempt == max_attempts - 1:
                raise RuntimeError(f"Server failed to start after {max_attempts} attempts")
            time.sleep(wait_time)

    return f"{scheme}://{hostname}:{port}", server_thread


@fixture(scope="module")
def http_server(tmp_path_factory, worker_id):
    port = (
        9077
        + random.randint(0, 10000)
        + (int(worker_id.lstrip("gw")) if worker_id != "master" else 0)
    )  # don't collide if tests running in parallel with multiple servers

    server_dir = tmp_path_factory.mktemp("server_files").resolve()

    host, server_thread = _http_server(server_dir, port)

    yield host, server_dir

    server_thread.join(0)

    if server_dir.exists():
        shutil.rmtree(server_dir)


@fixture(scope="module")
def https_server(tmp_path_factory, worker_id):
    port = (
        4443
        + random.randint(0, 10000)
        + (int(worker_id.lstrip("gw")) if worker_id != "master" else 0)
    )  # don't collide if tests running in parallel with multiple servers

    server_dir = tmp_path_factory.mktemp("server_files").resolve()

    # Command for generating self-signed localhost cert
    # openssl req -x509 -out localhost.crt -keyout localhost.key \
    #   -newkey rsa:2048 -nodes -sha256 \
    #   -subj '/CN=localhost' -extensions EXT -config <( \
    #    printf "[dn]\nCN=localhost\n[req]\ndistinguished_name = dn\n[EXT]\nsubjectAltName=DNS:localhost\nkeyUsage=digitalSignature\nextendedKeyUsage=serverAuth")
    #
    # openssl x509 -in localhost.crt -out localhost.pem -outform PEM

    host, server_thread = _http_server(
        server_dir,
        port,
        use_ssl=True,
        certfile=utilities_dir / "insecure-test.pem",
        keyfile=utilities_dir / "insecure-test.key",
    )

    # Add this self-signed cert at the library level so it is used in tests
    _original_create_context = ssl._create_default_https_context

    def _create_context_with_self_signed_cert(*args, **kwargs):
        context = _original_create_context(*args, **kwargs)
        context.load_cert_chain(
            certfile=utilities_dir / "insecure-test.pem",
            keyfile=utilities_dir / "insecure-test.key",
        )
        context.load_verify_locations(cafile=utilities_dir / "insecure-test.pem")
        return context

    ssl._create_default_https_context = _create_context_with_self_signed_cert

    yield host, server_dir

    ssl._create_default_https_context = _original_create_context

    server_thread.join(0)

    if server_dir.exists():
        shutil.rmtree(server_dir)
