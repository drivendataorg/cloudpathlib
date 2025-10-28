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
import socket

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
        """Handle GET requests with optional Range header support."""
        # Check if this is a range request
        range_header = self.headers.get("Range")
        if range_header:
            self._handle_range_request(range_header)
        else:
            super().do_GET()

    def _handle_range_request(self, range_header):
        """Handle Range requests for partial content."""
        path = Path(self.translate_path(self.path))

        if not path.exists() or not path.is_file():
            self.send_error(404, "File not found")
            return

        # Parse the Range header (format: "bytes=start-end")
        try:
            range_spec = range_header.replace("bytes=", "").strip()
            parts = range_spec.split("-")
            start = int(parts[0]) if parts[0] else 0

            file_size = path.stat().st_size

            # Handle end byte
            if len(parts) > 1 and parts[1]:
                end = int(parts[1])
            else:
                end = file_size - 1

            # Validate range
            if start < 0 or end >= file_size or start > end:
                self.send_error(416, "Requested Range Not Satisfiable")
                self.send_header("Content-Range", f"bytes */{file_size}")
                self.end_headers()
                return

            # Read the requested range
            with path.open("rb") as f:
                f.seek(start)
                content = f.read(end - start + 1)

            # Send partial content response
            self.send_response(206)  # Partial Content
            self.send_header("Content-Type", self.guess_type(str(path)))
            self.send_header("Content-Length", str(len(content)))
            self.send_header("Content-Range", f"bytes {start}-{end}/{file_size}")
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()
            self.wfile.write(content)

        except (ValueError, IndexError) as e:
            self.send_error(400, f"Bad Range header: {e}")

    @retry(stop=stop_after_attempt(5), wait=wait_fixed(0.1))
    def do_HEAD(self):
        super().do_HEAD()


def _http_server(
    root_dir,
    port=None,
    hostname="127.0.0.1",
    use_ssl=False,
    certfile=None,
    keyfile=None,
    threaded=True,
):
    root_dir.mkdir(exist_ok=True)

    scheme = "http" if not use_ssl else "https"

    # Find a free port if not specified
    if port is None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((hostname, 0))
            port = s.getsockname()[1]

    def start_server(server_ready_event):
        handler = partial(TestHTTPRequestHandler, directory=str(root_dir))
        httpd = HTTPServer((hostname, port), handler)

        if use_ssl:
            if not certfile or not keyfile:
                raise ValueError("certfile and keyfile must be provided if `ssl=True`")

            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(certfile=certfile, keyfile=keyfile)
            context.check_hostname = False
            httpd.socket = context.wrap_socket(httpd.socket, server_side=True)

        server_ready_event.set()
        httpd.serve_forever()

    server_ready_event = threading.Event()
    if threaded:
        server_thread = threading.Thread(
            target=start_server, args=(server_ready_event,), daemon=True
        )
        server_thread.start()
        server_ready_event.wait()
    else:
        start_server(server_ready_event)

    # Wait for server to be ready to accept connections
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
    # port is now None, so OS will pick a free port
    port = None
    server_dir = tmp_path_factory.mktemp("server_files").resolve()
    host, server_thread = _http_server(server_dir, port)
    yield host, server_dir
    server_thread.join(0)
    if server_dir.exists():
        shutil.rmtree(server_dir)


@fixture(scope="module")
def https_server(tmp_path_factory, worker_id):
    port = None
    server_dir = tmp_path_factory.mktemp("server_files").resolve()

    # # Self‑signed cert for 127.0.0.1 (≈273 years validity)
    # openssl req -x509 -out 127.0.0.1.crt -keyout 127.0.0.1.key \
    #   -newkey rsa:2048 -nodes -sha256 -days 99999 \
    #   -subj '/CN=127.0.0.1' \
    #   -extensions EXT -config <( \
    #       printf "[dn]\nCN=127.0.0.1\n\
    # [req]\ndistinguished_name = dn\n\
    # [EXT]\nsubjectAltName=IP:127.0.0.1\n\
    # keyUsage=digitalSignature\nextendedKeyUsage=serverAuth" )
    # # Convert to PEM (optional)
    # openssl x509 -in 127.0.0.1.crt -out 127.0.0.1.pem -outform PEM

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
