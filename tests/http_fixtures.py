from datetime import datetime
from functools import partial
from http.server import HTTPServer, SimpleHTTPRequestHandler
import os
from pathlib import Path
import shutil
import threading
import time
from urllib.request import urlopen

from pytest import fixture


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


@fixture(scope="module")
def http_server(tmp_path_factory, worker_id):
    hostname = "localhost"
    port = (
        9077 + int(worker_id.lstrip("gw")) if worker_id != "master" else 0
    )  # don't collide if tests running in parallel with multiple servers

    # Create a temporary directory to serve files from
    server_dir = tmp_path_factory.mktemp("server_files").resolve()
    server_dir.mkdir(exist_ok=True)

    # Function to start the server
    def start_server():
        handler = partial(TestHTTPRequestHandler, directory=str(server_dir))
        httpd = HTTPServer((hostname, port), handler)
        httpd.serve_forever()

    # Start the server in a separate thread
    server_thread = threading.Thread(target=start_server, daemon=True)
    server_thread.start()

    # Wait for the server to start
    for _ in range(10):
        try:
            urlopen(f"http://{hostname}:{port}")
            break
        except Exception:
            time.sleep(0.1)

    yield f"http://{hostname}:{port}", server_dir

    # Stop the server by exiting the thread
    server_thread.join(0)

    # Clean up the temporary directory if it still exists
    if server_dir.exists():
        shutil.rmtree(server_dir)
