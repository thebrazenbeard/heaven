"""Loopback WebDAV security and byte-exact integration tests (no cloud account)."""

from __future__ import annotations

import base64
import http.client
import socket
import tempfile
import threading
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path

from heaven.backends import LocalDirectoryBackend
from heaven.store import HeavenStore
from heaven.webdav import make_server


class WebDavIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="heaven-webdav-qa-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = HeavenStore(
            self.root / "index.sqlite3",
            [LocalDirectoryBackend("one", self.root / "one", 128), LocalDirectoryBackend("two", self.root / "two", 128)],
            chunk_size=7,
        )
        self.server = make_server(self.store, port=0, username="tester", password="secret")
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self._shutdown)
        self.port = self.server.server_address[1]
        credentials = base64.b64encode(b"tester:secret").decode("ascii")
        self.auth = {"Authorization": "Basic " + credentials}

    def _shutdown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def _request(self, method: str, path: str, body: bytes | None = None, headers: dict | None = None):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=3)
        conn.request(method, path, body=body, headers={**self.auth, **(headers or {})})
        response = conn.getresponse()
        status, response_headers, data = response.status, dict(response.getheaders()), response.read()
        conn.close()
        return status, response_headers, data

    def test_authentication_is_required_on_read_and_write(self) -> None:
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=3)
        conn.request("GET", "/")
        response = conn.getresponse()
        self.assertEqual(response.status, 401)
        self.assertIn("Basic", response.getheader("WWW-Authenticate"))
        response.read()
        conn.close()
        self.assertEqual(self._request("GET", "/", headers={"Authorization": "Basic abc"})[0], 401)

    def test_collections_upload_download_ranges_listing_and_delete(self) -> None:
        data = b"exact-byte-contents-spanning-many-chunks"
        self.assertEqual(self._request("MKCOL", "/folder/")[0], 201)
        self.assertEqual(self._request("PUT", "/folder/file.bin", data)[0], 201)
        status, headers, result = self._request("GET", "/folder/file.bin")
        self.assertEqual(status, 200)
        self.assertEqual(result, data)
        self.assertEqual(int(headers["Content-Length"]), len(data))
        status, headers, result = self._request("HEAD", "/folder/file.bin")
        self.assertEqual(status, 200)
        self.assertFalse(result)
        self.assertEqual(int(headers["Content-Length"]), len(data))
        status, headers, result = self._request("GET", "/folder/file.bin", headers={"Range": "bytes=5-22"})
        self.assertEqual(status, 206)
        self.assertEqual(result, data[5:23])
        self.assertEqual(headers["Content-Range"], f"bytes 5-22/{len(data)}")
        status, headers, result = self._request("GET", "/folder/file.bin", headers={"Range": "bytes=-4"})
        self.assertEqual(status, 206)
        self.assertEqual(result, data[-4:])
        status, _, result = self._request("PROPFIND", "/folder/", headers={"Depth": "1"})
        self.assertEqual(status, 207)
        tree = ET.fromstring(result)
        hrefs = [node.text for node in tree.findall(".//{DAV:}href")]
        self.assertIn("/folder/", hrefs)
        self.assertIn("/folder/file.bin", hrefs)
        self.assertEqual(self._request("DELETE", "/folder/file.bin")[0], 204)
        self.assertEqual(self._request("GET", "/folder/file.bin")[0], 404)

    def test_rejects_traversal_and_unsatisfiable_ranges(self) -> None:
        self.assertEqual(self._request("PUT", "/%2e%2e/outside", b"unsafe")[0], 400)
        self.assertEqual(self._request("PUT", "/safe%5c..%5coutside", b"unsafe")[0], 400)
        self.assertFalse((self.root / "outside").exists())
        self.assertEqual(self._request("PUT", "/ok.bin", b"ok")[0], 201)
        status, headers, _ = self._request("GET", "/ok.bin", headers={"Range": "bytes=30-60"})
        self.assertEqual(status, 416)
        self.assertEqual(headers["Content-Range"], "bytes */2")

    def test_near_end_range_skips_unrelated_physical_chunks(self) -> None:
        data = bytes(range(100))
        self.store.put_file("/large.bin", __import__("io").BytesIO(data))
        reads: list[str] = []
        for backend in self.store.backends.values():
            original = backend.get_chunk

            def monitored(chunk_id: str, original=original) -> bytes:
                reads.append(chunk_id)
                return original(chunk_id)

            backend.get_chunk = monitored
        # Request just the last byte. A provider-aware Range should avoid
        # downloading the many unrelated chunks preceding the requested byte.
        status, _, body = self._request("GET", "/large.bin", headers={"Range": "bytes=99-99"})
        self.assertEqual(status, 206)
        self.assertEqual(body, data[99:100])
        self.assertLessEqual(len(reads), 2, "near-end Range downloaded unrequested prefix chunks")

    def test_refuses_non_loopback_binding(self) -> None:
        with self.assertRaises(ValueError):
            make_server(self.store, host="0.0.0.0", port=0, username="tester", password="secret")

    def test_truncated_put_must_not_commit_a_shorter_file(self) -> None:
        # Deliberately speak raw HTTP: a buggy server may treat EOF as end of
        # file rather than requiring the declared Content-Length bytes.
        auth = self.auth["Authorization"]
        request = (
            "PUT /truncated.bin HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            f"Authorization: {auth}\r\n"
            "Content-Length: 12\r\n"
            "Connection: close\r\n\r\n"
        ).encode("ascii") + b"abc"
        with socket.create_connection(("127.0.0.1", self.port), timeout=3) as sock:
            sock.settimeout(3)
            sock.sendall(request)
            sock.shutdown(socket.SHUT_WR)
            response = sock.recv(8192)
        self.assertTrue(response.startswith(b"HTTP/1.1 "))
        self.assertFalse(response.startswith(b"HTTP/1.1 2"), "partial upload was falsely acknowledged")
        self.assertEqual(self._request("GET", "/truncated.bin")[0], 404)


if __name__ == "__main__":
    unittest.main()
