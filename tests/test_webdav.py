"""Real loopback HTTP probes for Heaven's experimental WebDAV gateway."""

from __future__ import annotations

import http.client
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from heaven import Pool, Store  # noqa: E402
from heaven.webdav import make_server  # noqa: E402


class WebDAVTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.pool = Pool(
            root / "manifest.sqlite3",
            [Store("first", root / "first", 128), Store("second", root / "second", 128)],
            chunk_size=7,
        )
        self.server = make_server(self.pool)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self._stop_server)

    def _stop_server(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.assertFalse(self.thread.is_alive())

    def request(
        self,
        method: str,
        path: str,
        body: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, bytes, dict[str, str]]:
        client = http.client.HTTPConnection("127.0.0.1", self.server.server_address[1], timeout=5)
        try:
            client.request(method, path, body=body, headers=headers or {})
            response = client.getresponse()
            return response.status, response.read(), dict(response.getheaders())
        finally:
            client.close()

    def test_create_get_head_and_delete_over_real_http(self) -> None:
        payload = b"ABCD-" * 15
        self.assertEqual(self.request("OPTIONS", "/")[0], 204)
        status, _body, response_headers = self.request("PUT", "/work/example.txt", payload)
        self.assertEqual(status, 201)
        self.assertEqual(response_headers["Location"], "/work/example.txt")
        status, body, headers = self.request("GET", "/work/example.txt")
        self.assertEqual(status, 200)
        self.assertEqual(body, payload)
        self.assertEqual(headers["Content-Length"], str(len(payload)))
        self.assertTrue(headers["ETag"].startswith('"'))
        status, body, headers = self.request("HEAD", "/work/example.txt")
        self.assertEqual((status, body), (200, b""))
        self.assertEqual(headers["Content-Length"], str(len(payload)))
        self.assertEqual(self.request("DELETE", "/work/example.txt")[0], 204)
        self.assertEqual(self.request("GET", "/work/example.txt")[0], 404)

    def test_propfind_lists_synthetic_directory_and_requested_props(self) -> None:
        self.assertEqual(self.request("PUT", "/folder/a.txt", b"a")[0], 201)
        self.assertEqual(self.request("PUT", "/folder/b.txt", b"b")[0], 201)
        status, data, headers = self.request("PROPFIND", "/", headers={"Depth": "1"})
        self.assertEqual(status, 207)
        self.assertIn("xml", headers["Content-Type"])
        root = ET.fromstring(data)
        hrefs = [node.text for node in root.findall(".//{DAV:}href")]
        self.assertEqual(hrefs, ["/", "/folder/"])

        wanted = b'<d:propfind xmlns:d="DAV:"><d:prop><d:getetag/><d:unknown/></d:prop></d:propfind>'
        status, data, _headers = self.request(
            "PROPFIND", "/folder/a.txt", body=wanted, headers={"Depth": "0"}
        )
        self.assertEqual(status, 207)
        self.assertIn(b"200 OK", data)
        self.assertIn(b"404 Not Found", data)
        self.assertIn(b"getetag", data)
        self.assertIn(b"unknown", data)

        status, data, _headers = self.request("PROPFIND", "/folder/", headers={"Depth": "1"})
        self.assertEqual(status, 207)
        hrefs = [node.text for node in ET.fromstring(data).findall(".//{DAV:}href")]
        self.assertEqual(hrefs, ["/folder/", "/folder/a.txt", "/folder/b.txt"])

    def test_overwrite_does_not_destroy_previous_object(self) -> None:
        self.assertEqual(self.request("PUT", "/saved", b"original")[0], 201)
        self.assertEqual(self.request("PUT", "/saved", b"replacement")[0], 412)
        self.assertEqual(self.request("GET", "/saved")[1], b"original")

    def test_successful_put_can_reuse_http11_connection(self) -> None:
        client = http.client.HTTPConnection("127.0.0.1", self.server.server_address[1], timeout=5)
        try:
            client.request("PUT", "/persistent", body=b"one request")
            created = client.getresponse()
            self.assertEqual(created.status, 201)
            created.read()
            client.request("GET", "/persistent")
            fetched = client.getresponse()
            self.assertEqual(fetched.status, 200)
            self.assertEqual(fetched.read(), b"one request")
        finally:
            client.close()

    def test_traversal_and_unsupported_depth_fail_closed(self) -> None:
        self.assertEqual(self.request("PUT", "/%2e%2e/private", b"bad")[0], 400)
        self.assertEqual(self.request("GET", "/%2e%2e/private")[0], 400)
        self.assertEqual(self.request("PROPFIND", "/", headers={"Depth": "infinity"})[0], 403)
        self.assertEqual(self.pool.list_files(), [])

    def test_missing_chunk_never_emits_partial_success_body(self) -> None:
        self.assertEqual(self.request("PUT", "/damaged.bin", b"D" * 24)[0], 201)
        first = next((Path(self.tmp.name) / "first" / ".heaven_objects").glob("*.chunk"), None)
        if first is None:
            first = next((Path(self.tmp.name) / "second" / ".heaven_objects").glob("*.chunk"))
        first.unlink()
        status, data, _headers = self.request("GET", "/damaged.bin")
        self.assertEqual(status, 500)
        self.assertNotEqual(data, b"D" * 24)

    def test_capacity_and_empty_files(self) -> None:
        self.assertEqual(self.request("PUT", "/empty", b"")[0], 201)
        status, content, _headers = self.request("GET", "/empty")
        self.assertEqual((status, content), (200, b""))
        status, _data, _headers = self.request("PUT", "/too-large", b"x" * 300)
        self.assertEqual(status, 507)
        self.assertEqual([item.name for item in self.pool.list_files()], ["empty"])

    def test_refuses_nonloopback_listener(self) -> None:
        for host in ("0.0.0.0", "192.168.1.1", "::"):
            with self.subTest(host=host), self.assertRaises(ValueError):
                make_server(self.pool, host=host)


if __name__ == "__main__":
    unittest.main()
