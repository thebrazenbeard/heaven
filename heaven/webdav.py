"""Loopback-only WebDAV facade for HeavenStore.

This intentionally small implementation supports the common read/write WebDAV
verbs without introducing a web framework.  It is not an internet-facing
server: callers must authenticate, and binding to a non-loopback address is
refused.  Parent directories are virtual; an empty directory is represented
by a reserved zero-byte marker in the durable HeavenStore manifest.
"""

from __future__ import annotations

import base64
import binascii
from datetime import datetime, timezone
from email.utils import format_datetime
import hmac
import io
import ipaddress
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import quote, unquote, urlsplit
import xml.etree.ElementTree as ET


_FOLDER_MARKER = ".heaven-folder"
_DAV = "DAV:"
ET.register_namespace("d", _DAV)
_RANGE_RE = re.compile(r"bytes=(\d*)-(\d*)\Z")


class _IncompleteBodyError(OSError):
    """The socket closed before the declared upload body was received."""


def _normalize_path(request_target: str) -> str:
    """Turn a URL request target into a canonical, safe manifest path."""
    raw = unquote(urlsplit(request_target).path)
    if not raw.startswith("/") or "\\" in raw or "\x00" in raw:
        raise ValueError("invalid path")
    parts = raw.split("/")
    if any(part in (".", "..") or any(ord(c) < 32 for c in part) for part in parts):
        raise ValueError("path traversal or control character")
    if any(part == _FOLDER_MARKER for part in parts):
        raise ValueError("reserved path")
    if any(part == "" for part in parts[1:-1]):
        raise ValueError("duplicate path separator")
    return "/" + "/".join(part for part in parts if part)


def _parent(path: str) -> str:
    return path.rpartition("/")[0] or "/"


def _marker(path: str) -> str:
    return path.rstrip("/") + "/" + _FOLDER_MARKER


def _is_marker(path: str) -> bool:
    return path.rsplit("/", 1)[-1] == _FOLDER_MARKER


def _file_url(path: str, collection: bool) -> str:
    encoded = quote(path, safe="/")
    if collection and not encoded.endswith("/"):
        encoded += "/"
    return encoded


class _LimitedReader:
    """Prevent one PUT from consuming bytes from a subsequent request."""

    def __init__(self, source: Any, remaining: int):
        self.source = source
        self.remaining = remaining

    def read(self, n: int = -1) -> bytes:
        if self.remaining <= 0:
            return b""
        if n is None or n < 0 or n > self.remaining:
            n = self.remaining
        data = self.source.read(n)
        if not data:
            raise _IncompleteBodyError("upload shorter than Content-Length")
        self.remaining -= len(data)
        return data


def _resource_tree(store: Any) -> tuple[dict[str, Any], set[str]]:
    """Snapshot file metadata and virtual/marker directories."""
    files = {}
    directories = {"/"}
    for info in store.list_files("/"):
        path = info.path
        if _is_marker(path):
            directories.add(_parent(path))
        else:
            files[path] = info
        parent = _parent(path)
        while parent not in directories:
            directories.add(parent)
            parent = _parent(parent)
    return files, directories


def _children(path: str, files: dict[str, Any], directories: set[str]) -> list[tuple[str, bool, Any | None]]:
    result = []
    for directory in directories:
        if directory != path and _parent(directory) == path:
            result.append((directory, True, None))
    for item_path, info in files.items():
        if _parent(item_path) == path:
            result.append((item_path, False, info))
    return sorted(result, key=lambda item: item[0])


def _property_element(path: str, collection: bool, info: Any | None) -> ET.Element:
    response = ET.Element(f"{{{_DAV}}}response")
    ET.SubElement(response, f"{{{_DAV}}}href").text = _file_url(path, collection)
    propstat = ET.SubElement(response, f"{{{_DAV}}}propstat")
    prop = ET.SubElement(propstat, f"{{{_DAV}}}prop")
    rt = ET.SubElement(prop, f"{{{_DAV}}}resourcetype")
    if collection:
        ET.SubElement(rt, f"{{{_DAV}}}collection")
    ET.SubElement(prop, f"{{{_DAV}}}displayname").text = (
        path.rstrip("/").rsplit("/", 1)[-1] or "/"
    )
    ET.SubElement(prop, f"{{{_DAV}}}getcontentlength").text = str(0 if collection else info.size)
    when = datetime.fromtimestamp(
        datetime.now(timezone.utc).timestamp() if collection else info.mtime, tz=timezone.utc
    )
    ET.SubElement(prop, f"{{{_DAV}}}getlastmodified").text = format_datetime(when, usegmt=True)
    if not collection:
        ET.SubElement(prop, f"{{{_DAV}}}getetag").text = f'"{info.sha256}"'
        ET.SubElement(prop, f"{{{_DAV}}}getcontenttype").text = "application/octet-stream"
    ET.SubElement(propstat, f"{{{_DAV}}}status").text = "HTTP/1.1 200 OK"
    return response


def _range_from_header(value: str, size: int) -> tuple[int, int]:
    match = _RANGE_RE.fullmatch(value.strip())
    if not match or not any(match.groups()):
        raise ValueError("unsupported byte range")
    left, right = match.groups()
    if not left:
        suffix = int(right)
        if suffix == 0 or size == 0:
            raise ValueError("unsatisfiable byte range")
        start = max(0, size - suffix)
        end = size - 1
    else:
        start = int(left)
        end = min(size - 1, int(right)) if right else size - 1
    if start >= size or end < start:
        raise ValueError("unsatisfiable byte range")
    return start, end


def make_handler(store: Any, username: str, password: str, max_upload_bytes: int = 64 * 1024**3):
    """Produce an isolated handler class suitable for ThreadingHTTPServer."""
    if not username or not password:
        raise ValueError("WebDAV requires nonempty username and password")
    if max_upload_bytes <= 0:
        raise ValueError("max_upload_bytes must be positive")

    class HeavenWebDAVHandler(BaseHTTPRequestHandler):
        server_version = "HeavenWebDAV/0.1"
        protocol_version = "HTTP/1.1"

        def setup(self):
            super().setup()
            self.connection.settimeout(30)

        def _auth(self) -> bool:
            header = self.headers.get("Authorization", "")
            valid = False
            if header.startswith("Basic "):
                try:
                    candidate = base64.b64decode(header[6:].strip(), validate=True).decode("utf-8")
                    got_user, separator, got_password = candidate.partition(":")
                    valid = bool(separator) and hmac.compare_digest(got_user, username) and hmac.compare_digest(
                        got_password, password
                    )
                except (binascii.Error, TypeError, UnicodeError, ValueError):
                    valid = False
            if not valid:
                self._respond(401, headers={"WWW-Authenticate": 'Basic realm="Heaven"'})
            return valid

        def _path(self) -> str | None:
            try:
                return _normalize_path(self.path)
            except ValueError:
                self._respond(400)
                return None

        def _respond(self, code: int, data: bytes = b"", headers: dict[str, str] | None = None):
            self.send_response(code)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Connection", "close")
            for key, value in (headers or {}).items():
                self.send_header(key, value)
            self.end_headers()
            if data and self.command != "HEAD":
                self.wfile.write(data)
            self.close_connection = True

        def _tree(self):
            return _resource_tree(store)

        def do_OPTIONS(self):
            if self._auth():
                self._respond(200, headers={"Allow": "OPTIONS, PROPFIND, GET, HEAD, PUT, DELETE, MKCOL", "DAV": "1"})

        def do_PROPFIND(self):
            if not self._auth():
                return
            path = self._path()
            if path is None:
                return
            depth = self.headers.get("Depth", "1")
            if depth not in ("0", "1"):
                self._respond(403)
                return
            files, directories = self._tree()
            if path not in files and path not in directories:
                self._respond(404)
                return
            multi = ET.Element(f"{{{_DAV}}}multistatus")
            collection = path in directories
            multi.append(_property_element(path, collection, files.get(path)))
            if depth == "1" and collection:
                for child, child_dir, info in _children(path, files, directories):
                    multi.append(_property_element(child, child_dir, info))
            data = ET.tostring(multi, encoding="utf-8", xml_declaration=True)
            self._respond(207, data, {"Content-Type": 'application/xml; charset="utf-8"'})

        def _serve_file(self, head_only: bool = False):
            if not self._auth():
                return
            path = self._path()
            if path is None:
                return
            files, directories = self._tree()
            if path in directories:
                self._respond(403)
                return
            info = files.get(path)
            if info is None:
                self._respond(404)
                return
            offset = 0
            length = info.size
            code = 200
            headers = {"Accept-Ranges": "bytes", "ETag": f'"{info.sha256}"', "Content-Type": "application/octet-stream"}
            if self.headers.get("Range") is not None:
                try:
                    offset, end = _range_from_header(self.headers["Range"], info.size)
                except ValueError:
                    self._respond(416, headers={"Content-Range": f"bytes */{info.size}"})
                    return
                length = end - offset + 1
                code = 206
                headers["Content-Range"] = f"bytes {offset}-{end}/{info.size}"
            self.send_response(code)
            self.send_header("Content-Length", str(length))
            self.send_header("Connection", "close")
            for key, value in headers.items():
                self.send_header(key, value)
            self.end_headers()
            self.close_connection = True
            if head_only:
                return
            try:
                if code == 206:
                    # Never enumerate/download the bytes preceding a Range.
                    # The store yields only intersecting verified chunks;
                    # no large in-memory response reconstruction is required.
                    sent = 0
                    for selected in store.iter_range(path, offset, length):
                        self.wfile.write(selected)
                        sent += len(selected)
                    if sent != length:
                        raise OSError("short backend range read")
                else:
                    # Sequential full reads verify the complete-file checksum
                    # once all chunks have been consumed.
                    for chunk in store.iter_file(path):
                        self.wfile.write(chunk)
            except (BrokenPipeError, ConnectionResetError, OSError, ValueError):
                # Headers were already sent, so abruptly closing is safer than
                # fabricating a successful remainder on backend failure.
                self.close_connection = True

        def do_GET(self):
            self._serve_file()

        def do_HEAD(self):
            self._serve_file(head_only=True)

        def do_PUT(self):
            if not self._auth():
                return
            path = self._path()
            if path is None:
                return
            if path == "/" or self.path.split("?", 1)[0].endswith("/"):
                self._respond(409)
                return
            if self.headers.get("Transfer-Encoding"):
                self._respond(501)
                return
            raw_length = self.headers.get("Content-Length")
            if raw_length is None or not raw_length.isdecimal():
                self._respond(411)
                return
            if len(raw_length) > 20:
                self._respond(413)
                return
            length = int(raw_length)
            if length > max_upload_bytes:
                self._respond(413)
                return
            files, directories = self._tree()
            if _parent(path) not in directories or path in directories:
                self._respond(409)
                return
            existed = path in files
            reader = _LimitedReader(self.rfile, length)
            try:
                store.put_file(path, reader)
            except _IncompleteBodyError:
                self._respond(400)
                return
            except (ValueError, OSError, RuntimeError):
                self._respond(507)
                return
            self._respond(204 if existed else 201)

        def do_MKCOL(self):
            if not self._auth():
                return
            path = self._path()
            if path is None:
                return
            if path == "/":
                self._respond(405)
                return
            files, directories = self._tree()
            if path in directories or path in files:
                self._respond(405)
                return
            if _parent(path) not in directories:
                self._respond(409)
                return
            try:
                store.put_file(_marker(path), io.BytesIO(b""))
            except (ValueError, OSError, RuntimeError):
                self._respond(507)
                return
            self._respond(201)

        def do_DELETE(self):
            if not self._auth():
                return
            path = self._path()
            if path is None:
                return
            if path == "/":
                self._respond(403)
                return
            files, directories = self._tree()
            if path not in files and path not in directories:
                self._respond(404)
                return
            if path in directories:
                names = sorted((info.path for info in store.list_files("/") if info.path.startswith(path + "/")), reverse=True)
                if path in files:
                    names.append(path)
            else:
                names = [path]
            try:
                for name in names:
                    store.delete_file(name)
            except (ValueError, OSError, RuntimeError):
                self._respond(503)
                return
            self._respond(204)

    return HeavenWebDAVHandler


def make_server(store: Any, *, host: str = "127.0.0.1", port: int = 8767, username: str, password: str) -> ThreadingHTTPServer:
    """Bind a local, authenticated WebDAV server. Caller closes the server."""
    try:
        loopback = ipaddress.ip_address(host).is_loopback
    except ValueError:
        loopback = host.lower() == "localhost"
    if not loopback:
        raise ValueError("WebDAV exposure refused: bind to loopback only")
    handler = make_handler(store, username, password)
    return ThreadingHTTPServer((host, port), handler)


def serve(store: Any, *, host: str = "127.0.0.1", port: int = 8767, username: str, password: str) -> None:
    """Serve until interrupted. Credentials must be supplied by the caller."""
    with make_server(store, host=host, port=port, username=username, password=password) as server:
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass

