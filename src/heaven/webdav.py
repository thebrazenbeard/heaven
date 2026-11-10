"""Small loopback-only, class-1 WebDAV gateway for a Heaven storage pool.

This is an experimental WebDAV subset, not a general WebDAV server: locking,
PATCH, MOVE, COPY, authentication, and overwriting existing files are not
implemented. GET reconstructs into a spooled temporary file before responding
so large downloads require equivalent local temporary disk space. Mount it
behind a separately authenticated local transport if remote access is needed.
"""

from __future__ import annotations

import io
import ipaddress
import mimetypes
import tempfile
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import BinaryIO
from urllib.parse import quote, unquote, urlsplit
from xml.etree import ElementTree as ET

DAV = "DAV:"
ET.register_namespace("d", DAV)


class _LimitedBody(io.RawIOBase):
    def __init__(self, source: BinaryIO, length: int) -> None:
        self._source = source
        self._remaining = length

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        if size == 0:
            return b""
        if self._remaining == 0:
            return b""
        size = min(self._remaining, 1024 * 1024 if size < 0 else size)
        data = self._source.read(size)
        if not data:
            raise EOFError("Request ended before Content-Length bytes arrived")
        self._remaining -= len(data)
        return data


def _href(name: str, collection: bool = False) -> str:
    if not name:
        return "/"
    return "/" + "/".join(quote(part, safe="") for part in name.split("/")) + ("/" if collection else "")


def _parse_name(request_path: str) -> tuple[str, bool]:
    parsed = urlsplit(request_path)
    if parsed.query or parsed.fragment:
        raise ValueError("Query and fragment components are not valid storage paths")
    if not parsed.path.startswith("/"):
        raise ValueError("Not an absolute HTTP path")
    try:
        decoded = unquote(parsed.path, encoding="utf-8", errors="strict")
    except UnicodeError as exc:
        raise ValueError("Invalid UTF-8 path") from exc
    if decoded == "/":
        return "", True
    is_collection = decoded.endswith("/")
    parts = decoded[1:-1].split("/") if is_collection else decoded[1:].split("/")
    if not parts or any(
        part in ("", ".", "..")
        or "\\" in part
        or any(ord(character) < 32 or ord(character) == 127 for character in part)
        for part in parts
    ):
        raise ValueError("Invalid storage path")
    return "/".join(parts), is_collection


def _file_size(info: object) -> int:
    return int(getattr(info, "size"))


def _file_name(info: object) -> str:
    return str(getattr(info, "name"))


def _file_etag(info: object) -> str:
    return '"' + str(getattr(info, "sha256")) + '"'


def _child_resources(prefix: str, infos: list[object]) -> list[tuple[str, bool, object | None]]:
    """List immediate files and synthetic directories below a collection."""
    start = prefix + "/" if prefix else ""
    output: dict[str, tuple[str, bool, object | None]] = {}
    for info in infos:
        name = _file_name(info)
        if not name.startswith(start):
            continue
        remainder = name[len(start):]
        if not remainder:
            continue
        first, separator, _tail = remainder.partition("/")
        if separator:
            child = start + first
            output[child] = (child, True, None)
        else:
            output[name] = (name, False, info)
    return [output[key] for key in sorted(output)]


def _append_resource(
    multistatus: ET.Element,
    name: str,
    is_collection: bool,
    info: object | None,
    requested_props: set[str] | None = None,
    propname_only: bool = False,
) -> None:
    response = ET.SubElement(multistatus, f"{{{DAV}}}response")
    ET.SubElement(response, f"{{{DAV}}}href").text = _href(name, is_collection)
    supported: dict[str, str | None] = {
        f"{{{DAV}}}displayname": name.rsplit("/", 1)[-1] if name else "/",
        f"{{{DAV}}}resourcetype": None,
    }
    if not is_collection:
        assert info is not None
        supported.update(
            {
                f"{{{DAV}}}getcontentlength": str(_file_size(info)),
                f"{{{DAV}}}getetag": _file_etag(info),
                f"{{{DAV}}}getcontenttype": mimetypes.guess_type(name)[0] or "application/octet-stream",
            }
        )
    accepted = set(supported) if requested_props is None else requested_props & set(supported)
    for status, tags in (("200 OK", accepted), ("404 Not Found", (requested_props or set()) - set(supported))):
        if not tags:
            continue
        propstat = ET.SubElement(response, f"{{{DAV}}}propstat")
        props = ET.SubElement(propstat, f"{{{DAV}}}prop")
        for tag in sorted(tags):
            element = ET.SubElement(props, tag)
            if not propname_only and status == "200 OK":
                element.text = supported[tag]
                if tag == f"{{{DAV}}}resourcetype" and is_collection:
                    ET.SubElement(element, f"{{{DAV}}}collection")
        ET.SubElement(propstat, f"{{{DAV}}}status").text = f"HTTP/1.1 {status}"


def make_handler(pool: object) -> type[BaseHTTPRequestHandler]:
    """Return an HTTP request handler bound to the supplied Pool instance."""

    class HeavenHandler(BaseHTTPRequestHandler):
        server_version = "HeavenDAV/0.1"
        protocol_version = "HTTP/1.1"

        def _error(self, status: HTTPStatus | int, message: str = "") -> None:
            # Fail closed when an error can leave unread request body bytes.
            self.close_connection = True
            payload = (message or HTTPStatus(status).phrase).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Connection", "close")
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(payload)

        def _name(self) -> tuple[str, bool] | None:
            try:
                return _parse_name(self.path)
            except ValueError as exc:
                self._error(HTTPStatus.BAD_REQUEST, str(exc))
                return None

        def _infos(self) -> list[object]:
            return pool.list_files()

        def _lookup(self, name: str, infos: list[object]) -> object | None:
            return next((info for info in infos if _file_name(info) == name), None)

        def do_OPTIONS(self) -> None:
            self.send_response(HTTPStatus.NO_CONTENT)
            self.send_header("Allow", "OPTIONS, GET, HEAD, PUT, DELETE, PROPFIND")
            self.send_header("DAV", "1")
            self.send_header("MS-Author-Via", "DAV")
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_HEAD(self) -> None:
            self._get(head_only=True)

        def do_GET(self) -> None:
            self._get(head_only=False)

        def _get(self, *, head_only: bool) -> None:
            parsed = self._name()
            if parsed is None:
                return
            name, requested_collection = parsed
            if not name or requested_collection:
                self._error(HTTPStatus.METHOD_NOT_ALLOWED, "Use PROPFIND for directories")
                return
            info = self._lookup(name, self._infos())
            if info is None:
                self._error(HTTPStatus.NOT_FOUND)
                return
            etag = _file_etag(info)
            if self.headers.get("If-None-Match", "") == etag:
                self.send_response(HTTPStatus.NOT_MODIFIED)
                self.send_header("ETag", etag)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            if not head_only:
                # Spooling before headers prevents serving a partially corrupt
                # file when a later missing chunk/checksum fails verification.
                with tempfile.SpooledTemporaryFile(max_size=8 * 1024 * 1024) as spool:
                    try:
                        pool.get(name, spool)
                    except (FileNotFoundError, OSError, ValueError):
                        self._error(HTTPStatus.INTERNAL_SERVER_ERROR, "Stored file is unavailable or damaged")
                        return
                    spool.seek(0)
                    self._file_headers(info)
                    while chunk := spool.read(1024 * 1024):
                        self.wfile.write(chunk)
                    return
            self._file_headers(info)

        def _file_headers(self, info: object) -> None:
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", mimetypes.guess_type(_file_name(info))[0] or "application/octet-stream")
            self.send_header("Content-Length", str(_file_size(info)))
            self.send_header("ETag", _file_etag(info))
            self.end_headers()

        def do_PUT(self) -> None:
            # A rejected request may leave an unread upload body in the socket.
            # Never parse those bytes as a subsequent HTTP request.
            self.close_connection = True
            parsed = self._name()
            if parsed is None:
                return
            name, requested_collection = parsed
            if not name or requested_collection:
                self._error(HTTPStatus.CONFLICT, "PUT requires a file path")
                return
            if "chunked" in self.headers.get("Transfer-Encoding", "").lower():
                self._error(HTTPStatus.NOT_IMPLEMENTED, "Chunked request transfer is unsupported")
                return
            raw_length = self.headers.get("Content-Length")
            if raw_length is None:
                self._error(HTTPStatus.LENGTH_REQUIRED)
                return
            try:
                length = int(raw_length)
            except ValueError:
                self._error(HTTPStatus.BAD_REQUEST, "Invalid Content-Length")
                return
            if length < 0:
                self._error(HTTPStatus.BAD_REQUEST, "Negative Content-Length")
                return
            existing = self._lookup(name, self._infos())
            if existing is not None:
                # The core intentionally provides create-only put; pretending
                # PUT can overwrite would risk deleting the only valid copy.
                self._error(HTTPStatus.PRECONDITION_FAILED, "Overwriting existing files is not implemented")
                self.close_connection = True
                return
            body = _LimitedBody(self.rfile, length)
            try:
                info = pool.put(name, body)
                if body._remaining != 0:
                    raise ValueError("Upload body was not fully consumed")
            except FileExistsError:
                self._error(HTTPStatus.PRECONDITION_FAILED, "File already exists")
                self.close_connection = True
                return
            except (OSError, EOFError, ValueError) as exc:
                status = (
                    HTTPStatus.INSUFFICIENT_STORAGE
                    if exc.__class__.__name__ == "CapacityError"
                    else HTTPStatus.BAD_REQUEST
                )
                self._error(status, "Upload could not be stored")
                return
            # The announced Content-Length has been consumed completely, so a
            # successful client can safely reuse its HTTP/1.1 connection.
            self.close_connection = False
            self.send_response(HTTPStatus.CREATED)
            self.send_header("Location", _href(name))
            self.send_header("ETag", _file_etag(info))
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_DELETE(self) -> None:
            parsed = self._name()
            if parsed is None:
                return
            name, requested_collection = parsed
            if not name or requested_collection:
                self._error(HTTPStatus.METHOD_NOT_ALLOWED, "Deleting directories is unsupported")
                return
            try:
                pool.delete(name)
            except FileNotFoundError:
                self._error(HTTPStatus.NOT_FOUND)
                return
            except OSError:
                self._error(HTTPStatus.INTERNAL_SERVER_ERROR)
                return
            self.send_response(HTTPStatus.NO_CONTENT)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_PROPFIND(self) -> None:
            parsed = self._name()
            if parsed is None:
                return
            name, requested_collection = parsed
            depth = self.headers.get("Depth", "1").lower()
            if depth not in ("0", "1"):
                self._error(HTTPStatus.FORBIDDEN, "Only Depth: 0 and Depth: 1 are supported")
                return
            raw_length = self.headers.get("Content-Length", "0")
            try:
                length = int(raw_length)
            except ValueError:
                self._error(HTTPStatus.BAD_REQUEST, "Invalid Content-Length")
                return
            if length < 0 or length > 64 * 1024:
                self._error(HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
                self.close_connection = True
                return
            requested_props: set[str] | None = None
            propname_only = False
            if length:
                request_body = self.rfile.read(length)
                try:
                    request = ET.fromstring(request_body)
                except ET.ParseError:
                    self._error(HTTPStatus.BAD_REQUEST, "Malformed XML")
                    return
                if request.tag != f"{{{DAV}}}propfind" or len(request) != 1:
                    self._error(HTTPStatus.BAD_REQUEST, "Unsupported PROPFIND request")
                    return
                selection = request[0]
                if selection.tag == f"{{{DAV}}}prop":
                    requested_props = {child.tag for child in selection}
                elif selection.tag == f"{{{DAV}}}propname":
                    propname_only = True
                elif selection.tag != f"{{{DAV}}}allprop":
                    self._error(HTTPStatus.BAD_REQUEST, "Unsupported PROPFIND request")
                    return
            infos = self._infos()
            info = self._lookup(name, infos)
            is_collection = name == "" or requested_collection or info is None
            if is_collection:
                prefix = name + "/" if name else ""
                if name and not any(_file_name(item).startswith(prefix) for item in infos):
                    self._error(HTTPStatus.NOT_FOUND)
                    return
            elif info is None:
                self._error(HTTPStatus.NOT_FOUND)
                return
            root = ET.Element(f"{{{DAV}}}multistatus")
            _append_resource(root, name, is_collection, info if not is_collection else None, requested_props, propname_only)
            if depth == "1" and is_collection:
                for child_name, child_is_collection, child_info in _child_resources(name, infos):
                    _append_resource(root, child_name, child_is_collection, child_info, requested_props, propname_only)
            payload = ET.tostring(root, encoding="utf-8", xml_declaration=True)
            self.send_response(207)
            self.send_header("Content-Type", "application/xml; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    return HeavenHandler


def make_server(pool: object, *, host: str = "127.0.0.1", port: int = 0) -> ThreadingHTTPServer:
    """Create a loopback-bound server. Call ``serve_forever()`` to start it.

    Non-loopback binding is refused: this minimal gateway has no authentication
    or TLS and must never be exposed directly to a public network.
    """
    try:
        address = ipaddress.ip_address("127.0.0.1" if host == "localhost" else host)
    except ValueError as exc:
        raise ValueError("Host must be a numeric loopback address or 'localhost'") from exc
    if address.version != 4 or not address.is_loopback:
        raise ValueError("Heaven's unauthenticated WebDAV gateway is loopback-only")
    return ThreadingHTTPServer((host, port), make_handler(pool))
