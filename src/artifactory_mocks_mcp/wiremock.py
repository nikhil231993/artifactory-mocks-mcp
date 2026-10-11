"""WireMock mock archives: read their stubs, and match a request against them. No I/O."""

from __future__ import annotations

import io
import json
import posixpath
import re
import zipfile
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

from .kafka import read_kafka

MAX_UNCOMPRESSED_BYTES = 300 * 1024 * 1024

# WireMock's URL matchers, most specific first. A stub uses at most one of them.
URL_KEYS = ("urlPathTemplate", "urlPath", "url", "urlPathPattern", "urlPattern")
PATTERN_KEYS = {"urlPathPattern", "urlPattern"}

# Request conditions other than method and URL, shown so callers can tell stubs apart.
CONDITION_KEYS = (
    "queryParameters",
    "headers",
    "cookies",
    "pathParameters",
    "bodyPatterns",
    "multipartPatterns",
    "basicAuthCredentials",
    "host",
    "port",
    "scheme",
)
STUB_KEYS = ("priority", "scenarioName", "requiredScenarioState", "newScenarioState")
RESPONSE_KEYS = (
    "status",
    "statusMessage",
    "headers",
    "bodyFileName",
    "transformers",
    "fixedDelayMilliseconds",
    "fault",
    "proxyBaseUrl",
)

# Values tried in place of {placeholders} when a templated path meets a regex stub.
PROBES = (
    "1",
    "123",
    "12345678",
    "abc",
    "ABC123",
    "A123456",
    "a1b2c3",
    "00000000-0000-0000-0000-000000000000",
)
_PLACEHOLDER = re.compile(r"\$?\{[^/}]*\}")
_HOST_RE = re.compile(
    r"(localhost|\d{1,3}(\.\d{1,3}){3}|([a-z0-9-]+\.)+[a-z][a-z0-9-]*)(:\d+)?", re.IGNORECASE
)

MATCH_SCORES = {"exact": 1000, "pattern": 800, "suffix": 500, "any-url": 100}


# ---- reading archives ------------------------------------------------------------


@dataclass
class Stub:
    file: str  # the mapping file inside the zip, e.g. mappings/SOR/Addresses/SOR-Addresses.json
    index: int  # position of the stub in that file
    raw: dict[str, Any]

    @property
    def request(self) -> dict[str, Any]:
        value = self.raw.get("request")
        return value if isinstance(value, dict) else {}

    @property
    def response(self) -> dict[str, Any]:
        value = self.raw.get("response")
        return value if isinstance(value, dict) else {}

    @property
    def method(self) -> str:
        return str(self.request.get("method") or "ANY").upper()

    @property
    def url(self) -> tuple[str | None, str | None]:
        """(matcher, value), e.g. ('urlPathTemplate', '/v1/customers/{cgid}'), or (None, None)."""
        for key in URL_KEYS:
            value = self.request.get(key)
            if isinstance(value, str):
                return key, value
        return None, None


def _find_root(names: list[str]) -> str:
    """The folder holding mappings/, '' if it's at the top of the zip."""
    roots = [name[:i] for name in names if (i := ("/" + name).find("/mappings/")) >= 0]
    return min(roots, key=len) if roots else ""


def _parse_properties(data: bytes) -> dict[str, str]:
    props: dict[str, str] = {}
    for line in data.decode("utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line[0] in "#!":
            continue
        key, sep, value = line.partition("=")
        if sep:
            props[key.strip()] = value.strip()
    return props


class MockArchive:
    """The files of one mock zip, with its stubs parsed. Paths are relative to the zip's root."""

    def __init__(self, files: dict[str, bytes]):
        self.root = _find_root(list(files))
        self.files = {
            name[len(self.root) :]: data for name, data in files.items() if name.startswith(self.root)
        }
        self.stubs: list[Stub] = []
        self.errors: list[str] = []
        for name in sorted(self.files):
            if name.startswith("mappings/") and name.lower().endswith(".json"):
                self._load_mappings(name)
        self.properties = _parse_properties(self.files.get("mock.properties", b""))
        self.kafka_events, self.kafka_errors = read_kafka(self.files)

    @classmethod
    def from_zip(cls, data: bytes) -> "MockArchive":
        try:
            archive = zipfile.ZipFile(io.BytesIO(data))
        except zipfile.BadZipFile as exc:
            raise ValueError(f"not a valid zip file ({exc})") from exc
        with archive:
            infos = [
                i for i in archive.infolist() if not i.is_dir() and not i.filename.startswith("__MACOSX/")
            ]
            if sum(i.file_size for i in infos) > MAX_UNCOMPRESSED_BYTES:
                raise ValueError("it unpacks to more than 300 MB; refusing to read it")
            return cls({i.filename: archive.read(i) for i in infos})

    def _load_mappings(self, name: str) -> None:
        try:
            doc = json.loads(self.files[name].decode("utf-8-sig"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            self.errors.append(f"{name}: {exc}")
            return
        # A mapping file holds {"mappings": [...]}, a list of stubs, or a single stub.
        if isinstance(doc, dict) and isinstance(doc.get("mappings"), list):
            items = doc["mappings"]
        elif isinstance(doc, list):
            items = doc
        else:
            items = [doc]
        for index, item in enumerate(items):
            if isinstance(item, dict) and isinstance(item.get("request"), dict):
                self.stubs.append(Stub(name, index, item))

    def body_file(self, body_file_name: str) -> bytes | None:
        """A response body referenced by bodyFileName, which is relative to __files/."""
        return self.files.get(posixpath.normpath(posixpath.join("__files", body_file_name.lstrip("/"))))


# ---- matching ----------------------------------------------------------------------


def request_path(api_url: str) -> str:
    """The path of a full URL, host + path, or bare path, without query: -> '/v1/x/{id}'."""
    value = api_url.strip()
    if "://" not in value:
        bare = value.lstrip("/")
        first = re.split(r"[/?#]", bare, maxsplit=1)[0]
        # A host only without a leading slash (api.example.com/v1) or after '//'.
        is_host = _HOST_RE.fullmatch(first) and (not value.startswith("/") or value.startswith("//"))
        value = ("//" if is_host else "/") + bare
    path = urlparse(value).path
    return "/" + "/".join(segment for segment in path.split("/") if segment)


def _segments(path: str) -> list[str]:
    return [s for s in path.split("/") if s]


def _is_param(segment: str) -> bool:
    return "{" in segment and "}" in segment


def _compare(want: list[str], have: list[str], have_is_template: bool) -> tuple[str, int, int] | None:
    """('exact' | 'suffix', literal segments matched, params) if the paths line up, else None.

    A {param} matches any value: the graph's /customers/{id} matches the stub's
    /customers/{cgid}, and a stub's concrete urlPath /customers/C123. But a {param} in the
    requested path never matches a fixed word of a urlPathTemplate: /customers/{id} must not
    match /customers/addresses. 'suffix' means one path has extra leading segments (e.g. a
    gateway prefix); it needs at least 2 matching literal segments.
    """
    if not want and not have:
        return "exact", 0, 0
    n = min(len(want), len(have))
    literals = params = 0
    for a, b in zip(want[len(want) - n :], have[len(have) - n :]):
        if _is_param(b) or (_is_param(a) and not have_is_template):
            params += 1
        elif _is_param(a):
            return None
        elif a.lower() == b.lower():
            literals += 1
        else:
            return None
    if len(want) == len(have):
        return "exact", literals, params
    if n >= 2 and literals >= 2:
        return "suffix", literals, params
    return None


def _pattern_matches(pattern: str, path: str) -> bool:
    """Does a urlPathPattern/urlPattern regex accept the path? {params} are tried as samples."""
    try:
        regex = re.compile(pattern)
    except re.error:
        return False
    candidates = [path]
    if _PLACEHOLDER.search(path):
        candidates += [_PLACEHOLDER.sub(probe, path) for probe in PROBES]
    # urlPattern also covers the query string, so allow for one.
    return any(regex.fullmatch(c) or regex.fullmatch(c + "?") for c in candidates)


@dataclass
class StubMatch:
    stub: Stub
    match_type: str  # exact, pattern, suffix or any-url
    score: int


def match_stub(stub: Stub, path: str, method: str | None = None) -> StubMatch | None:
    want_method = method.upper() if method else None
    if want_method and stub.method not in {"ANY", want_method}:
        return None

    key, value = stub.url
    specificity = 0
    if key is None or value is None:
        match_type = "any-url"
    elif key in PATTERN_KEYS:
        if not _pattern_matches(value, path):
            return None
        match_type = "pattern"
    else:
        stub_path = value.split("?", 1)[0] if key == "url" else value
        compared = _compare(_segments(path), _segments(stub_path), key == "urlPathTemplate")
        if compared is None:
            return None
        match_type, literals, params = compared
        specificity = literals * 10 - params

    score = MATCH_SCORES[match_type] + specificity
    if want_method and stub.method == want_method:
        score += 50  # an explicit method beats ANY
    return StubMatch(stub, match_type, score)


def match_stubs(stubs: list[Stub], path: str, method: str | None = None) -> list[StubMatch]:
    """Every stub that matches, best first."""
    found = [m for stub in stubs if (m := match_stub(stub, path, method))]
    return sorted(found, key=lambda m: (-m.score, m.stub.file, m.stub.index))


def request_conditions(stub: Stub) -> dict[str, Any]:
    """What else a stub requires (headers, body, scenario...), to tell same-path stubs apart."""
    conditions = {key: stub.request[key] for key in CONDITION_KEYS if stub.request.get(key)}
    conditions.update({key: stub.raw[key] for key in STUB_KEYS if stub.raw.get(key) is not None})
    return conditions
