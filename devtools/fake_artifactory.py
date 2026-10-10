"""A small fake JFrog Artifactory, seeded with dummy WireMock mocks, for tests and local trials.

It serves just what the MCP server uses:
  GET /artifactory/api/storage/<repo>/<path>   folder listing (or file info)
  GET /artifactory/<repo>/<path>               file download; <name>-<x.y.z>-SNAPSHOT.zip in a
                                               -SNAPSHOT folder returns its newest build

Run it:   python devtools/fake_artifactory.py [port]     (default 8081)
Then set ARTIFACTORY_URL=http://localhost:8081/artifactory, MOCKS_REPO=mocks-repo,
MOCKS_GROUP_PATH=com/example/mocks and MOCKS_SYSTEMS=customer-master=cm-*.

All data is made up. The layout copies a real setup: one zip per component, -SNAPSHOT folders
with timestamped builds and maven-metadata.xml, and fixed x.y.z-<commit> versions.
"""

from __future__ import annotations

import io
import json
import re
import sys
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

REPO = "mocks-repo"
GROUP = "com/example/mocks"
PREFIX = f"{REPO}/{GROUP}"


def make_zip(files: dict[str, str | bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, content in files.items():
            archive.writestr(name, content)
    return buffer.getvalue()


def snapshot_metadata(artifact: str, version: str, builds: list[tuple[str, int]]) -> str:
    """maven-metadata.xml for a -SNAPSHOT folder. builds: [(yyyyMMdd.HHmmss, number)], oldest first."""
    base = version.removesuffix("-SNAPSHOT")
    stamp, number = builds[-1]
    updated = stamp.replace(".", "")
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<metadata>
  <groupId>{GROUP.replace("/", ".")}</groupId>
  <artifactId>{artifact}</artifactId>
  <version>{version}</version>
  <versioning>
    <snapshot><timestamp>{stamp}</timestamp><buildNumber>{number}</buildNumber></snapshot>
    <lastUpdated>{updated}</lastUpdated>
    <snapshotVersions>
      <snapshotVersion><extension>pom</extension><value>{base}-{stamp}-{number}</value><updated>{updated}</updated></snapshotVersion>
      <snapshotVersion><extension>zip</extension><value>{base}-{stamp}-{number}</value><updated>{updated}</updated></snapshotVersion>
    </snapshotVersions>
  </versioning>
</metadata>
"""


ADDRESS_STUBS = {
    "mappings": [
        {
            "request": {"method": "GET", "urlPathTemplate": "/v1/customers/{cgid}/addresses"},
            "response": {
                "status": 200,
                "headers": {"Content-Type": "application/json"},
                "bodyFileName": "SOR/Addresses/addressesReadResponse.json",
            },
        },
        {
            "request": {
                "method": "PATCH",
                "urlPathTemplate": "/v1/customers/{cgid}/addresses/{addressId}",
                "bodyPatterns": [
                    {"matchesJsonPath": "$.validateOnly"},
                    {"matchesJsonPath": "$.details[0].postCode"},
                ],
            },
            "response": {
                "status": 200,
                "headers": {"Content-Type": "application/json"},
                "bodyFileName": "SOR/Addresses/addressPatchResponse.json",
                "transformers": ["response-template"],
            },
        },
        {
            "priority": 10,
            "request": {"method": "PATCH", "urlPathTemplate": "/v1/customers/{cgid}/addresses/{addressId}"},
            "response": {"status": 400, "jsonBody": {"code": "INVALID_ADDRESS", "message": "Invalid address"}},
        },
        {
            "request": {"method": "GET", "urlPath": "/v2/customers/C1000001/addresses"},
            "response": {"status": 200, "bodyFileName": "SOR/Addresses/addressesV2Response.json"},
        },
    ]
}

ADDRESSES_CURRENT = make_zip(
    {
        "mock.properties": "org=CM\nurl=git@ghes.example.com:CM/cm-addresses.git\n",
        "mappings/SOR/Addresses/SOR-Addresses.json": json.dumps(ADDRESS_STUBS, indent=2),
        "__files/SOR/Addresses/addressesReadResponse.json": json.dumps(
            {"addresses": [{"addressId": "A1", "streetName": "Example Street", "postCode": "3000"}]}
        ),
        "__files/SOR/Addresses/addressPatchResponse.json": (
            '{"customerId": "{{request.path.cgid}}", "addressId": "{{request.path.addressId}}", "status": "UPDATED"}'
        ),
        "__files/SOR/Addresses/addressesV2Response.json": json.dumps({"addresses": [], "version": 2}),
        "kafka/addresses-audit.json": json.dumps({"topic": "addresses-audit", "message": {}}),
    }
)

# Higher version number, but built two months earlier and with fewer stubs.
ADDRESSES_OLD = make_zip(
    {
        "mappings/SOR/Addresses/SOR-Addresses.json": json.dumps(
            {"mappings": [ADDRESS_STUBS["mappings"][0]]}, indent=2
        ),
        "__files/SOR/Addresses/addressesReadResponse.json": json.dumps({"addresses": [], "stale": True}),
    }
)

ADDRESSES_FIXED = make_zip(
    {
        "mappings/SOR/Addresses/SOR-Addresses.json": json.dumps(
            {"mappings": [ADDRESS_STUBS["mappings"][0]]}, indent=2
        ),
        "__files/SOR/Addresses/addressesReadResponse.json": json.dumps({"addresses": [], "fixed": True}),
    }
)

# Zipped with a top-level folder, and no maven-metadata.xml in its snapshot folder.
CORE_STUBS = [
    {
        "request": {"method": "GET", "urlPathTemplate": "/v1/customers/customer-core/{cgid}"},
        "response": {
            "status": 200,
            "bodyFileName": "SOR/Core/customerCoreReadResponse.json",
            "transformers": ["response-template"],
        },
    },
    {
        "request": {"method": "GET", "urlPathPattern": "/v1/customers/customer-core/[0-9]+/status"},
        "response": {"status": 200, "jsonBody": {"status": "ACTIVE"}},
    },
]
CORE_ZIP = make_zip(
    {
        "cm-customer-core-2.0.0/mappings/core.json": json.dumps(CORE_STUBS, indent=2),
        "cm-customer-core-2.0.0/__files/SOR/Core/customerCoreReadResponse.json": json.dumps(
            {"cgid": "{{request.path.cgid}}", "name": "Test Customer", "segment": "RETAIL"}
        ),
    }
)
CORE_ZIP_OLD = make_zip({"mappings/core.json": "[]"})

TLM_ZIP = make_zip(
    {
        "mappings/tlm.json": json.dumps(
            {"request": {"method": "POST", "url": "/tlm/v1/verify"}, "response": {"status": 200, "body": "OK"}}
        )
    }
)

FILES: dict[str, bytes] = {
    # cm-addresses: fixed versions, plus two snapshot lines; 1.2.0-SNAPSHOT is the newest build.
    f"{PREFIX}/cm-addresses/maven-metadata.xml": b"<metadata/>",
    f"{PREFIX}/cm-addresses/1.2.0-484a3fa/cm-addresses-1.2.0-484a3fa.zip": ADDRESSES_FIXED,
    f"{PREFIX}/cm-addresses/1.2.0-SNAPSHOT/maven-metadata.xml": snapshot_metadata(
        "cm-addresses", "1.2.0-SNAPSHOT", [("20260818.101500", 18), ("20260819.052644", 19)]
    ).encode(),
    f"{PREFIX}/cm-addresses/1.2.0-SNAPSHOT/cm-addresses-1.2.0-20260818.101500-18.zip": ADDRESSES_OLD,
    f"{PREFIX}/cm-addresses/1.2.0-SNAPSHOT/cm-addresses-1.2.0-20260819.052644-19.zip": ADDRESSES_CURRENT,
    f"{PREFIX}/cm-addresses/1.3.1-SNAPSHOT/maven-metadata.xml": snapshot_metadata(
        "cm-addresses", "1.3.1-SNAPSHOT", [("20260624.152900", 3)]
    ).encode(),
    f"{PREFIX}/cm-addresses/1.3.1-SNAPSHOT/cm-addresses-1.3.1-20260624.152900-3.zip": ADDRESSES_OLD,
    # cm-customer-core: no maven-metadata.xml, so the newest build comes from the file names.
    f"{PREFIX}/cm-customer-core/2.0.0-SNAPSHOT/cm-customer-core-2.0.0-20260901.080000-1.zip": CORE_ZIP_OLD,
    f"{PREFIX}/cm-customer-core/2.0.0-SNAPSHOT/cm-customer-core-2.0.0-20260902.090000-2.zip": CORE_ZIP,
    f"{PREFIX}/cm-customer-core/2.0.0-SNAPSHOT/cm-customer-core-2.0.0-20260902.090000-2.pom": b"<project/>",
    # cm-warnings: release versions only, so it's skipped.
    f"{PREFIX}/cm-warnings/1.0.0/cm-warnings-1.0.0.zip": make_zip({"mappings/w.json": "[]"}),
    # Not Customer Master.
    f"{PREFIX}/collateral-valuation-service/1.0.0/collateral-valuation-service-1.0.0.zip": make_zip({}),
    f"{PREFIX}/mock-tlm-avp/1.0.1/mock-tlm-avp-1.0.1.zip": TLM_ZIP,
}


def _children(path: str) -> list[dict[str, object]] | None:
    """Storage-API children of a folder, or None if the folder doesn't exist."""
    prefix = path.rstrip("/") + "/"
    children: dict[str, bool] = {}
    for name in FILES:
        if name.startswith(prefix):
            rest = name[len(prefix) :]
            child, _, more = rest.partition("/")
            children[child] = children.get(child, False) or bool(more)
    if not children:
        return None
    return [{"uri": f"/{c}", "folder": folder} for c, folder in sorted(children.items())]


def _resolve_snapshot(path: str) -> str | None:
    """Artifactory serves <name>-<x.y.z>-SNAPSHOT.zip as the newest timestamped build."""
    folder, _, file_name = path.rpartition("/")
    if not (folder.endswith("-SNAPSHOT") and file_name.endswith("-SNAPSHOT.zip")):
        return None
    stem = re.escape(file_name.removesuffix("-SNAPSHOT.zip"))
    builds = [n for n in FILES if n.startswith(folder + "/") and re.fullmatch(rf".*/{stem}-\d{{8}}\.\d{{6}}-\d+\.zip", n)]
    return max(builds) if builds else None


def handle(path: str) -> tuple[int, str, bytes]:
    """(status, content type, body) for a path under /artifactory/."""
    path = path.split("?", 1)[0].strip("/")
    if path.startswith("api/storage/"):
        repo_path = path[len("api/storage/") :]
        if repo_path in FILES:
            info = {"repo": REPO, "path": "/" + repo_path.split("/", 1)[1], "size": str(len(FILES[repo_path]))}
            return 200, "application/json", json.dumps(info).encode()
        children = _children(repo_path)
        if children is None:
            return 404, "application/json", json.dumps({"errors": [{"status": 404, "message": "Unable to find item"}]}).encode()
        info = {"repo": REPO, "path": "/" + repo_path.split("/", 1)[-1], "children": children}
        return 200, "application/json", json.dumps(info).encode()
    name = path if path in FILES else _resolve_snapshot(path)
    if name is None:
        return 404, "application/json", json.dumps({"errors": [{"status": 404, "message": "File not found."}]}).encode()
    content_type = "application/zip" if name.endswith(".zip") else "application/xml"
    return 200, content_type, FILES[name]


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802 - http.server API
        if not self.path.startswith("/artifactory/"):
            status, content_type, body = 404, "text/plain", b"Not found"
        else:
            status, content_type, body = handle(self.path[len("/artifactory/") :])
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main() -> None:
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8081
    server = ThreadingHTTPServer(("127.0.0.1", port), _Handler)
    print(f"Fake Artifactory on http://localhost:{port}/artifactory (Ctrl+C to stop)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
