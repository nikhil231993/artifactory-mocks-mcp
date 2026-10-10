"""Choosing which build of a mock component to use. Pure functions, no I/O.

The rule: among a component's -SNAPSHOT versions, use the one built most recently. See
"Architecture decisions" in the README for why this beats "highest version number".
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass

SNAPSHOT = "-SNAPSHOT"
EXTENSION = "zip"


@dataclass(frozen=True)
class Build:
    version: str  # the version folder, e.g. 1.2.0-SNAPSHOT or 1.2.0-484a3fa
    value: str  # the version in the file name, e.g. 1.2.0-20260819.052644-19
    updated: str  # build time as yyyyMMddHHmmss (UTC), or "" if unknown

    def file_name(self, artifact_id: str) -> str:
        return f"{artifact_id}-{self.value}.{EXTENSION}"


def is_snapshot(version: str) -> bool:
    return version.endswith(SNAPSHOT)


def version_key(version: str) -> tuple[int, ...]:
    """Compare versions as numbers: '1.10.0' -> (1, 10, 0), which beats '1.9.0'."""
    return tuple(int(n) for n in re.findall(r"\d+", version.removesuffix(SNAPSHOT)))


def _digits(stamp: str | None) -> str:
    """'20260819.052644' or '20260819052644' -> '20260819052644'."""
    return re.sub(r"\D", "", stamp or "")[:14]


def format_timestamp(stamp: str) -> str | None:
    """'20260819052644' -> '2026-08-19T05:26:44Z'. Maven metadata times are UTC."""
    if len(stamp) != 14:
        return None
    return f"{stamp[:4]}-{stamp[4:6]}-{stamp[6:8]}T{stamp[8:10]}:{stamp[10:12]}:{stamp[12:14]}Z"


def parse_snapshot_metadata(xml_text: str, version: str) -> Build | None:
    """The newest zip build listed in a -SNAPSHOT folder's maven-metadata.xml, if any."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return None
    versioning = root.find("versioning")
    if versioning is None:
        return None

    for item in versioning.findall("snapshotVersions/snapshotVersion"):
        value = (item.findtext("value") or "").strip()
        if item.findtext("extension") == EXTENSION and not item.findtext("classifier") and value:
            updated = _digits(item.findtext("updated") or versioning.findtext("lastUpdated"))
            return Build(version, value, updated)

    # Older metadata lists only the newest build's timestamp and number.
    timestamp = versioning.findtext("snapshot/timestamp")
    number = versioning.findtext("snapshot/buildNumber")
    if timestamp and number:
        base = version.removesuffix(SNAPSHOT)
        return Build(version, f"{base}-{timestamp.strip()}-{number.strip()}", _digits(timestamp))
    return None


def build_from_listing(file_names: list[str], artifact_id: str, version: str) -> Build | None:
    """The newest timestamped zip among a -SNAPSHOT folder's file names, if any."""
    base = version.removesuffix(SNAPSHOT)
    pattern = re.compile(
        rf"{re.escape(artifact_id)}-({re.escape(base)}-(\d{{8}})\.(\d{{6}})-(\d+))\.{EXTENSION}"
    )
    best: tuple[tuple[str, int], Build] | None = None
    for name in file_names:
        match = pattern.fullmatch(name)
        if match:
            key = (match[2] + match[3], int(match[4]))
            if best is None or key > best[0]:
                best = (key, Build(version, match[1], match[2] + match[3]))
    return best[1] if best else None


def latest_build(builds: list[Build]) -> Build | None:
    """The most recently built; on a tie (or unknown times), the higher version."""
    return max(builds, key=lambda b: (b.updated, version_key(b.version)), default=None)
