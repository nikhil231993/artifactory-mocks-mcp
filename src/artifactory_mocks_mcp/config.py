"""Settings loaded from environment variables, or from a .env file."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def load_dotenv(*paths: Path) -> None:
    """Read KEY=VALUE lines from the first .env file found into os.environ.

    Variables already set in the environment (e.g. by the MCP client's "env" block) win.
    """
    for path in paths:
        if not path.is_file():
            continue
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key, value = key.strip().removeprefix("export ").strip(), value.strip().strip("'\"")
            if value and not os.environ.get(key):
                os.environ[key] = value
        return


def _env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def parse_systems(value: str) -> dict[str, list[str]]:
    """'customer-master=cm-*; tlm=mock-tlm,mock-tlm-*' -> {'customer-master': ['cm-*'], ...}.

    Maps a system name, as the dependency graph reports it, to the component folders that
    hold its mocks. Names are matched case-insensitively.
    """
    systems: dict[str, list[str]] = {}
    for entry in value.split(";"):
        if not entry.strip():
            continue
        if "=" not in entry:
            raise RuntimeError(
                f"MOCKS_SYSTEMS entry '{entry.strip()}' must look like system=pattern[,pattern]"
            )
        name, patterns = entry.split("=", 1)
        globs = [p.strip() for p in patterns.split(",") if p.strip()]
        if name.strip() and globs:
            systems[name.strip().lower()] = globs
    return systems


def normalise_base_url(url: str) -> str:
    """Artifactory serves its API under /artifactory. Accept the host alone or the full URL."""
    url = url.strip().rstrip("/")
    if url and not urlparse(url).path:
        url += "/artifactory"
    return url


@dataclass(frozen=True)
class Settings:
    base_url: str  # e.g. https://artifactory.example.com/artifactory
    token: str | None
    repo: str  # the Artifactory repository holding the mocks
    group_path: str  # Maven group as a path, e.g. com/example/mocks
    systems: dict[str, list[str]]
    verify: bool | str  # True/False, or a path to a CA bundle
    timeout: float
    max_concurrency: int
    cache_ttl: float  # seconds before folder listings and version choices are re-read
    max_body_chars: int

    @classmethod
    def from_env(cls) -> "Settings":
        load_dotenv(Path.cwd() / ".env", PROJECT_ROOT / ".env")
        base_url = normalise_base_url(os.environ.get("ARTIFACTORY_URL", ""))
        repo = os.environ.get("MOCKS_REPO", "").strip().strip("/")
        group = os.environ.get("MOCKS_GROUP_PATH", "").strip().strip("/")
        missing = [
            name
            for name, value in (
                ("ARTIFACTORY_URL", base_url),
                ("MOCKS_REPO", repo),
                ("MOCKS_GROUP_PATH", group),
            )
            if not value
        ]
        if missing:
            raise RuntimeError(f"{', '.join(missing)} not set. Copy .env.example to .env and fill it in.")
        if "/" not in group:
            group = group.replace(".", "/")  # com.example.mocks -> com/example/mocks

        ca_bundle = os.environ.get("ARTIFACTORY_CA_BUNDLE", "").strip()
        return cls(
            base_url=base_url,
            token=os.environ.get("ARTIFACTORY_TOKEN", "").strip() or None,
            repo=repo,
            group_path=group,
            systems=parse_systems(os.environ.get("MOCKS_SYSTEMS", "")),
            verify=ca_bundle or _env_bool("ARTIFACTORY_VERIFY_SSL", True),
            timeout=float(os.environ.get("ARTIFACTORY_TIMEOUT", "30")),
            max_concurrency=int(os.environ.get("MOCKS_MAX_CONCURRENCY", "8")),
            cache_ttl=float(os.environ.get("MOCKS_CACHE_TTL", "300")),
            max_body_chars=int(os.environ.get("MOCKS_MAX_BODY_CHARS", "20000")),
        )
