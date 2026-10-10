"""Thin async client for the JFrog Artifactory REST API."""

from __future__ import annotations

import asyncio
from urllib.parse import quote

import httpx

from .config import Settings

RETRY_STATUSES = {429, 500, 502, 503, 504}
MAX_ATTEMPTS = 3
MAX_DOWNLOAD_BYTES = 100 * 1024 * 1024


class ArtifactoryError(RuntimeError):
    """An error from Artifactory, with a message that is useful to show to the caller."""


class ArtifactoryNotFound(ArtifactoryError):
    """404: the folder or file doesn't exist, or the caller can't see it."""


def _retry_delay(resp: httpx.Response | None, attempt: int) -> float:
    """Seconds to wait before the next attempt: Retry-After if sent, else 0.5s, 1s, 2s..."""
    if resp is not None:
        try:
            return min(float(resp.headers["retry-after"]), 30.0)
        except (KeyError, ValueError):
            pass
    return 0.5 * 2 ** (attempt - 1)


def _detail(resp: httpx.Response) -> str:
    try:
        errors = resp.json().get("errors") or []
        detail = errors[0].get("message", "") if errors else resp.text
    except (ValueError, AttributeError):
        detail = resp.text
    return (detail or "").strip()[:300]


class ArtifactoryClient:
    def __init__(self, settings: Settings, transport: httpx.AsyncBaseTransport | None = None):
        headers = {"User-Agent": "artifactory-mocks-mcp"}
        if settings.token:
            headers["Authorization"] = f"Bearer {settings.token}"
        self._client = httpx.AsyncClient(
            base_url=settings.base_url,
            headers=headers,
            verify=settings.verify,
            timeout=settings.timeout,
            transport=transport,
            follow_redirects=True,
        )
        self._sleep = asyncio.sleep  # replaced in tests

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _get(self, path: str) -> httpx.Response:
        """GET a path under the base URL, retrying network errors, 429 and 5xx."""
        url = "/" + quote(path.lstrip("/"), safe="/")
        resp: httpx.Response | None = None
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                resp = await self._client.get(url)
            except httpx.HTTPError as exc:
                if attempt == MAX_ATTEMPTS:
                    raise ArtifactoryError(
                        f"Could not reach Artifactory at {self._client.base_url}: {exc}"
                    ) from exc
                await self._sleep(_retry_delay(None, attempt))
                continue
            if resp.status_code in RETRY_STATUSES and attempt < MAX_ATTEMPTS:
                await self._sleep(_retry_delay(resp, attempt))
                continue
            break
        assert resp is not None

        if resp.status_code < 400:
            return resp
        detail = _detail(resp)
        if resp.status_code == 401:
            raise ArtifactoryError(
                "Artifactory needs a token for this (401). Set ARTIFACTORY_TOKEN to an identity "
                "or access token, or check that it hasn't expired."
            )
        if resp.status_code == 403:
            raise ArtifactoryError(
                f"Forbidden (403) for {path}: {detail}. The token, or anonymous access, can't read it."
            )
        if resp.status_code == 404:
            raise ArtifactoryNotFound(f"Not found (404): {path}")
        if resp.status_code == 429:
            raise ArtifactoryError(f"Artifactory rate limit (429) for {path}. Wait and retry.")
        raise ArtifactoryError(f"Artifactory returned {resp.status_code} for {path}: {detail}")

    async def list_folder(self, repo_path: str) -> list[tuple[str, bool]]:
        """A folder's children as [(name, is_folder)], via the storage API."""
        data = (await self._get(f"api/storage/{repo_path}")).json()
        return [
            (child["uri"].strip("/"), bool(child.get("folder")))
            for child in data.get("children", [])
            if child.get("uri")
        ]

    async def get_text(self, repo_path: str) -> str:
        return (await self._get(repo_path)).text

    async def get_bytes(self, repo_path: str) -> bytes:
        content = (await self._get(repo_path)).content
        if len(content) > MAX_DOWNLOAD_BYTES:
            raise ArtifactoryError(f"{repo_path} is larger than 100 MB; refusing to load it.")
        return content
