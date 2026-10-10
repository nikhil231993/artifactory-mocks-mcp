"""MCP server exposing tools to find WireMock provider mocks stored in JFrog Artifactory."""

from __future__ import annotations

from typing import Any

from mcp.server.fastmcp import FastMCP

from .client import ArtifactoryClient
from .config import Settings
from .mocks import MockService

mcp = FastMCP("artifactory-mocks")

_service: MockService | None = None


def _get_service() -> MockService:
    global _service
    if _service is None:
        settings = Settings.from_env()
        _service = MockService(ArtifactoryClient(settings), settings)
    return _service


@mcp.tool()
async def find_mock(
    path: str,
    method: str | None = None,
    system: str | None = None,
    component: str | None = None,
    include_bodies: bool = True,
    max_stubs: int = 10,
) -> dict[str, Any]:
    """Find the WireMock stubs (provider mocks) for an API endpoint, with their responses.

    path: the endpoint's path, as a dependency graph reports it (/v1/customers/customer-core/{id}),
    or a full URL. {params} match any value, so {id} matches a stub's {cgid} or a concrete C123.
    method: GET, POST, ... Optional, but recommended.
    system: the provider system, e.g. customer-master. Its component folders come from
    MOCKS_SYSTEMS. Omit to search every configured system.
    component: search only this component folder, e.g. cm-addresses.

    For each component, the most recently built -SNAPSHOT version is used. Returns the matching
    stubs, best first: each stub's request (method, URL and other conditions such as body
    patterns or headers, which tell same-path stubs apart) and response (status, headers and
    body from __files). 'artifacts' says which build each component came from; read 'notes'
    for skipped components and other warnings.
    """
    service = _get_service()
    return await service.find(
        path,
        method=method,
        system=system,
        component=component,
        include_bodies=include_bodies,
        max_stubs=max(1, min(max_stubs, 100)),
    )


@mcp.tool()
async def list_mock_components(system: str | None = None, with_builds: bool = False) -> dict[str, Any]:
    """List the configured systems and their mock component folders in Artifactory.

    with_builds=True also shows the build each component would use (the most recently built
    -SNAPSHOT version). That reads every component's metadata, so it's slower.
    """
    return await _get_service().list_components(system, with_builds=with_builds)


@mcp.tool()
async def get_mock_stubs(
    component: str,
    version: str | None = None,
    include_bodies: bool = False,
    max_stubs: int = 200,
) -> dict[str, Any]:
    """List every stub in a mock component, e.g. cm-addresses.

    version: a folder name such as 1.3.1-SNAPSHOT (its newest build) or 1.2.0-484a3fa.
    Default: the most recently built -SNAPSHOT version. include_bodies adds response bodies.
    """
    return await _get_service().stubs(
        component, version=version, include_bodies=include_bodies, max_stubs=max(1, min(max_stubs, 1000))
    )


@mcp.tool()
async def get_mock_file(component: str, file_path: str, version: str | None = None) -> dict[str, Any]:
    """Read one file from a mock component's zip: a mapping file, mock.properties, or a response
    body (a stub's bodyFileName works as is). version works as in get_mock_stubs.
    """
    return await _get_service().file(component, file_path, version=version)


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
