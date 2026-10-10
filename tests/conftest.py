"""Shared fixtures: the MCP server wired to the fake Artifactory in devtools/."""

import sys
from pathlib import Path

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "devtools"))

import fake_artifactory  # noqa: E402

from artifactory_mocks_mcp import server  # noqa: E402
from artifactory_mocks_mcp.client import ArtifactoryClient  # noqa: E402
from artifactory_mocks_mcp.config import Settings  # noqa: E402
from artifactory_mocks_mcp.mocks import MockService  # noqa: E402

BASE = "https://af.test/artifactory"


def make_settings(**overrides) -> Settings:
    values = dict(
        base_url=BASE,
        token=None,
        repo=fake_artifactory.REPO,
        group_path=fake_artifactory.GROUP,
        systems={"customer-master": ["cm-*"], "tlm": ["mock-tlm*"]},
        verify=True,
        timeout=5,
        max_concurrency=4,
        cache_ttl=300,
        max_body_chars=20000,
    )
    values.update(overrides)
    return Settings(**values)


class FakeArtifactory:
    """Routes httpx requests to devtools/fake_artifactory.py and records them."""

    def __init__(self):
        self.requests: list[httpx.Request] = []
        self.failures: dict[str, list[int]] = {}  # path -> statuses to return first

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path.removeprefix("/artifactory/")
        queued = self.failures.get(path)
        if queued:
            return httpx.Response(queued.pop(0), json={"errors": [{"message": "injected"}]})
        status, content_type, body = fake_artifactory.handle(path)
        return httpx.Response(status, content=body, headers={"content-type": content_type})


@pytest.fixture
def fake():
    return FakeArtifactory()


@pytest.fixture
async def make_service(fake, monkeypatch):
    """Build a MockService on the fake and install it as the server's service."""
    clients: list[ArtifactoryClient] = []

    def build(**overrides) -> MockService:
        settings = make_settings(**overrides)
        client = ArtifactoryClient(settings, transport=httpx.MockTransport(fake))

        async def no_sleep(_seconds):
            return None

        client._sleep = no_sleep
        clients.append(client)
        service = MockService(client, settings)
        monkeypatch.setattr(server, "_service", service)
        return service

    yield build
    for client in clients:
        await client.aclose()


@pytest.fixture
def service(make_service):
    return make_service()
