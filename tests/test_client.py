"""HTTP behaviour: auth, retries, errors, and settings."""

import pytest

from artifactory_mocks_mcp import server
from artifactory_mocks_mcp.config import Settings, normalise_base_url, parse_systems

LISTING = "api/storage/mocks-repo/com/example/mocks"


async def test_no_token_sends_no_auth_header(service, fake):
    await server.list_mock_components()
    assert "authorization" not in fake.requests[0].headers


async def test_token_is_sent_as_bearer(make_service, fake):
    make_service(token="secret")
    await server.list_mock_components()
    assert fake.requests[0].headers["authorization"] == "Bearer secret"
    assert fake.requests[0].url.path == f"/artifactory/{LISTING}"


async def test_retries_5xx_and_429_then_succeeds(service, fake):
    fake.failures[LISTING] = [503, 429]
    result = await server.list_mock_components()
    assert result["systems"]["customer-master"]["components"]
    assert len(fake.requests) == 3


async def test_gives_up_after_three_attempts(service, fake):
    fake.failures[LISTING] = [500, 500, 500]
    with pytest.raises(Exception, match="returned 500"):
        await server.list_mock_components()


@pytest.mark.parametrize(
    "status, message",
    [(401, "needs a token"), (403, "Forbidden"), (404, "Not found")],
)
async def test_errors_are_explained(service, fake, status, message):
    fake.failures[LISTING] = [status]
    with pytest.raises(Exception, match=message):
        await server.list_mock_components()


def test_parse_systems():
    assert parse_systems("Customer-Master=cm-* ; tlm=mock-tlm,mock-tlm-*;") == {
        "customer-master": ["cm-*"],
        "tlm": ["mock-tlm", "mock-tlm-*"],
    }
    with pytest.raises(RuntimeError, match="system=pattern"):
        parse_systems("cm-*")


def test_normalise_base_url():
    assert normalise_base_url("https://af.example.com/") == "https://af.example.com/artifactory"
    assert normalise_base_url("https://af.example.com/artifactory/") == "https://af.example.com/artifactory"


def test_settings_from_dotenv_and_env(tmp_path, monkeypatch):
    for key in ("ARTIFACTORY_URL", "ARTIFACTORY_TOKEN", "MOCKS_REPO", "MOCKS_GROUP_PATH", "MOCKS_SYSTEMS"):
        monkeypatch.setenv(key, "")  # restored after the test; empty counts as unset
    (tmp_path / ".env").write_text(
        "ARTIFACTORY_URL=https://af.corp\nMOCKS_REPO=mocks\nMOCKS_GROUP_PATH=com.example.mocks\n"
        "MOCKS_SYSTEMS=customer-master=cm-*\n"
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("MOCKS_REPO", "from-env")
    settings = Settings.from_env()
    assert settings.base_url == "https://af.corp/artifactory"
    assert settings.repo == "from-env"
    assert settings.group_path == "com/example/mocks"
    assert settings.systems == {"customer-master": ["cm-*"]}
    assert settings.token is None


def test_settings_missing(tmp_path, monkeypatch):
    for key in ("ARTIFACTORY_URL", "MOCKS_REPO", "MOCKS_GROUP_PATH"):
        monkeypatch.setenv(key, "")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("artifactory_mocks_mcp.config.PROJECT_ROOT", tmp_path)
    with pytest.raises(RuntimeError, match="ARTIFACTORY_URL, MOCKS_REPO, MOCKS_GROUP_PATH not set"):
        Settings.from_env()
