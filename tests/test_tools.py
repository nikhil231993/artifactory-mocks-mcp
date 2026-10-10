"""The MCP tools, end to end against the fake Artifactory."""

import pytest

from artifactory_mocks_mcp import server


async def test_find_mock_uses_most_recent_build_not_highest_version(service):
    result = await server.find_mock("/v1/customers/{id}/addresses", method="GET", system="customer-master")

    artifact = result["artifacts"]["cm-addresses"]
    # 1.3.1-SNAPSHOT has the higher number but was built in June; 1.2.0-SNAPSHOT in August.
    assert artifact["version"] == "1.2.0-SNAPSHOT"
    assert artifact["build"] == "1.2.0-20260819.052644-19"
    assert artifact["built"] == "2026-08-19T05:26:44Z"
    assert [s["version"] for s in artifact["snapshots_considered"]] == ["1.2.0-SNAPSHOT", "1.3.1-SNAPSHOT"]
    assert artifact["source_repo"] == "git@ghes.example.com:CM/cm-addresses.git"
    assert artifact["download_url"].endswith(
        "/com/example/mocks/cm-addresses/1.2.0-SNAPSHOT/cm-addresses-1.2.0-20260819.052644-19.zip"
    )

    match = result["matches"][0]
    assert match["component"] == "cm-addresses"
    assert match["request"] == {"method": "GET", "urlPathTemplate": "/v1/customers/{cgid}/addresses"}
    assert match["match_type"] == "exact"
    assert match["response"]["status"] == 200
    assert match["response"]["body"]["addresses"][0]["postCode"] == "3000"  # parsed JSON


async def test_find_mock_returns_every_stub_for_the_path_with_their_conditions(service):
    result = await server.find_mock(
        "https://stg01.example.com/v1/customers/C1/addresses/A1", method="patch", system="customer-master"
    )
    assert result["query"]["path"] == "/v1/customers/C1/addresses/A1"
    assert result["total_matches"] == 2
    first, second = result["matches"]
    assert first["request"]["conditions"]["bodyPatterns"][0] == {"matchesJsonPath": "$.validateOnly"}
    # Template placeholders are returned unfilled; inside JSON strings the body still parses.
    assert first["response"]["body"]["customerId"] == "{{request.path.cgid}}"
    assert first["response"]["transformers"] == ["response-template"]
    assert second["request"]["conditions"] == {"priority": 10}
    assert second["response"]["body"] == {"code": "INVALID_ADDRESS", "message": "Invalid address"}
    assert any("Several stubs share this path" in n for n in result["notes"])


async def test_find_mock_matches_templates_concrete_paths_and_regexes(service):
    # Graph template vs a stub's concrete urlPath.
    result = await server.find_mock("/v2/customers/{customerId}/addresses", method="GET")
    assert result["matches"][0]["request"]["urlPath"] == "/v2/customers/C1000001/addresses"

    # A zip with a top-level folder and no maven-metadata.xml: the build comes from file names.
    result = await server.find_mock("/v1/customers/customer-core/{id}", method="GET", system="customer-master")
    assert list(result["artifacts"]) == ["cm-customer-core"]  # not cm-addresses' /{cgid}/addresses
    assert result["artifacts"]["cm-customer-core"]["build"] == "2.0.0-20260902.090000-2"
    assert result["matches"][0]["response"]["body"]["name"] == "Test Customer"

    # A urlPathPattern regex, tried with sample values for {id}.
    result = await server.find_mock("/v1/customers/customer-core/{id}/status", method="GET")
    assert result["matches"][0]["request"]["urlPathPattern"] == "/v1/customers/customer-core/[0-9]+/status"
    assert result["matches"][0]["match_type"] == "pattern"


async def test_find_mock_notes_skipped_components_and_no_match(service):
    result = await server.find_mock("/v9/unknown", system="customer-master")
    assert result["total_matches"] == 0
    notes = " ".join(result["notes"])
    assert "cm-warnings: skipped, it has no -SNAPSHOT version" in notes
    assert "No stub matches this path" in notes
    assert result["components_searched"] == 3  # cm-addresses, cm-customer-core, cm-warnings


async def test_find_mock_wrong_method_finds_nothing(service):
    result = await server.find_mock("/v1/customers/{id}/addresses", method="DELETE", system="customer-master")
    assert result["total_matches"] == 0
    assert "and method DELETE" in " ".join(result["notes"])


async def test_find_mock_unknown_system_lists_configured_ones(service):
    with pytest.raises(ValueError, match="Configured systems: customer-master, tlm"):
        await server.find_mock("/v1/x", system="payments")


async def test_find_mock_component_and_options(make_service):
    make_service(max_body_chars=20)
    result = await server.find_mock("/v1/customers/{id}/addresses", component="cm-addresses", max_stubs=1)
    response = result["matches"][0]["response"]
    assert response["body_truncated"] is True and len(response["body"]) == 20

    result = await server.find_mock("/v1/customers/{id}/addresses", component="cm-addresses", include_bodies=False)
    assert "body" not in result["matches"][0]["response"]
    assert result["matches"][0]["response"]["bodyFileName"] == "SOR/Addresses/addressesReadResponse.json"


async def test_find_mock_caches_listings_and_zips(service, fake):
    await server.find_mock("/v1/customers/{id}/addresses", method="GET", system="customer-master")
    first = len(fake.requests)
    await server.find_mock("/v1/customers/{id}/addresses/{a}", method="PATCH", system="customer-master")
    assert len(fake.requests) == first  # everything came from the cache


async def test_list_mock_components(service):
    result = await server.list_mock_components()
    assert result["systems"]["customer-master"]["components"] == [
        "cm-addresses",
        "cm-customer-core",
        "cm-warnings",
    ]
    assert result["systems"]["tlm"]["components"] == ["mock-tlm-avp"]

    result = await server.list_mock_components("customer-master", with_builds=True)
    assert result["builds"]["cm-addresses"]["build"] == "1.2.0-20260819.052644-19"
    assert "no -SNAPSHOT version" in result["builds"]["cm-warnings"]["error"]


async def test_get_mock_stubs_default_and_pinned_versions(service):
    result = await server.get_mock_stubs("cm-addresses")
    assert result["total_stubs"] == 4
    assert result["artifact"]["version"] == "1.2.0-SNAPSHOT"
    assert "kafka/" in " ".join(result["notes"])
    assert "body" not in result["stubs"][0]["response"]

    result = await server.get_mock_stubs("cm-addresses", version="1.3.1-SNAPSHOT", include_bodies=True)
    assert result["artifact"]["build"] == "1.3.1-20260624.152900-3"
    assert result["stubs"][0]["response"]["body"] == {"addresses": [], "stale": True}

    result = await server.get_mock_stubs("cm-addresses", version="1.2.0-484a3fa", include_bodies=True)
    assert result["stubs"][0]["response"]["body"]["fixed"] is True

    result = await server.get_mock_stubs("mock-tlm-avp", version="1.0.1")
    assert result["stubs"][0]["request"] == {"method": "POST", "url": "/tlm/v1/verify"}


async def test_get_mock_stubs_unknown_version(service):
    with pytest.raises(Exception, match="has no version 9.9.9-SNAPSHOT"):
        await server.get_mock_stubs("cm-addresses", version="9.9.9-SNAPSHOT")


async def test_get_mock_file(service):
    result = await server.get_mock_file("cm-addresses", "SOR/Addresses/addressesV2Response.json")
    assert result["path"] == "__files/SOR/Addresses/addressesV2Response.json"
    assert '"version": 2' in result["content"]

    result = await server.get_mock_file("cm-addresses", "mock.properties")
    assert "org=CM" in result["content"]

    with pytest.raises(ValueError, match="Files: "):
        await server.get_mock_file("cm-addresses", "nope.json")
