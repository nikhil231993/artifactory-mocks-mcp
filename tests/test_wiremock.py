"""Reading mock zips and matching paths against WireMock stubs."""

import json

import pytest
from fake_artifactory import make_zip

from artifactory_mocks_mcp.wiremock import MockArchive, Stub, match_stub, match_stubs, request_path


def stub(request, **extra):
    return Stub("mappings/x.json", 0, {"request": request, "response": {"status": 200}, **extra})


@pytest.mark.parametrize(
    "value, path",
    [
        ("/v1/customers/{id}", "/v1/customers/{id}"),
        ("v1/customers/{id}", "/v1/customers/{id}"),
        ("https://api.example.com/v1/customers/C1?x=1", "/v1/customers/C1"),
        ("api.example.com:8443/v1/customers", "/v1/customers"),
        ("//v1//customers/", "/v1/customers"),
        ("/", "/"),
    ],
)
def test_request_path(value, path):
    assert request_path(value) == path


@pytest.mark.parametrize(
    "request_, path, expected",
    [
        ({"urlPathTemplate": "/v1/customers/{cgid}"}, "/v1/customers/{id}", "exact"),
        ({"urlPathTemplate": "/v1/customers/{cgid}"}, "/v1/customers/C1", "exact"),
        ({"urlPath": "/v1/customers/C1"}, "/v1/customers/{id}", "exact"),
        ({"url": "/v1/customers/C1?expand=all"}, "/v1/customers/{id}", "exact"),
        ({"urlPathPattern": "/v1/customers/[A-Z][0-9]+"}, "/v1/customers/{id}", "pattern"),
        ({"urlPattern": "/v1/customers\\?.*"}, "/v1/customers", "pattern"),
        ({"urlPathTemplate": "/v1/customers/{cgid}"}, "/customer-master/v1/customers/{id}", "suffix"),
        ({}, "/anything", "any-url"),
        ({"urlPathTemplate": "/v1/customers/{cgid}"}, "/v1/accounts/{id}", None),
        ({"urlPathTemplate": "/v1/customers/{cgid}"}, "/v1/customers/{id}/addresses", None),
        # A placeholder in the request never matches a fixed word of a template.
        ({"urlPathTemplate": "/v1/customers/{cgid}/addresses"}, "/v1/customers/customer-core/{id}", None),
        ({"urlPathTemplate": "/v1/customers/search"}, "/v1/customers/{id}", None),
        ({"urlPath": "/v1/customers/search"}, "/v1/customers/{id}", "exact"),  # concrete stub: may be an id
        ({"urlPathTemplate": "/{a}/{b}"}, "/x/{a}/{b}", None),  # suffix needs 2 literal segments
        ({"urlPathPattern": "[invalid"}, "/v1", None),
    ],
)
def test_match_types(request_, path, expected):
    match = match_stub(stub({"method": "GET", **request_}), path)
    assert (match.match_type if match else None) == expected


def test_method_rules():
    get = stub({"method": "GET", "urlPath": "/v1/x"})
    any_method = stub({"urlPath": "/v1/x"})
    assert match_stub(get, "/v1/x", "post") is None
    assert match_stub(any_method, "/v1/x", "POST").score < match_stub(get, "/v1/x", "GET").score
    assert match_stub(get, "/v1/x") is not None  # no method given: any method matches


def test_more_literal_segments_rank_first():
    general = stub({"method": "GET", "urlPathTemplate": "/v1/{a}/{b}"})
    specific = stub({"method": "GET", "urlPathTemplate": "/v1/customers/{id}"})
    assert [m.stub for m in match_stubs([general, specific], "/v1/customers/C1")] == [specific, general]


def test_archive_reads_every_mapping_file_shape_and_finds_the_root():
    archive = MockArchive.from_zip(
        make_zip(
            {
                "root/mock.properties": "# comment\norg=CM\nurl=git@x:CM/y.git\n",
                "root/mappings/a.json": json.dumps({"mappings": [{"request": {"url": "/a"}}]}),
                "root/mappings/b.json": json.dumps([{"request": {"url": "/b"}}, {"not": "a stub"}]),
                "root/mappings/c.json": json.dumps({"request": {"url": "/c"}, "response": {}}),
                "root/mappings/broken.json": "{not json",
                "root/__files/body.json": "{}",
                "__MACOSX/root/._a.json": "junk",
            }
        )
    )
    assert archive.root == "root/"
    assert [s.url for s in archive.stubs] == [("url", "/a"), ("url", "/b"), ("url", "/c")]
    assert archive.errors and archive.errors[0].startswith("mappings/broken.json")
    assert archive.properties == {"org": "CM", "url": "git@x:CM/y.git"}
    assert archive.body_file("/body.json") == b"{}"
    assert archive.body_file("missing.json") is None


def test_archive_rejects_non_zip():
    with pytest.raises(ValueError, match="not a valid zip"):
        MockArchive.from_zip(b"<html>login page</html>")
