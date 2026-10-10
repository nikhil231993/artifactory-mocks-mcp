"""Choosing the build: most recently built snapshot."""

from fake_artifactory import snapshot_metadata

from artifactory_mocks_mcp.versions import (
    Build,
    build_from_listing,
    format_timestamp,
    latest_build,
    parse_snapshot_metadata,
    version_key,
)


def test_parse_snapshot_metadata_picks_the_zip_entry():
    xml = snapshot_metadata("cm-a", "1.2.0-SNAPSHOT", [("20260818.101500", 18), ("20260819.052644", 19)])
    build = parse_snapshot_metadata(xml, "1.2.0-SNAPSHOT")
    assert build == Build("1.2.0-SNAPSHOT", "1.2.0-20260819.052644-19", "20260819052644")
    assert build.file_name("cm-a") == "cm-a-1.2.0-20260819.052644-19.zip"


def test_parse_snapshot_metadata_old_format_and_junk():
    old = (
        "<metadata><versioning><snapshot><timestamp>20260101.120000</timestamp>"
        "<buildNumber>4</buildNumber></snapshot></versioning></metadata>"
    )
    assert parse_snapshot_metadata(old, "2.0.0-SNAPSHOT").value == "2.0.0-20260101.120000-4"
    assert parse_snapshot_metadata("<metadata/>", "2.0.0-SNAPSHOT") is None
    assert parse_snapshot_metadata("not xml", "2.0.0-SNAPSHOT") is None


def test_build_from_listing_takes_newest_timestamp_then_build_number():
    files = [
        "cm-a-2.0.0-20260901.080000-1.zip",
        "cm-a-2.0.0-20260902.090000-2.zip",
        "cm-a-2.0.0-20260902.090000-3.pom",
        "cm-a-2.0.0-20260902.090000-3-sources.zip",
        "other-2.0.0-20261231.000000-9.zip",
    ]
    assert build_from_listing(files, "cm-a", "2.0.0-SNAPSHOT").value == "2.0.0-20260902.090000-2"
    assert build_from_listing([], "cm-a", "2.0.0-SNAPSHOT") is None


def test_latest_build_prefers_recency_over_version_number():
    june_131 = Build("1.3.1-SNAPSHOT", "1.3.1-20260624.152900-3", "20260624152900")
    august_120 = Build("1.2.0-SNAPSHOT", "1.2.0-20260819.052644-19", "20260819052644")
    assert latest_build([june_131, august_120]) == august_120
    unknown_a, unknown_b = Build("1.9.0-SNAPSHOT", "x", ""), Build("1.10.0-SNAPSHOT", "y", "")
    assert latest_build([unknown_b, unknown_a]) == unknown_b  # tie: higher version, as numbers
    assert latest_build([]) is None


def test_helpers():
    assert version_key("1.10.0-SNAPSHOT") > version_key("1.9.0-SNAPSHOT")
    assert format_timestamp("20260819052644") == "2026-08-19T05:26:44Z"
    assert format_timestamp("") is None
