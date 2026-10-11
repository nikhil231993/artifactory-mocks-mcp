"""Find the WireMock stubs for an endpoint across the configured mock components."""

from __future__ import annotations

import asyncio
import fnmatch
import json
import posixpath
import time
from dataclasses import dataclass
from typing import Any

from .client import ArtifactoryClient, ArtifactoryError, ArtifactoryNotFound
from .config import Settings
from .kafka import KafkaEvent
from .versions import (
    Build,
    build_from_listing,
    format_timestamp,
    is_snapshot,
    latest_build,
    parse_snapshot_metadata,
)
from .wiremock import (
    RESPONSE_KEYS,
    MockArchive,
    Stub,
    StubMatch,
    match_stubs,
    request_conditions,
    request_path,
)

MAX_CACHED_ARCHIVES = 256
MAX_LISTED_TOPICS = 30


class NoSnapshotError(ArtifactoryError):
    """The component has no -SNAPSHOT version to choose from."""


@dataclass(frozen=True)
class Choice:
    build: Build
    considered: tuple[Build, ...]  # every snapshot build compared, for transparency


def _limit(text: str, max_chars: int) -> tuple[Any, bool]:
    """(body, truncated). JSON bodies come back parsed; long ones are cut as text."""
    if len(text) > max_chars:
        return text[:max_chars], True
    try:
        return json.loads(text), False
    except ValueError:
        return text, False  # e.g. a {{template}} outside a JSON string


def _limit_json(value: Any, max_chars: int) -> tuple[Any, bool]:
    """(value, truncated): the value as is, or its JSON text cut to max_chars."""
    dumped = json.dumps(value)
    if len(dumped) <= max_chars:
        return value, False
    return dumped[:max_chars], True


class MockService:
    def __init__(self, client: ArtifactoryClient, settings: Settings):
        self.client = client
        self.settings = settings
        self._listings: dict[str, tuple[float, list[tuple[str, bool]]]] = {}
        self._choices: dict[tuple[str, str | None], tuple[float, Choice]] = {}
        self._archives: dict[str, MockArchive] = {}
        self._sem = asyncio.Semaphore(settings.max_concurrency)
        self._clock = time.monotonic

    # ---- Artifactory paths and caching ---------------------------------------------

    def _path(self, *parts: str) -> str:
        """Repo-relative path under the mocks group, e.g. repo/com/example/mocks/cm-x/1.0."""
        return "/".join([self.settings.repo, self.settings.group_path, *(p.strip("/") for p in parts if p)])

    def _fresh(self, stamp: float) -> bool:
        return self._clock() - stamp < self.settings.cache_ttl

    async def _children(self, *parts: str) -> list[tuple[str, bool]]:
        path = self._path(*parts)
        cached = self._listings.get(path)
        if cached and self._fresh(cached[0]):
            return cached[1]
        async with self._sem:
            children = await self.client.list_folder(path)
        self._listings[path] = (self._clock(), children)
        return children

    # ---- which components, which build -----------------------------------------------

    def patterns(self, system: str | None) -> list[str]:
        systems = self.settings.systems
        if not systems:
            raise ValueError(
                "No systems are configured. Set MOCKS_SYSTEMS (e.g. 'customer-master=cm-*'), "
                "or pass component."
            )
        if system is None:
            return [p for globs in systems.values() for p in globs]
        key = system.strip().lower()
        if key not in systems:
            raise ValueError(
                f"Unknown system '{system}'. Configured systems: {', '.join(sorted(systems))}. "
                "Add it to MOCKS_SYSTEMS, or pass component."
            )
        return systems[key]

    async def components(self, system: str | None = None) -> list[str]:
        """Component folders matching the system's patterns (all systems if None)."""
        patterns = self.patterns(system)
        return sorted(
            name
            for name, folder in await self._children()
            if folder and any(fnmatch.fnmatchcase(name, p) for p in patterns)
        )

    async def choose(self, component: str, version: str | None = None) -> Choice:
        """The build to use: the most recently built snapshot, or the version asked for."""
        key = (component, version)
        cached = self._choices.get(key)
        if cached and self._fresh(cached[0]):
            return cached[1]
        choice = await self._choose(component, version)
        self._choices[key] = (self._clock(), choice)
        return choice

    async def _choose(self, component: str, version: str | None) -> Choice:
        if version and not is_snapshot(version):
            build = Build(version, version, "")  # a fixed version, e.g. 1.2.0-484a3fa
            return Choice(build, (build,))

        folders = {name for name, folder in await self._children(component) if folder}
        snapshots = sorted(v for v in folders if is_snapshot(v))
        if version:
            if version not in folders:
                raise ArtifactoryNotFound(
                    f"{component} has no version {version}. Snapshot versions: "
                    f"{', '.join(snapshots) or 'none'}"
                )
            snapshots = [version]
        if not snapshots:
            raise NoSnapshotError(
                f"{component}: skipped, it has no -SNAPSHOT version ({len(folders)} other versions)"
            )

        builds = await asyncio.gather(*(self._snapshot_build(component, v) for v in snapshots))
        found = tuple(b for b in builds if b)
        best = latest_build(list(found))
        if best is None:
            raise ArtifactoryError(
                f"{component}: no zip build found in its snapshot folders ({', '.join(snapshots)})"
            )
        return Choice(best, found)

    async def _snapshot_build(self, component: str, version: str) -> Build | None:
        """Newest zip build in a -SNAPSHOT folder: from maven-metadata.xml, else the file names."""
        try:
            async with self._sem:
                text = await self.client.get_text(self._path(component, version, "maven-metadata.xml"))
            build = parse_snapshot_metadata(text, version)
        except ArtifactoryNotFound:
            build = None
        if build:
            return build
        files = [name for name, folder in await self._children(component, version) if not folder]
        return build_from_listing(files, component, version)

    async def archive(self, component: str, build: Build) -> MockArchive:
        """The build's zip, downloaded once and kept: a given build never changes."""
        path = self._path(component, build.version, build.file_name(component))
        if path not in self._archives:
            async with self._sem:
                data = await self.client.get_bytes(path)
            try:
                archive = MockArchive.from_zip(data)
            except ValueError as exc:
                raise ArtifactoryError(f"{path}: {exc}") from exc
            if len(self._archives) >= MAX_CACHED_ARCHIVES:
                self._archives.pop(next(iter(self._archives)))
            self._archives[path] = archive
        return self._archives[path]

    # ---- result formatting ---------------------------------------------------------

    def artifact_info(self, component: str, choice: Choice, archive: MockArchive) -> dict[str, Any]:
        build = choice.build
        info: dict[str, Any] = {
            "version": build.version,
            "build": build.value,
            "built": format_timestamp(build.updated),
            "download_url": f"{self.settings.base_url}/"
            + self._path(component, build.version, build.file_name(component)),
            "source_repo": archive.properties.get("url"),
            "org": archive.properties.get("org"),
        }
        if len(choice.considered) > 1:
            newest_first = sorted(choice.considered, key=lambda b: b.updated, reverse=True)
            info["snapshots_considered"] = [
                {"version": b.version, "built": format_timestamp(b.updated)} for b in newest_first
            ]
        return {k: v for k, v in info.items() if v is not None}

    def stub_entry(
        self,
        archive: MockArchive,
        stub: Stub,
        include_bodies: bool,
        match: StubMatch | None = None,
    ) -> dict[str, Any]:
        key, value = stub.url
        entry: dict[str, Any] = {"mapping_file": stub.file, "stub_index": stub.index}
        for field in ("name", "id", "uuid"):
            if stub.raw.get(field):
                entry[field] = stub.raw[field]
        entry["request"] = {"method": stub.method, **({key: value} if key else {})}
        conditions = request_conditions(stub)
        if conditions:
            entry["request"]["conditions"] = conditions
        if match:
            entry["match_type"] = match.match_type
            entry["score"] = match.score
        entry["response"] = self._response(archive, stub, include_bodies)
        return entry

    def _response(self, archive: MockArchive, stub: Stub, include_bodies: bool) -> dict[str, Any]:
        resp = stub.response
        out = {key: resp[key] for key in RESPONSE_KEYS if key in resp}
        if not include_bodies:
            return out

        max_chars = self.settings.max_body_chars
        text: str | None = None
        if "jsonBody" in resp:
            text = json.dumps(resp["jsonBody"])
            body, truncated = _limit_json(resp["jsonBody"], max_chars)
        elif "body" in resp:
            text = str(resp["body"])
            body, truncated = _limit(text, max_chars)
        elif "base64Body" in resp:
            out["body_note"] = "base64 body, not returned"
            return out
        elif resp.get("bodyFileName"):
            data = archive.body_file(str(resp["bodyFileName"]))
            if data is None:
                out["body_missing"] = f"__files/{resp['bodyFileName']} is not in the zip"
                return out
            text = data.decode("utf-8", errors="replace")
            body, truncated = _limit(text, max_chars)
        else:
            return out

        out["body"] = body
        if truncated:
            out["body_truncated"] = True
            out["body_chars"] = len(text)
        return out

    def kafka_entry(self, event: KafkaEvent) -> dict[str, Any]:
        entry: dict[str, Any] = {
            "topic_folder": event.folder or None,
            "file": event.file,
            "envelope_file": event.envelope_file,
            "event_type": event.event_type,
            "customer_type": event.customer_type,
        }
        for label, part in (("key", event.key), ("value", event.value)):
            if part is None:
                continue
            value, truncated = _limit_json(part.value, self.settings.max_body_chars)
            out = {"format": part.format, "schema_file": part.schema_file, "value": value}
            if truncated:
                out["value_truncated"] = True
            entry[label] = {k: v for k, v in out.items() if v is not None}
        return {k: v for k, v in entry.items() if v is not None}

    def kafka_topics(self, archive: MockArchive) -> list[dict[str, Any]]:
        """A zip's Kafka topics: each topic folder's events and schema files."""
        topics: dict[str, dict[str, Any]] = {}
        for event in archive.kafka_events:
            topic = topics.setdefault(event.folder, {"topic_folder": event.folder or None, "events": []})
            info = {"file": event.file, "event_type": event.event_type, "customer_type": event.customer_type}
            topic["events"].append({k: v for k, v in info.items() if v is not None})
        for folder, topic in topics.items():
            directory = posixpath.join("kafka", folder) if folder else "kafka"
            topic["schemas"] = sorted(
                n for n in archive.files if posixpath.dirname(n) == directory and n.endswith(".avsc")
            )
        return [{k: v for k, v in t.items() if v is not None} for t in topics.values()]

    def kafka_schemas(
        self, component: str, archive: MockArchive, events: list[KafkaEvent], notes: list[str]
    ) -> dict[str, Any]:
        """The Avro schemas the events name, by path in the zip."""
        schemas: dict[str, Any] = {}
        files = sorted({p.schema_file for e in events for p in (e.key, e.value) if p and p.schema_file})
        for path in files:
            data = archive.files.get(path)
            if data is None:
                notes.append(f"{component}: schema {path} is not in the zip")
                continue
            text = data.decode("utf-8", errors="replace")
            schemas[path], _ = _limit(text, self.settings.max_body_chars)
        return schemas

    # ---- tool entry points ----------------------------------------------------------

    async def _load(self, component: str) -> tuple[Choice, MockArchive] | str:
        """The component's chosen build and zip, or why it was skipped."""
        try:
            choice = await self.choose(component)
            return choice, await self.archive(component, choice.build)
        except ArtifactoryError as exc:
            message = str(exc)
            return message if message.startswith(component) else f"{component}: {message}"

    async def _search(
        self, component: str, path: str, method: str | None
    ) -> tuple[Choice, MockArchive, list[StubMatch]] | str:
        loaded = await self._load(component)
        if isinstance(loaded, str):
            return loaded
        choice, archive = loaded
        return choice, archive, match_stubs(archive.stubs, path, method)

    async def find(
        self,
        api_url: str,
        method: str | None = None,
        system: str | None = None,
        component: str | None = None,
        include_bodies: bool = True,
        max_stubs: int = 10,
    ) -> dict[str, Any]:
        path = request_path(api_url)
        components = [component] if component else await self.components(system)
        notes: list[str] = []
        if not components:
            notes.append(
                f"No component folder under {self._path()} matches {', '.join(self.patterns(system))}."
            )

        outcomes = await asyncio.gather(*(self._search(c, path, method) for c in components))
        found: list[tuple[str, MockArchive, StubMatch]] = []
        artifacts: dict[str, Any] = {}
        for name, outcome in zip(components, outcomes):
            if isinstance(outcome, str):
                notes.append(outcome)
                continue
            choice, archive, matches = outcome
            if archive.errors:
                notes.append(f"{name}: could not parse {'; '.join(archive.errors)}")
            if matches:
                artifacts[name] = self.artifact_info(name, choice, archive)
                found += [(name, archive, m) for m in matches]

        found.sort(key=lambda f: (-f[2].score, f[0], f[2].stub.file, f[2].stub.index))
        shown = found[:max_stubs]
        if len(found) > len(shown):
            notes.append(
                f"Showing {len(shown)} of {len(found)} matching stubs. Raise max_stubs, or pass "
                "component to narrow the search."
            )
        if not found and components:
            notes.append(
                "No stub matches this path"
                + (f" and method {method.upper()}" if method else "")
                + ". Check the path, try without method, or list a component's stubs with get_mock_stubs."
            )
        if len({m.stub.url for _, _, m in shown[:2]}) == 1 and len(shown) > 1:
            notes.append(
                "Several stubs share this path. They differ in request.conditions (body, headers, "
                "priority...): pick the one that fits the request you will send."
            )

        result: dict[str, Any] = {
            "query": {
                "path": path,
                "method": method.upper() if method else None,
                "system": system,
                "component": component,
            },
            "components_searched": len(components),
            "total_matches": len(found),
            "artifacts": artifacts,
            "matches": [
                {"component": name, **self.stub_entry(archive, m.stub, include_bodies, m)}
                for name, archive, m in shown
            ],
        }
        if notes:
            result["notes"] = notes
        return result

    async def find_kafka(
        self,
        topic: str,
        event_type: str | None = None,
        system: str | None = None,
        component: str | None = None,
        include_schemas: bool = False,
        max_events: int = 20,
    ) -> dict[str, Any]:
        components = [component] if component else await self.components(system)
        notes: list[str] = []
        if not components:
            notes.append(
                f"No component folder under {self._path()} matches {', '.join(self.patterns(system))}."
            )

        outcomes = await asyncio.gather(*(self._load(c) for c in components))
        found: list[tuple[str, MockArchive, KafkaEvent]] = []
        artifacts: dict[str, Any] = {}
        topics_seen: set[str] = set()
        on_topic: list[KafkaEvent] = []
        for name, outcome in zip(components, outcomes):
            if isinstance(outcome, str):
                notes.append(outcome)
                continue
            choice, archive = outcome
            if archive.kafka_errors:
                notes.append(f"{name}: could not parse {'; '.join(archive.kafka_errors)}")
            topics_seen.update(e.folder for e in archive.kafka_events if e.folder)
            events = [e for e in archive.kafka_events if e.matches_topic(topic)]
            on_topic += events
            matched = [e for e in events if not event_type or e.matches_event(event_type)]
            if matched:
                artifacts[name] = self.artifact_info(name, choice, archive)
                found += [(name, archive, e) for e in matched]

        shown = found[:max_events]
        if len(found) > len(shown):
            notes.append(f"Showing {len(shown)} of {len(found)} events. Raise max_events, or pass event_type.")
        if not on_topic and components:
            listed = sorted(topics_seen)
            notes.append(
                f"No Kafka mock for topic '{topic}'. "
                + (
                    f"Topic folders found: {', '.join(listed[:MAX_LISTED_TOPICS])}"
                    + (" ..." if len(listed) > MAX_LISTED_TOPICS else "")
                    if listed
                    else "None of the searched components has a kafka/ folder."
                )
            )
        elif not found and event_type:
            kinds = sorted({f"{e.event_type or '-'} ({e.name})" for e in on_topic})
            notes.append(f"No '{event_type}' event on this topic. Its events: {', '.join(kinds)}")
        elif len(found) > 1 and not event_type:
            notes.append(
                "Several events on this topic. They differ in event_type and customer_type: "
                "pass event_type to get one."
            )

        result: dict[str, Any] = {
            "query": {"topic": topic, "event_type": event_type, "system": system, "component": component},
            "components_searched": len(components),
            "total_matches": len(found),
            "artifacts": artifacts,
            "events": [{"component": name, **self.kafka_entry(e)} for name, _, e in shown],
        }
        if include_schemas and shown:
            by_component: dict[str, tuple[MockArchive, list[KafkaEvent]]] = {}
            for name, archive, event in shown:
                by_component.setdefault(name, (archive, []))[1].append(event)
            result["schemas"] = {
                name: self.kafka_schemas(name, archive, events, notes)
                for name, (archive, events) in by_component.items()
            }
        if notes:
            result["notes"] = notes
        return result

    async def list_components(self, system: str | None = None, with_builds: bool = False) -> dict[str, Any]:
        children = [name for name, folder in await self._children() if folder]
        names = [system.strip().lower()] if system else sorted(self.settings.systems)
        systems: dict[str, Any] = {}
        for name in names:
            patterns = self.patterns(name)
            systems[name] = {
                "patterns": patterns,
                "components": sorted(
                    c for c in children if any(fnmatch.fnmatchcase(c, p) for p in patterns)
                ),
            }
        result: dict[str, Any] = {"group": self._path(), "systems": systems}
        if with_builds:
            components = sorted({c for s in systems.values() for c in s["components"]})
            choices = await asyncio.gather(
                *(self.choose(c) for c in components), return_exceptions=True
            )
            result["builds"] = {
                c: (
                    {"error": str(choice)}
                    if isinstance(choice, Exception)
                    else {
                        "version": choice.build.version,
                        "build": choice.build.value,
                        "built": format_timestamp(choice.build.updated),
                    }
                )
                for c, choice in zip(components, choices)
            }
        return result

    async def stubs(
        self,
        component: str,
        version: str | None = None,
        include_bodies: bool = False,
        max_stubs: int = 200,
    ) -> dict[str, Any]:
        choice = await self.choose(component, version)
        archive = await self.archive(component, choice.build)
        shown = archive.stubs[:max_stubs]
        result: dict[str, Any] = {
            "component": component,
            "artifact": self.artifact_info(component, choice, archive),
            "total_stubs": len(archive.stubs),
            "stubs": [self.stub_entry(archive, stub, include_bodies) for stub in shown],
        }
        notes = []
        if len(archive.stubs) > len(shown):
            notes.append(f"Showing {len(shown)} of {len(archive.stubs)} stubs; raise max_stubs.")
        if archive.errors:
            notes.append(f"Could not parse: {'; '.join(archive.errors)}")
        if archive.kafka_events:
            result["kafka_topics"] = self.kafka_topics(archive)
            notes.append("The zip also has Kafka message mocks: get them with find_kafka_mock.")
        if archive.kafka_errors:
            notes.append(f"Could not parse: {'; '.join(archive.kafka_errors)}")
        if notes:
            result["notes"] = notes
        return result

    async def file(self, component: str, file_path: str, version: str | None = None) -> dict[str, Any]:
        choice = await self.choose(component, version)
        archive = await self.archive(component, choice.build)
        name = file_path.strip().lstrip("/")
        data = archive.files.get(name)
        if data is None:
            name = posixpath.join("__files", name)  # allow a bodyFileName as given in a stub
            data = archive.files.get(name)
        if data is None:
            base = posixpath.basename(file_path)
            similar = [n for n in archive.files if posixpath.basename(n) == base][:5]
            listing = similar or sorted(archive.files)[:50]
            raise ValueError(
                f"'{file_path}' is not in {component} {choice.build.value}. "
                f"{'Similar' if similar else 'Files'}: {', '.join(listing)}"
            )
        text = data.decode("utf-8", errors="replace")
        limit = self.settings.max_body_chars
        result: dict[str, Any] = {
            "component": component,
            "build": choice.build.value,
            "path": name,
            "size": len(data),
            "content": text[:limit],
        }
        if len(text) > limit:
            result["truncated"] = True
        return result
