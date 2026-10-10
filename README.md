# artifactory-mocks-mcp

An MCP server that finds **provider mocks** in **JFrog Artifactory**: given an API endpoint, it
returns the WireMock stubs that mock it, with their responses.

A provider mock is a fake version of an API: "when you call `GET /v1/customers/{id}`, answer
with this JSON". Teams publish them as zips to Artifactory so that callers can test without the
real service. This server lets an agent (or any MCP client) look them up by endpoint, for
example for each API that a dependency graph's `downstream_apis` reports.

It has 4 tools:

- **`find_mock`**: the main one. Endpoint in, matching stubs and response bodies out.
- **`list_mock_components`**: which mock components exist for each system, and which build
  each would use.
- **`get_mock_stubs`**: every stub in one component.
- **`get_mock_file`**: one file from a component's zip.

It reads Artifactory over its REST API, anonymously or with a token. It only reads.

## How the mocks are stored

The server expects this layout, which is how the target Artifactory is organised:

```
<repo>/<group path>/<component>/<version>/<component>-<version>.zip
e.g.  mocks-repo/com/example/mocks/cm-addresses/1.2.0-SNAPSHOT/cm-addresses-1.2.0-20260819.052644-19.zip
```

- **One zip per component**, for example `cm-addresses`, `cm-tax-profiles`. A component's
  zip holds the mocks *of* that component's API.
- **Versions in a component folder:**
  - `x.y.z-SNAPSHOT` folders, each with timestamped builds
    (`<component>-x.y.z-yyyyMMdd.HHmmss-N.zip`) and a `maven-metadata.xml` that names the newest;
  - fixed versions such as `1.2.0-484a3fa`;
  - `maven-metadata.xml` for the component.
- **Each zip is a WireMock project:**
  ```
  mock.properties        org=<team>, url=<git repo the mocks were built from>
  mappings/**/*.json     the stubs: {"mappings": [ {request, response}, ... ]}
  __files/**             response bodies, referenced by a stub's response.bodyFileName
  kafka/                 Kafka message mocks (not read yet)
  ```
- **A stub** looks like this:
  ```json
  {"request":  {"method": "PATCH",
                "urlPathTemplate": "/v1/customers/{cgid}/addresses/{addressId}",
                "bodyPatterns": [{"matchesJsonPath": "$.validateOnly"}]},
   "response": {"status": 200, "bodyFileName": "SOR/Addresses/patchResponse.json",
                "transformers": ["response-template"]}}
  ```

## Tools

### `find_mock`

Finds the stubs that mock an endpoint.

| Parameter | Default | Meaning |
|---|---|---|
| `path` | required | The endpoint, as the dependency graph reports it: `/v1/customers/customer-core/{id}`. A full URL (`https://host/v1/...`), host and path, or a concrete path (`/v1/customers/C123`) also work. The query string is ignored. |
| `method` | none | `GET`, `POST`, ... Optional, but recommended: without it, every method matches. |
| `system` | all systems | The provider system as the graph names it, e.g. `customer-master`. `MOCKS_SYSTEMS` says which component folders belong to it. |
| `component` | none | Search only this component folder, e.g. `cm-addresses`. Overrides `system`. |
| `include_bodies` | `true` | Return response bodies. `false` returns only the stub definitions, which is much smaller. |
| `max_stubs` | `10` | The most stubs to return (1–100) |

It returns:
- `matches`: the matching stubs, best first. Each has:
  - `component`, `mapping_file` and `stub_index`: where the stub is;
  - `request`: method, URL matcher, and `conditions`, which are the other things the stub
    requires (body patterns, headers, query parameters, priority, scenario state);
  - `match_type` and `score`: how well it matched (see below);
  - `response`: status, headers, `bodyFileName`, and the `body` (parsed if it's JSON).
- `artifacts`: for each component that matched, the build used (`version`, `build`, `built`),
  its `download_url`, `source_repo` and `org` from `mock.properties`, and
  `snapshots_considered` when there was a choice.
- `total_matches`, `components_searched`.
- `notes`: skipped components, parse errors, "several stubs share this path", and so on.

**Several stubs for one path** are normal: WireMock picks between them using the request body,
headers or priority. They are all returned, with their `conditions`, so the caller can pick the
one that fits the request it will send.

**Response bodies are returned as stored.** Stubs with `"transformers": ["response-template"]`
contain placeholders such as `{{request.path.cgid}}`, which WireMock fills in at run time.

### `list_mock_components`

| Parameter | Default | Meaning |
|---|---|---|
| `system` | all | One system only |
| `with_builds` | `false` | Also show which build each component would use. Reads every component's metadata, so it's slower. |

### `get_mock_stubs`

| Parameter | Default | Meaning |
|---|---|---|
| `component` | required | e.g. `cm-addresses` |
| `version` | most recent snapshot | A version folder: `1.3.1-SNAPSHOT` (its newest build) or a fixed one like `1.2.0-484a3fa` |
| `include_bodies` | `false` | Add response bodies |
| `max_stubs` | `200` | The most stubs to return |

### `get_mock_file`

Reads one file from a component's zip: a mapping file, `mock.properties`, or a response body.
A stub's `bodyFileName` works as it is (`SOR/Addresses/x.json` means `__files/SOR/Addresses/x.json`).
`component` and `version` work as in `get_mock_stubs`.

## How a lookup works

1. **Which components:** the folders under `<repo>/<group path>` that match the system's
   patterns in `MOCKS_SYSTEMS`, e.g. `customer-master=cm-*`.
2. **Which build of each:** the **most recently built** `-SNAPSHOT` version (see the
   architecture decisions). For each snapshot folder, the newest build comes from its
   `maven-metadata.xml`, or, if that's missing, from the timestamps in the file names. A component
   without any `-SNAPSHOT` version is skipped, with a note.
3. **Download and read the zip**, once: a given build never changes, so it's kept in memory.
   Folder listings and version choices are re-read after `MOCKS_CACHE_TTL` seconds (default 300).
4. **Match each stub** against the method and path:

   | Stub URL matcher | How it's matched | `match_type` |
   |---|---|---|
   | `urlPathTemplate`, `urlPath`, `url` | Segment by segment. A `{param}` matches any value: the graph's `{id}` matches the stub's `{cgid}` and a concrete `C123`. A `{param}` in the request never matches a fixed word of a `urlPathTemplate`. The query part of `url` is ignored. | `exact` |
   | the same, with extra leading segments on one side (e.g. a gateway prefix) | At least 2 fixed segments must match | `suffix` |
   | `urlPathPattern`, `urlPattern` (regex) | The regex must match the path; `{params}` are tried with sample values (`1`, `ABC123`, a UUID, ...) | `pattern` |
   | none | Matches every URL | `any-url` |

   The method must be equal, or the stub's method is `ANY`. Scores: exact 1000, pattern 800,
   suffix 500, any-url 100, plus 10 per matching fixed segment, plus 50 for an explicit
   (non-`ANY`) method match.
5. **Return** the best `max_stubs` stubs across all components.

## Architecture decisions

### 1. Use the most recently built snapshot, not the highest version number

**Context.** A component folder can hold several `-SNAPSHOT` lines. In one real component,
`1.3.1-SNAPSHOT` was last built in June and `1.2.0-SNAPSHOT` in August. The June build had fewer
stubs, and no `mock.properties` or `kafka/` folder: it was a line that had stopped being built,
while the team kept building 1.2.0.

**Options considered:**
1. Highest version number: simple, but picked the stale June build.
2. **Most recently built** (chosen): follows what the team actually builds.
3. Ask each team which version consumers should use: accurate, but needs a list that someone
   keeps up to date.

**Decision.** Use the `-SNAPSHOT` version whose newest build is the most recent. The build time
comes from `maven-metadata.xml` (`<snapshotVersion><updated>` for the zip), or from the file
name's timestamp. On a tie, the higher version wins. Fixed versions (`x.y.z-<commit>`) are
ignored unless asked for by `version`.

**Consequences.** If a team builds a short-lived branch snapshot, that build wins until the
main line is built again. `artifacts.<component>.snapshots_considered` shows every snapshot line
and its build time, so a wrong pick is visible. To pin a version, pass `version` to
`get_mock_stubs` or `get_mock_file`.

### 2. Find mocks by endpoint, not by component name

The dependency graph names the *system* (`customer-master`), not the component
(`cm-addresses`), so there's no direct name link. The server searches every component of the
system and matches stubs by method and path. The system-to-folders mapping is configuration
(`MOCKS_SYSTEMS`), so a new system needs no code change.

### 3. Return response bodies by default

The caller is an agent or program that needs the mock's responses, so bodies are included.
`include_bodies=false` returns only the stubs, and `MOCKS_MAX_BODY_CHARS` (default 20,000)
cuts long bodies. If the calling agent is AI-based, the mock data reaches that AI model; this
is fine for synthetic test data, which is what mocks are expected to hold.

### 4. A separate server from `ghes-openapi-mcp`

Artifactory and GitHub need different settings, credentials and failure handling. The path
matching follows the same rules as `find_openapi_spec` in `ghes-openapi-mcp`.

## Setup

You need Python 3.10 or later.

### 1. Install

```bash
cd artifactory-mocks-mcp
python3 -m venv .venv
source .venv/bin/activate                  # Windows: .venv\Scripts\activate
pip install -r requirements.txt -e .       # requirements-dev.txt to also run the tests
```

This installs the `artifactory-mocks-mcp` command into `.venv/bin/` (Windows: `.venv\Scripts\`).

### 2. Configure

```bash
cp .env.example .env
```

Fill in `.env` with your Artifactory's URL, repository and group path: `.env.example` only
has placeholders, as this repository is shared outside the company. The server reads `.env`
from the folder it's started in, or from this project folder. Variables set by the MCP
client's `env` block take precedence.

| Variable | Required | Meaning |
|---|---|---|
| `ARTIFACTORY_URL` | yes | e.g. `https://artifactory.example.com/artifactory`. The host alone also works; `/artifactory` is added. |
| `MOCKS_REPO` | yes | The Artifactory repository with the mocks |
| `MOCKS_GROUP_PATH` | yes | The Maven group as a path (`com/example/mocks`) or with dots (`com.example.mocks`) |
| `MOCKS_SYSTEMS` | for `system` | System name to component folder patterns: `customer-master=cm-*`. Several: `a=cm-*;b=mock-x,mock-x-*`. |
| `ARTIFACTORY_TOKEN` | no | Only if anonymous read isn't allowed: an identity token (Artifactory → your profile → **Generate an Identity Token**) or an access token. Sent as `Authorization: Bearer`. |
| `ARTIFACTORY_CA_BUNDLE` | no | Path to your corporate root CA (PEM), if you get TLS errors |
| `ARTIFACTORY_VERIFY_SSL` | no | `false` turns off TLS checks. Only for local debugging. |
| `ARTIFACTORY_TIMEOUT` | no | Seconds per request. Default `30`. |
| `MOCKS_CACHE_TTL` | no | Seconds before folder listings and version choices are re-read. Default `300`. |
| `MOCKS_MAX_BODY_CHARS` | no | Longest body returned, in characters. Default `20000`. |
| `MOCKS_MAX_CONCURRENCY` | no | Parallel requests to Artifactory. Default `8`. |

`.env` is in `.gitignore`. Never commit a token.

### 3. Check access

```bash
set -a && source .env && set +a
G="$MOCKS_GROUP_PATH"
curl -s "$ARTIFACTORY_URL/api/storage/$MOCKS_REPO/$G" | head -20              # list components
curl -s "$ARTIFACTORY_URL/api/storage/$MOCKS_REPO/$G/cm-addresses" | head -20  # list versions
```

You should see JSON with `children`. A 401 means a token is needed (`ARTIFACTORY_TOKEN`).
Use `api/storage/` only for listings: downloads go to `$ARTIFACTORY_URL/$MOCKS_REPO/...` without it.

## Run it

The server speaks MCP over stdio: an MCP client starts it. Run by hand, it waits silently for a
client; press Ctrl+C to stop it.

### MCP Inspector

```bash
npx @modelcontextprotocol/inspector /absolute/path/to/artifactory-mocks-mcp/.venv/bin/artifactory-mocks-mcp
```

Open the printed URL, click **Connect**, then **Tools** → **List Tools**.

Or from the command line, from the project folder (works in bash and zsh):

```bash
m() { npx -y @modelcontextprotocol/inspector --cli ./.venv/bin/artifactory-mocks-mcp "$@"; }
m --method tools/list 2>/dev/null
m --method tools/call --tool-name find_mock --tool-arg 'path=/v1/customers/customer-core/{id}' \
  --tool-arg method=GET --tool-arg system=customer-master 2>/dev/null
```

`2>/dev/null` hides the server's log lines. Leave it out when something fails: the log shows
every request and the status Artifactory returned.

### Cursor: `.cursor/mcp.json`

```json
{
  "mcpServers": {
    "artifactory-mocks": {
      "command": "/absolute/path/to/artifactory-mocks-mcp/.venv/bin/artifactory-mocks-mcp"
    }
  }
}
```

Settings come from this project's `.env`. In **Cursor Settings → MCP** (**Tools & MCP** in newer
versions), `artifactory-mocks` should show a green dot and 4 tools. Use the chat in **Agent**
mode. After changing `.env` or the code, turn the server off and on there.

### Claude Code

```bash
claude mcp add artifactory-mocks -- /absolute/path/to/artifactory-mocks-mcp/.venv/bin/artifactory-mocks-mcp
```

### VS Code: `.vscode/mcp.json`

```json
{
  "servers": {
    "artifactory-mocks": {
      "type": "stdio",
      "command": "/absolute/path/to/artifactory-mocks-mcp/.venv/bin/artifactory-mocks-mcp"
    }
  }
}
```

## Try it without Artifactory

`devtools/fake_artifactory.py` is a small fake Artifactory with made-up mocks in the same
layout, including a higher-numbered snapshot that is older (decision 1). The tests use it, and
you can run it to try the server end to end:

```bash
.venv/bin/python devtools/fake_artifactory.py 8091        # in one terminal

mkdir -p /tmp/mocks-trial && cd /tmp/mocks-trial         # in another
cat > .env <<'EOF'
ARTIFACTORY_URL=http://localhost:8091/artifactory
MOCKS_REPO=mocks-repo
MOCKS_GROUP_PATH=com/example/mocks
MOCKS_SYSTEMS=customer-master=cm-*
EOF
npx -y @modelcontextprotocol/inspector --cli /absolute/path/to/artifactory-mocks-mcp/.venv/bin/artifactory-mocks-mcp \
  --method tools/call --tool-name find_mock --tool-arg 'path=/v1/customers/{id}/addresses' --tool-arg method=GET
```

It should pick `cm-addresses` build `1.2.0-20260819.052644-19`, not the June `1.3.1` build.
The trial folder keeps its own `.env`, so it doesn't disturb your real one.

## First test against the real Artifactory

Run from inside the company network, from the project folder, with `.env` filled in.

```bash
m() { npx -y @modelcontextprotocol/inspector --cli ./.venv/bin/artifactory-mocks-mcp "$@"; }

# a. Can it see the components, and which builds would it use?
m --method tools/call --tool-name list_mock_components --tool-arg with_builds=true 2>/dev/null

# b. One component's stubs
m --method tools/call --tool-name get_mock_stubs --tool-arg component=cm-addresses 2>/dev/null

# c. A real endpoint from downstream_apis
m --method tools/call --tool-name find_mock --tool-arg 'path=/v1/customers/customer-core/{id}' \
  --tool-arg method=GET --tool-arg system=customer-master 2>/dev/null
```

| Check | Good | If not |
|---|---|---|
| a | Lists the `cm-*` components, each with a build | An error: see [Troubleshooting](#troubleshooting). A component with `error: ... no -SNAPSHOT version` is skipped by design. |
| a | The builds look current | Compare with the Artifactory UI. If a stale snapshot line wins, see decision 1 and pin `version`. |
| b | `total_stubs` > 0 | Check `notes` for files that failed to parse |
| c | The expected component and stub | Read `notes`. Try without `method`, or with `component`. List that component's stubs with b and compare the paths. |

## Using it with a dependency graph

```
downstream_apis(service)   →  systems[]: {name: "customer-master",
                                          endpoints: [{method: "GET", path: "/v1/customers/customer-core/{id}"}]}
for each endpoint:
  find_mock(path, method, system=<system name>)  →  stubs and responses
```

An instruction you can give an agent:

> For each endpoint returned by downstream_apis, call find_mock with its path, its method, and
> the system name. Report the component, the build, and each matching stub's request
> conditions and response status. If notes mention skipped components or several stubs for one
> path, show them.

The same endpoint can be looked up in `ghes-openapi-mcp` with `find_openapi_spec` to get its
OpenAPI spec; the two use the same path-matching rules.

## Open questions and later work

**TLM mocks (later).** They follow different conventions:
- folders named `mock-tlm`, `mock-tlm-avp`;
- release versions only (`1.0.0`, `1.0.1`, no snapshots);
- a different uploading team.

To add them:
1. Decide which version to use when there's no snapshot. Probably the highest release, which
   needs a change in `MockService._choose` (`mocks.py`).
2. Find TLM's system name in the `downstream_apis` output, and which folders belong to it.
   Then add it to `MOCKS_SYSTEMS`, e.g. `;tlm=mock-tlm*`.
3. Check a TLM zip has the same WireMock layout.

Until then, `get_mock_stubs` and `get_mock_file` work for a TLM component when given an
explicit `version`, e.g. `version=1.0.1`.

**Kafka mocks (later).** Zips may have a `kafka/` folder. It's reported in `get_mock_stubs`
notes but not read. Its format needs looking at first.

**Data.** Confirm with the mock owners that the mock responses are synthetic test data
(decision 3).

## Known limitations

- **Request conditions aren't evaluated.** Stubs that share a path but differ in body
  patterns, headers or query parameters are all returned, with their conditions; the caller
  chooses.
- **Regex stubs are matched with sample values.** A `{param}` in the request is tried as `1`,
  `123`, `ABC123`, a UUID and a few others. A regex that accepts none of them (e.g. exactly
  9 digits) won't match a templated path; pass a concrete path instead.
- **Query strings aren't matched.** `url` and `urlPattern` stubs are compared on their path.
- **Each `find_mock` call without `component` reads every component of the system** the
  first time. Later calls use the cache.
- **A new snapshot is noticed after `MOCKS_CACHE_TTL` seconds** (default 300). Restart the
  server to see it at once.

## Troubleshooting

### Find where it fails

| # | Layer | Check | Works if |
|---|---|---|---|
| 1 | Network and access | The `curl` commands in [Check access](#3-check-access) | You see JSON with `children`. Otherwise, it's the network, TLS, or a token problem, not this code. |
| 2 | Python install | `.venv/bin/python -c "import artifactory_mocks_mcp, mcp, httpx; print('ok')"` | It prints `ok` |
| 3 | The server | The Inspector commands above, without `2>/dev/null` | JSON comes back. The log lines show each request and status. |
| 4 | Your MCP client | Cursor Settings → MCP shows a green dot and 4 tools | If 3 works but this doesn't, check the path in `.cursor/mcp.json`, and Cursor's log under **View → Output** (choose the MCP log) |
| 5 | The answer | Read `notes`, `artifacts` and `components_searched` | See below |

### Corporate network

- **Proxy.** The server uses `HTTPS_PROXY`, `HTTP_PROXY` and `NO_PROXY`. An internal
  Artifactory usually must bypass the proxy: add its host to `NO_PROXY`.
- **TLS errors** (`CERTIFICATE_VERIFY_FAILED`). Python doesn't use the system's certificate
  store. Set `ARTIFACTORY_CA_BUNDLE` to your root CA as a PEM file. On macOS, this exports every
  certificate the system trusts:
  `security find-certificate -a -p /System/Library/Keychains/SystemRootCertificates.keychain /Library/Keychains/System.keychain > ~/company-ca.pem`
- **`pip install` fails** because PyPI is blocked: use the company's package mirror
  (`pip install --index-url https://<mirror>/simple ...` or `PIP_INDEX_URL`). The same goes for
  `npx` and npm.

### Wrong or missing results

| Symptom | Cause | Fix |
|---|---|---|
| `components_searched` is 0 | `MOCKS_SYSTEMS` patterns don't match any folder | Check with `list_mock_components` and the Artifactory UI |
| A component is "skipped, it has no -SNAPSHOT version" | It only has fixed versions | Expected (decision 1). Use `get_mock_stubs` with `version` for it. |
| An old build was used | That snapshot line was built most recently | Check `snapshots_considered`. Pin `version` in `get_mock_stubs` if needed. |
| No match, but the stub exists | The path differs, e.g. a gateway prefix with only one fixed segment in common, or a regex that rejects the sample values | Compare with `get_mock_stubs`. Try a concrete path, or no `method`. |
| Too many matches | A short path matches many stubs | Pass `method` and `component` |
| `body_missing` | The stub's `bodyFileName` isn't in the zip | A problem in the mock itself; tell its owners |

### Error messages

| Error | Fix |
|---|---|
| `ARTIFACTORY_URL, MOCKS_REPO, MOCKS_GROUP_PATH not set` | Fill in `.env` |
| `No systems are configured` | Set `MOCKS_SYSTEMS`, or pass `component` |
| `Unknown system 'x'` | Use a name from the message, or add it to `MOCKS_SYSTEMS` |
| `needs a token (401)` | Set `ARTIFACTORY_TOKEN` |
| `Forbidden (403)` | The token, or anonymous access, can't read that repository |
| `Not found (404)` | Check `MOCKS_REPO`, `MOCKS_GROUP_PATH` and the component name; check you didn't add `api/storage` to a download URL |
| `Could not reach Artifactory` | Off the company network or VPN, or a proxy problem (`NO_PROXY`) |
| `not a valid zip file` | Artifactory returned something else, often a login page: check the URL and the token |

## Development

```bash
pip install -r requirements-dev.txt -e .
pytest
```

The tests run against `devtools/fake_artifactory.py` through `httpx.MockTransport`, so they need
no network or token.

### Project layout

```
src/artifactory_mocks_mcp/
  server.py       MCP tool definitions
  mocks.py        MockService: choose components and builds, cache, search, format results
  wiremock.py     Read mock zips and match paths against stubs (no I/O)
  versions.py     Pick the most recently built snapshot (no I/O)
  client.py       Artifactory REST client (auth, retries, errors)
  config.py       Settings from the environment or .env
devtools/
  fake_artifactory.py   Fake Artifactory with made-up mocks, for tests and local trials
tests/
```

Dependency files work as in `ghes-openapi-mcp`: version ranges in `pyproject.toml`, exact pins
in `requirements.txt` and `requirements-dev.txt`.
