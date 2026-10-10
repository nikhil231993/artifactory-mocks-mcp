# Notes for AI coding agents

An MCP server (Python, stdio) that finds WireMock provider mocks in JFrog Artifactory by
endpoint. Read `README.md` first: it documents the tools, the storage layout, the architecture
decisions and the open questions.

- **Debugging:** follow README → Troubleshooting → "Find where it fails", layer by layer. Ask
  for the output of the first failing layer before changing code; most failures on a company
  laptop are network, proxy, TLS or token problems.
- **Version choice is a deliberate decision** (README → Architecture decisions → 1): the most
  recently built snapshot, not the highest version. Don't change it without updating that section.
- **Planned work:** README → "Open questions and later work" (TLM, Kafka).
- **Code layout:** `server.py` (tools) → `mocks.py` (MockService: components, builds, caching,
  results) → `wiremock.py` (zip reading and path matching, no I/O), `versions.py` (build choice,
  no I/O), `client.py` (REST calls, retries, errors). Settings come from `.env` via `config.py`.
- **Tests:** `.venv/bin/python -m pytest -q`. They use `devtools/fake_artifactory.py`; add any
  new storage layout case there, then a test. No network needed.
- **Never** commit `.env`, tokens, or company details: the Artifactory host, repository and
  group path, or real topic names, namespaces and hosts seen in the mocks. They go in `.env`
  only; `.env.example`, the docs and `devtools/` test data use placeholders and made-up names.
