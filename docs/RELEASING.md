# Releasing

A release is a git tag. Pushing `v<version>` runs `.github/workflows/publish.yml`, which checks
the tag, runs every CI job, builds once, uploads to TestPyPI, installs that upload in a clean
environment, uploads to PyPI and creates the GitHub Release; `docs.yml` deploys the docs for the
same tag. Uploads use PyPI **trusted publishing** (OIDC): no token lives in the repo, in CI
secrets or in `.pypirc`, and there is no manual upload path.

```
git push origin v0.1.0b1
  verify    tag == "v" + __version__ (canonical PEP 440); tagged commit on main;
            CHANGELOG.md has "## [0.1.0b1] - <date>" (not "unreleased")
  checks    every ci.yml job (lint, types, unit tests 3.11-3.14, lowest deps,
            LangChain 0.3 line, integration on Docker, zizmor)
  build     sdist + wheel once; twine check --strict
  testpypi  upload (skip-existing)
  smoke     download the wheel from TestPyPI (no deps), same SHA-256 as the build,
            install with dependencies from PyPI, import, version, `llm-queue --help`
  pypi      upload (environment `pypi`: a required reviewer approves here)
  release   GitHub Release with the CHANGELOG section and the files;
            pre-release when the version has a / b / rc / dev
```

## One-time setup

1. On <https://test.pypi.org> and <https://pypi.org>: *Your account → Publishing → Add a new
   pending publisher*: project `llm-queue`, owner `techiewonk`, repository `llm-queue`,
   workflow `publish.yml`, environment `testpypi` (TestPyPI) / `pypi` (PyPI).
2. GitHub *Settings → Environments*: `testpypi`, `pypi` (add yourself as a **required
   reviewer** on `pypi`, so PyPI waits for one click), and `github-pages` (allow `main` and
   tags `v*` to deploy).
3. GitHub *Settings → Pages → Source: GitHub Actions*.
4. Optional: protect tags `v*` (*Settings → Rules → Rulesets*) so only maintainers can push them.

## Each release

1. Set `src/llm_queue/_version.py` (PEP 440: `0.1.0a1`, `0.1.0b1`, `0.1.0rc1`, `0.1.0`).
2. In `CHANGELOG.md`, add `## [<version>] - <date>` with the changes (Keep a Changelog), and the
   compare links at the bottom.
3. Check locally (Docker stack up):

   ```bash
   python scripts/check.py all                 # every step, gates, every scenario at every load
   python scripts/check.py soak20              # 20-minute soak with chaos
   python scripts/samples.py --live --store env --env-file <app .env>   # real Bedrock and store
   ```

4. Optional dry run: *Actions → publish → Run workflow* stops after the TestPyPI smoke test.
5. Merge to `main`, then tag the merge commit and push the tag:

   ```bash
   git tag -a v<version> -m "llm-queue <version>"
   git push origin v<version>
   ```

6. Approve the `pypi` deployment when the workflow asks. A pre-release installs only with
   `pip install --pre llm-queue` (or `llm-queue==<version>`).

A version number can be uploaded only once (PyPI never allows re-use, even after deleting it).
If the workflow fails before `pypi`, fix it, delete the tag (`git push origin :v<version>`),
and tag again; after `pypi`, bump the version.
