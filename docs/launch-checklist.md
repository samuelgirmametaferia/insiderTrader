# GitHub launch checklist

This checklist keeps the public launch polished without making claims the evidence
does not support.

## Before publishing

- Replace the illustrative `docs/media/insidertrader-demo.gif` and `.mp4` with a
  credential-free paper-mode recording, keeping the same filenames.
- Set the repository owner in the GitHub Actions badge if a badge is added later.
- Confirm the repository description, topics (`trading`, `rust`, `algorithmic-trading`,
  `terminal`, `quantitative-finance`), homepage, and Apache-2.0 license.
- Enable the workflow in `.github/workflows/ci.yml`; require it on the default branch.
- Enable private security reporting and keep [`SECURITY.md`](../SECURITY.md) visible.
- Review the complete history for credentials, personal data, generated runtime state,
  and accidental broker identifiers before making the repository public.
- Run `./scripts/check.sh`, `make paper-check`, and `cargo test --workspace` from a
  clean checkout. Record the revision and outputs in the release notes.

## Product claims

- Say “paper-ready” and “live certification required” until the external certification
  gates in `AGENTS.md` are complete.
- Do not publish performance claims from synthetic data or starter strategies.
- Describe the AI as an optional, schema-validated coordinator; risk, execution,
  reconciliation, and journal state remain deterministic runtime responsibilities.
- Keep API-provider free-tier and licensing limitations in the provider documentation.

## First release

1. Tag a version after CI and paper composition checks pass.
2. Publish the changelog and a short release post linking to the GIF and MP4 demo.
3. Invite issues using the structured templates and require paper fixtures for bug reports.
4. Announce the project with the two-minute quickstart, architecture diagram, and safety
   boundary—not a promise of returns.
