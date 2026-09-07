# Agent guidance

These instructions apply to all automated coding agents working in this repository.

## Public repository and data handling

**This repository is public. Never publish customer data or internal information here.**
This applies to code, comments, documentation, test fixtures, commit messages,
PR titles and descriptions, review comments, issues, screenshots, and CI logs or artifacts.

- Do not include customer names, customer or organization identifiers, production data,
  credentials, tokens, or other secrets.
- Do not include real AWS account IDs, role ARNs, internal hostnames, cluster names,
  private endpoints, or other internal-only resource identifiers.
- Use clearly synthetic examples or placeholders such as `<account-id>`, `<org-id>`,
  and `<internal-endpoint>`. Do not copy diagnostic output into a public artifact
  without checking and redacting it first.
- Keep necessary internal diagnostic details in an approved private channel, not in
  this repository or its GitHub discussions.
- Obtain deployment-specific values through approved runtime configuration or GitHub
  Actions secrets. Never print secret values, and mask internal identifiers in CI logs.
- Review the diff and all text intended for publication before committing or posting.
  If information might be internal, redact it or ask before publishing it.
- If internal information was already published, stop further disclosure and report
  it privately. Removing it from the latest file does not remove it from Git history,
  logs, or previously published artifacts. Do not rewrite shared history without approval.

## Repository conventions

- Follow [CLAUDE.md](CLAUDE.md) and applicable directory-specific guidance.
- Before writing Java code, read [.github/DEVELOPMENT.md](.github/DEVELOPMENT.md) in full.
  Use its build, test, and style instructions rather than copying commands from other projects.
- Keep changes focused and preserve unrelated work. Prefer correctness, maintainability,
  and explicit configuration over shortcuts.
- For behavior changes, add or update realistic regression tests and run the relevant
  checks. Report failures accurately; do not weaken tests to make CI pass.
- Document configuration defaults and operational changes where they belong.
- Report what was tested and distinguish local validation from a real publication or deployment.
