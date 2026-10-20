# Security

## Scope and posture

This is a reference implementation of a payment backend that runs against
Stripe **test mode** and a local simulator. It is not deployed anywhere and
holds no real card data or credentials.

- The Stripe client refuses live-mode keys (`sk_live_…`, `rk_live_…`) and the
  webhook endpoint refuses live-mode events.
- Webhooks are verified with HMAC-SHA256 over `timestamp.payload` against one
  or more endpoint secrets, with a five-minute replay tolerance.
- Card data never enters the service: the API takes a Stripe test
  `payment_method` id.
- API keys are bearer tokens from the `API_KEYS` environment variable;
  the repository contains none.

## Automated checks

Every push and pull request runs `govulncheck`, a Gitleaks secret scan over
the full history and `go vet`; the vulnerability scan also runs weekly
([`.github/workflows/security.yml`](.github/workflows/security.yml)).
GitHub Actions are pinned to commit SHAs and updated by Dependabot.

## Reporting a vulnerability

Please open a private security advisory on GitHub
(Security → Report a vulnerability) rather than a public issue. Include the
commit, a description and, if you can, a failing test.
