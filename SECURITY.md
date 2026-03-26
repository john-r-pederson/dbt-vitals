# Security Policy

## Supported Versions

| Version | Supported |
| :------ | :-------- |
| `v0.1.x` (latest) | ✅ |

Only the latest release receives security fixes.

## Reporting a Vulnerability

**Do not file a public GitHub issue for security vulnerabilities.**

dbt-vitals handles sensitive credentials (Snowflake private keys, GitHub tokens) and executes SQL against production warehouses. Please report vulnerabilities privately so they can be addressed before public disclosure.

**How to report:**

1. Open a [GitHub Security Advisory](https://github.com/Laskr/dbt-vitals/security/advisories/new) (preferred — keeps the report confidential within GitHub)
2. Or email [john@laskrconsulting.com](mailto:john@laskrconsulting.com) with the subject line `[SECURITY] <brief description>`

Please include:

- Description of the vulnerability and its potential impact
- Steps to reproduce (proof-of-concept if possible)
- Any suggested mitigations

You can expect an acknowledgement within 48 hours and a resolution or status update within 7 days.

## Scope

Issues in scope:

- Credential exposure (private keys, tokens logged or transmitted insecurely)
- SQL injection via unsanitized warehouse identifiers or query parameters
- GitHub token scope escalation
- Container image vulnerabilities in the published Docker image

Issues out of scope:

- Vulnerabilities in third-party dependencies (report those upstream)
- Issues requiring physical access to the runner environment
- Social engineering
