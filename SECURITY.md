# Security Policy

## Reporting Vulnerabilities

If you discover a security vulnerability in Varpulis, please report it responsibly.

**Email:** security@varpulis-cep.com

Please include:
- A description of the vulnerability and its potential impact
- Steps to reproduce the issue
- Any proof-of-concept code, if applicable
- Your name and contact information (for credit in the Hall of Fame)

Do NOT open a public GitHub issue for security vulnerabilities.

## Scope

The following components are in scope for security reports:

- **VPL parser** -- injection, denial-of-service via crafted input, parser crashes
- **Runtime engine** -- unsafe memory access, resource exhaustion, sandbox escapes
- **REST API endpoints** -- authentication bypass, authorization flaws, injection
- **Connectors** (MQTT, Kafka, database, Redis, S3, Kinesis, Elasticsearch) -- credential leakage, connection hijacking
- **Authentication and RBAC** -- privilege escalation, API key handling flaws
- **Cluster communication** -- Raft protocol manipulation, worker impersonation, mTLS bypass
- **Web UI** -- XSS, CSRF, session management issues

## Out of Scope

- Social engineering attacks against maintainers or users
- Denial-of-service attacks against live demo or production services
- Vulnerabilities in third-party dependencies (please report these upstream and notify us so we can update)
- Issues requiring physical access to the host machine
- Bugs in development tooling (benchmarks, test harnesses)

## Response Timeline

| Stage | Timeline |
|-------|----------|
| Acknowledgment of report | Within 48 hours |
| Initial assessment and severity rating | Within 7 days |
| Fix timeline communicated to reporter | Within 14 days |
| Patch released (critical/high) | Best effort within 30 days |
| Patch released (medium/low) | Best effort within 90 days |

## Disclosure Policy

We follow a 90-day coordinated disclosure policy:

1. Reporter submits the vulnerability privately via email.
2. We acknowledge receipt and begin assessment.
3. We work with the reporter to understand and validate the issue.
4. We develop and test a fix.
5. We release the fix and publish a security advisory.
6. The reporter may publish their findings 90 days after the initial report, or after the fix is released, whichever comes first.

We ask that reporters refrain from public disclosure until the coordinated timeline expires or we have released a patch.

## Security Measures in Place

Varpulis includes the following security measures:

- **API key authentication** -- all API endpoints require a valid `X-API-Key` header
- **Role-based access control (RBAC)** -- configurable roles (admin, operator, viewer) with per-endpoint permissions
- **Rate limiting** -- per-IP request rate limiting to prevent abuse
- **Request body size limits** -- enforced maximum payload sizes to prevent memory exhaustion
- **Path traversal prevention** -- file path inputs are sanitized and restricted
- **Secrets zeroization** -- API keys and credentials are zeroized in memory on drop via `SecretString`
- **mTLS support** -- mutual TLS for cluster node communication
- **Distributed tracing** -- OpenTelemetry integration for audit trails
- **Event resource limits** -- bounded Kleene closure events, bounded join buffers, bounded stream queues
- **Circuit breakers** -- connector failures are isolated with circuit breaker patterns
- **Dead letter queues** -- failed events are preserved rather than silently dropped
- **cargo-audit in CI** -- automated dependency vulnerability scanning on every build
- **cargo-deny in CI** -- license and advisory checks for all dependencies

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.3.x | Yes |
| < 0.3.0 | No |

## Hall of Fame

We gratefully acknowledge security researchers who have responsibly disclosed vulnerabilities:

*No reports yet -- be the first!*
