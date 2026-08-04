# Security policy

## Prototype scope

Flipbench is designed for a trusted, single-user development machine. It is not hardened for public or shared-network deployment.

The local stack intentionally uses:

- Kafka plaintext listeners without SASL;
- unauthenticated Kafka Connect REST endpoints;
- PostgreSQL credentials from a local `.env` file;
- a loopback control API and restart supervisor without user authentication;
- an administrative runner role for lifecycle operations.

All published host ports are bound to `127.0.0.1`. Keep those bindings intact and do not place the UI, APIs, Kafka, Connect, or PostgreSQL ports behind a public tunnel or shared ingress.

## Credentials and artifacts

- Copy `.env.example` to `.env`, set mode `600`, and use local-only credentials.
- Never commit `.env`, database dumps, connector secrets, production DDL/data, or unsanitized result artifacts.
- Explicit `CDC_PASSWORD` and `SINK_PASSWORD` values must be distinct from the PostgreSQL administrator password and from each other.
- Treat saved benchmark results as potentially sensitive: they contain topology, tuning, workload, host, timing, and failure information.

## Before any production or shared deployment

Add authentication and authorization, TLS for every hop, managed secret storage and rotation, least-privilege lane-specific identities, network policies, CSRF/authentication protection for control mutations, audit logging, rate limits, production observability, backup/recovery procedures, and an operator-approved rollout/rollback design.

## Reporting a vulnerability

Do not open a public issue containing credentials, private topology, production data, or an exploitable proof. Report it privately to the repository owner and rotate any exposed credential immediately.
