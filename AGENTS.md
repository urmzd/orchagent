# AGENTS.md

orchagent is one Go module (`github.com/urmzd/orchagent`) with three components:
`durable/` (durable runs), `sandbox/` (sandboxes and access control) and
`flow/` (the event flow and the durable turn). `sandbox/runner/k8s` is a second
module so importers do not inherit client-go.

## Rules

- `flow` may import `durable` and `sandbox`. `durable` and `sandbox` never import `flow`, and never each other.
- Nothing here is a service: no listener, no config files, no environment variables read to decide what to build. Backends are chosen in code. Humagen owns configuration.
- Names stored in databases or on the wire keep their old spelling (`fiber_*`, `fiber://`, `fiber.*` kinds). Do not rename them without a migration.
- Each component keeps its own `docs/` and `README.md`. Component-specific guidance lives in its `AGENTS.md`.

## Commands

```sh
make check              # fmt, vet, lint, race tests
make test-integration   # Postgres and Redis
```
