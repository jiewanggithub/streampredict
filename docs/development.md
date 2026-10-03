# Development environment

This document defines the M0 development baseline for StreamPredict.

## Required tools

| Tool | Version policy | Current baseline |
| --- | --- | --- |
| Python | Exact patch version for local and CI environments | `3.11.17` |
| Conda | Recent version capable of reading `environment.yml` | `24+` |
| Node.js | Exact version in `.nvmrc`; update intentionally | `22.19.0` |
| Docker Engine | Minimum major version; CI images remain pinned | `29+` |
| Docker Compose | Minimum major version | `5+` |

Python development tools are constrained to compatible major-version ranges in
`environment.yml`. Application dependencies will be introduced and locked by the module that owns
them. Container images must use immutable version tags and should use digests for release builds.

## Initial setup

```bash
conda env update -n streampredict -f environment.yml --prune
conda activate streampredict
make hooks
make check
```

For Node.js work, use the repository version before installing dashboard dependencies:

```bash
nvm use
```

## Configuration

Copy `.env.example` to `.env` and replace only values needed for local development. `.env` is
ignored by Git and must never be committed. The checked-in example contains placeholders, not real
credentials.

## Running the Phase 1 stack

| Command | What it starts |
| --- | --- |
| `make up` / `make down` / `make logs` | Redis, the API gateway, and the dashboard via Docker Compose |
| `make api-dev` | API gateway with auto-reload on <http://localhost:8000> (OpenAPI at `/docs`) |
| `make dashboard-install && make dashboard-dev` | Next.js dashboard on <http://localhost:3000> |

Without Redis the API keeps serving predictions with `cache: "bypass"` and `/ready` reports
`degraded`. Inference is a deterministic mock until TorchServe lands in M5.

`services/api/requirements.in` lists direct runtime dependencies; `make api-lock` regenerates the
pinned `requirements.txt` used by both the Conda environment and the Docker image.

The Makefile resolves the `streampredict` env's interpreter by absolute path, so another activated
Conda env or a system Python earlier on `PATH` cannot shadow it.

## Quality gates

`make check` is the local equivalent of the future CI quality gate. It runs:

- Ruff linting and formatting verification
- strict mypy type checking
- pytest
- repository structure validation
- whitespace validation
- dashboard ESLint, TypeScript, and production build

Use `make format` to apply safe automatic Python formatting before committing.

