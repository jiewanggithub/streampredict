"""Validate the repository files required by the M0 foundation module."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

REQUIRED_FILES = (
    ".editorconfig",
    ".env.example",
    ".gitignore",
    ".nvmrc",
    ".pre-commit-config.yaml",
    ".python-version",
    "Makefile",
    "README.md",
    "environment.yml",
    "pyproject.toml",
)

REQUIRED_DIRECTORIES = (
    "apps",
    "docs",
    "infra",
    "services",
    "tests",
    "tools",
)

FORBIDDEN_TRACKED_SECRET_NAMES = (
    ".env",
    "id_rsa",
    "id_ed25519",
)


def missing_paths() -> list[str]:
    """Return required paths that are absent from the repository."""
    missing = [path for path in REQUIRED_FILES if not (ROOT / path).is_file()]
    missing.extend(path for path in REQUIRED_DIRECTORIES if not (ROOT / path).is_dir())
    return sorted(missing)


def exposed_secret_files() -> list[str]:
    """Return known local secret filenames if they exist in the repository root."""
    return [name for name in FORBIDDEN_TRACKED_SECRET_NAMES if (ROOT / name).exists()]


def main() -> int:
    """Run foundation validation and return a process exit code."""
    missing = missing_paths()
    secrets = exposed_secret_files()

    if missing:
        print("Missing required project paths:")
        for path in missing:
            print(f"  - {path}")

    if secrets:
        print("Potential secret files found in the repository root:")
        for path in secrets:
            print(f"  - {path}")

    if missing or secrets:
        return 1

    print("Project foundation validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
