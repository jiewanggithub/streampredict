"""Check text files for trailing whitespace and missing final newlines."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
IGNORED_PARTS = {
    ".cache",
    ".git",
    ".mypy_cache",
    ".next",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    "node_modules",
}
TEXT_SUFFIXES = {
    ".md",
    ".py",
    ".toml",
    ".txt",
    ".yaml",
    ".yml",
}
TEXT_NAMES = {
    ".editorconfig",
    ".env.example",
    ".gitignore",
    ".nvmrc",
    ".python-version",
    "Makefile",
}


def text_files() -> list[Path]:
    """Return repository text files covered by the foundation quality gate."""
    return sorted(
        path
        for path in ROOT.rglob("*")
        if path.is_file()
        and not IGNORED_PARTS.intersection(path.relative_to(ROOT).parts)
        and (path.suffix in TEXT_SUFFIXES or path.name in TEXT_NAMES)
    )


def violations(path: Path) -> list[str]:
    """Return whitespace violations for one file."""
    content = path.read_bytes()
    issues: list[str] = []
    if content and not content.endswith(b"\n"):
        issues.append("missing final newline")

    for number, line in enumerate(content.decode("utf-8").splitlines(), start=1):
        if line != line.rstrip():
            issues.append(f"line {number}: trailing whitespace")
    return issues


def main() -> int:
    """Run whitespace checks and return a process exit code."""
    failed = False
    for path in text_files():
        for issue in violations(path):
            failed = True
            print(f"{path.relative_to(ROOT)}: {issue}")

    if failed:
        return 1

    print("Whitespace validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
