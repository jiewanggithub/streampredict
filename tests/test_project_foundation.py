"""Tests for the StreamPredict repository foundation."""

from tools.validate_project import exposed_secret_files, missing_paths


def test_required_project_paths_exist() -> None:
    assert missing_paths() == []


def test_local_secret_files_are_not_present() -> None:
    assert exposed_secret_files() == []
