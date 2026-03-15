"""
Unit tests for CLI commands.
"""

import pytest
from click.testing import CliRunner

from run_pipeline import cli


@pytest.fixture
def runner():
    """Create CLI test runner."""
    return CliRunner()


def test_cli_help(runner):
    """Test CLI help command."""
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "ROSTA Scraper Pipeline" in result.output
    assert "run" in result.output
    assert "export-only" in result.output
    assert "validate" in result.output


def test_run_help(runner):
    """Test run command help."""
    result = runner.invoke(cli, ["run", "--help"])
    assert result.exit_code == 0
    assert "Execute full pipeline" in result.output
    assert "--provider" in result.output
    assert "--skip-geocoding" in result.output
    assert "--skip-ai" in result.output


def test_export_only_help(runner):
    """Test export-only command help."""
    result = runner.invoke(cli, ["export-only", "--help"])
    assert result.exit_code == 0
    assert "Regenerate CSV exports" in result.output


def test_validate_help(runner):
    """Test validate command help."""
    result = runner.invoke(cli, ["validate", "--help"])
    assert result.exit_code == 0
    assert "Validate canonical store" in result.output


def test_validate_empty_store(runner):
    """Test validate command with empty store."""
    result = runner.invoke(cli, ["validate"])
    assert result.exit_code == 0
    assert "Store loaded successfully" in result.output
    assert "Providers: 0" in result.output
