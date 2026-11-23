from click.testing import CliRunner
from conduit.cli import cli


def test_cli_version():
    runner = CliRunner()
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0
    assert "conduit-db" in result.output
    assert "1.0.0" in result.output


def test_cli_help():
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "transfer" in result.output
    assert "inspect" in result.output
    assert "schema" in result.output
    assert "resume" in result.output
    assert "verify" in result.output
