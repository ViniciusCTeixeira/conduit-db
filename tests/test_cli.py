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
    assert "menu" in result.output


def test_cli_interactive_menu_exit(tmp_path):
    cfg_file = tmp_path / "profiles.yaml"
    cfg_file.write_text("""
profiles:
  test_src:
    driver: mysql
    host: 127.0.0.1
    port: 3306
    user: root
    password: ""
    database: test
""")
    runner = CliRunner()
    result = runner.invoke(cli, ["--config", str(cfg_file)], input="0\n")
    assert result.exit_code == 0
    assert "Main Menu" in result.output
    assert "Exiting ConduitDB" in result.output


def test_cli_interactive_menu_missing_config(tmp_path):
    runner = CliRunner()
    result = runner.invoke(cli, ["--config", str(tmp_path / "nonexistent.yaml")])
    assert result.exit_code == 0
    assert "No database profiles found" in result.output
