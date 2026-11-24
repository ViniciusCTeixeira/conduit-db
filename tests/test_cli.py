from click.testing import CliRunner
from conduit.cli import cli


def test_cli_version():
    runner = CliRunner()
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0
    assert "conduit-db" in result.output
    assert "1.1.0" in result.output


def test_cli_help():
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "transfer" in result.output
    assert "sync" in result.output
    assert "indexes" in result.output
    assert "inspect" in result.output
    assert "schema" in result.output
    assert "resume" in result.output
    assert "verify" in result.output
    assert "menu" in result.output


def test_cli_indexes_help():
    runner = CliRunner()
    result = runner.invoke(cli, ["indexes", "--help"])
    assert result.exit_code == 0
    assert "list" in result.output
    assert "restore" in result.output


def test_cli_sync_help():
    runner = CliRunner()
    result = runner.invoke(cli, ["sync", "--help"])
    assert result.exit_code == 0
    assert "--prune" in result.output
    assert "--dry-run" in result.output
    assert "--auto-index" in result.output


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


def test_progress_columns():
    from unittest.mock import MagicMock
    from conduit.cli import OperationColumn, ProgressCounterColumn, RowsSpeedColumn, TableNameColumn

    mock_task = MagicMock()
    mock_task.description = "Transferring"
    mock_task.fields = {"table": "[Total Progress]", "is_total": True, "operation": "1/3 tables", "unit": "rows"}
    mock_task.completed = 70517432
    mock_task.total = 144326811
    mock_task.speed = 13500.0

    table_col = TableNameColumn()
    counter_col = ProgressCounterColumn()
    speed_col = RowsSpeedColumn()
    op_col = OperationColumn()

    assert "[Total Progress]" in table_col.render(mock_task).plain
    assert "70,517,432/144,326,811 rows" in counter_col.render(mock_task).plain
    assert "13,500 rows/s" in speed_col.render(mock_task).plain
    assert "[1/3 tables]" in op_col.render(mock_task).plain

