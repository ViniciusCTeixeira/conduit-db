import pytest
import pymysql
from click.testing import CliRunner
from conduit.cli import cli


@pytest.fixture(scope="module")
def mysql_available():
    try:
        conn = pymysql.connect(
            host="127.0.0.1",
            port=3306,
            user="root",
            password="123",
            connect_timeout=2,
        )
        conn.close()
        return True
    except Exception:
        return False


def test_e2e_migration_pipeline(tmp_path, mysql_available):
    if not mysql_available:
        pytest.skip("Local MySQL instance not available on 127.0.0.1:3306")

    db_src = "conduit_test_source"
    db_dst = "conduit_test_target"

    admin_conn = pymysql.connect(host="127.0.0.1", port=3306, user="root", password="123")
    with admin_conn.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS `{db_src}`")
        cur.execute(f"DROP DATABASE IF EXISTS `{db_dst}`")
        cur.execute(f"CREATE DATABASE `{db_src}` CHARACTER SET utf8mb4")
        cur.execute(f"CREATE DATABASE `{db_dst}` CHARACTER SET utf8mb4")

        cur.execute(f"USE `{db_src}`")
        cur.execute("""
            CREATE TABLE `users` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `name` VARCHAR(100) NOT NULL,
                `email` VARCHAR(100) NOT NULL,
                `score` DECIMAL(10,2) DEFAULT 0.00,
                `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB;
        """)

        # Insert 1500 rows for chunk testing
        values = [(f"User {i}", f"user{i}@example.com", i * 1.5) for i in range(1, 1501)]
        cur.executemany("INSERT INTO `users` (`name`, `email`, `score`) VALUES (%s, %s, %s)", values)
    admin_conn.commit()

    config_content = f"""
profiles:
  e2e_src:
    driver: mysql
    host: 127.0.0.1
    port: 3306
    user: root
    password: "123"
    database: {db_src}
  e2e_dst:
    driver: mysql
    host: 127.0.0.1
    port: 3306
    user: root
    password: "123"
    database: {db_dst}
"""
    cfg_file = tmp_path / "profiles.yaml"
    cfg_file.write_text(config_content)

    runner = CliRunner()

    # 1. Test Connections
    res_test = runner.invoke(cli, ["test", "--source", "e2e_src", "--target", "e2e_dst", "--config", str(cfg_file)])
    assert res_test.exit_code == 0
    assert "All connections verified successfully" in res_test.output

    # 2. Inspect Source
    res_inspect = runner.invoke(cli, ["inspect", "--profile", "e2e_src", "--config", str(cfg_file)])
    assert res_inspect.exit_code == 0
    assert "users" in res_inspect.output

    # 3. Schema Sync
    res_schema = runner.invoke(cli, ["schema", "--source", "e2e_src", "--target", "e2e_dst", "--config", str(cfg_file)])
    assert res_schema.exit_code == 0
    assert "Successfully migrated structure" in res_schema.output

    # 4. Data Transfer with chunk size 400
    state_file = tmp_path / "test_state.db"
    res_transfer = runner.invoke(cli, [
        "transfer",
        "--source", "e2e_src",
        "--target", "e2e_dst",
        "--chunk-size", "400",
        "--state-db", str(state_file),
        "--config", str(cfg_file),
    ])
    assert res_transfer.exit_code == 0
    assert "Transfer completed successfully" in res_transfer.output

    # 5. Verification
    res_verify = runner.invoke(cli, [
        "verify",
        "--source", "e2e_src",
        "--target", "e2e_dst",
        "--hash",
        "--config", str(cfg_file),
    ])
    assert res_verify.exit_code == 0
    assert "All verified tables match 100%" in res_verify.output

    # Clean up databases
    with admin_conn.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS `{db_src}`")
        cur.execute(f"DROP DATABASE IF EXISTS `{db_dst}`")
    admin_conn.commit()
    admin_conn.close()
