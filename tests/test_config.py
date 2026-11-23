import os
from pathlib import Path
from conduit.config import ConfigManager, DatabaseConfig, SSHConfig


def test_ssh_config_from_dict():
    data = {
        "enabled": True,
        "host": "bastion.example.com",
        "port": 2222,
        "username": "deploy",
        "private_key": "~/.ssh/id_rsa",
    }
    ssh = SSHConfig.from_dict(data)
    assert ssh.enabled is True
    assert ssh.host == "bastion.example.com"
    assert ssh.port == 2222
    assert ssh.username == "deploy"
    assert ssh.private_key == "~/.ssh/id_rsa"


def test_database_config_env_expansion(tmp_path: Path):
    os.environ["TEST_DB_PASS"] = "secret123"
    yaml_content = """
profiles:
  test_source:
    driver: mysql
    host: 127.0.0.1
    port: 3306
    user: admin
    password: "${TEST_DB_PASS}"
    database: test_db
"""
    config_file = tmp_path / "profiles.yaml"
    config_file.write_text(yaml_content, encoding="utf-8")

    profiles = ConfigManager.load_profiles(config_file)
    assert "test_source" in profiles
    cfg = profiles["test_source"]
    assert cfg.password == "secret123"
    assert cfg.database == "test_db"
