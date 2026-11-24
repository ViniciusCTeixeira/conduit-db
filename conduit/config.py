from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional
import yaml


_ENV_VAR_PATTERN = re.compile(r"\$\{([A-Za-z0-9_]+)(?::-(.*?))?\}")


def _expand_env_vars(value: Any) -> Any:
    if isinstance(value, str):
        def repl(match: re.Match) -> str:
            var_name = match.group(1)
            default_val = match.group(2) if match.group(2) is not None else ""
            return os.environ.get(var_name, default_val)

        return _ENV_VAR_PATTERN.sub(repl, value)
    elif isinstance(value, dict):
        return {k: _expand_env_vars(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [_expand_env_vars(v) for v in value]
    return value


@dataclass
class SSHConfig:
    enabled: bool = False
    host: str = ""
    port: int = 22
    username: str = ""
    password: Optional[str] = None
    private_key: Optional[str] = None
    passphrase: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> SSHConfig:
        return cls(
            enabled=bool(data.get("enabled", False)),
            host=str(data.get("host", "")),
            port=int(data.get("port", 22)),
            username=str(data.get("username", "")),
            password=data.get("password") or None,
            private_key=data.get("private_key") or None,
            passphrase=data.get("passphrase") or None,
        )


@dataclass
class DatabaseConfig:
    name: str
    driver: str = "mysql"
    host: str = "127.0.0.1"
    port: int = 3306
    user: str = "root"
    password: str = ""
    database: str = ""
    charset: str = "utf8mb4"
    connect_timeout: int = 15
    read_timeout: Optional[int] = None
    write_timeout: Optional[int] = None
    ssh: SSHConfig = field(default_factory=SSHConfig)

    @classmethod
    def from_dict(cls, name: str, data: Dict[str, Any]) -> DatabaseConfig:
        ssh_data = data.get("ssh") or {}
        return cls(
            name=name,
            driver=str(data.get("driver", "mysql")),
            host=str(data.get("host", "127.0.0.1")),
            port=int(data.get("port", 3306)),
            user=str(data.get("user", "root")),
            password=str(data.get("password", "")),
            database=str(data.get("database", "")),
            charset=str(data.get("charset", "utf8mb4")),
            connect_timeout=int(data.get("connect_timeout", 15)),
            read_timeout=int(data["read_timeout"]) if data.get("read_timeout") is not None else None,
            write_timeout=int(data["write_timeout"]) if data.get("write_timeout") is not None else None,
            ssh=SSHConfig.from_dict(ssh_data),
        )


class ConfigManager:
    @staticmethod
    def load_profiles(config_path: Optional[str | Path] = None) -> Dict[str, DatabaseConfig]:
        paths_to_check = []
        if config_path:
            paths_to_check.append(Path(config_path))
        else:
            paths_to_check.extend([
                Path("profiles.yaml"),
                Path("profiles.yml"),
                Path.home() / ".conduit" / "profiles.yaml",
            ])

        found_path: Optional[Path] = None
        for p in paths_to_check:
            if p.is_file():
                found_path = p
                break

        if not found_path:
            return {}

        with open(found_path, "r", encoding="utf-8") as f:
            raw_content = yaml.safe_load(f) or {}

        expanded = _expand_env_vars(raw_content)
        profiles_dict = expanded.get("profiles", {})

        configs: Dict[str, DatabaseConfig] = {}
        for name, data in profiles_dict.items():
            if isinstance(data, dict):
                configs[name] = DatabaseConfig.from_dict(name, data)

        return configs
