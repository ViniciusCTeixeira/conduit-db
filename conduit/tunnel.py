from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Generator, Optional, Tuple
from sshtunnel import SSHTunnelForwarder

from conduit.config import DatabaseConfig, SSHConfig


class TunnelManager:
    """Manages secure SSH tunnels for remote database connections."""

    @staticmethod
    @contextmanager
    def open_tunnel(db_config: DatabaseConfig) -> Generator[Tuple[str, int], None, None]:
        ssh = db_config.ssh
        if not ssh.enabled or not ssh.host:
            # Direct connection without tunnel
            yield db_config.host, db_config.port
            return

        ssh_kwargs: dict = {
            "ssh_address_or_host": (ssh.host, ssh.port),
            "ssh_username": ssh.username,
            "remote_bind_address": (db_config.host, db_config.port),
            "local_bind_address": ("127.0.0.1", 0),  # Bind to any free port
        }

        if ssh.private_key:
            key_path = os.path.expanduser(ssh.private_key)
            if os.path.isfile(key_path):
                ssh_kwargs["ssh_pkey"] = key_path
                if ssh.passphrase:
                    ssh_kwargs["ssh_private_key_password"] = ssh.passphrase
            else:
                raise FileNotFoundError(f"SSH private key file not found: {key_path}")

        if ssh.password:
            ssh_kwargs["ssh_password"] = ssh.password

        forwarder = SSHTunnelForwarder(**ssh_kwargs)
        try:
            forwarder.start()
            local_host = "127.0.0.1"
            local_port = int(forwarder.local_bind_port)
            yield local_host, local_port
        finally:
            forwarder.stop()
