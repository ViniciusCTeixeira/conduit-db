from __future__ import annotations

import sys
import time
from typing import List, Optional
import click
import pymysql
from rich.console import Console
from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn
from rich.table import Table

from conduit import __version__
from conduit.checkpoint import CheckpointStore
from conduit.config import ConfigManager, DatabaseConfig
from conduit.inspector import DatabaseInspector
from conduit.schema import SchemaMigrator
from conduit.streamer import DataStreamer
from conduit.tunnel import TunnelManager
from conduit.verifier import DataVerifier
from conduit.writer import DataWriter


console = Console()


def get_db_connection(cfg: DatabaseConfig, host_override: str, port_override: int) -> pymysql.Connection:
    return pymysql.connect(
        host=host_override,
        port=port_override,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database,
        charset=cfg.charset,
        connect_timeout=cfg.connect_timeout,
        read_timeout=cfg.read_timeout,
        write_timeout=cfg.write_timeout,
        autocommit=False,
    )


@click.group()
@click.version_option(version=__version__, prog_name="conduit-db")
def cli() -> None:
    """ConduitDB: Resilient, high-throughput, and resumable database migration CLI."""
    pass


@cli.command("test")
@click.option("--source", required=True, help="Profile name for source database.")
@click.option("--target", required=True, help="Profile name for target database.")
@click.option("--config", "config_path", default=None, help="Path to profiles.yaml.")
def test_cmd(source: str, target: str, config_path: Optional[str]) -> None:
    """Tests connectivity and SSH tunnels for source and target profiles."""
    profiles = ConfigManager.load_profiles(config_path)
    if source not in profiles:
        console.print(f"[bold red]Source profile '{source}' not found in profiles.yaml[/]")
        sys.exit(1)
    if target not in profiles:
        console.print(f"[bold red]Target profile '{target}' not found in profiles.yaml[/]")
        sys.exit(1)

    src_cfg = profiles[source]
    dst_cfg = profiles[target]

    console.print(Panel(f"Testing connections for [bold cyan]{source}[/] -> [bold green]{target}[/]", title="ConduitDB Connectivity Test"))

    # Test Source
    with console.status(f"Connecting to source [cyan]{source}[/]..."):
        try:
            with TunnelManager.open_tunnel(src_cfg) as (s_host, s_port):
                conn = get_db_connection(src_cfg, s_host, s_port)
                with conn.cursor() as cur:
                    cur.execute("SELECT VERSION()")
                    ver = cur.fetchone()[0]
                conn.close()
            console.print(f"  [green]✔[/] Source [bold cyan]{source}[/] OK (MySQL/MariaDB {ver})")
        except Exception as e:
            console.print(f"  [red]✘[/] Source [bold cyan]{source}[/] failed: {e}")
            sys.exit(1)

    # Test Target
    with console.status(f"Connecting to target [green]{target}[/]..."):
        try:
            with TunnelManager.open_tunnel(dst_cfg) as (t_host, t_port):
                conn = get_db_connection(dst_cfg, t_host, t_port)
                with conn.cursor() as cur:
                    cur.execute("SELECT VERSION()")
                    ver = cur.fetchone()[0]
                conn.close()
            console.print(f"  [green]✔[/] Target [bold green]{target}[/] OK (MySQL/MariaDB {ver})")
        except Exception as e:
            console.print(f"  [red]✘[/] Target [bold green]{target}[/] failed: {e}")
            sys.exit(1)

    console.print("[bold green]All connections verified successfully![/]")


@cli.command("inspect")
@click.option("--profile", required=True, help="Profile name to inspect.")
@click.option("--config", "config_path", default=None, help="Path to profiles.yaml.")
def inspect_cmd(profile: str, config_path: Optional[str]) -> None:
    """Inspects tables, primary keys, and row counts of a database profile."""
    profiles = ConfigManager.load_profiles(config_path)
    if profile not in profiles:
        console.print(f"[bold red]Profile '{profile}' not found in profiles.yaml[/]")
        sys.exit(1)

    cfg = profiles[profile]
    with TunnelManager.open_tunnel(cfg) as (host, port):
        conn = get_db_connection(cfg, host, port)
        try:
            inspector = DatabaseInspector(conn, cfg.database)
            tables = inspector.list_tables()

            table_ui = Table(title=f"Schema Overview: {cfg.database} ({len(tables)} tables)", header_style="bold magenta")
            table_ui.add_column("Table Name", style="cyan")
            table_ui.add_column("Primary Key", style="yellow")
            table_ui.add_column("Columns", justify="right")
            table_ui.add_column("Est. Rows", justify="right", style="green")
            table_ui.add_column("Dependencies", style="dim")

            for t in tables:
                meta = inspector.get_table_metadata(t)
                pk_display = meta.primary_key or (", ".join(meta.composite_pks) if meta.composite_pks else "[dim italic]none[/]")
                deps = ", ".join(meta.depends_on) if meta.depends_on else "-"
                table_ui.add_row(t, pk_display, str(len(meta.columns)), f"{meta.estimated_rows:,}", deps)

            console.print(table_ui)
        finally:
            conn.close()


@cli.command("schema")
@click.option("--source", required=True, help="Source profile name.")
@click.option("--target", required=True, help="Target profile name.")
@click.option("--drop", is_flag=True, default=False, help="Drop existing tables in target before creating.")
@click.option("--config", "config_path", default=None, help="Path to profiles.yaml.")
def schema_cmd(source: str, target: str, drop: bool, config_path: Optional[str]) -> None:
    """Copies schema structure (tables, columns, indexes) from source to target."""
    profiles = ConfigManager.load_profiles(config_path)
    src_cfg = profiles[source]
    dst_cfg = profiles[target]

    with TunnelManager.open_tunnel(src_cfg) as (s_host, s_port):
        with TunnelManager.open_tunnel(dst_cfg) as (t_host, t_port):
            src_conn = get_db_connection(src_cfg, s_host, s_port)
            dst_conn = get_db_connection(dst_cfg, t_host, t_port)
            try:
                inspector = DatabaseInspector(src_conn, src_cfg.database)
                tables = inspector.list_tables()
                sorted_tables = inspector.sort_tables_topologically(tables)
                migrator = SchemaMigrator(src_conn, dst_conn)

                with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), console=console) as progress:
                    task = progress.add_task(f"Migrating schema ({len(sorted_tables)} tables)...", total=len(sorted_tables))
                    for t in sorted_tables:
                        progress.update(task, description=f"Creating table [cyan]{t}[/]...")
                        migrator.apply_schema(t, drop_if_exists=drop)
                        progress.advance(task)

                    # Apply Foreign Keys
                    fk_task = progress.add_task("Applying foreign key constraints...", total=len(sorted_tables))
                    for t in sorted_tables:
                        migrator.apply_foreign_keys(t)
                        progress.advance(fk_task)

                console.print(f"[bold green]✔ Successfully migrated structure for {len(sorted_tables)} tables![/]")
            finally:
                src_conn.close()
                dst_conn.close()


@cli.command("transfer")
@click.option("--source", required=True, help="Source profile name.")
@click.option("--target", required=True, help="Target profile name.")
@click.option("--tables", default=None, help="Comma-separated table list to transfer.")
@click.option("--chunk-size", default=5000, help="Batch size per operation / transaction.")
@click.option("--truncate", is_flag=True, default=False, help="Truncate target table before migration.")
@click.option("--upsert", is_flag=True, default=False, help="Use ON DUPLICATE KEY UPDATE instead of plain insert.")
@click.option("--state-db", default=".conduit_state.db", help="Path to SQLite checkpoint database.")
@click.option("--config", "config_path", default=None, help="Path to profiles.yaml.")
def transfer_cmd(
    source: str,
    target: str,
    tables: Optional[str],
    chunk_size: int,
    truncate: bool,
    upsert: bool,
    state_db: str,
    config_path: Optional[str],
) -> None:
    """Streams and transfers table data with atomic checkpointing and auto-resume."""
    profiles = ConfigManager.load_profiles(config_path)
    src_cfg = profiles[source]
    dst_cfg = profiles[target]

    store = CheckpointStore(state_db)
    job_id = store.get_or_create_job(f"{source}:{src_cfg.database}", f"{target}:{dst_cfg.database}")

    with TunnelManager.open_tunnel(src_cfg) as (s_host, s_port):
        with TunnelManager.open_tunnel(dst_cfg) as (t_host, t_port):
            src_conn = get_db_connection(src_cfg, s_host, s_port)
            dst_conn = get_db_connection(dst_cfg, t_host, t_port)
            try:
                inspector = DatabaseInspector(src_conn, src_cfg.database)
                streamer = DataStreamer(src_conn, chunk_size=chunk_size)
                writer = DataWriter(dst_conn)

                if tables:
                    table_list = [t.strip() for t in tables.split(",") if t.strip()]
                else:
                    table_list = inspector.list_tables()
                    table_list = inspector.sort_tables_topologically(table_list)

                console.print(Panel(f"Transferring {len(table_list)} tables: [cyan]{source}[/] -> [green]{target}[/] (Batch: {chunk_size:,})", title="ConduitDB Transfer"))

                progress = Progress(
                    SpinnerColumn(),
                    TextColumn("[bold cyan]{task.fields[table]}[/]"),
                    BarColumn(),
                    TaskProgressColumn(),
                    TextColumn("{task.completed:,}/{task.total:,}"),
                    TimeElapsedColumn(),
                    TimeRemainingColumn(),
                    console=console,
                )

                with progress:
                    for table_name in table_list:
                        meta = inspector.get_table_metadata(table_name)
                        exact_count = inspector.get_exact_row_count(table_name)

                        cp = store.init_table(
                            job_id=job_id,
                            table_name=table_name,
                            pk_column=meta.primary_key,
                            pk_type=meta.pk_type,
                            total_rows=exact_count,
                        )

                        if cp.status == "COMPLETED" and not truncate:
                            console.print(f"  [dim]Skipping already completed table: [cyan]{table_name}[/] ({cp.migrated_rows:,} rows)[/]")
                            continue

                        if truncate:
                            writer.truncate_table(table_name)
                            store.reset_job(job_id)
                            cp.last_cursor_value = None
                            cp.migrated_rows = 0

                        task_id = progress.add_task("Transferring", total=exact_count, completed=cp.migrated_rows, table=table_name)

                        try:
                            resume_cursor = cp.last_cursor_value
                            for rows_chunk, col_names, last_pk in streamer.stream_table(meta, resume_cursor=resume_cursor):
                                writer.write_batch(table_name, col_names, rows_chunk, upsert=upsert)
                                store.update_cursor(job_id, table_name, last_pk, len(rows_chunk))
                                progress.advance(task_id, len(rows_chunk))

                            store.mark_completed(job_id, table_name)
                        except Exception as e:
                            store.mark_failed(job_id, table_name, str(e))
                            console.print(f"[bold red]Failed on table {table_name}: {e}[/]")
                            raise

                writer.restore_session()
                console.print("[bold green]✔ Transfer completed successfully![/]")
            finally:
                src_conn.close()
                dst_conn.close()


@cli.command("resume")
@click.option("--source", default=None, help="Source profile (optional if inferred).")
@click.option("--target", default=None, help="Target profile (optional if inferred).")
@click.option("--chunk-size", default=5000, help="Batch size per operation.")
@click.option("--state-db", default=".conduit_state.db", help="Path to SQLite checkpoint database.")
@click.option("--config", "config_path", default=None, help="Path to profiles.yaml.")
@click.pass_context
def resume_cmd(
    ctx: click.Context,
    source: Optional[str],
    target: Optional[str],
    chunk_size: int,
    state_db: str,
    config_path: Optional[str],
) -> None:
    """Resumes an interrupted transfer from the exact last saved cursor."""
    store = CheckpointStore(state_db)
    with store._get_connection() as conn:
        job = conn.execute("SELECT * FROM conduit_jobs ORDER BY updated_at DESC LIMIT 1").fetchone()

    if not job:
        console.print("[yellow]No active migration job found to resume.[/]")
        return

    src_name = source or job["source_name"].split(":")[0]
    dst_name = target or job["target_name"].split(":")[0]
    console.print(f"[bold cyan]Resuming job:[/] {job['job_id']} ([cyan]{src_name}[/] -> [green]{dst_name}[/])")

    ctx.invoke(
        transfer_cmd,
        source=src_name,
        target=dst_name,
        chunk_size=chunk_size,
        state_db=state_db,
        config_path=config_path,
    )


@cli.command("verify")
@click.option("--source", required=True, help="Source profile name.")
@click.option("--target", required=True, help="Target profile name.")
@click.option("--tables", default=None, help="Comma-separated table list to verify.")
@click.option("--hash", "check_hash", is_flag=True, default=False, help="Compute row-level CRC32 checksums.")
@click.option("--config", "config_path", default=None, help="Path to profiles.yaml.")
def verify_cmd(source: str, target: str, tables: Optional[str], check_hash: bool, config_path: Optional[str]) -> None:
    """Verifies row counts and checksums across source and target databases."""
    profiles = ConfigManager.load_profiles(config_path)
    src_cfg = profiles[source]
    dst_cfg = profiles[target]

    with TunnelManager.open_tunnel(src_cfg) as (s_host, s_port):
        with TunnelManager.open_tunnel(dst_cfg) as (t_host, t_port):
            src_conn = get_db_connection(src_cfg, s_host, s_port)
            dst_conn = get_db_connection(dst_cfg, t_host, t_port)
            try:
                inspector = DatabaseInspector(src_conn, src_cfg.database)
                verifier = DataVerifier(src_conn, dst_conn)

                if tables:
                    table_list = [t.strip() for t in tables.split(",") if t.strip()]
                else:
                    table_list = inspector.list_tables()

                table_ui = Table(title=f"Verification Report: {source} -> {target}", header_style="bold magenta")
                table_ui.add_column("Table", style="cyan")
                table_ui.add_column("Source Rows", justify="right")
                table_ui.add_column("Target Rows", justify="right")
                table_ui.add_column("Count Match", justify="center")
                if check_hash:
                    table_ui.add_column("Hash Match", justify="center")
                table_ui.add_column("Status", style="bold")

                all_ok = True
                with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), console=console) as progress:
                    task = progress.add_task(f"Verifying {len(table_list)} tables...", total=len(table_list))
                    for t in table_list:
                        progress.update(task, description=f"Auditing [cyan]{t}[/]...")
                        res = verifier.verify_table(t, check_hash=check_hash)
                        progress.advance(task)

                        count_status = "[green]YES[/]" if res.count_matches else "[red]NO[/]"
                        is_ok = res.count_matches and (res.checksum_matches if check_hash else True)
                        if not is_ok:
                            all_ok = False
                        status_str = "[bold green]PASS[/]" if is_ok else "[bold red]FAIL[/]"

                        row = [t, f"{res.source_count:,}", f"{res.target_count:,}", count_status]
                        if check_hash:
                            hash_status = "[green]MATCH[/]" if res.checksum_matches else "[red]MISMATCH[/]"
                            row.append(hash_status)
                        row.append(status_str)
                        table_ui.add_row(*row)

                console.print(table_ui)
                if all_ok:
                    console.print("[bold green]✔ All verified tables match 100%![/]")
                else:
                    console.print("[bold red]✘ Discrepancies detected between source and target![/]")
            finally:
                src_conn.close()
                dst_conn.close()


@cli.command("reset")
@click.option("--source", required=True, help="Source profile name.")
@click.option("--target", required=True, help="Target profile name.")
@click.option("--state-db", default=".conduit_state.db", help="Path to SQLite checkpoint database.")
def reset_cmd(source: str, target: str, state_db: str) -> None:
    """Clears checkpoint state for a given source and target migration."""
    store = CheckpointStore(state_db)
    with store._get_connection() as conn:
        job = conn.execute(
            "SELECT job_id FROM conduit_jobs WHERE source_name LIKE ? AND target_name LIKE ? LIMIT 1",
            (f"{source}%", f"{target}%"),
        ).fetchone()

    job_id = job["job_id"] if job else f"{source}_to_{target}"
    store.reset_job(job_id)
    console.print(f"[bold yellow]Checkpoint state cleared for job:[/] {job_id}")


if __name__ == "__main__":
    cli()
