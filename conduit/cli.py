from __future__ import annotations

import sys
import time
from typing import Any, Dict, List, Optional
import click
import pymysql
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    ProgressColumn,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.prompt import Confirm, IntPrompt, Prompt
from rich.table import Table
from rich.text import Text

from conduit import __version__
from conduit.checkpoint import CheckpointStore
from conduit.config import ConfigManager, DatabaseConfig
from conduit.inspector import DatabaseInspector, IndexInfo, TableMeta
from conduit.schema import SchemaMigrator
from conduit.streamer import DataStreamer
from conduit.synchronizer import DataSynchronizer, TableSyncStats
from conduit.tunnel import TunnelManager
from conduit.verifier import DataVerifier
from conduit.writer import DataWriter, configure_socket_keepalive


console = Console()


class TableNameColumn(ProgressColumn):
    """Renders table name with consistent padding for clean column alignment."""

    def render(self, task) -> Text:
        tname = task.fields.get("table", task.description)
        is_total = task.fields.get("is_total", False) or tname == "[Total Progress]"
        style = "bold magenta" if is_total else "bold cyan"
        return Text(f"{tname:<22}", style=style)


class ProgressCounterColumn(ProgressColumn):
    """Renders completed and total count with dynamic unit (rows or tables)."""

    def render(self, task) -> Text:
        unit = task.fields.get("unit", "rows")
        completed = int(task.completed or 0)
        total = int(task.total or 0)
        return Text(f"{completed:,}/{total:,} {unit}", style="white")


class RowsSpeedColumn(ProgressColumn):
    """Renders transfer throughput in rows per second with styling."""

    def render(self, task) -> Text:
        if task.fields.get("unit") == "tables":
            return Text("")
        if task.speed is None:
            return Text("-- rows/s", style="dim yellow")
        return Text(f"{task.speed:,.0f} rows/s", style="bold yellow")


class OperationColumn(ProgressColumn):
    """Renders current phase, operation or table counter."""

    def render(self, task) -> Text:
        op = task.fields.get("operation", "")
        if not op:
            return Text("")
        is_total = task.fields.get("is_total", False) or task.fields.get("table") == "[Total Progress]"
        style = "bold cyan" if is_total else "dim cyan"
        return Text(f"[{op}]", style=style)


def get_db_connection(cfg: DatabaseConfig, host_override: str, port_override: int) -> pymysql.Connection:
    conn = pymysql.connect(
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
    configure_socket_keepalive(conn)
    return conn


def wait_for_active_alter_table(conn: pymysql.Connection, table_name: str, check_interval: int = 5) -> None:
    """If an ALTER TABLE query is already executing on the server, waits and polls until it finishes."""
    while True:
        try:
            with conn.cursor() as cursor:
                cursor.execute("SHOW PROCESSLIST")
                running = False
                for row in cursor.fetchall():
                    if len(row) >= 8:
                        info = str(row[7] or "").lower()
                        tbl = table_name.lower()
                        if "alter table" in info and tbl in info:
                            running = True
                            state = row[6] or "altering table"
                            time_sec = row[5] or 0
                            console.print(f"  [yellow]⏳ Query in progress on server:[/] {state} ({time_sec}s elapsed)... waiting...")
                            break
            if not running:
                break
            time.sleep(check_interval)
        except Exception:
            time.sleep(check_interval)


@click.group(invoke_without_command=True)
@click.version_option(version=__version__, prog_name="conduit-db")
@click.option("--config", "config_path", default=None, help="Path to profiles.yaml.")
@click.pass_context
def cli(ctx: click.Context, config_path: Optional[str]) -> None:
    """ConduitDB: Resilient, high-throughput, and resumable database migration CLI."""
    if ctx.invoked_subcommand is None:
        interactive_menu(config_path)


@cli.command("menu")
@click.option("--config", "config_path", default=None, help="Path to profiles.yaml.")
def menu_cmd(config_path: Optional[str]) -> None:
    """Launches the interactive wizard menu."""
    interactive_menu(config_path)


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
@click.option("--auto-index/--no-auto-index", default=True, help="Temporarily drop secondary indexes for big tables.")
@click.option("--index-threshold", default=100000, type=int, help="Threshold to drop secondary indexes.")
@click.option("--state-db", default=".conduit_state.db", help="Path to SQLite checkpoint database.")
@click.option("--config", "config_path", default=None, help="Path to profiles.yaml.")
def transfer_cmd(
    source: str,
    target: str,
    tables: Optional[str],
    chunk_size: int,
    truncate: bool,
    upsert: bool,
    auto_index: bool,
    index_threshold: int,
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

                # Pre-inspect row counts to provide accurate global row-level progress
                table_counts: Dict[str, int] = {}
                table_metas: Dict[str, Any] = {}
                grand_total_rows = 0
                grand_migrated_rows = 0
                completed_tables = 0

                for t_name in table_list:
                    cp_existing = store.get_table_checkpoint(job_id, t_name)
                    if cp_existing is not None and cp_existing.total_rows > 0:
                        c_count = cp_existing.total_rows
                        c_migrated = cp_existing.migrated_rows
                        if cp_existing.status == "COMPLETED" and not truncate:
                            completed_tables += 1
                        elif (cp_existing.last_cursor_value is None or c_migrated > c_count) and not truncate:
                            c_migrated = 0
                    else:
                        meta_t = inspector.get_table_metadata(t_name)
                        table_metas[t_name] = meta_t
                        c_count = inspector.get_exact_row_count(t_name)
                        c_migrated = 0
                    table_counts[t_name] = c_count
                    grand_total_rows += c_count
                    grand_migrated_rows += c_migrated

                num_tables = len(table_list)
                display_grand_total = max(1, grand_total_rows) if grand_total_rows == 0 else grand_total_rows

                console.print(Panel(
                    f"Transferring {num_tables} tables: [cyan]{source}[/] -> [green]{target}[/] (Batch: {chunk_size:,}) | Total Dataset: [bold yellow]{grand_total_rows:,}[/] rows",
                    title="ConduitDB Transfer",
                ))

                progress = Progress(
                    SpinnerColumn(),
                    TableNameColumn(),
                    BarColumn(),
                    TaskProgressColumn(),
                    ProgressCounterColumn(),
                    RowsSpeedColumn(),
                    TimeElapsedColumn(),
                    TimeRemainingColumn(),
                    OperationColumn(),
                    console=console,
                )

                with progress:
                    overall_task = progress.add_task(
                        "Total Progress",
                        total=display_grand_total,
                        completed=grand_migrated_rows,
                        table="[Total Progress]",
                        unit="rows",
                        is_total=True,
                        operation=f"{completed_tables}/{num_tables} tables",
                    ) if num_tables > 1 else None

                    for table_name in table_list:
                        meta = table_metas.get(table_name) or inspector.get_table_metadata(table_name)
                        exact_count = table_counts.get(table_name)
                        if exact_count is None:
                            exact_count = inspector.get_exact_row_count(table_name)
                            table_counts[table_name] = exact_count

                        cp = store.init_table(
                            job_id=job_id,
                            table_name=table_name,
                            pk_column=meta.pk_columns_str,
                            pk_type=meta.pk_type,
                            total_rows=exact_count,
                        )

                        if cp.status == "COMPLETED" and not truncate:
                            console.print(f"  [dim]Skipping already completed table data: [cyan]{table_name}[/] ({cp.migrated_rows:,} rows)[/]")
                            # Restore any secondary indexes that might still be pending
                            pending_indexes = store.get_dropped_indexes(job_id, table_name)
                            if pending_indexes:
                                if overall_task is not None:
                                    progress.update(overall_task, operation=f"{completed_tables}/{num_tables} tbls | 🔨 Rebuilding Indexes")
                                console.print(f"  [cyan]🔨 Smart Indexing:[/] Rebuilding {len(pending_indexes)} pending secondary indexes on [green]{table_name}[/] (single-pass batch)...")
                                wait_for_active_alter_table(dst_conn, table_name)
                                definitions = [p["definition"] for p in pending_indexes]
                                try:
                                    writer.recreate_secondary_indexes_batch(table_name, definitions)
                                    for p in pending_indexes:
                                        store.mark_index_restored(job_id, table_name, p["index_name"])
                                        console.print(f"    [green]✔ Restored index:[/] {p['index_name']}")
                                except Exception as r_err:
                                    console.print(f"    [bold red]✖ Failed to restore indexes on {table_name}: {r_err}[/]")
                                    raise
                            if overall_task is not None:
                                progress.update(overall_task, operation=f"{completed_tables}/{num_tables} tables")
                            continue

                        if truncate:
                            writer.truncate_table(table_name)
                            store.reset_job(job_id)
                            if cp.migrated_rows > 0 and overall_task is not None:
                                grand_migrated_rows = max(0, grand_migrated_rows - cp.migrated_rows)
                                progress.update(overall_task, completed=grand_migrated_rows)
                            cp.last_cursor_value = None
                            cp.migrated_rows = 0
                        elif cp.status != "COMPLETED" and (cp.last_cursor_value is None or cp.migrated_rows > exact_count):
                            if cp.migrated_rows > 0:
                                with store._get_connection() as conn:
                                    conn.execute(
                                        "UPDATE conduit_tables SET migrated_rows = 0, last_cursor_value = NULL WHERE job_id = ? AND table_name = ?",
                                        (job_id, table_name),
                                    )
                                    conn.commit()
                                if overall_task is not None:
                                    grand_migrated_rows = max(0, grand_migrated_rows - cp.migrated_rows)
                                    progress.update(overall_task, completed=grand_migrated_rows)
                                cp.migrated_rows = 0
                                cp.last_cursor_value = None

                        # Smart Indexing: Pre-Stream Phase
                        if auto_index and exact_count >= index_threshold:
                            tgt_inspector = DatabaseInspector(dst_conn, dst_cfg.database)
                            sec_indexes = tgt_inspector.get_secondary_indexes(table_name)
                            if sec_indexes:
                                if overall_task is not None:
                                    progress.update(overall_task, operation=f"{completed_tables}/{num_tables} tbls | ⚡ Dropping Indexes")
                                console.print(f"  [yellow]⚡ Smart Indexing:[/] Temporarily dropping {len(sec_indexes)} secondary indexes on [green]{table_name}[/] to maximize bulk write throughput...")
                                for s_idx in sec_indexes:
                                    try:
                                        store.record_dropped_index(job_id, table_name, s_idx.name, s_idx.definition)
                                        writer.drop_secondary_index(table_name, s_idx.name)
                                        console.print(f"    [dim yellow]Dropped index:[/] {s_idx.name}")
                                    except Exception as idx_err:
                                        console.print(f"    [dim yellow]Warning: Could not drop index {s_idx.name}: {idx_err}[/]")

                        if overall_task is not None:
                            progress.update(overall_task, operation=f"{completed_tables}/{num_tables} tbls | Streaming")

                        task_id = progress.add_task(
                            "Transferring",
                            total=exact_count,
                            completed=cp.migrated_rows,
                            table=table_name,
                            unit="rows",
                            operation="Streaming",
                        )

                        try:
                            resume_cursor = cp.last_cursor_value
                            for rows_chunk, col_names, last_pk in streamer.stream_table(meta, resume_cursor=resume_cursor):
                                writer.write_batch(table_name, col_names, rows_chunk, upsert=upsert)
                                store.update_cursor(job_id, table_name, last_pk, len(rows_chunk))
                                progress.advance(task_id, len(rows_chunk))
                                if overall_task is not None:
                                    progress.advance(overall_task, len(rows_chunk))

                            # Smart Indexing: Post-Stream Phase
                            pending_indexes = store.get_dropped_indexes(job_id, table_name)
                            if pending_indexes:
                                if overall_task is not None:
                                    progress.update(overall_task, operation=f"{completed_tables}/{num_tables} tbls | 🔨 Rebuilding Indexes")
                                progress.update(task_id, operation="🔨 Rebuilding Indexes")
                                console.print(f"  [cyan]🔨 Smart Indexing:[/] Rebuilding {len(pending_indexes)} secondary indexes on [green]{table_name}[/] (single-pass batch)...")
                                wait_for_active_alter_table(dst_conn, table_name)
                                definitions = [p["definition"] for p in pending_indexes]
                                try:
                                    writer.recreate_secondary_indexes_batch(table_name, definitions)
                                    for p in pending_indexes:
                                        store.mark_index_restored(job_id, table_name, p["index_name"])
                                        console.print(f"    [green]✔ Restored index:[/] {p['index_name']}")
                                except Exception as r_err:
                                    console.print(f"    [bold red]✖ Failed to restore indexes on {table_name}: {r_err}[/]")
                                    raise

                            store.mark_completed(job_id, table_name)
                            completed_tables += 1
                            progress.update(task_id, operation="✔ Done")
                            if overall_task is not None:
                                progress.update(overall_task, operation=f"{completed_tables}/{num_tables} tables")
                        except KeyboardInterrupt:
                            raise
                        except Exception as e:
                            store.mark_failed(job_id, table_name, str(e))
                            console.print(f"[bold red]Failed on table {table_name}: {e}[/]")
                            raise

                writer.restore_session()
                console.print("[bold green]✔ Transfer completed successfully![/]")
            except KeyboardInterrupt:
                console.print("\n[bold yellow]⚠ Transfer aborted by user (Ctrl+C). Progress and checkpoints preserved.[/]")
                try:
                    writer.restore_session()
                except Exception:
                    pass
                return
            finally:
                try:
                    src_conn.close()
                except Exception:
                    pass
                try:
                    dst_conn.close()
                except Exception:
                    pass


@cli.command("resume")
@click.option("--source", default=None, help="Source profile (optional if inferred).")
@click.option("--target", default=None, help="Target profile (optional if inferred).")
@click.option("--chunk-size", default=5000, help="Batch size per operation.")
@click.option("--auto-index/--no-auto-index", default=True, help="Temporarily drop secondary indexes for big tables.")
@click.option("--index-threshold", default=100000, type=int, help="Threshold to drop secondary indexes.")
@click.option("--state-db", default=".conduit_state.db", help="Path to SQLite checkpoint database.")
@click.option("--config", "config_path", default=None, help="Path to profiles.yaml.")
def resume_cmd(
    source: Optional[str],
    target: Optional[str],
    chunk_size: int,
    auto_index: bool,
    index_threshold: int,
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

    transfer_cmd.callback(
        source=src_name,
        target=dst_name,
        tables=None,
        chunk_size=chunk_size,
        truncate=False,
        upsert=False,
        auto_index=auto_index,
        index_threshold=index_threshold,
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


@cli.command("sync")
@click.option("--source", "-s", required=True, help="Source profile name.")
@click.option("--target", "-t", required=True, help="Target profile name.")
@click.option("--tables", default=None, help="Comma-separated table list to synchronize.")
@click.option("--chunk-size", default=5000, type=int, help="Batch size for windowed checksum.")
@click.option("--prune/--no-prune", default=False, help="Delete target rows missing from source.")
@click.option("--dry-run", is_flag=True, default=False, help="Preview diff counts without modifying target.")
@click.option("--auto-index/--no-auto-index", default=True, help="Temporarily drop secondary indexes for big tables.")
@click.option("--index-threshold", default=100000, type=int, help="Row threshold to trigger index optimization.")
@click.option("--config", "config_path", default=None, help="Path to profiles.yaml.")
def sync_cmd(
    source: str,
    target: str,
    tables: Optional[str],
    chunk_size: int,
    prune: bool,
    dry_run: bool,
    auto_index: bool,
    index_threshold: int,
    config_path: Optional[str],
) -> None:
    """Differential synchronization: compares checksums and synchronizes INSERT, UPDATE, and DELETE."""
    profiles = ConfigManager.load_profiles(config_path)
    src_cfg = profiles[source]
    dst_cfg = profiles[target]

    store = CheckpointStore()
    job_id = store.get_or_create_job(f"{source}:{src_cfg.database}", f"{target}:{dst_cfg.database}")

    mode_label = "[bold yellow]DRY RUN[/] " if dry_run else ""
    prune_label = "[bold red]PRUNE ENABLED[/]" if prune else "prune disabled"

    console.print(Panel(
        f"{mode_label}Synchronizing: [cyan]{source}[/] <-> [green]{target}[/] ({prune_label}, Batch: {chunk_size:,})",
        title="ConduitDB Differential Sync",
    ))

    with TunnelManager.open_tunnel(src_cfg) as (s_host, s_port):
        with TunnelManager.open_tunnel(dst_cfg) as (t_host, t_port):
            src_conn = get_db_connection(src_cfg, s_host, s_port)
            dst_conn = get_db_connection(dst_cfg, t_host, t_port)
            try:
                inspector = DatabaseInspector(src_conn, src_cfg.database)
                synchronizer = DataSynchronizer(
                    src_conn,
                    dst_conn,
                    chunk_size=chunk_size,
                    prune=prune,
                    dry_run=dry_run,
                )

                if tables:
                    table_list = [t.strip() for t in tables.split(",") if t.strip()]
                else:
                    table_list = inspector.list_tables()
                    table_list = inspector.sort_tables_topologically(table_list)

                summary_stats: List[TableSyncStats] = []

                for table_name in table_list:
                    meta = inspector.get_table_metadata(table_name)
                    exact_count = inspector.get_exact_row_count(table_name)

                    console.print(f"\n[bold cyan]Syncing table:[/] {table_name} ({exact_count:,} rows in source)")

                    # Smart Indexing: Pre-Sync Phase
                    if auto_index and exact_count >= index_threshold and not dry_run:
                        tgt_inspector = DatabaseInspector(dst_conn, dst_cfg.database)
                        sec_indexes = tgt_inspector.get_secondary_indexes(table_name)
                        if sec_indexes:
                            console.print(f"  [yellow]⚡ Smart Indexing:[/] Dropping {len(sec_indexes)} secondary indexes during sync...")
                            for s_idx in sec_indexes:
                                try:
                                    store.record_dropped_index(job_id, table_name, s_idx.name, s_idx.definition)
                                    synchronizer.writer.drop_secondary_index(table_name, s_idx.name)
                                except Exception as idx_err:
                                    console.print(f"    [dim yellow]Warning: {idx_err}[/]")

                    stats = synchronizer.sync_table(meta)
                    summary_stats.append(stats)

                    # Smart Indexing: Post-Sync Phase
                    if auto_index and not dry_run:
                        pending_indexes = store.get_dropped_indexes(job_id, table_name)
                        if pending_indexes:
                            console.print(f"  [cyan]🔨 Smart Indexing:[/] Rebuilding {len(pending_indexes)} secondary indexes on [green]{table_name}[/] (single-pass batch)...")
                            wait_for_active_alter_table(dst_conn, table_name)
                            definitions = [p_idx["definition"] for p_idx in pending_indexes]
                            try:
                                synchronizer.writer.recreate_secondary_indexes_batch(table_name, definitions)
                                for p_idx in pending_indexes:
                                    store.mark_index_restored(job_id, table_name, p_idx["index_name"])
                                    console.print(f"    [green]✔ Restored index:[/] {p_idx['index_name']}")
                            except Exception as r_err:
                                console.print(f"    [red]✖ Failed to restore indexes on {table_name}: {r_err}[/]")
                                raise

                synchronizer.writer.restore_session()

                # Print summary table
                res_table = Table(title="ConduitDB Synchronization Summary", header_style="bold cyan")
                res_table.add_column("Table", style="bold")
                res_table.add_column("Inserted", justify="right", style="green")
                res_table.add_column("Updated", justify="right", style="yellow")
                res_table.add_column("Deleted", justify="right", style="red")
                res_table.add_column("Unchanged", justify="right", style="dim")
                res_table.add_column("Duration", justify="right", style="cyan")

                for st in summary_stats:
                    res_table.add_row(
                        st.table_name,
                        f"+{st.inserted:,}" if st.inserted else "0",
                        f"~{st.updated:,}" if st.updated else "0",
                        f"-{st.deleted:,}" if st.deleted else "0",
                        f"{st.unchanged:,}",
                        f"{st.time_taken_seconds:.2f}s",
                    )
                console.print(res_table)
                console.print(f"[bold green]✔ Synchronization completed ({'DRY-RUN PREVIEW' if dry_run else 'APPLIED'})![/]")
            finally:
                src_conn.close()
                dst_conn.close()


@cli.group("indexes")
def indexes_group() -> None:
    """Manage dropped/pending secondary indexes from Smart Indexing."""
    pass


@indexes_group.command("list")
@click.option("--state-db", default=".conduit_state.db", help="Path to SQLite checkpoint database.")
def indexes_list_cmd(state_db: str) -> None:
    """Lists all dropped and restored secondary indexes in checkpoint store."""
    store = CheckpointStore(state_db)
    all_indexes = store.list_all_indexes()
    if not all_indexes:
        console.print("[yellow]No index tracking records found in checkpoint store.[/]")
        return

    table = Table(title="Smart Indexing - Tracked Indexes", header_style="bold cyan")
    table.add_column("Job ID", style="dim")
    table.add_column("Table", style="bold")
    table.add_column("Index Name", style="cyan")
    table.add_column("Status", style="bold")
    table.add_column("Updated At", style="dim")

    for idx in all_indexes:
        st_style = "yellow" if idx["status"] == "DROPPED" else "green"
        table.add_row(
            idx["job_id"],
            idx["table_name"],
            idx["index_name"],
            f"[{st_style}]{idx['status']}[/]",
            idx["updated_at"],
        )
    console.print(table)


@indexes_group.command("restore")
@click.option("--target", "-t", required=True, help="Target profile name to restore indexes on.")
@click.option("--table", default=None, help="Filter by specific table name.")
@click.option("--state-db", default=".conduit_state.db", help="Path to SQLite checkpoint database.")
@click.option("--config", "config_path", default=None, help="Path to profiles.yaml.")
def indexes_restore_cmd(
    target: str,
    table: Optional[str],
    state_db: str,
    config_path: Optional[str],
) -> None:
    """Restores any pending DROPPED secondary indexes on target database."""
    profiles = ConfigManager.load_profiles(config_path)
    dst_cfg = profiles[target]
    store = CheckpointStore(state_db)

    pending = store.get_dropped_indexes(table_name=table) if table else [
        idx for idx in store.list_all_indexes() if idx["status"] == "DROPPED"
    ]

    if not pending:
        console.print("[green]✔ No pending dropped indexes found to restore![/]")
        return

    console.print(Panel(
        f"Restoring {len(pending)} dropped secondary indexes on target [green]{target}[/]",
        title="ConduitDB Index Restoration",
    ))

    with TunnelManager.open_tunnel(dst_cfg) as (t_host, t_port):
        dst_conn = get_db_connection(dst_cfg, t_host, t_port)
        try:
            writer = DataWriter(dst_conn)
            from collections import defaultdict
            by_table = defaultdict(list)
            for idx in pending:
                by_table[idx["table_name"]].append(idx)

            for t_name, idx_list in by_table.items():
                console.print(f"  Rebuilding {len(idx_list)} secondary indexes on [bold]{t_name}[/] (single-pass batch)...")
                wait_for_active_alter_table(dst_conn, t_name)
                definitions = [i["definition"] for i in idx_list]
                try:
                    writer.recreate_secondary_indexes_batch(t_name, definitions)
                    for i in idx_list:
                        store.mark_index_restored(i["job_id"], t_name, i["index_name"])
                        console.print(f"    [green]✔ Successfully restored {i['index_name']}[/]")
                except Exception as e:
                    console.print(f"    [bold red]✖ Failed to restore indexes on {t_name}: {e}[/]")
            writer.restore_session()
        finally:
            dst_conn.close()


def select_profile(prompt_text: str, profiles: dict[str, DatabaseConfig], default: Optional[str] = None) -> str:
    """Helper to select a database profile with interactive prompt."""
    names = list(profiles.keys())
    if not names:
        raise ValueError("No database profiles configured.")
    if len(names) == 1:
        return names[0]
    if default not in names:
        default = names[0]
    return Prompt.ask(prompt_text, choices=names, default=default)


def interactive_menu(config_path: Optional[str] = None) -> None:
    """Displays an interactive terminal wizard menu."""
    try:
        profiles = ConfigManager.load_profiles(config_path)
    except Exception as e:
        console.print(Panel(
            f"[bold red]Failed to load database profiles:[/] {e}\n\n"
            "[yellow]Tip:[/] Create [bold cyan]profiles.yaml[/] from [bold cyan]profiles.example.yaml[/].",
            title="[bold yellow]ConduitDB - Missing Configuration[/]",
            border_style="yellow",
        ))
        return

    profile_names = list(profiles.keys())
    if not profile_names:
        console.print(Panel(
            "[bold yellow]No database profiles found![/]\n\n"
            "Create [bold cyan]profiles.yaml[/] from [bold cyan]profiles.example.yaml[/] to configure your databases.",
            title="[bold yellow]ConduitDB - Configuration Required[/]",
            border_style="yellow",
        ))
        return

    console.print(Panel.fit(
        f"[bold cyan]ConduitDB {__version__}[/] - [dim]Resilient Database Migration & Synchronization[/]\n"
        f"[yellow]Loaded Profiles:[/] {', '.join(f'[bold green]{p}[/]' for p in profile_names)}",
        border_style="cyan",
    ))

    while True:
        try:
            console.print("\n[bold cyan]── Main Menu ───────────────────────────────────────────────────[/]")
            console.print("  [bold yellow]1[/]  🔍  [bold]Inspect database[/] (tables, columns, primary keys, and row counts)")
            console.print("  [bold yellow]2[/]  🔗  [bold]Test connectivity & SSH tunnels[/] (Source <-> Target)")
            console.print("  [bold yellow]3[/]  📐  [bold]Synchronize structure (DDL)[/] (CREATE TABLE with deferred foreign keys)")
            console.print("  [bold yellow]4[/]  🚀  [bold]Start data transfer[/] (Streaming with progress bars and Smart Indexing)")
            console.print("  [bold yellow]5[/]  ⏯️   [bold]Resume interrupted migration[/] (Resume from last saved cursor)")
            console.print("  [bold yellow]6[/]  🔄  [bold]Differential sync[/] (Diff with windowed CRC32: INSERT, UPDATE, prune DELETE)")
            console.print("  [bold yellow]7[/]  ✅  [bold]Audit & verify integrity[/] (Row count & CRC32 row checksums)")
            console.print("  [bold yellow]8[/]  ⚡  [bold]Smart Indexing management[/] (List or restore dropped secondary indexes)")
            console.print("  [bold yellow]9[/]  🧹  [bold]Reset migration checkpoints[/] (Clear saved state)")
            console.print("  [bold yellow]0[/]  🚪  [bold]Exit[/]")

            choice = Prompt.ask("\n[bold]Select an operation[/]", choices=["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"], default="1")
        except (KeyboardInterrupt, EOFError):
            console.print("\n[dim]Operation cancelled by user.[/]")
            break

        if choice == "0":
            console.print("[dim]Exiting ConduitDB. Goodbye![/]")
            break

        try:
            if choice == "1":
                p = select_profile("Select profile to inspect", profiles)
                inspect_cmd.callback(profile=p, config_path=config_path)

            elif choice == "2":
                src = select_profile("Select Source profile", profiles, default=profile_names[0])
                dst_default = profile_names[1] if len(profile_names) > 1 else profile_names[0]
                dst = select_profile("Select Target profile", profiles, default=dst_default)
                test_cmd.callback(source=src, target=dst, config_path=config_path)

            elif choice == "3":
                src = select_profile("Select Source profile", profiles, default=profile_names[0])
                dst_default = profile_names[1] if len(profile_names) > 1 else profile_names[0]
                dst = select_profile("Select Target profile", profiles, default=dst_default)
                drop = Confirm.ask("Drop existing tables in target before creating? (--drop)", default=False)
                schema_cmd.callback(source=src, target=dst, drop=drop, config_path=config_path)

            elif choice == "4":
                src = select_profile("Select Source profile", profiles, default=profile_names[0])
                dst_default = profile_names[1] if len(profile_names) > 1 else profile_names[0]
                dst = select_profile("Select Target profile", profiles, default=dst_default)
                chunk_sz = IntPrompt.ask("Batch size per operation (chunk-size)", default=5000)
                t_input = Prompt.ask("Specific tables (comma-separated, or Enter for all)", default="").strip()
                selected_tables = t_input if t_input else None
                trunc = Confirm.ask("Truncate target tables before transfer?", default=False)
                ups = Confirm.ask("Use UPSERT mode (ON DUPLICATE KEY UPDATE)?", default=False)
                auto_idx = Confirm.ask("Enable Smart Indexing (temporarily drop secondary indexes for big tables)?", default=True)

                transfer_cmd.callback(
                    source=src,
                    target=dst,
                    tables=selected_tables,
                    chunk_size=chunk_sz,
                    truncate=trunc,
                    upsert=ups,
                    auto_index=auto_idx,
                    index_threshold=100000,
                    state_db=".conduit_state.db",
                    config_path=config_path,
                )

            elif choice == "5":
                resume_cmd.callback(
                    source=None,
                    target=None,
                    chunk_size=5000,
                    auto_index=True,
                    index_threshold=100000,
                    state_db=".conduit_state.db",
                    config_path=config_path,
                )

            elif choice == "6":
                src = select_profile("Select Source profile", profiles, default=profile_names[0])
                dst_default = profile_names[1] if len(profile_names) > 1 else profile_names[0]
                dst = select_profile("Select Target profile", profiles, default=dst_default)
                t_input = Prompt.ask("Specific tables (comma-separated, or Enter for all)", default="").strip()
                selected_tables = t_input if t_input else None
                chunk_sz = IntPrompt.ask("Window chunk size for checksum", default=5000)
                is_dry = Confirm.ask("Run in Preview mode without applying changes (--dry-run)?", default=False)
                do_prune = Confirm.ask("Delete rows from target missing from source (--prune)?", default=False)

                sync_cmd.callback(
                    source=src,
                    target=dst,
                    tables=selected_tables,
                    chunk_size=chunk_sz,
                    prune=do_prune,
                    dry_run=is_dry,
                    auto_index=True,
                    index_threshold=100000,
                    config_path=config_path,
                )

            elif choice == "7":
                src = select_profile("Select Source profile", profiles, default=profile_names[0])
                dst_default = profile_names[1] if len(profile_names) > 1 else profile_names[0]
                dst = select_profile("Select Target profile", profiles, default=dst_default)
                t_input = Prompt.ask("Specific tables (comma-separated, or Enter for all)", default="").strip()
                selected_tables = t_input if t_input else None
                do_hash = Confirm.ask("Compute full CRC32 row checksums?", default=True)
                verify_cmd.callback(
                    source=src,
                    target=dst,
                    tables=selected_tables,
                    check_hash=do_hash,
                    config_path=config_path,
                )

            elif choice == "8":
                act = Prompt.ask("Smart Indexing action", choices=["list", "restore"], default="list")
                if act == "list":
                    indexes_list_cmd.callback(state_db=".conduit_state.db")
                else:
                    dst = select_profile("Select Target profile to restore indexes on", profiles)
                    indexes_restore_cmd.callback(target=dst, table=None, state_db=".conduit_state.db", config_path=config_path)

            elif choice == "9":
                src = select_profile("Select Source profile", profiles, default=profile_names[0])
                dst_default = profile_names[1] if len(profile_names) > 1 else profile_names[0]
                dst = select_profile("Select Target profile", profiles, default=dst_default)
                reset_cmd.callback(source=src, target=dst, state_db=".conduit_state.db")

        except Exception as err:
            console.print(f"[bold red]Error during operation:[/] {err}")


if __name__ == "__main__":
    cli()
