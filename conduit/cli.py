from __future__ import annotations

import sys
import time
from typing import List, Optional
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
from conduit.inspector import DatabaseInspector
from conduit.schema import SchemaMigrator
from conduit.streamer import DataStreamer
from conduit.tunnel import TunnelManager
from conduit.verifier import DataVerifier
from conduit.writer import DataWriter


console = Console()


class TableNameColumn(ProgressColumn):
    """Renders table name with consistent padding for clean column alignment."""

    def render(self, task) -> Text:
        tname = task.fields.get("table", task.description)
        is_total = task.fields.get("unit") == "tables"
        style = "bold magenta" if is_total else "bold cyan"
        return Text(f"{tname:<22}", style=style)


class ProgressCounterColumn(ProgressColumn):
    """Renders completed and total count with dynamic unit (rows or tables)."""

    def render(self, task) -> Text:
        unit = task.fields.get("unit", "rows")
        return Text(f"{task.completed:,}/{task.total:,} {unit}", style="white")


class RowsSpeedColumn(ProgressColumn):
    """Renders transfer throughput in rows per second with styling."""

    def render(self, task) -> Text:
        if task.fields.get("unit") == "tables":
            return Text("")
        if task.speed is None:
            return Text("-- rows/s", style="dim yellow")
        return Text(f"{task.speed:,.0f} rows/s", style="bold yellow")


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

                console.print(Panel(
                    f"Transferring {len(table_list)} tables: [cyan]{source}[/] -> [green]{target}[/] (Batch: {chunk_size:,})",
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
                    console=console,
                )

                with progress:
                    overall_task = progress.add_task(
                        "Total Progress",
                        total=len(table_list),
                        completed=0,
                        table="[Total Progress]",
                        unit="tables",
                    ) if len(table_list) > 1 else None

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
                            if overall_task is not None:
                                progress.advance(overall_task, 1)
                            continue

                        if truncate:
                            writer.truncate_table(table_name)
                            store.reset_job(job_id)
                            cp.last_cursor_value = None
                            cp.migrated_rows = 0

                        task_id = progress.add_task(
                            "Transferring",
                            total=exact_count,
                            completed=cp.migrated_rows,
                            table=table_name,
                            unit="rows",
                        )

                        try:
                            resume_cursor = cp.last_cursor_value
                            for rows_chunk, col_names, last_pk in streamer.stream_table(meta, resume_cursor=resume_cursor):
                                writer.write_batch(table_name, col_names, rows_chunk, upsert=upsert)
                                store.update_cursor(job_id, table_name, last_pk, len(rows_chunk))
                                progress.advance(task_id, len(rows_chunk))

                            store.mark_completed(job_id, table_name)
                            if overall_task is not None:
                                progress.advance(overall_task, 1)
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
def resume_cmd(
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

    transfer_cmd.callback(
        source=src_name,
        target=dst_name,
        tables=None,
        chunk_size=chunk_size,
        truncate=False,
        upsert=False,
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
            console.print("  [bold yellow]4[/]  🚀  [bold]Start data transfer[/] (Streaming with progress bars and checkpoints)")
            console.print("  [bold yellow]5[/]  ⏯️   [bold]Resume interrupted migration[/] (Resume from last saved cursor)")
            console.print("  [bold yellow]6[/]  ✅  [bold]Audit & verify integrity[/] (Row count & CRC32 row checksums)")
            console.print("  [bold yellow]7[/]  🧹  [bold]Reset migration checkpoints[/] (Clear saved state)")
            console.print("  [bold yellow]0[/]  🚪  [bold]Exit[/]")

            choice = Prompt.ask("\n[bold]Select an operation[/]", choices=["0", "1", "2", "3", "4", "5", "6", "7"], default="1")
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

                transfer_cmd.callback(
                    source=src,
                    target=dst,
                    tables=selected_tables,
                    chunk_size=chunk_sz,
                    truncate=trunc,
                    upsert=ups,
                    state_db=".conduit_state.db",
                    config_path=config_path,
                )

            elif choice == "5":
                resume_cmd.callback(
                    source=None,
                    target=None,
                    chunk_size=5000,
                    state_db=".conduit_state.db",
                    config_path=config_path,
                )

            elif choice == "6":
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

            elif choice == "7":
                src = select_profile("Select Source profile", profiles, default=profile_names[0])
                dst_default = profile_names[1] if len(profile_names) > 1 else profile_names[0]
                dst = select_profile("Select Target profile", profiles, default=dst_default)
                reset_cmd.callback(source=src, target=dst, state_db=".conduit_state.db")

        except Exception as err:
            console.print(f"[bold red]Error during operation:[/] {err}")


if __name__ == "__main__":
    cli()
