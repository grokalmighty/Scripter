import click
from pathlib import Path

from .database import Database
from .scripts_repo import add_script, list_scripts, get_script
from .schedules_repo import add_schedule
from .scheduler import run_loop
from .runs_repo import list_runs, get_run
from .timefmt import to_local_display

@click.group()
def cli():
    """Scripter: script scheduler and automation engine."""
    pass

@cli.command()
def version():
    """Show version."""
    click.echo("Scripter v0.0.1")

@cli.group()
def script():
    """Manage scripts."""
    pass

@script.command("add")
@click.option("--db", "db_path", type=click.Path(dir_okay=False, path_type=Path), default=None)
@click.option("--name", required=True)
@click.option("--command", required=True)
@click.option("--cwd", "working_dir", default=None)
def script_add(db_path, name, command, working_dir):
    db = Database(db_path)
    script_id = add_script(db, name=name, command=command, working_dir=working_dir)
    click.echo(f"Added script #{script_id}: {name}")

@script.command("list")
@click.option("--db", "db_path", type=click.Path(dir_okay=False, path_type=Path), default=None)
def script_list(db_path):
    db = Database(db_path)
    scripts = list_scripts(db)
    if not scripts:
        click.echo("No scripts found.")
        return
    for s in scripts:
        click.echo(f"{s.id}\t{s.name}\t{s.command}")

@script.command("show")
@click.argument("script-id", type=int)
@click.option("--db", "db_path", type=click.Path(dir_okay=False, path_type=Path), default=None)
def script_show(script_id, db_path):
    db = Database(db_path)
    s = get_script(db, script_id)
    if s is None:
        raise click.ClickException(f"Script {script_id} not found")
    click.echo(f"id: {s.id}")
    click.echo(f"name: {s.name}")
    click.echo(f"command: {s.command}")
    click.echo(f"cwd: {s.working_dir or ''}")
    click.echo(f"created_at: {s.created_at}")
    click.echo(f"updated_at: {s.updated_at}")

@cli.group()
def schedule():
    """Manage schedules."""
    pass

@cli.command()
@click.option("--db", "db_path", type=click.Path(dir_okay=False, path_type=Path), default=None)
@click.option("--tick", "tick_seconds", type=int, default=2)
@click.option("--once", is_flag=True, help="Run a single scheduler tick then exit.")
def daemon(db_path, tick_seconds, once):
    """Start the scheduler loop."""
    click.echo(f"Starting scheduler (tick={tick_seconds}s)... Ctrl+C to stop.")
    run_loop(db_path=db_path, tick_seconds=tick_seconds, once=once)

@schedule.command("add")
@click.option("--db", "db_path", type=click.Path(dir_okay=False, path_type=Path), default=None)
@click.option("--script-id", type=int, required=True)
@click.option("--interval", "interval_seconds", type=int, required=True)
def schedule_add(db_path, script_id, interval_seconds):
    db = Database(db_path)
    sid = add_schedule(db, script_id=script_id, interval_seconds=interval_seconds)
    click.echo(f"Added schedule #{sid} for script {script_id} every {interval_seconds}s")

@cli.group()
def runs():
    """Inspect execution history."""
    pass

@runs.command("list")
@click.option("--db", "db_path", type=click.Path(dir_okay=False, path_type=Path), default=None)
@click.option("--limit", type=int, default=10)
@click.option("--script-id", type=int, default=None)
def runs_list(db_path, limit, script_id):
    db = Database(db_path)
    rows = list_runs(db, limit=limit, script_id=script_id)
    if not rows:
        click.echo("No runs found.")
        return 
    
    click.echo("id\tscript\tstatus\texit\tstarted\t\t\tfinished")
    for r in rows:
        click.echo(
            f"{r['id']}\t{r['script_id']}\t{r['status']}\t{r['exit_code']}\t"
            f"{to_local_display(r['started_at'])}\t{to_local_display(r['finished_at'])}"
        )

@runs.command("show")
@click.argument("run_id", type=int)
@click.option("--db", "db_path", type=click.Path(dir_okay=False, path_type=Path), default=None)
@click.option("--max", "max_chars", type=int, default=4000, help="Max chars to display for stdout/stderr.")
def runs_show(run_id, db_path, max_chars):
    db = Database(db_path)
    r = get_run(db, run_id)
    if r is None:
        raise click.ClickException(f"Run {run_id} not found")
    
    def clip(s: str) -> str:
        s = s or ""
        return s if len(s) <= max_chars else s[:max_chars] + "\n...[truncated]"
    
    click.echo(f"id: {r['id']}")
    click.echo(f"script_id: {r['script_id']}")
    click.echo(f"status: {r['status']}")
    click.echo(f"exit_code: {r['exit_code']}")
    click.echo(f"started: {to_local_display(r['started_at'])}")
    click.echo(f"finished: {to_local_display(r['finished_at'])}")
    click.echo(f"\n--- stdout ---")
    click.echo(clip(r["stdout"]))
    click.echo(f"\n--- stderr ---")
    click.echo(clip(r["stderr"]))