import click
from pathlib import Path

from .database import Database
from .scripts_repo import add_script, list_scripts, get_script

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
@click.argument("script_id", type=int)
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