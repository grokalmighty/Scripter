from __future__ import annotations

import click
import uuid

from .database import Database
from .models import Script
from .executor import ScriptExecutor

@click.group()
def cli():
    """Script Scheduler"""
    pass

@cli.command()
def hello():
    """Smoke test command"""
    click.echo("hello from scheduler")

@cli.command()
@click.argument("name")
@click.argument("path")
@click.option("--desc", default=None, help="Optional description")
def add(name: str, path: str, desc: str | None):
    db = Database.get_default()

    script = Script(
        id=str(uuid.uuid4()),
        name=name,
        path=path,
        description=desc,
    )

    try:
        existing = db.get_script_by_name(name)
        if existing is not None:
            raise click.ClickException(
                f"Script '{name}' already exists (id={existing.id}, path={existing.path})."
                f"Use a different name or implement an update command."
            )
        db.add_script(script)
    except Exception as e:
        raise click.ClickException(str(e))
    finally:
        db.close()
    
    click.echo(f"Added script '{name}'")

@cli.command("list")
def list_scripts():
    db = Database.get_default()

    try:
        rows = db.conn.execute(
            "SELECT id, name, path, enabled FROM scripts ORDER BY name;"
        ).fetchall()
    
        if not rows:
            click.echo("No scripts registered.")
            return
        
        for r in rows:
            status = "Y" if r["enabled"] else "N"
            click.echo(f"{status} {r['name']} -> {r['path']} ({r['id']})")
    finally:
        db.close()

@cli.command()
@click.argument("name")
@click.option("--timeout", type=int, default=None, help="Timeout seconds")
def run(name: str, timeout: int | None):
    db = Database.get_default()
    try:
        script = db.get_script_by_name(name)
        if script is None:
            raise click.ClickException(f"No script named '{name}'")
    
        executor = ScriptExecutor(db)
        res = executor.execute(script.id, timeout=timeout)

        click.echo(f"Exit code: {res.returncode}")
        if res.stdout:
            click.echo(res.stdout, nl=False)
        if res.stderr: 
            click.echo(res.stderr, err=True, nl=False)
    
    finally:
        db.close()
