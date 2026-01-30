import click

from scheduler.database import Database
from scheduler.executor import ScriptExecutor

@click.group()
def cli():
    """Script Scheduler"""
    pass

@cli.command()
def ping():
    """Sanity check command"""
    click.echo("pong")

@cli.command()
@click.argument("script_name")
def run(script_name):
    """
    Manually run a script by name.
    """
    db = Database("scheduler.db")
    db.connect()

    script = db.get_script_by_name(script_name)
    if not script:
        raise click.ClickException(f"No script named '{script_name}'")
    
    executor = ScriptExecutor(db)
    res = executor.execute(script.id)

    click.echo(f"Exit code: {res.returncode}")
    if res.stdout:
        click.echo(res.stdout, nl=False)
    if res.stderr:
        click.echo(res.stderr, err=True, nl=False)

if __name__ == "__main__":
    cli()