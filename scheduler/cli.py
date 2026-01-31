import click
import time as pytime
import uuid

from scheduler.database import Database
from scheduler.executor import ScriptExecutor, calculate_hash

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

@cli.command()
@click.argument("name")
@click.argument("path")
@click.option("--time", "run_time", required=True, help="Time to run (HH:MM)")
@click.option("--days", default="", help="Comma-separated days. Optional.")
def add(name, path, run_time, days):
    """
    Add a new script scheduled at a specific time.
    """
    db = Database("scheduler.db")
    db.connect()

    days_list = [d.strip().lower() for d in days.split(",") if d.strip()]
    trigger_config = {"time": run_time}
    if days_list:
        trigger_config["days"] = days_list
    
    script_id = str(uuid.uuid4())
    script_hash = calculate_hash(path)

    from scheduler.models import Script
    s = Script(id=script_id, name=name, path=path, hash=script_hash)
    db.add_script(s)

    schedule_id = str(uuid.uuid4())
    db.add_schedule(schedule_id, script_id, "time", trigger_config)

    click.echo(f"Added '{name}' scheduled at {run_time}" + (f" on {days_list}" if days_list else ""))

@cli.command()
@click.option("--tick", default=30, show_default=True, help="Polling interval in seconds")
def start(tick):
    """
    Start the scheduler loop (Ctril+C to stop).
    """
    db = Database("scheduler.db")
    db.connect()

    executor = ScriptExecutor(db)

    from scheduler.scheduler import Scheduler
    sched = Scheduler(db, executor, tick_seconds=tick)

    click.echo("Starting scheduler... (Ctrl+C to stop)")
    sched.start()

    try:
        while True:
            pytime.sleep(1)
    except KeyboardInterrupt:
        click.echo("\nStopping scheduler...")
        sched.stop()

if __name__ == "__main__":
    cli()