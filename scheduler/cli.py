import click

@click.group()
def cli():
    """Script Scheduler"""
    pass

@cli.command()
def hello():
    """Smoke test command"""
    click.echo("hello from scheduler")
