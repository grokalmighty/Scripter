import click

@click.group()
def cli():
    """Scripter: script scheduler and automation engine."""
    pass

@cli.command()
def version():
    """Show version."""
    click.echo("Scripter v0.0.1")