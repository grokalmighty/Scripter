import click

@click.group()
def cli():
    """Script Scheduler"""
    pass

@cli.command()
def ping():
    """Sanity check command"""
    click.echo("pong")

if __name__ == "__main__":
    cli()