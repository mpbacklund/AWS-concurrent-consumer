import click

@click.command()
@click.option('-s', type=str, required=True, help="Specify the producer source, e.g. 'producer'")
@click.option('-t', type=click.Choice(['s3', 'dynamodb', 'sqs']), required=True, help="Specify storage strategy: bucket3 or dynamodb")
@click.option('-d', type=str, required=True, help="Specify the resource name, e.g. 'widgets'")
def parse(s, t, d):
    config = {
        "source": s, 
        "storageType": t,
        "destination": d
    }

    return config

def get_config():
    return parse.main(standalone_mode=False)