#!/usr/bin/env python

import click
from redis import Redis  # type: ignore


@click.command()
@click.option("--port")
@click.option("--host")
@click.option("--db", "-d", multiple=True, type=int)
def main(port, host, db):
    for database in db:
        r = Redis.from_url(f"redis://{host}:{port}/{database}")
        for key in r.keys():
            print(f"Deleting key: {key}")
            r.delete(key)

if __name__ == "__main__":
    main()
