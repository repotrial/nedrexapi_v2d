import json

from more_itertools import chunked
from py2neo import Graph  # type: ignore
from interchange.time import DateTime

from nedrexapi.config import config as _config

_NEO4J_PORT = _config[f'db.{_config["api.status"]}.neo4j_bolt_port_internal']
_NEO4J_HOST = _config[f'db.{_config["api.status"]}.neo4j_name']
_NEO4J_DRIVER = Graph(f"bolt://{_NEO4J_HOST}:{_NEO4J_PORT}")


def prepare_results(neo4j_result_entry):
    new_entry = dict()
    for k, v in neo4j_result_entry.items():
        if isinstance(v, DateTime):
            new_entry[k] = str(v)
        else:
            new_entry[k] = v
    return new_entry


def chunk_records(cursor):
    for chunk in chunked(cursor, 1_000):
        yield json.dumps([json.loads(json.dumps(prepare_results(i), default=lambda o: dict(o))) for i in chunk]) + "\n"


def run_query(cursor):
    return json.dumps([json.loads(json.dumps(prepare_results(i), default=lambda o: dict(o))) for i in cursor])
