from py2neo import Graph  # type: ignore

from nedrexapi.config import config as _config

_NEO4J_PORT = _config[f'db.{_config["api.status"]}.neo4j_bolt_port_internal']
_NEO4J_HOST = _config[f'db.{_config["api.status"]}.neo4j_name']
_NEO4J_DRIVER = Graph(f"bolt://{_NEO4J_HOST}:{_NEO4J_PORT}")
