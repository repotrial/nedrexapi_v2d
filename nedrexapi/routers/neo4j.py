import json
from py2neo.errors import Neo4jError

from fastapi import APIRouter as _APIRouter
from fastapi.responses import StreamingResponse, Response
from more_itertools import chunked
from py2neo import Graph  # type: ignore
from interchange.time import DateTime

from nedrexapi.config import config as _config

_NEO4J_PORT = _config[f'db.{_config["api.status"]}.neo4j_bolt_port_internal']
_NEO4J_HOST = _config[f'db.{_config["api.status"]}.neo4j_name']
_NEO4J_DRIVER = Graph(f"bolt://{_NEO4J_HOST}:{_NEO4J_PORT}")

router = _APIRouter()


def prepare_results(neo4j_result_entry):
    new_entry = dict()
    for k,v in neo4j_result_entry.items():
        if isinstance(v,DateTime):
            new_entry[k] = str(v)
        else:
            new_entry[k] = v
    return new_entry

async def run_query_stream(cursor):
    for chunk in chunked(cursor, 1_000):
        yield json.dumps([json.loads(json.dumps(prepare_results(i), default=lambda o: dict(o))) for i in chunk]) + "\n"

def run_query(cursor):
    return json.dumps([json.loads(json.dumps(prepare_results(i), default=lambda o: dict(o))) for i in cursor])


@router.get("/query", summary="Neo4j query")
def neo4j_query(query: str, stream: bool = True):
    """
    Runs a Neo4j query and returns the result.
    The result is returned as a streaming response, so it is up to the user to handle the streaming response.
    An example of this using Python's requests library is below:

        import json
        import requests
        query = "MATCH (n) RETURN n LIMIT 25"
        url = "http://82.148.225.92:8022/neo4j/query"
        response = requests.get(url, params={"query":query, "stream":True}, stream=True)
        for line in response.iter_lines():
            print(json.loads(line.decode()))
    """
    try:
        result = _NEO4J_DRIVER.run(query)

        result.keys()

    except Neo4jError as e:
        error_payload = json.dumps({"error": "Bad Request", "details": e.message})

        return Response(content=error_payload, status_code=400, media_type="application/json")

    if stream:
        return StreamingResponse(run_query_stream(result))
    else:
        return Response(run_query(result), media_type="application/json")


from pydantic import BaseModel as _BaseModel
class QueryRequest(_BaseModel):
    query: str

@router.post("/query", summary="Neo4j query")
def neo4j_query(qr: QueryRequest):
    """
    Runs a Neo4j query and returns the result.
    The result is returned as a streaming response, so it is up to the user to handle the streaming response.
    An example of this using Python's requests library is below:

        import json
        import requests
        query = "MATCH (n) RETURN n LIMIT 25"
        url = "https://api.nedrex.net/neo4j/query"
        response = requests.post(url, json={"query":query, "stream":True}, stream=True)
        for line in response.iter_lines():
            print(json.loads(line.decode()))
    """
    try:
        result = _NEO4J_DRIVER.run(qr.query)

        result.keys()

    except Neo4jError as e:
        error_payload = json.dumps({"error": "Bad Request", "details": e.message})

        return Response(content=error_payload, status_code=400, media_type="application/json")

    if qr.stream:
        return StreamingResponse(run_query_stream(result))
    else:
        return Response(run_query(result), media_type="application/json")