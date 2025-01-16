from fastapi import APIRouter as _APIRouter
from fastapi import HTTPException as _HTTPException
from fastapi import Response as _Response
from pydantic import BaseModel as _BaseModel
from pydantic import Field as _Field
from langchain_community.graphs import Neo4jGraph
import json

from nedrexapi.common import (
    _API_KEY_HEADER_ARG,
    check_api_key_decorator,
)

from nedrexapi.llm import (
_LLM_BASE, _LLM_model, _LLM_path
)

from nedrexapi.config import config as _config

_NEO4J_PORT = _config[f'db.{_config["api.status"]}.neo4j_bolt_port_internal']
_NEO4J_HOST = _config[f'db.{_config["api.status"]}.neo4j_name']
_NEO4J_DRIVER = Neo4jGraph(f"bolt://{_NEO4J_HOST}:{_NEO4J_PORT}", username="", password="", database='neo4j')

router = _APIRouter()

class QueryEmbeddingRequest(_BaseModel):
    query: str = _Field("", title="Query that is used to search for hits in the knowledge graph.", description="This query will be embedded by the same technique as the data in the KG is, and then cosine similarity is used to identify the closest matches", examples=["What is AD5?", "What is a prion disease?"])
    collection: str = _Field(None, title="Collection in the knowledge graph that is searched for hits", description="Defines collection that will be searched using the query. If none is given, all embedded collections are searched and the top X best matches, based on the cosine similarity are returned.")
    top: int = _Field(10, title="Number of results to return")


DEFAULT_QUERY_EMBEDDING_REQUEST = QueryEmbeddingRequest()

def run_neo4j_query(query):
    res = _NEO4J_DRIVER.query(query=query)
    return res


def to_json(result):
    return json.dumps([json.loads(json.dumps(result, default=lambda o: dict(o)))]) + "\n"

def get_available_collections():
    query = "SHOW vector INDEXES WHERE state='ONLINE'"
    result = run_neo4j_query(query)
    map = dict()
    for r in result:
        for type in r["labelsOrTypes"]:
            map[type] = {"index_name": r["name"], "properties": set()}
            for property in r["properties"]:
                map[type]["properties"].add(property)
    return map

@router.get("/available", summary="Lists collections, aka node and edge types, with an available embedding index.")
@check_api_key_decorator
def available_collections(x_api_key: str = _API_KEY_HEADER_ARG):
    """
    Returns a list of available collections in the knowledge graph where embeddings are available.
    """
    result = get_available_collections()
    return _Response(json.dumps([n for n in result.keys()]))

def get_properties(type):
    query = f"MATCH (n: {type}) RETURN PROPERTIES(n) AS props LIMIT 1"
    result = run_neo4j_query(query)
    return {k for k,v in result[0]["props"].items()}


def create_embedding_cypher_query(query, type, top):
    collections = get_available_collections()
    if type not in collections:
        raise _HTTPException(status_code=404, detail=f"Collection {type} not available or ready.")
    collection = collections[type]

    properties = get_properties(type)
    property_string = ""
    for property in properties:
        if property not in collection["properties"]:
            property_string = property_string + f"n.{property},"
    if len(property_string) > 0:
        property_string = property_string[:-1]+";"


    cypher = """CALL apoc.ml.openai.embedding(['"""+query+"""'], "no-key", 
        {
            endpoint: '"""+_LLM_BASE+"""',
            path: '"""+_LLM_path+"""',
            model: '"""+_LLM_model+"""'
        }) yield index, embedding

    CALL db.index.vector.queryNodes('"""+collection['index_name']+"""', """+str(top)+""", embedding) YIELD node AS n, score
    RETURN score, """+property_string

    return cypher


def get_query_hits(query,type, top):
    cypher_query = create_embedding_cypher_query(query, type, top)
    return run_neo4j_query(cypher_query)

def query_single_embedding(query, type, top):
    return run_neo4j_query(create_embedding_cypher_query(query, type, top))

def query_all_embeddings(query, top):
    collections = get_available_collections()

    results = dict()
    scores = dict()
    for type in collections.keys():
        best_hits = get_query_hits(query, type, top)
        for hit in best_hits:
            results[hit["n.primaryDomainId"]] = hit
            scores[hit["n.primaryDomainId"]] = hit["score"]

    scores_sorted = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    top_hits = []
    for id,score in scores_sorted:
        if len(top_hits) >= top:
            break
        top_hits.append(results[id])
    return top_hits


@router.post("/query")
@check_api_key_decorator
def query_embeddings(request: QueryEmbeddingRequest = DEFAULT_QUERY_EMBEDDING_REQUEST):
    type = request.collection
    top = request.top if request.top else 5
    if type is None:
        results = query_all_embeddings(request.query, top)
    else:
        results = query_single_embedding(request.query, type, top)
    return _Response(to_json(results))



