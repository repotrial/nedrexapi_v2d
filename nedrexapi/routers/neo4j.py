import json
from uuid import uuid4 as _uuid4

from py2neo.errors import Neo4jError

from fastapi import APIRouter as _APIRouter
from fastapi import BackgroundTasks as _BackgroundTasks
from fastapi import HTTPException as _HTTPException
from fastapi.responses import FileResponse, StreamingResponse, Response
from pydantic import BaseModel as _BaseModel

from nedrexapi.common import _NEO4J_QUERY_COLL, _NEO4J_QUERY_COLL_LOCK, _NEO4J_QUERY_DIR_INTERNAL
from nedrexapi.neo4j_utils import _NEO4J_DRIVER, chunk_records, run_query
from nedrexapi.tasks import queue_and_wait_for_job

router = _APIRouter()


async def run_query_stream(cursor):
    for line in chunk_records(cursor):
        yield line


def _submit_download_job(query: str, background_tasks: _BackgroundTasks) -> str:
    with _NEO4J_QUERY_COLL_LOCK:
        existing = _NEO4J_QUERY_COLL.find_one({"query": query})
        if existing:
            uid = existing["uid"]
        else:
            uid = f"{_uuid4()}"
            _NEO4J_QUERY_COLL.insert_one({"query": query, "status": "submitted", "uid": uid})
            background_tasks.add_task(queue_and_wait_for_job, "neo4j_query", uid)
    return uid


@router.get("/query", summary="Neo4j query")
def neo4j_query(background_tasks: _BackgroundTasks, query: str, stream: bool = True, download: bool = False):
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

    For large results that don't fit comfortably in a single response, set `download=true` instead. This
    returns a UID immediately, computes the result in the background, and lets you fetch the completed result
    as a file via `/details/{uid}` (status) and `/download/{uid}.json` (the file itself), rather than returning
    everything in one large synchronous response.
    """
    if download:
        uid = _submit_download_job(query, background_tasks)
        return Response(json.dumps({"uid": uid}), media_type="application/json")

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


class QueryRequest(_BaseModel):
    query: str
    stream: bool = False
    download: bool = False


@router.post("/query", summary="Neo4j query")
def neo4j_query_post(background_tasks: _BackgroundTasks, qr: QueryRequest):
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

    For large results that don't fit comfortably in a single response, set `download: true` instead. This
    returns a UID immediately, computes the result in the background, and lets you fetch the completed result
    as a file via `/details/{uid}` (status) and `/download/{uid}.json` (the file itself), rather than returning
    everything in one large synchronous response.
    """
    if qr.download:
        uid = _submit_download_job(qr.query, background_tasks)
        return Response(json.dumps({"uid": uid}), media_type="application/json")

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


@router.get("/details/{uid}", summary="Neo4j query job details")
def neo4j_query_details(uid: str):
    """
    Returns the details of the query job with the given UID, including the original query and the status of the
    job (`submitted`, `running`, `failed`, or `completed`). If the job fails, this contains the error message.
    """
    data = _NEO4J_QUERY_COLL.find_one({"uid": uid})

    if data:
        data.pop("_id")
        return data

    raise _HTTPException(status_code=404, detail=f"No Neo4j query job with UID {uid!r} is recorded.")


@router.get("/download/{uid}.json", summary="Neo4j query result download")
def neo4j_query_download(uid: str):
    """
    Downloads the result of the query job with the given `uid`, in newline-delimited JSON (each line is a JSON
    array of up to 1000 result records).
    """
    data = _NEO4J_QUERY_COLL.find_one({"uid": uid})

    if data and data["status"] == "completed":
        return FileResponse(
            _NEO4J_QUERY_DIR_INTERNAL / f"{uid}.json",
            media_type="application/json",
            filename=f"{uid}.json",
        )
    elif data and data["status"] == "failed":
        error_payload = json.dumps({"error": "Bad Request", "details": data.get("error")})
        return Response(content=error_payload, status_code=400, media_type="application/json")
    elif data:
        raise _HTTPException(status_code=102, detail=f"Neo4j query job with UID {uid!r} does not have completed status.")

    raise _HTTPException(status_code=404, detail=f"No Neo4j query job with UID {uid!r} is recorded.")
