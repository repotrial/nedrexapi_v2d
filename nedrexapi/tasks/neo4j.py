from py2neo.errors import Neo4jError

from nedrexapi.common import _NEO4J_QUERY_COLL, _NEO4J_QUERY_COLL_LOCK, _NEO4J_QUERY_DIR_INTERNAL
from nedrexapi.logger import logger
from nedrexapi.neo4j_utils import _NEO4J_DRIVER, chunk_records


def neo4j_query_job_wrapper(uid):
    try:
        neo4j_query_job(uid)
    except Exception as e:
        with _NEO4J_QUERY_COLL_LOCK:
            _NEO4J_QUERY_COLL.update_one({"uid": uid}, {"$set": {"status": "failed", "error": f"{e}"}})
        raise e


def neo4j_query_job(uid):
    doc = _NEO4J_QUERY_COLL.find_one({"uid": uid})
    if not doc:
        raise Exception(f"No Neo4j query job with UID {uid!r} is recorded.")

    with _NEO4J_QUERY_COLL_LOCK:
        _NEO4J_QUERY_COLL.update_one({"uid": uid}, {"$set": {"status": "running"}})

    logger.info(f"starting neo4j query job {uid!r}")

    try:
        result = _NEO4J_DRIVER.run(doc["query"])
        result.keys()
    except Neo4jError as e:
        raise Exception(e.message)

    out_path = _NEO4J_QUERY_DIR_INTERNAL / f"{uid}.json"
    with out_path.open("w") as fh:
        for line in chunk_records(result):
            fh.write(line)

    with _NEO4J_QUERY_COLL_LOCK:
        _NEO4J_QUERY_COLL.update_one({"uid": uid}, {"$set": {"status": "completed"}})

    logger.info(f"finished neo4j query job {uid!r}")
