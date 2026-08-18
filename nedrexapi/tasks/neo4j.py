from neo4j import GraphDatabase as _GraphDatabase
from neo4j.exceptions import Neo4jError

from nedrexapi.common import _NEO4J_QUERY_COLL, _NEO4J_QUERY_COLL_LOCK, _NEO4J_QUERY_DIR_INTERNAL
from nedrexapi.config import config as _config
from nedrexapi.logger import logger
from nedrexapi.neo4j_serialization import chunk_records

# Generous but bounded: py2neo (used for the synchronous /query routes) has no way to override Neo4j's
# default transaction timeout, so long-running queries submitted via download=true are run through the
# official neo4j driver instead, which supports an explicit per-transaction timeout (see
# nedrexapi/networks.py for the existing use of this same mechanism).
_QUERY_TIMEOUT_SECONDS = 60 * 60 * 4

_NEO4J_PORT = _config[f'db.{_config["api.status"]}.neo4j_bolt_port_internal']
_NEO4J_HOST = _config[f'db.{_config["api.status"]}.neo4j_name']
# GraphDatabase.driver() connects lazily, unlike py2neo's Graph(), so this is safe at import time even
# if Neo4j is briefly unreachable when the RQ worker process starts.
_OFFICIAL_DRIVER = _GraphDatabase.driver(uri=f"bolt://{_NEO4J_HOST}:{_NEO4J_PORT}")


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

    out_path = _NEO4J_QUERY_DIR_INTERNAL / f"{uid}.json"
    try:
        with _OFFICIAL_DRIVER.session(fetch_size=1000) as session:
            with session.begin_transaction(timeout=_QUERY_TIMEOUT_SECONDS) as tx:
                result = tx.run(doc["query"])
                with out_path.open("w") as fh:
                    for line in chunk_records(result):
                        fh.write(line)
    except Neo4jError as e:
        raise Exception(e.message)

    with _NEO4J_QUERY_COLL_LOCK:
        _NEO4J_QUERY_COLL.update_one({"uid": uid}, {"$set": {"status": "completed"}})

    logger.info(f"finished neo4j query job {uid!r}")
