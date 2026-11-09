import os
import datetime as _datetime
import subprocess as _subprocess
import threading as _threading
from functools import wraps
from inspect import getfullargspec
from pathlib import Path
from typing import Optional

from fastapi import Header as _Header
from fastapi import HTTPException as _HTTPException
from pottery import RedisDict as _RedisDict
from pottery import Redlock as _Redlock
from pottery.exceptions import ReleaseUnlockedLock as _ReleaseUnlockedLock
from pymongo import MongoClient as _MongoClient  # type: ignore
from pymongo.collection import Collection as _Collection  # type: ignore
from redis import Redis as _Redis  # type: ignore
from slowapi import Limiter
from slowapi.util import get_remote_address

from nedrexapi.config import config as _config
from nedrexapi.db import MongoInstance
from nedrexapi.logger import logger

_MONGO_CLIENT = _MongoClient(host=_config["db.live.mongo_name"], port=_config["api.mongo_port_internal"])
_MONGO_DB = _MONGO_CLIENT[_config["api.mongo_db"]]

_REDIS = _Redis.from_url(
    f"redis://{_config['api.redis_host']}:{_config['api.redis_port_internal']}/{_config['api.redis_nedrex_db']}")
_STATUS = _RedisDict(redis=_REDIS, key="static-file-status")

_COLLECTION_LOCK_AUTO_RELEASE_MS = 60 * 1000  # 60 seconds
_LONG_LOCK_AUTO_RELEASE_MS = 30 * 60 * 1000  # 30 minutes


class _RedisLock:
    """Thread-safe wrapper around pottery.Redlock that instantiates a fresh
    Redlock per acquisition so concurrent threads never overwrite each other's
    internal `_uuid` token.
    """

    def __init__(self, key: str, masters, auto_release_time: int = _COLLECTION_LOCK_AUTO_RELEASE_MS):
        self.key = key
        self.masters = masters
        self.auto_release_time = auto_release_time
        self._local = _threading.local()

    def _new_lock(self) -> _Redlock:
        return _Redlock(
            key=self.key,
            masters=self.masters,
            auto_release_time=self.auto_release_time,
        )

    def acquire(self, *args, **kwargs) -> bool:
        lock = self._new_lock()
        acquired = lock.acquire(*args, **kwargs)
        if acquired:
            stack = getattr(self._local, "stack", None)
            if stack is None:
                stack = []
                self._local.stack = stack
            stack.append(lock)
        return acquired

    def release(self) -> None:
        stack = getattr(self._local, "stack", None)
        if stack:
            lock = stack.pop()
            try:
                lock.release()
            except _ReleaseUnlockedLock:
                logger.warning(f"Lock {self.key!r} had already expired or been released before release()")

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()
        return False


# Locks
_BICON_COLL_LOCK = _RedisLock(key="bicon_collection_lock", masters={_REDIS})
_CLOSENESS_COLL_LOCK = _RedisLock(key="closeness_collection_lock", masters={_REDIS})
_COMORBIDITOME_COLL_LOCK = _RedisLock(key="comorbiditome_collection_lock", masters={_REDIS})
_DIAMOND_COLL_LOCK = _RedisLock(key="diamond_collection_lock", masters={_REDIS})
_DOMINO_COLL_LOCK = _RedisLock(key="domino_collection_lock", masters={_REDIS})
_GRAPH_COLL_LOCK = _RedisLock(key="graph_collection_lock", masters={_REDIS})
_NEO4J_QUERY_COLL_LOCK = _RedisLock(key="neo4j_query_collection_lock", masters={_REDIS})
_KPM_COLL_LOCK = _RedisLock(key="kpm_collection_lock", masters={_REDIS})
_MUST_COLL_LOCK = _RedisLock(key="must_collection_lock", masters={_REDIS})
_NETWORK_GEN_LOCK = _RedisLock(
    key="network_generation_lock", masters={_REDIS}, auto_release_time=_LONG_LOCK_AUTO_RELEASE_MS
)
_ROBUST_COLL_LOCK = _RedisLock(key="robust_collection_lock", masters={_REDIS})
_STATIC_RANKING_LOCK = _RedisLock(
    key="static-ranking-lock", masters={_REDIS}, auto_release_time=_LONG_LOCK_AUTO_RELEASE_MS
)
_STATIC_VALIDATION_LOCK = _RedisLock(
    key="static-validation-lock", masters={_REDIS}, auto_release_time=_LONG_LOCK_AUTO_RELEASE_MS
)
_TRUSTRANK_COLL_LOCK = _RedisLock(key="trustrank_collection_lock", masters={_REDIS})
_VALIDATION_COLL_LOCK = _RedisLock(key="validation_collection_lock", masters={_REDIS})


# Collections
def get_api_collection(coll_name) -> _Collection:
    return _MONGO_DB[coll_name]


_API_KEY_COLLECTION = get_api_collection("api_keys_")
_BICON_COLL = get_api_collection("bicon_")
_CLOSENESS_COLL = get_api_collection("closeness_")
_COMORBIDITOME_COLL = get_api_collection("comorbiditome_")
_DIAMOND_COLL = get_api_collection("diamond_")
_DOMINO_COLL = get_api_collection("domino_")
_GRAPH_COLL = get_api_collection("graphs_")
_NEO4J_QUERY_COLL = get_api_collection("neo4j_queries_")
_KPM_COLL = get_api_collection("kpm_")
_ROBUST_COLL = get_api_collection("robust_")
_TRUSTRANK_COLL = get_api_collection("trustrank_")
_MUST_COLL = get_api_collection("must_")
_VALIDATION_COLL = get_api_collection("validation_")

# Directories
_DATA_DIR = Path(_config["api.directories.data_outside"])
_DATA_DIR_INTERNAL = Path(_config["api.directories.data"])
_STATIC_DIR = Path(_config["api.directories.static_outside"])
_STATIC_DIR_INTERNAL = Path(_config["api.directories.static"])

_DIAMOND_SUFFIX = "diamond_"
_MUST_SUFFIX = "must_"
_ROBUST_SUFFIX = "robust_"
_BICON_SUFFIX = "bicon_"
_GRAPH_SUFFIX = "graphs_"
_NEO4J_QUERY_SUFFIX = "neo4j_queries_"
_CLOSENESS_SUFFIX = "closeness_"
_COMORBIDITOME_SUFFIX = "comorbiditome_"
_TRUSTRANK_SUFFIX = "trustrank_"
_NETWORK_CACHE_SUFFIX = "network_cache_"

_DIAMOND_DIR = _DATA_DIR / _DIAMOND_SUFFIX
_MUST_DIR = _DATA_DIR / _MUST_SUFFIX
_ROBUST_DIR = _DATA_DIR / _ROBUST_SUFFIX
_BICON_DIR_INTERNAL = _DATA_DIR_INTERNAL / _BICON_SUFFIX
_GRAPH_DIR = _DATA_DIR / _GRAPH_SUFFIX
_GRAPH_DIR_INTERNAL = _DATA_DIR_INTERNAL / _GRAPH_SUFFIX
_NEO4J_QUERY_DIR = _DATA_DIR / _NEO4J_QUERY_SUFFIX
_NEO4J_QUERY_DIR_INTERNAL = _DATA_DIR_INTERNAL / _NEO4J_QUERY_SUFFIX
_CLOSENESS_DIR = _DATA_DIR / _CLOSENESS_SUFFIX
_COMORBIDITOME_DIR_INTERNAL = _STATIC_DIR_INTERNAL / _COMORBIDITOME_SUFFIX
_TRUSTRANK_DIR = _DATA_DIR / _TRUSTRANK_SUFFIX
_TRUSTRANK_DIR_INTERNAL = _DATA_DIR_INTERNAL / _TRUSTRANK_SUFFIX
_NETWORK_CACHE_DIR = _DATA_DIR_INTERNAL / _NETWORK_CACHE_SUFFIX

for directory in [
    _DIAMOND_DIR,
    _MUST_DIR,
    _ROBUST_DIR,
    _BICON_DIR_INTERNAL,
    _GRAPH_DIR,
    _GRAPH_DIR_INTERNAL,
    _NEO4J_QUERY_DIR_INTERNAL,
    _CLOSENESS_DIR,
    _TRUSTRANK_DIR,
    _STATIC_DIR_INTERNAL,
    _COMORBIDITOME_DIR_INTERNAL,
    _NETWORK_CACHE_DIR,
]:
    directory.mkdir(exist_ok=True, parents=True)

limiter = Limiter(
    key_func=get_remote_address,
    default_limits=[_config["api.rate_limit"]],
    storage_uri=f"redis://{_config['api.redis_host']}:{_config['api.redis_port_internal']}/{_config['api.redis_rate_limit_db']}",
)


def generate_ranking_static_files():
    """Generates the GGI and PPI necessary for ranking routes"""

    with _STATIC_RANKING_LOCK:
        if _STATUS.get("static-ranking") is True:
            return

        logger.info("generating static files for ranking routes")
        proc = _subprocess.Popen(
            ["python", f"{_config['api.directories.scripts']}/generate_ranking_input_networks.py", _STATIC_DIR_INTERNAL],
            cwd=_config["api.directories.static"],
            stdout=_subprocess.PIPE,
            stderr=_subprocess.PIPE,
        )
        proc.communicate()

        if proc.returncode == 0:
            logger.info("static files for ranking routes generated successfully")
            _STATUS["static-ranking"] = True
        else:
            logger.critical("static files for ranking routes exited with non-zero exit code")
            _STATUS["static-ranking"] = False


def generate_validation_static_files():
    """Generates the GGI and PPI necessary for validation routes"""

    with _STATIC_VALIDATION_LOCK:
        if _STATUS.get("static-validation") is True:
            return

        logger.info("generating static files (GGI and PPI) for validation methods")
        network_generator_script = f"{_config['api.directories.scripts']}/nedrex_validation/network_generator.py"

        proc = _subprocess.Popen(
            ["python", network_generator_script],
            cwd=_config["api.directories.static"],
            stdout=_subprocess.PIPE,
            stderr=_subprocess.PIPE,
        )
        proc.communicate()

        if proc.returncode == 0:
            logger.info("static files for validation routes generated successfully")
            _STATUS["static-validation"] = True
        else:
            logger.critical("static files for validation routes exited with non-zero exit code")
            _STATUS["static-validation"] = False


def invalidate_expired_keys() -> None:
    to_remove = []

    for entry in _API_KEY_COLLECTION.find():
        if entry["expiry"] < _datetime.datetime.utcnow():
            to_remove.append(entry["_id"])

    for _id in to_remove:
        _API_KEY_COLLECTION.delete_one({"_id": _id})


def check_api_key(api_key: Optional[str]) -> bool:
    invalidate_expired_keys()

    if api_key is None:
        raise _HTTPException(status_code=401, detail="An API key is required to access the requested data")

    entry = _API_KEY_COLLECTION.find_one({"key": api_key})

    if not entry:
        raise _HTTPException(
            status_code=401,
            detail="Invalid API key supplied. If they key has worked before, it may have expired or been revoked.",
        )
    if entry["expiry"] < _datetime.datetime.utcnow():
        raise _HTTPException(status_code=401, detail="An expired API key was supplied")

    return True


def check_api_key_decorator(func):
    @wraps(func)
    def new(*args, **kwargs):
        if _config["api.require_api_keys"] is not True:
            return func(*args, **kwargs)

        params = dict(kwargs)
        for k, v in zip(getfullargspec(func).args, args):
            params[k] = v

        if "x_api_key" in params:
            check_api_key(params["x_api_key"])
        else:
            pass
        return func(*args, **kwargs)

    return new


_API_KEY_HEADER_ARG = _Header(default=None, include_in_schema=_config["api.require_api_keys"])

NODE_COLLECTIONS = [node_coll for node_coll in _config["api.node_collections"] if node_coll in MongoInstance.DB().list_collection_names() ]
EDGE_COLLECTIONS = [ edge_coll for edge_coll in _config["api.edge_collections"] if edge_coll in MongoInstance.DB().list_collection_names() ]
