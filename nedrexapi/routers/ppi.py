from fastapi import APIRouter as _APIRouter
from fastapi import HTTPException as _HTTPException
from fastapi import Query as _Query

from nedrexapi.common import _API_KEY_HEADER_ARG, check_api_key_decorator
from nedrexapi.config import config as _config
from nedrexapi.db import MongoInstance

DEFAULT_QUERY = _Query(None)

router = _APIRouter()


@router.post("/ppi", summary="Paginated PPI query")
@check_api_key_decorator
def get_paginated_protein_protein_interactions(
    iid_evidence: list[str] = ['exp'],
    skip: int = 0,
    limit: int = 10000,
    reviewed_proteins: list[str] = ["True", "False"],
    skip_proteins: int = 0,
    limit_proteins: int = 250000,
    x_api_key: str = _API_KEY_HEADER_ARG,
):
    """
    Returns an array of protein protein interactions (PPIs in a paginated manner). A skip and a limit can be
    specified, defaulting to `0` and `10_000`, respectively, if not specified.
    """
    if not iid_evidence:
        return []

    if not skip:
        skip = 0
    if not limit:
        limit = _config["api.pagination_max"]
    elif limit > _config["api.pagination_max"]:
        raise _HTTPException(status_code=422, detail=f"Limit specified ({limit}) greater than maximum limit allowed")

    protein_query = {"is_reviewed": {"$in": reviewed_proteins}}

    filtered_proteins = [
        protein["primaryDomainId"]
        for protein in MongoInstance.DB()["protein"].find(protein_query).skip(skip_proteins).limit(limit_proteins)
    ]

    query = {"evidenceTypes": {"$in": iid_evidence}, "memberOne": {"$in": filtered_proteins},
             "memberTwo": {"$in": filtered_proteins}}
    coll_name = "protein_interacts_with_protein"

    return [
        {k: v for k, v in doc.items() if k != "_id"}
        # each entry is one document -> finds all documents by conditions
        for doc in MongoInstance.DB()[coll_name].find(query).skip(skip).limit(limit)
    ]
