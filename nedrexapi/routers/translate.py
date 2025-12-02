from itertools import chain as _chain
from fastapi import APIRouter as _APIRouter
from fastapi import Query as _Query
from nedrexapi.common import _API_KEY_HEADER_ARG, check_api_key_decorator
from nedrexapi.db import MongoInstance
from pydantic import BaseModel as _BaseModel
from pydantic import Field as _Field

from itertools import chain
import re

router = _APIRouter()

class NodeListRequest(_BaseModel):
    nodes: list[str] = _Field(None, title="Gene Identifier: Gene name, Entrez ID, or Ensembl ID",
                              description="Gene Identifier which is used to translate to Ensembl ID or Entrez ID")

    class Config:
        extra = "forbid"

_DEFAULT_NODE_REQUEST = NodeListRequest()

def make_node_list_request(items: list[str]) -> NodeListRequest:
    request = NodeListRequest()
    request.nodes = items
    return request

@router.post("/translate_entrez")
@check_api_key_decorator
def get_entrez_id(genes: NodeListRequest = _DEFAULT_NODE_REQUEST, x_api_key: str = _API_KEY_HEADER_ARG):
    """
    Given a set of gene identifiers, this route returns the Entrez IDs for those genes.
    Returns a mapping from input identifier to list of Entrez IDs (usually one).
    """
    if not genes.nodes:
        return {}
    
    normalized_genes = [
        f"entrez.{i}" if re.fullmatch(r"\d+", i)
        else f"ensembl.{i}" if re.fullmatch(r"ENS[GTP]\d+(\.\d+)?", i)
        else i
        for i in genes.nodes
    ]

    entrez_ids, gene_ids, uniprot_ids, gene_names = sort_list(normalized_genes)

    coll = MongoInstance.DB()["gene"]

    query_conditions = []
    
    if entrez_ids or gene_ids:
        all_domain_ids = entrez_ids + gene_ids
        query_conditions.append({"primaryDomainId": {"$in": all_domain_ids}})
        query_conditions.append({"domainIds": {"$in": all_domain_ids}})
    
    if gene_names:
        query_conditions.append({"symbols": {"$in": gene_names}})
        query_conditions.append({"approvedSymbol": {"$in": gene_names}})
        query_conditions.append({"displayName": {"$in": gene_names}})

    if not query_conditions:
        return {gene: [] for gene in genes.nodes}

    query = {"$or": query_conditions} if len(query_conditions) > 1 else query_conditions[0]

    results: dict[str, list[str]] = {gene: [] for gene in genes.nodes}

    input_to_normalized = dict(zip(genes.nodes, normalized_genes))
    
    for doc in coll.find(query):
        entrez_id = None
        primary_id = doc.get("primaryDomainId", "")
        if primary_id.startswith("entrez."):
            entrez_id = primary_id.replace("entrez.", "")
        else:
            for domain_id in doc.get("domainIds", []):
                if domain_id.startswith("entrez."):
                    entrez_id = domain_id.replace("entrez.", "")
                    break
        
        if not entrez_id:
            continue

        doc_domain_ids = {primary_id} | set(doc.get("domainIds", []))
        doc_symbols = set(doc.get("symbols", []))
        if doc.get("approvedSymbol"):
            doc_symbols.add(doc.get("approvedSymbol"))
        if doc.get("displayName"):
            doc_symbols.add(doc.get("displayName"))
        
        for original_input in genes.nodes:
            normalized = input_to_normalized[original_input]

            matches = (
                normalized in doc_domain_ids or
                original_input in doc_symbols
            )
            
            if matches:
                if entrez_id not in results[original_input]:
                    results[original_input].append(entrez_id)

    return results




@router.post("/translate_uniprot")
@check_api_key_decorator
def get_uniprot_id(genes: NodeListRequest = _DEFAULT_NODE_REQUEST, x_api_key: str = _API_KEY_HEADER_ARG):
    """
    Given a set of gene identifiers, this route returns the UniProt protein IDs 
    encoded by those genes.
    Returns a mapping from input identifier to list of UniProt IDs.
    """
    if not genes.nodes:
        return {}
    
    normalized_genes = [
        f"entrez.{i}" if re.fullmatch(r"\d+", i)
        else f"ensembl.{i}" if re.fullmatch(r"ENS[GTP]\d+(\.\d+)?", i)
        else i
        for i in genes.nodes
    ]

    entrez_ids, gene_ids, uniprot_ids, gene_names = sort_list(normalized_genes)

    gene_coll = MongoInstance.DB()["gene"]
    edge_coll = MongoInstance.DB()["protein_encoded_by_gene"]
    
    query_conditions = []
    if entrez_ids or gene_ids:
        all_domain_ids = entrez_ids + gene_ids
        query_conditions.append({"primaryDomainId": {"$in": all_domain_ids}})
        query_conditions.append({"domainIds": {"$in": all_domain_ids}})
    if gene_names:
        query_conditions.append({"symbols": {"$in": gene_names}})
        query_conditions.append({"approvedSymbol": {"$in": gene_names}})
        query_conditions.append({"displayName": {"$in": gene_names}})

    if not query_conditions:
        return {gene: [] for gene in genes.nodes}

    query = {"$or": query_conditions} if len(query_conditions) > 1 else query_conditions[0]
    
    results: dict[str, list[str]] = {gene: [] for gene in genes.nodes}
    input_to_normalized = dict(zip(genes.nodes, normalized_genes))
    
    input_to_gene_id = {}
    
    for doc in gene_coll.find(query):
        primary_id = doc.get("primaryDomainId", "")
        doc_domain_ids = {primary_id} | set(doc.get("domainIds", []))
        doc_symbols = set(doc.get("symbols", []))
        if doc.get("approvedSymbol"):
            doc_symbols.add(doc.get("approvedSymbol"))
        if doc.get("displayName"):
            doc_symbols.add(doc.get("displayName"))
        
        for original_input in genes.nodes:
            normalized = input_to_normalized[original_input]
            matches = (normalized in doc_domain_ids or original_input in doc_symbols)
            
            if matches and original_input not in input_to_gene_id:
                input_to_gene_id[original_input] = primary_id

    if not input_to_gene_id:
        return results

    matched_gene_ids = set(input_to_gene_id.values())

    for doc in edge_coll.find({"targetDomainId": {"$in": list(matched_gene_ids)}}):
        protein_id = doc.get("sourceDomainId", "")
        if not protein_id:
            continue
        
        if protein_id.startswith("uniprot."):
            protein_id = protein_id.replace("uniprot.", "")
        
        gene_id = doc.get("targetDomainId", "")
        
        for original_input, mapped_gene_id in input_to_gene_id.items():
            if mapped_gene_id == gene_id:
                if protein_id and protein_id not in results[original_input]:
                    results[original_input].append(protein_id)

    return results


def sort_list(genes):
    entrez_ids = []
    gene_ids = []
    uniprot_ids = []
    gene_names = []

    for gene in genes:
        if re.fullmatch(r"entrez\.\d+", gene):
            entrez_ids.append(gene)
        elif re.fullmatch(r"ensembl\.ENS[GTP]\d+(\.\d+)?", gene):
            gene_ids.append(gene)
        elif re.fullmatch(r"uniprot\.[A-Za-z0-9]+", gene):
            uniprot_ids.append(gene)
        else:
            gene_names.append(gene)

    return entrez_ids, gene_ids, uniprot_ids, gene_names