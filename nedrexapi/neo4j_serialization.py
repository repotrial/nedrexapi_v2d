import json

from more_itertools import chunked


def prepare_results(neo4j_result_entry):
    # Duck-typed rather than isinstance-checked against a specific temporal type: this is shared by
    # py2neo cursors (interchange.time.DateTime/Date/Time/Duration) and the official neo4j driver's
    # Result records (neo4j.time.DateTime/Date/Time/Duration) - both expose iso_format().
    new_entry = dict()
    for k, v in neo4j_result_entry.items():
        if hasattr(v, "iso_format"):
            new_entry[k] = v.iso_format()
        else:
            new_entry[k] = v
    return new_entry


def chunk_records(cursor):
    for chunk in chunked(cursor, 1_000):
        yield json.dumps([json.loads(json.dumps(prepare_results(i), default=lambda o: dict(o))) for i in chunk]) + "\n"


def run_query(cursor):
    return json.dumps([json.loads(json.dumps(prepare_results(i), default=lambda o: dict(o))) for i in cursor])
