from typing import Any

from opensearchpy.exceptions import NotFoundError as OSNotFoundError

from api.config import index_name, opensearch_client


def index_document(
    doc_id: str,
    body: dict[str, Any],
    routing: str | None = None,
    *,
    refresh: bool = True,
) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "index": index_name,
        "id": doc_id,
        "body": body,
        "refresh": refresh,
    }
    if routing is not None:
        kwargs["routing"] = routing
    return opensearch_client.index(**kwargs)


def get_document(doc_id: str, routing: str | None = None) -> dict[str, Any] | None:
    kwargs: dict[str, Any] = {
        "index": index_name,
        "id": doc_id,
    }
    if routing is not None:
        kwargs["routing"] = routing
    try:
        response = opensearch_client.get(**kwargs)
        return response["_source"]
    except OSNotFoundError:
        return None


def update_document(doc_id: str, partial_body: dict[str, Any], routing: str | None = None) -> dict[str, Any]:
    """Partial update of a document (only the given fields)."""
    kwargs: dict[str, Any] = {
        "index": index_name,
        "id": doc_id,
        "body": {"doc": partial_body},
        "refresh": True,
    }
    if routing is not None:
        kwargs["routing"] = routing
    return opensearch_client.update(**kwargs)


def search(
    body: dict[str, Any],
    size: int = 50,
    offset: int = 0,
    source_excludes: list[str] | None = None,
) -> dict[str, Any]:
    kwargs: dict[str, Any] = {"index": index_name, "body": body, "size": size, "from_": offset}
    if source_excludes:
        kwargs["_source_excludes"] = source_excludes
    return opensearch_client.search(**kwargs)


def bulk_operation(body: list[dict[str, Any]], *, refresh: bool = False) -> dict[str, Any]:
    return opensearch_client.bulk(body=body, index=index_name, refresh=refresh)


def refresh_index() -> None:
    opensearch_client.indices.refresh(index=index_name)


def mget_documents(doc_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not doc_ids:
        return {}
    response = opensearch_client.mget(
        body={"ids": doc_ids},
        index=index_name,
    )
    return {doc["_id"]: doc["_source"] for doc in response["docs"] if doc.get("found")}


def extract_hits(response: dict[str, Any]) -> list[dict[str, Any]]:
    return [{**hit["_source"], "id": hit["_id"]} for hit in response["hits"]["hits"]]
