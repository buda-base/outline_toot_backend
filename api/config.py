import os

import orjson
from dotenv import load_dotenv
from opensearchpy import OpenSearch
from opensearchpy.serializer import JSONSerializer

load_dotenv()


class OrjsonSerializer(JSONSerializer):
    """Custom JSON serializer using orjson for better performance.

    orjson handles Unicode (including Tibetan) natively without escaping,
    which is more efficient and readable than ASCII-escaped JSON.
    """

    def dumps(self, data: dict) -> str:
        """Serialize data to JSON using orjson.

        Note: orjson does not escape non-ASCII characters by default,
        making it ideal for Tibetan text content.
        """
        # orjson.dumps returns bytes, opensearch-py expects str
        return orjson.dumps(data).decode("utf-8")

    def loads(self, data: str | bytes) -> dict:
        """Deserialize JSON data using orjson.ß"""
        # Handle both str and bytes input
        if isinstance(data, str):
            data = data.encode("utf-8")
        return orjson.loads(data)


class Config:
    """Application configuration loaded from environment variables."""

    OPENSEARCH_HOST: str = os.getenv("OPENSEARCH_HOST", "localhost")
    OPENSEARCH_PORT: int = int(os.getenv("OPENSEARCH_PORT", "9200"))
    OPENSEARCH_INDEX: str = os.getenv("OPENSEARCH_INDEX", "bec")
    OPENSEARCH_USE_SSL: bool = os.getenv("OPENSEARCH_USE_SSL", "false").lower() == "true"
    OPENSEARCH_VERIFY_CERTS: bool = os.getenv("OPENSEARCH_VERIFY_CERTS", "true").lower() == "true"
    OPENSEARCH_USER: str | None = os.getenv("OPENSEARCH_USER")
    OPENSEARCH_PASSWORD: str | None = os.getenv("OPENSEARCH_PASSWORD")

    S3_OCR_BUCKET: str = os.getenv("S3_OCR_BUCKET", "bec.bdrc.io")

    API_HOST: str = os.getenv("API_HOST", "127.0.0.1")
    API_PORT: int = int(os.getenv("API_PORT", "8000"))


def get_opensearch_client() -> OpenSearch:
    """
    Create and return an OpenSearch client instance.

    Returns:
        OpenSearch: Configured OpenSearch client
    """
    config = Config()

    http_auth = None
    if config.OPENSEARCH_USER and config.OPENSEARCH_PASSWORD:
        http_auth = (config.OPENSEARCH_USER, config.OPENSEARCH_PASSWORD)

    return OpenSearch(
        hosts=[{"host": config.OPENSEARCH_HOST, "port": config.OPENSEARCH_PORT}],
        use_ssl=config.OPENSEARCH_USE_SSL,
        verify_certs=config.OPENSEARCH_VERIFY_CERTS,
        ssl_show_warn=False,
        http_auth=http_auth,
        serializer=OrjsonSerializer(),
    )


# Global client instance (reused across requests)
opensearch_client = get_opensearch_client()
index_name = Config.OPENSEARCH_INDEX
