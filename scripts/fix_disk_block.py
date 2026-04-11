"""
Utility script to fix OpenSearch indices blocked by disk usage watermarks.

This script identifies indices with the 'read-only-allow-delete' block
(caused by disk usage exceeding the flood-stage watermark) and attempts
to clear the block.

Usage:
    python -m scripts.fix_disk_block [--index INDEX_NAME]
"""

import argparse
import logging
from api.config import opensearch_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

def clear_disk_block(index_name: str = "*") -> None:
    """Clear the read-only-allow-delete block on specified indices."""
    logger.info("Attempting to clear disk blocks for index: %s", index_name)
    
    body = {
        "index": {
            "blocks": {
                "read_only_allow_delete": None
            }
        }
    }
    
    try:
        response = opensearch_client.indices.put_settings(
            index=index_name,
            body=body
        )
        if response.get("acknowledged"):
            logger.info("Successfully cleared disk block for %s", index_name)
        else:
            logger.warning("Failed to clear disk block for %s: %s", index_name, response)
    except Exception as e:
        logger.error("Error clearing disk block for %s: %s", index_name, e)

def list_indices() -> None:
    """List all indices with their sizes and document counts."""
    logger.info("Fetching index statistics...")
    try:
        # Get indices stats in a readable format
        indices = opensearch_client.cat.indices(v=True, s="store.size:desc")
        print("\n" + indices)
    except Exception as e:
        logger.error("Error fetching index list: %s", e)

def main() -> None:
    parser = argparse.ArgumentParser(description="Fix OpenSearch indices blocked by disk watermarks")
    parser.add_argument(
        "--index",
        default="*",
        help="Index name or pattern to clear (default: * for all indices)",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List all indices and their sizes",
    )
    args = parser.parse_args()
    
    if args.list:
        list_indices()
    else:
        clear_disk_block(args.index)

if __name__ == "__main__":
    main()
