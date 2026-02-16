"""
FastAPI application for OpenSearch volume data management.

Provides REST API endpoints for volumes, works, persons, stats, and imports.
"""

import logging
import sys
from typing import Any

from fastapi import FastAPI, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, ORJSONResponse
from opensearchpy.exceptions import TransportError

from api.config import index_name, opensearch_client
from api.routers import data_import as import_router
from api.routers import persons as persons_router
from api.routers import stats as stats_router
from api.routers import volumes as volumes_router
from api.routers import works as works_router

# Configure logging for development
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

# Set specific loggers to INFO level
logging.getLogger("api").setLevel(logging.INFO)
logging.getLogger("api.services.ocr_import").setLevel(logging.INFO)
logging.getLogger("api.routers.data_import").setLevel(logging.INFO)

app = FastAPI(
    title="OpenSearch Volume API",
    description="API layer for managing volumes, works, and persons in OpenSearch",
    version="0.1.0",
    root_path="/api/v1",
    default_response_class=ORJSONResponse,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount routers
app.include_router(volumes_router.router)
app.include_router(works_router.router)
app.include_router(persons_router.router)
app.include_router(stats_router.router)
app.include_router(import_router.router)


@app.get("/health", response_model=None)
async def health_check() -> dict[str, str] | JSONResponse:
    """
    Health check endpoint to verify API and OpenSearch connectivity.
    """
    try:
        if opensearch_client.ping():
            return {
                "status": "healthy",
                "opensearch": "connected",
                "index": index_name,
            }
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "status": "unhealthy",
                "opensearch": "disconnected",
            },
        )
    except TransportError as exc:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "status": "unhealthy",
                "error": str(exc),
            },
        )


@app.get("/")
async def root() -> dict[str, Any]:
    """
    Root endpoint with API information.
    """
    return {
        "name": "OpenSearch Volume API",
        "version": "0.1.0",
        "docs": "/api/v1/docs",
        "health": "/api/v1/health",
    }
