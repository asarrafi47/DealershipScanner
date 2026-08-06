"""Vector indexes in Postgres (pgvector): listings, dealers, BMW OEM, car knowledge, EPA master catalog."""

from backend.vector.pgvector_service import query_cars, reindex_all

__all__ = ["query_cars", "reindex_all"]
