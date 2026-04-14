"""ChromaDB vector index for inventory + BMW intake SQLite only.

Auth (users + admin_users in users.db) is never indexed in Chroma.
"""

from backend.vector.chroma_service import get_persist_dir, query_cars, reindex_all

__all__ = ["get_persist_dir", "query_cars", "reindex_all"]
