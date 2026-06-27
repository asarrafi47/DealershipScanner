#!/usr/bin/env python3
"""
Setup DealershipScanner databases in DatabaseManager.

This script creates connection profiles in DatabaseManager for:
- Postgres inventory (primary database)
- SQLite dev databases (users, credentials, etc.)

Usage:
    python scripts/setup_databasemanager.py [--postgres-url <url>] [--sqlite-dir <dir>]
"""

import json
import os
import sys
from pathlib import Path
from uuid import uuid4

def get_config_dir() -> Path:
    """Get DatabaseManager's config directory."""
    return Path.home() / "Library" / "Application Support" / "com.arman.databasemanager"


def load_connections(config_dir: Path) -> list:
    """Load existing connections from connections.json."""
    connections_file = config_dir / "connections.json"
    if connections_file.exists():
        with open(connections_file) as f:
            return json.load(f)
    return []


def save_connections(config_dir: Path, connections: list) -> None:
    """Save connections to connections.json."""
    config_dir.mkdir(parents=True, exist_ok=True)
    connections_file = config_dir / "connections.json"
    with open(connections_file, "w") as f:
        json.dump(connections, f, indent=2)
    print(f"✓ Saved connections to {connections_file}")


def remove_connection(connections: list, name: str) -> None:
    """Remove a connection by name."""
    return [c for c in connections if c["name"] != name]


def create_postgres_profile(
    name: str = "DealershipScanner - Postgres",
    host: str = "localhost",
    port: int = 5432,
    username: str = "postgres",
    database: str = "dealership_scanner",
) -> dict:
    """Create a PostgreSQL connection profile (engine name is case-sensitive: 'Postgres')."""
    return {
        "id": str(uuid4()),
        "name": name,
        "engine": "Postgres",  # Must be capitalized for DatabaseManager
        "host": host,
        "port": port,
        "username": username,
        "database": database,
        "sslmode": "prefer",
    }


def create_sqlite_profile(
    name: str,
    file_path: str,
) -> dict:
    """Create a SQLite connection profile (engine name is case-sensitive: 'Sqlite')."""
    return {
        "id": str(uuid4()),
        "name": name,
        "engine": "Sqlite",  # Must be capitalized for DatabaseManager
        "file_path": file_path,
    }


def setup_dealership_scanner_dbs(
    postgres_url: str = None,
    sqlite_dir: str = None,
) -> None:
    """Set up DealershipScanner databases in DatabaseManager."""
    config_dir = get_config_dir()
    print(f"DatabaseManager config: {config_dir}")

    # Load existing connections
    connections = load_connections(config_dir)
    print(f"Found {len(connections)} existing connection(s)")

    # Remove old DealershipScanner connections to avoid duplicates
    old_count = len(connections)
    connections = [
        c for c in connections
        if not c["name"].startswith("DealershipScanner")
    ]
    if len(connections) < old_count:
        print(f"Removed {old_count - len(connections)} old DealershipScanner profile(s)")

    # Parse Postgres URL or use defaults
    if postgres_url:
        # Parse postgresql://user:pass@host:port/dbname
        from urllib.parse import urlparse
        parsed = urlparse(postgres_url)
        pg_host = parsed.hostname or "localhost"
        pg_port = parsed.port or 5432
        pg_user = parsed.username or "postgres"
        pg_db = parsed.path.lstrip("/") or "dealership_scanner"
    else:
        # Try to read from .env
        env_file = Path(".env")
        pg_host = "localhost"
        pg_port = 5432
        pg_user = "postgres"
        pg_db = "dealership_scanner"

        if env_file.exists():
            with open(env_file) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("DATABASE_URL="):
                        from urllib.parse import urlparse
                        parsed = urlparse(line.split("=", 1)[1])
                        pg_host = parsed.hostname or pg_host
                        pg_port = parsed.port or pg_port
                        pg_user = parsed.username or pg_user
                        pg_db = parsed.path.lstrip("/") or pg_db
                        break

    # Add Postgres profile
    pg_profile = create_postgres_profile(
        host=pg_host,
        port=pg_port,
        username=pg_user,
        database=pg_db,
    )
    connections.append(pg_profile)
    print(f"✓ Added PostgreSQL: {pg_user}@{pg_host}:{pg_port}/{pg_db}")

    # Add SQLite profiles
    if sqlite_dir is None:
        sqlite_dir = "."  # Current directory for dev databases

    sqlite_dbs = [
        ("users.db", "User credentials & auth"),
        ("dealer_portal.db", "Dealer portal accounts"),
        ("incomplete_listings.db", "Incomplete listings index"),
        ("inventory.db", "Dev inventory (if not using Postgres)"),
    ]

    for db_file, description in sqlite_dbs:
        db_path = Path(sqlite_dir) / db_file
        # Only add if file exists or is in common locations
        if db_path.exists() or sqlite_dir == ".":
            profile = create_sqlite_profile(
                name=f"DealershipScanner - {description}",
                file_path=str(db_path.resolve()),
            )
            connections.append(profile)
            print(f"✓ Added SQLite: {db_file}")

    # Save all connections
    save_connections(config_dir, connections)
    print("\n✅ Setup complete! Open DatabaseManager to see the connections.")
    print(f"\nConnections stored at: {config_dir / 'connections.json'}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Set up DealershipScanner databases in DatabaseManager"
    )
    parser.add_argument(
        "--postgres-url",
        help="PostgreSQL connection URL (e.g., postgresql://user:pass@host:5432/db)",
    )
    parser.add_argument(
        "--sqlite-dir",
        help="Directory containing SQLite database files (default: current dir)",
    )

    args = parser.parse_args()
    setup_dealership_scanner_dbs(
        postgres_url=args.postgres_url,
        sqlite_dir=args.sqlite_dir,
    )
