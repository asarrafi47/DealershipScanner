#!/bin/bash
# Open DatabaseManager with DealershipScanner database connections.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Setup connections if needed
echo "Setting up DealershipScanner database connections in DatabaseManager..."
python3 "$SCRIPT_DIR/scripts/setup_databasemanager.py" "$@"

# Open DatabaseManager
echo ""
echo "Opening DatabaseManager..."
open -a DatabaseManager

echo "✅ Done! DatabaseManager is launching with DealershipScanner databases configured."
