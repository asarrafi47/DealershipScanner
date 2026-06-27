# DatabaseManager Integration for DealershipScanner

You now have DatabaseManager configured to manage DealershipScanner's databases. Here's what's been set up:

## Connection Profiles

DatabaseManager has been configured with 5 database connections:

### PostgreSQL (Production/Primary)
- **Name:** DealershipScanner - Postgres
- **Database:** `dealership_scanner`
- **Host:** `localhost:5432`
- **User:** asarrafi
- **Purpose:** Primary inventory database (all vehicle listings, enriched specs, embeddings)

### SQLite (Development/Admin)
1. **User credentials & auth** (`users.db`)
   - Flask login credentials, admin accounts

2. **Dealer portal accounts** (`dealer_portal.db`)
   - Dealer-facing admin portal logins

3. **Incomplete listings index** (`incomplete_listings.db`)
   - Cache of incomplete scrapes for recovery

4. **Dev inventory** (`inventory.db`)
   - Optional local SQLite fallback (if Postgres is offline)

## Quick Start

### Option 1: One-click launcher
```bash
./open_database_manager.sh
```
This sets up connections and opens DatabaseManager.

### Option 2: Manual setup
```bash
python3 scripts/setup_databasemanager.py
```
Then open DatabaseManager manually.

### Option 3: Custom Postgres URL
```bash
./open_database_manager.sh --postgres-url "postgresql://user:pass@host:5432/dbname"
```

### Option 4: Custom SQLite directory
```bash
./open_database_manager.sh --sqlite-dir "/path/to/databases"
```

## Using DatabaseManager

Once open, you'll see the DealershipScanner connections in the left sidebar:

1. **Click any connection** to connect to that database
2. **Expand the tree** to browse tables/schemas
3. **Write SQL** in the editor — **⌘↵** to run
4. **Double-click a table** to browse and edit data
5. **Export results** as CSV/JSON
6. **Manage transactions** — edit data and commit/rollback

### Passwords
Passwords are stored securely in your macOS Keychain. When you first test a connection in DatabaseManager, it will prompt you to save the password.

## Database Schema Quick Reference

### Postgres inventory tables
```sql
-- Main tables
vehicles           -- scraped vehicle listings (make, model, year, vin, price)
enrichment_mpg     -- EPA MPG specs per vehicle
trim_ladder        -- option bundles (used for pricing logic)

-- Search & embeddings
pg_search_results  -- pgvector semantic search cache
```

### SQLite users.db
```sql
admin_users        -- Flask login credentials
dealer_accounts    -- dealer portal accounts
```

## Re-running Setup

If you add new databases or change credentials, just run:
```bash
python3 scripts/setup_databasemanager.py
```

It will update the connections.json without overwriting Keychain passwords.

## Troubleshooting

**Connection fails:**
- Ensure Postgres is running: `brew services list | grep postgres`
- Check credentials in `.env` file
- Verify Postgres is listening on `localhost:5432`

**Can't find DatabaseManager:**
- Install from `/Personal/DatabaseManager`:
  ```bash
  cd /Personal/DatabaseManager
  npm run tauri dev   # dev mode
  npm run tauri build # build .app for Applications/
  ```

**Passwords not saving:**
- Keychain might be locked. Unlock it via System Settings → Security & Privacy

**SQLite files not found:**
- Run the Flask app or scanner once to generate `.db` files
- Or use `--sqlite-dir` flag to point to a different location

## Next Steps

- Use DatabaseManager to explore the schema
- Run migrations or backups
- Query vehicle data for analytics
- Manage user accounts

Happy querying! 🚗
