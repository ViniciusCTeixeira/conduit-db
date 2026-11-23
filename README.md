# ConduitDB

ConduitDB is a resilient, schema-agnostic CLI utility built in Python for migrating, synchronizing, and verifying large-scale MySQL and MariaDB databases.

Unlike conventional desktop GUI tools (such as Navicat or DBeaver) that run naive high-offset queries (`LIMIT N OFFSET M`) and load massive result sets into memory, ConduitDB uses streaming keyset pagination, unbuffered server-side cursors, and SQLite checkpointing to transfer millions of records with constant memory usage and zero downtime resume capability.

---

## Highlights

- **Keyset Streaming**: Uses indexed primary key pointers (`WHERE id > :cursor ORDER BY id ASC LIMIT :chunk`) for queries that stay fast regardless of table depth. Automatically falls back to unbuffered server cursors (`SSCursor`) for tables without sequential integer keys.
- **Resumable State Engine**: Progress is tracked transactionally in a local SQLite database (WAL mode). If a connection drops, a server restarts, or a migration is paused, run `conduit resume` to pick up exactly from the last saved cursor.
- **Session-Hardened Bulk Writes**: Executes multi-row parameterized bulk inserts (`ON DUPLICATE KEY UPDATE` or standard insert) while temporarily disengaging session-level foreign key and unique checks for maximum write throughput.
- **Topological Schema Sync**: Analyzes `information_schema.KEY_COLUMN_USAGE` to determine table dependencies, drops/strips foreign keys during the initial table creation, and applies deferred constraints after data loads.
- **Built-in SSH Tunnels**: Transparently opens encrypted SSH tunnels (using keys, passphrases, or passwords) to remote database servers without requiring external tooling.
- **Deterministic Verification**: Validates migration accuracy using row count comparisons and server-side checksum hashing (`BIT_XOR(CRC32(...))`).

---

## Installation

### Prerequisites
- Python 3.10 or higher
- Access to source and target MySQL / MariaDB instances

```bash
# Clone the repository
git clone <your-repo-url> conduit-db
cd conduit-db

# Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Or install as an editable CLI tool
pip install -e .
```

---

## Configuration

ConduitDB uses a YAML profile configuration file (`profiles.yaml`). Create your configuration from the provided example:

```bash
cp profiles.example.yaml profiles.yaml
```

### Profile Structure

```yaml
profiles:
  production_source:
    driver: mysql
    host: 10.0.0.15
    port: 3306
    user: db_reader
    password: "${PROD_DB_PASSWORD}"
    database: ecommerce_prod
    charset: utf8mb4
    ssh:
      enabled: true
      host: bastion.company.com
      port: 22
      username: deploy
      private_key: ~/.ssh/id_ed25519

  analytics_target:
    driver: mysql
    host: 127.0.0.1
    port: 3306
    user: root
    password: "${LOCAL_DB_PASSWORD:-secret}"
    database: ecommerce_replica
    charset: utf8mb4
    ssh:
      enabled: false
```

Environment variables are automatically resolved at runtime in the format `${VAR_NAME}` or `${VAR_NAME:-default_value}`.

---

## CLI Usage Reference

### 1. Test Connection & SSH Tunnels
Validate that both database engines and any configured SSH jump hosts are reachable:

```bash
conduit test -s production_source -t analytics_target
```

### 2. Inspect Source Database
Inspect table structures, primary keys, estimated row counts, and foreign key dependencies:

```bash
conduit inspect -p production_source
```

### 3. Migrate Table Structures (DDL)
Synchronize table definitions from source to target without data:

```bash
# Migrate all tables (recreates target tables if --drop-existing is passed)
conduit schema -s production_source -t analytics_target --drop-existing

# Migrate specific tables only
conduit schema -s production_source -t analytics_target -T users -T orders
```

### 4. Transfer Data
Stream table data from source to target with configurable chunk sizes:

```bash
# Transfer all tables in dependency order
conduit transfer -s production_source -t analytics_target --chunk-size 2000

# Transfer specific tables with upsert mode (ON DUPLICATE KEY UPDATE)
conduit transfer -s production_source -t analytics_target -T orders --upsert --chunk-size 5000
```

### 5. Resume Interrupted Job
If a network failure or manual interruption occurs, inspect and resume pending tables:

```bash
conduit resume -s production_source -t analytics_target
```

### 6. Verify Data Integrity
Compare row counts and run server-side cryptographic checksums to ensure 1:1 parity:

```bash
# Quick row count verification
conduit verify -s production_source -t analytics_target

# Deep verification with BIT_XOR CRC32 checksums
conduit verify -s production_source -t analytics_target --hash
```

### 7. Reset Migration State
Clear the checkpoint tracking database when you wish to start a fresh migration:

```bash
conduit reset -s production_source -t analytics_target
```

---

## Architecture

```
                      +-------------------+
                      |   Conduit CLI     |
                      +---------+---------+
                                |
             +------------------+------------------+
             |                                     |
   +---------v---------+                 +---------v---------+
   |   Streamer Engine |                 |    Writer Engine  |
   | (Keyset / SSCursor|                 | (Bulk Insert /    |
   |  Zero-memory RAM) |                 |  Constraint Bypass|
   +---------+---------+                 +---------+---------+
             |                                     |
             |       +--------------------+        |
             +------>| SQLite Checkpoint  |<-------+
                     | (WAL, Resume Point)|
                     +--------------------+
```

- **conduit.tunnel**: Dynamic local port forwarding via SSH jump hosts using Paramiko.
- **conduit.inspector**: Queries database metadata and produces topologically sorted migration plans.
- **conduit.streamer**: Generators that fetch row batches using keyset ranges or buffered streams.
- **conduit.writer**: Prepares optimized multi-row parameterized `INSERT` statements with disabled constraints.
- **conduit.verifier**: Computes full-table checksum aggregations directly on the database engine.
- **conduit.checkpoint**: SQLite-backed tracker guaranteeing zero duplicate migrations on recovery.

---

## Running the Test Suite

ConduitDB includes both isolated unit tests and end-to-end integration tests:

```bash
# Run all tests
pytest -v
```
