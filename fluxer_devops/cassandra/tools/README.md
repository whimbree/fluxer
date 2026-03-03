# Fluxer Cassandra Tools

Scripts for setting up and migrating data into ScyllaDB.

## `setup_fresh_scylla.sh`

Creates the `fluxer` keyspace and runs all CQL migrations against a fresh
ScyllaDB instance in order.

```bash
./tools/setup_fresh_scylla.sh
```

Configurable via env vars:

| Variable         | Default                      | Description                        |
|-----------------|------------------------------|------------------------------------|
| `CONTAINER`      | `fluxer-scylla`              | Podman container name              |
| `CQLSH_USER`     | `cassandra`                  | CQL username                       |
| `CQLSH_PASS`     | `cassandra`                  | CQL password                       |
| `KEYSPACE`       | `fluxer`                     | Target keyspace                    |
| `MIGRATIONS_DIR` | `../migrations` (relative)   | Path to `.cql` migration files     |

---

## `migrate_sqlite_kvshim_to_scylla.py`

### Background

Fluxer originally used a SQLite-backed KV shim (`Cassandra.tsx`) that emulated
the Cassandra driver API over a local SQLite database. Every row was stored as a
JSON blob in a single `kv_store` table, with values encoded in a custom
`__fluxer__` typed format (e.g. bigints, buffers, dates, sets, maps were all
wrapped in `{"__fluxer__": {"t": "bigint", "d": "..."}}` envelopes).

This script deserializes that encoding and inserts all rows into their proper
ScyllaDB tables.

### Why it's cursed

- The SQLite shim had no real TTL enforcement, no LWT, and emulated CQL
  filtering/ordering/pagination in-process with silent bugs.
- The `__fluxer__` encoding stores every value as JSON regardless of CQL type,
  so bigints come out as strings, Sets as arrays, Maps as `[key, value]` pair
  arrays, Buffers as base64, etc.
- Several tables gained new primary key columns in later migrations
  (`user_contact_change_logs.event_id`) that don't exist in the SQLite data —
  these are generated on insert.
- UDT columns (`message_call`, `channel_override`, `guild_folder`, etc.) require
  the cassandra-driver's registered namedtuple classes to serialize correctly.
  Plain dicts don't work. The driver must be given the UDT class definitions
  *before* the insert session is created.
- `map<text, text>` columns (`admin_audit_logs.metadata`,
  `guild_audit_logs*.options`) look like UDTs but aren't — values must be
  explicitly coerced to strings.

### Usage

```bash
pip install cassandra-driver apsw

python3 migrate_sqlite_kvshim_to_scylla.py \
  --db /services/fluxer/data/db/fluxer.db \
  --host fluxer-scylla \
  --keyspace fluxer \
  --username cassandra \
  --password cassandra \
  --dc dc1
```

Or via podman (if running from outside the fluxer network):

```bash
sudo podman run --rm -it \
  --network=fluxer \
  -v /services/fluxer/data/db/fluxer.db:/fluxer.db:ro \
  -v /services/fluxer/fluxer_devops/cassandra/tools/migrate_sqlite_kvshim_to_scylla.py:/migrate.py:ro \
  python:3.12 bash -c "pip install cassandra-driver apsw && python3 /migrate.py \
    --db /fluxer.db \
    --host fluxer-scylla \
    --keyspace fluxer \
    --username cassandra \
    --password cassandra \
    --dc dc1"
```

Add `--dry-run` to print rows without inserting.

### Notes

- Re-running is safe — Cassandra `INSERT` is an upsert, rows are overwritten by
  primary key.
- ZFS snapshot the VM before running: `zfs snapshot rpool/safe/services/fluxer@pre-cassandra-migration`
- Stop Fluxer before migrating to avoid writes to SQLite mid-flight.
