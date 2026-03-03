#!/usr/bin/env python3
"""
Fluxer SQLite → ScyllaDB/Cassandra migration script.
"""

import argparse
import base64
import collections
import json
import sys
from datetime import datetime, timezone
from typing import Any

import apsw
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.util import uuid_from_time

# ---------------------------------------------------------------------------
# Deserialize the __fluxer__ typed encoding
# ---------------------------------------------------------------------------

def deserialize_value(raw: Any) -> Any:
    if raw is None:
        return None
    if isinstance(raw, dict):
        if "__fluxer__" in raw:
            meta = raw["__fluxer__"]
            t = meta["t"]
            d = meta["d"]
            if t == "bigint":
                return int(d)
            elif t == "buffer":
                return bytes(base64.b64decode(d))
            elif t == "date":
                return datetime.fromisoformat(d.replace("Z", "+00:00"))
            elif t == "map":
                return {deserialize_value(k): deserialize_value(v) for k, v in d}
            elif t == "set":
                return set(deserialize_value(item) for item in d)
            elif t == "list":
                return [deserialize_value(item) for item in d]
            elif t == "float":
                return float(d)
            else:
                return d
        else:
            return {k: deserialize_value(v) for k, v in raw.items()}
    elif isinstance(raw, list):
        return [deserialize_value(item) for item in raw]
    else:
        return raw


def deserialize_row(json_bytes) -> dict:
    if isinstance(json_bytes, (bytes, bytearray)):
        json_bytes = json_bytes.decode("utf-8")
    raw = json.loads(json_bytes)
    return {k: deserialize_value(v) for k, v in raw.items()}


# ---------------------------------------------------------------------------
# Row coercion
# ---------------------------------------------------------------------------

def make_nt(udt_cls, data: dict):
    fields = udt_cls._fields
    return udt_cls(**{f: data.get(f, None) for f in fields})


def coerce_row(table_name: str, row: dict, udt_classes: dict) -> dict:
    row = dict(row)

    if table_name == 'admin_audit_logs':
        if row.get('metadata') is not None:
            row['metadata'] = {str(k): str(v) for k, v in row['metadata'].items()}

    if table_name in ('guild_audit_logs_v2', 'guild_audit_logs_v2_by_action',
                      'guild_audit_logs_v2_by_user', 'guild_audit_logs_v2_by_user_action'):
        if row.get('options') is not None:
            row['options'] = {str(k): str(v) for k, v in row['options'].items()}

    if table_name == 'messages':
        if row.get('call') is not None and isinstance(row['call'], dict):
            row['call'] = make_nt(udt_classes['message_call'], row['call'])

    if table_name == 'user_guild_settings':
        if row.get('channel_overrides') is not None:
            coerced = {}
            for k, v in row['channel_overrides'].items():
                if isinstance(v, dict):
                    if isinstance(v.get('mute_config'), dict):
                        v = dict(v)
                        v['mute_config'] = make_nt(udt_classes['mute_config'], v['mute_config'])
                    v = make_nt(udt_classes['channel_override'], v)
                coerced[k] = v
            row['channel_overrides'] = coerced

    if table_name == 'user_settings':
        if row.get('guild_folders') is not None:
            row['guild_folders'] = [
                make_nt(udt_classes['guild_folder'], f) if isinstance(f, dict) else f
                for f in row['guild_folders']
            ]

    if table_name == 'user_contact_change_logs':
        if 'event_id' not in row:
            ts = row.get('event_at', datetime.now(timezone.utc))
            row['event_id'] = uuid_from_time(ts)

    return row


# ---------------------------------------------------------------------------
# Main migration
# ---------------------------------------------------------------------------

def migrate(db_path, host, port, keyspace, username, password, dc, dry_run):
    print(f"Opening SQLite: {db_path}")
    conn = apsw.Connection(db_path)
    cur = conn.cursor()

    print(f"Connecting to Cassandra: {host}:{port} keyspace={keyspace}")
    auth = PlainTextAuthProvider(username=username, password=password)

    # Build UDT namedtuple classes from schema BEFORE connecting
    # We need a temporary connection just to read metadata
    cluster = Cluster(
        contact_points=[host],
        port=port,
        auth_provider=auth,
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc=dc),
    )
    # Connect to system to read keyspace metadata
    tmp_session = cluster.connect()
    ks_meta = cluster.metadata.keyspaces.get(keyspace)

    udt_classes = {}
    if ks_meta:
        for udt_name, udt_meta in ks_meta.user_types.items():
            fields = list(udt_meta.field_names)
            udt_cls = collections.namedtuple(udt_name, fields)
            udt_classes[udt_name] = udt_cls
            # Register BEFORE connecting to the keyspace
            cluster.register_user_type(keyspace, udt_name, udt_cls)

    tmp_session.shutdown()

    # Now reconnect — driver will use registered UDT classes for serialization
    cluster2 = Cluster(
        contact_points=[host],
        port=port,
        auth_provider=auth,
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc=dc),
    )
    ks_meta2 = cluster2.metadata.keyspaces.get(keyspace)
    if ks_meta2:
        for udt_name, udt_cls in udt_classes.items():
            cluster2.register_user_type(keyspace, udt_name, udt_cls)

    session = cluster2.connect(keyspace)

    # Fetch all rows grouped by table
    rows_by_table = {}
    for table_name, value_blob in cur.execute("SELECT table_name, value FROM kv_store ORDER BY table_name"):
        row = deserialize_row(value_blob)
        rows_by_table.setdefault(table_name, []).append(row)

    total_inserted = 0
    total_errors = 0

    # Cache prepared statements per table+columns combo
    prepared_cache = {}

    for table_name, rows in sorted(rows_by_table.items()):
        print(f"\n  [{table_name}] {len(rows)} rows")

        for row in rows:
            row = coerce_row(table_name, row, udt_classes)

            columns = tuple(row.keys())
            col_list = ", ".join(columns)
            cql = f"INSERT INTO {table_name} ({col_list}) VALUES ({', '.join('?' for _ in columns)})"

            if dry_run:
                print(f"    DRY RUN: {cql}")
                print(f"    params:  {row}")
                continue

            try:
                cache_key = (table_name, columns)
                if cache_key not in prepared_cache:
                    prepared_cache[cache_key] = session.prepare(cql)
                stmt = prepared_cache[cache_key]
                values = [row[c] for c in columns]
                session.execute(stmt, values)
                total_inserted += 1
            except Exception as e:
                print(f"    ERROR on {table_name}: {e}")
                print(f"    row: {row}")
                total_errors += 1

    cluster2.shutdown()
    print(f"\nDone. Inserted: {total_inserted}, Errors: {total_errors}")
    if total_errors > 0:
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate Fluxer SQLite KV store to Cassandra/ScyllaDB")
    parser.add_argument("--db", required=True, help="Path to fluxer.db")
    parser.add_argument("--host", default="127.0.0.1", help="Cassandra host")
    parser.add_argument("--port", type=int, default=9042, help="Cassandra port")
    parser.add_argument("--keyspace", required=True, help="Target keyspace")
    parser.add_argument("--username", default="cassandra")
    parser.add_argument("--password", default="cassandra")
    parser.add_argument("--dc", default="dc1", help="Local datacenter name")
    parser.add_argument("--dry-run", action="store_true", help="Print CQL without executing")
    args = parser.parse_args()

    migrate(
        db_path=args.db,
        host=args.host,
        port=args.port,
        keyspace=args.keyspace,
        username=args.username,
        password=args.password,
        dc=args.dc,
        dry_run=args.dry_run,
    )
