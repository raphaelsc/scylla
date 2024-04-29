#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from cassandra.query import SimpleStatement, ConsistencyLevel
from cassandra.cluster import Session, ConsistencyLevel

from test.pylib.internal_types import ServerInfo
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error_one_shot, HTTPError
from test.pylib.rest_client import inject_error
from test.pylib.util import wait_for_cql_and_get_hosts, read_barrier
from test.pylib.tablets import get_tablet_replica, get_all_tablet_replicas
from test.topology.conftest import skip_mode
from test.topology.util import reconnect_driver

import pytest
import asyncio
import logging
import time
import random
import os
import glob
from typing import NamedTuple
import threading


logger = logging.getLogger(__name__)


class KeyGenerator:
    def __init__(self):
        self.pk = None
        self.pk_lock = threading.Lock()

    def next_pk(self):
        with self.pk_lock:
            if self.pk is not None:
                self.pk += 1
            else:
                self.pk = 0
            return self.pk

    def last_pk(self):
        with self.pk_lock:
            return self.pk

async def start_writes(cql: Session, keyspace: str, table: str, concurrency: int = 3, ignore_errors=False):
    logger.info(f"Starting to asynchronously write, concurrency = {concurrency}")

    stop_event = asyncio.Event()

    warmup_writes = 128 // concurrency
    warmup_event = asyncio.Event()

    stmt = cql.prepare(f"INSERT INTO {keyspace}.{table} (pk, c) VALUES (?, ?)")
    stmt.consistency_level = ConsistencyLevel.QUORUM
    rd_stmt = cql.prepare(f"SELECT * FROM {keyspace}.{table} WHERE pk = ?")
    rd_stmt.consistency_level = ConsistencyLevel.QUORUM

    key_gen = KeyGenerator()

    async def do_writes(worker_id: int):
        write_count = 0
        while not stop_event.is_set():
            pk = key_gen.next_pk()

            # Once next_pk() is produced, key_gen.last_key() is assumed to be in the database
            # hence we can't give up on it.
            while True:
                try:
                    await cql.run_async(stmt, [pk, pk])
                    # Check read-your-writes
                    rows = await cql.run_async(rd_stmt, [pk])
                    assert(len(rows) == 1)
                    assert(rows[0].c == pk)
                    write_count += 1
                    break
                except Exception as e:
                    if ignore_errors:
                        pass # Expected when node is brought down temporarily
                    else:
                        raise e

            if pk == warmup_writes:
                warmup_event.set()

        logger.info(f"Worker #{worker_id} did {write_count} successful writes")

    tasks = [asyncio.create_task(do_writes(worker_id)) for worker_id in range(concurrency)]

    await asyncio.wait_for(warmup_event.wait(), timeout=60)

    async def finish():
        logger.info("Stopping workers")
        stop_event.set()
        await asyncio.gather(*tasks)

        last = key_gen.last_pk()
        if last is not None:
            return last + 1
        return 0

    return finish


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_intranode_migration(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    cmdline = [
        '--logger-log-level', 'storage_service=trace',
        '--logger-log-level', 'stream_session=trace',
        '--logger-log-level', 'tablets=trace',
        '--logger-log-level', 'database=trace',
    ]
    servers = [await manager.server_add(cmdline=cmdline)]

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1};")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")

    finish_writes = await start_writes(cql, "test", "test")

    tablet_token = 0 # Doesn't matter since there is one tablet
    replica = await get_tablet_replica(manager, servers[0], 'test', 'test', tablet_token)

    s0_host_id = await manager.get_host_id(servers[0].server_id)
    src_shard = replica[1]
    dst_shard = src_shard ^ 1

    await manager.api.move_tablet(servers[0].ip_addr, "test", "test", replica[0], src_shard, replica[0], dst_shard, tablet_token)

    key_count = await finish_writes()

    rows = await cql.run_async("SELECT * FROM test.test;")
    assert len(rows) == key_count
    for r in rows:
        assert r.c == r.pk


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_crash_during_intranode_migration(manager: ManagerClient):
    cmdline = [
        '--logger-log-level', 'tablets=trace',
        '--logger-log-level', 'database=trace',
        '--commitlog-sync', 'batch', # So that ACKed writes are not lost on crash
    ]
    servers = [await manager.server_add(cmdline=cmdline)]

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    cql = manager.get_cql()

    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
                        " AND tablets = {'initial': 4};")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")

    finish_writes = await start_writes(cql, "test", "test", ignore_errors=True)

    tablet_token = 0 # Choose one tablet, any of them
    replica = await get_tablet_replica(manager, servers[0], 'test', 'test', tablet_token)

    src_shard = replica[1]
    dst_shard = src_shard ^ 1

    await manager.api.enable_injection(servers[0].ip_addr, 'crash-in-tablet-write-both-read-new', one_shot=True)

    migration_task = asyncio.create_task(manager.api.move_tablet(servers[0].ip_addr, "test", "test",
                                                replica[0], src_shard, replica[0], dst_shard, tablet_token))

    s0_logs = await manager.server_open_log(servers[0].server_id)
    await s0_logs.wait_for('crash-in-tablet-write-both-read-new hit')
    await manager.rolling_restart(servers)

    # Wait for the tablet migration to finish
    await manager.api.quiesce_topology(servers[0].ip_addr)

    try:
        await migration_task
    except:
        pass

    key_count = await finish_writes()

    rows = await cql.run_async("SELECT * FROM test.test;")
    assert len(rows) == key_count
    for r in rows:
        assert r.c == r.pk
