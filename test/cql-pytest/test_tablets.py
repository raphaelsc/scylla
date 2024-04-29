# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Some tests for the new "tablets"-based replication, replicating the old
# "vnodes". Eventually, ScyllaDB will use tablets by default and all tests
# will run using tablets, but these tests are for specific issues discovered
# while developing tablets that didn't exist for vnodes. Note that most tests
# for tablets require multiple nodes, and are in the test/topology*
# directory, so here we'll probably only ever have a handful of single-node
# tests.
#############################################################################

import pytest
from util import new_test_keyspace, new_test_table, unique_name, index_table_name
from cassandra.protocol import ConfigurationException, InvalidRequest

# A fixture similar to "test_keyspace", just creates a keyspace that enables
# tablets with initial_tablets=128
# The "initial_tablets" feature doesn't work if the "tablets" experimental
# feature is not turned on; In such a case, the tests using this fixture
# will be skipped.
@pytest.fixture(scope="module")
def test_keyspace_128_tablets(cql, this_dc):
    name = unique_name()
    try:
        cql.execute("CREATE KEYSPACE " + name + " WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', '" + this_dc + "': 1 } AND TABLETS = { 'enabled': true, 'initial': 128 }")
    except ConfigurationException:
        pytest.skip('Scylla does not support initial_tablets, or the tablets feature is not enabled')
    yield name
    cql.execute("DROP KEYSPACE " + name)

# In the past (issue #16493), repeatedly creating and deleting a table
# would leak memory. Creating a table with 128 tablets would make this
# leak 128 times more serious and cause a failure faster. This is a
# reproducer for this problem. We basically expect this test not to
# OOM Scylla - the test doesn't "check" anything, the way it failed was
# for Scylla to run out of memory and then fail one of the CREATE TABLE
# or DROP TABLE operations in the loop.
# Note that this test doesn't even involve any data inside the table.
# Reproduces #16493.
def test_create_loop_with_tablets(cql, test_keyspace_128_tablets):
    table = test_keyspace_128_tablets + "." + unique_name()
    for i in range(100):
        cql.execute(f"CREATE TABLE {table} (p int PRIMARY KEY, v int)")
        cql.execute("DROP TABLE " + table)


# Converting vnodes-based keyspace to tablets-based in not implemented yet
def test_alter_cannot_change_vnodes_to_tablets(cql, skip_without_tablets):
    ksdef = "WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : '1' } AND TABLETS = { 'enabled' : false }"
    with new_test_keyspace(cql, ksdef) as keyspace:
        with pytest.raises(InvalidRequest, match="Cannot alter replication strategy vnode/tablets flavor"):
            cql.execute(f"ALTER KEYSPACE {keyspace} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND tablets = {{'initial': 1}};")


# Counters are not supported with tablets
def test_counters_are_not_supported(cql, skip_without_tablets):
    ksdef = "WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : '1' } AND TABLETS = { 'enabled' : true }"
    with new_test_keyspace(cql, ksdef) as keyspace:
        with pytest.raises(InvalidRequest, match="Counters are not supported"):
            cql.execute(f"CREATE TABLE {keyspace}.test (p int PRIMARY KEY, c counter)")


# Converting vnodes-based keyspace to tablets-based in not implemented yet
def test_alter_doesnt_enable_tablets(cql, skip_without_tablets):
    ksdef = "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
    with new_test_keyspace(cql, ksdef) as keyspace:
        cql.execute(f"ALTER KEYSPACE {keyspace} WITH replication = {{'class': 'NetworkTopologyStrategy'}};")

        res = cql.execute(f"DESCRIBE KEYSPACE {keyspace}").one()
        assert "NetworkTopologyStrategy" in res.create_statement

        res = cql.execute(f"SELECT * FROM system_schema.scylla_keyspaces WHERE keyspace_name = '{keyspace}'")
        assert len(list(res)) == 0, "tablets replication strategy turned on"


def test_tablet_default_initialization(cql, skip_without_tablets):
    ksdef = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};"
    with new_test_keyspace(cql, ksdef) as keyspace:
        res = cql.execute(f"SELECT * FROM system_schema.scylla_keyspaces WHERE keyspace_name = '{keyspace}'").one()
        assert res.initial_tablets == 0, "initial_tablets not configured"

        with new_test_table(cql, keyspace, "pk int PRIMARY KEY, c int") as table:
            table = table.split('.')[1]
            res = cql.execute("SELECT * FROM system.tablets")
            for row in res:
                if row.keyspace_name == keyspace and row.table_name == table:
                    assert row.tablet_count > 0, "zero tablets allocated"
                    break
            else:
                assert False, "tablets not allocated"


def test_tablets_can_be_explicitly_disabled(cql, skip_without_tablets):
    ksdef = "WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND TABLETS = {'enabled': false};"
    with new_test_keyspace(cql, ksdef) as keyspace:
        res = cql.execute(f"SELECT * FROM system_schema.scylla_keyspaces WHERE keyspace_name = '{keyspace}'")
        assert len(list(res)) == 0, "tablets replication strategy turned on"


def test_alter_changes_initial_tablets(cql, skip_without_tablets):
    ksdef = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1};"
    with new_test_keyspace(cql, ksdef) as keyspace:
        cql.execute(f"ALTER KEYSPACE {keyspace} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND tablets = {{'initial': 2}};")
        res = cql.execute(f"SELECT * FROM system_schema.scylla_keyspaces WHERE keyspace_name = '{keyspace}'").one()
        assert res.initial_tablets == 2


# Test that initial number of tablets is preserved in describe
def test_describe_initial_tablets(cql, skip_without_tablets):
    ksdef = "WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : '1' } " \
            "AND TABLETS = { 'initial' : 1 }"
    with new_test_keyspace(cql, ksdef) as keyspace:
        desc = cql.execute(f"DESCRIBE KEYSPACE {keyspace}")
        assert "and tablets = {'initial': 1}" in desc.one().create_statement.lower()


def verify_tablets_presence(cql, keyspace_name, table_name, expected:bool=True):
    res = cql.execute(f"SELECT * FROM system.tablets WHERE keyspace_name='{keyspace_name}' AND table_name='{table_name}' ALLOW FILTERING")
    if expected:
        assert res, f"{keyspace_name}.{table_name} not found in system.tablets"
        assert res.one().tablet_count > 0, f"table {keyspace_name}.{table_name}: zero tablets allocated"
    else:
        assert not res, f"{keyspace_name}.{table_name} was found in system.tablets after it was dropped"

# Test that when a tablets-enabled table is dropped, all of its tablets are dropped with it.
def test_tablets_are_dropped_when_dropping_table(cql, test_keyspace, skip_without_tablets):
    table_name = unique_name()
    schema = "pk int PRIMARY KEY, c int"
    cql.execute(f"CREATE TABLE {test_keyspace}.{table_name} ({schema})")
    verify_tablets_presence(cql, test_keyspace, table_name)

    cql.execute(f"DROP TABLE {test_keyspace}.{table_name}")
    verify_tablets_presence(cql, test_keyspace, table_name, expected=False)


# Test that when a view of a tablets-enabled table is dropped, all of its tablets are dropped with it.
#
# Reproduces https://github.com/scylladb/scylladb/issues/17627
# with materialized views, which were not part of the original scope of this issue.
def test_tablets_are_dropped_when_dropping_table_with_view(cql, test_keyspace):
    table_name = unique_name()
    schema = "pk int PRIMARY KEY, c int"
    # new_test_table is not used since we want to test a failure to drop the table
    cql.execute(f"CREATE TABLE {test_keyspace}.{table_name} ({schema})")
    try:
        view_name = unique_name()
        where = "c is not null and pk is not null"
        view_pk = "c, pk"
        cql.execute(f"CREATE MATERIALIZED VIEW {test_keyspace}.{view_name} AS SELECT * FROM {table_name} WHERE {where} PRIMARY KEY ({view_pk})")
        verify_tablets_presence(cql, test_keyspace, table_name)
        verify_tablets_presence(cql, test_keyspace, view_name)

        # When attempting to drop the table while it has views depending on, table drop is expected to fail.
        # Verify that all of their tablets still exist after the error is returned.
        with pytest.raises(InvalidRequest):
            cql.execute(f"DROP TABLE {test_keyspace}.{table_name}")

        # failure to drop the table should keep its tablets intact
        verify_tablets_presence(cql, test_keyspace, table_name)
        verify_tablets_presence(cql, test_keyspace, view_name)

        cql.execute(f"DROP MATERIALIZED VIEW {test_keyspace}.{view_name}")
        verify_tablets_presence(cql, test_keyspace, view_name, expected=False)
        verify_tablets_presence(cql, test_keyspace, table_name, expected=True)

        cql.execute(f"DROP TABLE {test_keyspace}.{table_name}")
        verify_tablets_presence(cql, test_keyspace, table_name, expected=False)
    except Exception as e:
        try:
            cql.execute(f"DROP MATERIALIZED VIEW {test_keyspace}.{view_name}")
        except:
            pass
        try:
            cql.execute(f"DROP TABLE {test_keyspace}.{table_name}")
        except:
            pass
        raise e


# Test the following cases:
# 1. When an index of a tablets-enabled table is dropped, all of its tablets are dropped with it.
# 2. When a tablets-enabled table that has an index is dropped, the tablets associated with the table and index are dropped with it.
#
# Reproduces https://github.com/scylladb/scylladb/issues/17627
@pytest.mark.parametrize("drop_index", [True, False])
def test_tablets_are_dropped_when_dropping_index(cql, test_keyspace, drop_index):
    table_name = unique_name()
    schema = "pk int PRIMARY KEY, c int"
    cql.execute(f"CREATE TABLE {test_keyspace}.{table_name} ({schema})")
    try:
        index_name = unique_name()
        cql.execute(f"CREATE INDEX {index_name} ON {test_keyspace}.{table_name} (c)")
        verify_tablets_presence(cql, test_keyspace, table_name)
        verify_tablets_presence(cql, test_keyspace, index_table_name(index_name))

        if drop_index:
            cql.execute(f"DROP INDEX {test_keyspace}.{index_name}")
            verify_tablets_presence(cql, test_keyspace, index_table_name(index_name), expected=False)

        cql.execute(f"DROP TABLE {test_keyspace}.{table_name}")
        verify_tablets_presence(cql, test_keyspace, table_name, expected=False)
        verify_tablets_presence(cql, test_keyspace, index_table_name(index_name), expected=False)
    except Exception as e:
        try:
            cql.execute(f"DROP TABLE {test_keyspace}.{table_name}")
        except:
            pass
        raise e
