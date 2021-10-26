/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <seastar/core/coroutine.hh>
#include <seastar/testing/test_case.hh>

#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"
#include "db/system_distributed_keyspace.hh"
#include "utils/UUID.hh"
#include "gms/inet_address.hh"
#include "types/set.hh"

SEASTAR_TEST_CASE(test_shared_sstable_refcounting) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        utils::UUID t1_id(0, 1);
        gms::inet_address addr1("127.0.0.1"), addr2("127.0.0.2");

        static const auto select_q = format("SELECT sstable, owners from {}.{} WHERE \"table\"=?",
            db::system_distributed_keyspace::NAME_EVERYWHERE,
            db::system_distributed_keyspace::SHARED_SSTABLES);
        auto select_stmt = e.prepare(select_q).get0();

        // Initial state of `system_distributed_everywhere.shared_sstables` should be empty
        require_rows(e, select_stmt, {cql3::raw_value::make_value(uuid_type->decompose(t1_id))}, {});

        // Test adding a reference to an sstable
        auto& sys_dist_ks = e.sys_dist_ks().local();
        sys_dist_ks.add_shared_sstable_owner(t1_id, "sst1", addr1).get0();
        sys_dist_ks.add_shared_sstable_owner(t1_id, "sst1", addr2).get0();

        auto owners_set_type = set_type_impl::get_instance(inet_addr_type, true);
        require_rows(e, select_stmt, {cql3::raw_value::make_value(uuid_type->decompose(t1_id))},
            {{utf8_type->decompose("sst1"),
              owners_set_type->decompose(make_set_value(owners_set_type, {addr1.addr(), addr2.addr()}))}});

        // Test listing sstables co-owned by a node
        std::vector<sstring> addr1_sstables = sys_dist_ks.shared_sstables_owned_by(addr1, t1_id).get0(),
            addr1_expected{"sst1"};
        BOOST_CHECK_EQUAL(addr1_sstables, addr1_expected);

        // Test removing references to an sstable
        sys_dist_ks.remove_shared_sstable_owner(t1_id, "sst1", addr1).get0();
        require_rows(e, select_stmt, {cql3::raw_value::make_value(uuid_type->decompose(t1_id))},
            {{utf8_type->decompose("sst1"),
              owners_set_type->decompose(make_set_value(owners_set_type, {addr2.addr()}))}});

        sys_dist_ks.remove_shared_sstable_owner(t1_id, "sst1", addr2).get0();
        require_rows(e, select_stmt, {cql3::raw_value::make_value(uuid_type->decompose(t1_id))}, {});
    });
}
