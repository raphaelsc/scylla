/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "tools/load_system_tablets.hh"

#include <seastar/core/thread.hh>
#include <seastar/util/closeable.hh>

#include "utils/log.hh"
#include "db/schema_tables.hh"
#include "db/system_keyspace.hh"
#include "mutation/mutation.hh"
#include "readers/combined.hh"
#include "replica/tablets.hh"
#include "sstables/sstables.hh"
#include "tools/read_mutation.hh"
#include "types/list.hh"
#include "types/map.hh"
#include "types/tuple.hh"

namespace {

logging::logger logger{"load_sys_tablets"};

tools::tablets_t do_load_system_tablets(const db::config& dbcfg,
                                        std::filesystem::path scylla_data_path,
                                        table_id table,
                                        reader_permit permit,
                                        std::optional<std::filesystem::path> tablets_directory) {
    sharded<sstable_manager_service> sst_man;
    auto scf = make_sstable_compressor_factory_for_tests_in_thread();
    sst_man.start(std::ref(dbcfg), std::ref(*scf)).get();
    auto stop_sst_man_service = deferred_stop(sst_man);

    auto schema = db::system_keyspace::tablets();
    auto tablets_table_directory = tablets_directory
            ? *tablets_directory
            : get_table_directory(scylla_data_path,
                                  db::system_keyspace::NAME,
                                  schema->cf_name()).get();
    auto mut = read_mutation_from_table_offline(sst_man,
                                                permit,
                                                tablets_table_directory,
                                                db::system_keyspace::NAME,
                                                db::system_keyspace::tablets,
                                                data_value(table.uuid()),
                                                {});
    if (!mut || mut->partition().row_count() == 0) {
        throw std::runtime_error(fmt::format("failed to find tablets for table {}", table));
    }

    tools::tablets_t tablets;
    query::result_set result_set{*mut};
    for (auto& row : result_set.rows()) {
        auto last_token = row.get_nonnull<int64_t>("last_token");
        auto replica_set = row.get_data_value("replicas");
        if (replica_set) {
            tablets.emplace(last_token,
                            replica::tablet_replica_set_from_cell(*replica_set));
        }
    }
    return tablets;
}

std::optional<data_dictionary::storage_options> do_load_keyspace_storage_options(const db::config& dbcfg,
                                        std::filesystem::path scylla_data_path,
                                        std::string_view keyspace,
                                        reader_permit permit) {
    sharded<sstable_manager_service> sst_man;
    auto scf = make_sstable_compressor_factory_for_tests_in_thread();
    sst_man.start(std::ref(dbcfg), std::ref(*scf)).get();
    auto stop_sst_man_service = deferred_stop(sst_man);

    auto table_directory = get_table_directory(scylla_data_path,
                                               db::schema_tables::NAME,
                                               db::schema_tables::SCYLLA_KEYSPACES).get();
    auto mut = read_mutation_from_table_offline(sst_man,
                                                permit,
                                                table_directory,
                                                db::schema_tables::NAME,
                                                db::schema_tables::scylla_keyspaces,
                                                data_value(sstring(keyspace)),
                                                {});
    if (!mut) {
        return std::nullopt;
    }
    query::result_set result_set{*mut};
    if (result_set.empty()) {
        return std::nullopt;
    }
    const auto& row = result_set.row(0);
    auto storage_type = row.get<sstring>("storage_type");
    auto storage_options = row.get<map_type_impl::native_type>("storage_options");
    if (!storage_type || !storage_options) {
        return std::nullopt;
    }
    std::map<sstring, sstring> values;
    for (const auto& [key, value] : *storage_options) {
        values.emplace(value_cast<sstring>(key), value_cast<sstring>(value));
    }
    data_dictionary::storage_options options;
    options.value = data_dictionary::storage_options::from_map(*storage_type, values);
    return options;
}

std::vector<tools::sstables_registry_entry> do_load_system_sstables_registry(const db::config& dbcfg,
                                        std::filesystem::path scylla_data_path,
                                        table_id table,
                                        locator::host_id node_owner,
                                        reader_permit permit) {
    sharded<sstable_manager_service> sst_man;
    auto scf = make_sstable_compressor_factory_for_tests_in_thread();
    sst_man.start(std::ref(dbcfg), std::ref(*scf)).get();
    auto stop_sst_man_service = deferred_stop(sst_man);

    auto table_directory = get_table_directory(scylla_data_path,
                                               db::system_keyspace::NAME,
                                               db::system_keyspace::SSTABLES_REGISTRY).get();
    auto mut = read_mutation_from_table_offline(sst_man,
                                                permit,
                                                table_directory,
                                                db::system_keyspace::NAME,
                                                db::system_keyspace::sstables_registry,
                                                {data_value(table.uuid()), data_value(node_owner.uuid())},
                                                {});
    if (!mut) {
        return {};
    }
    std::vector<tools::sstables_registry_entry> entries;
    query::result_set result_set{*mut};
    for (auto& row : result_set.rows()) {
        auto status = row.get<sstring>("status");
        auto state = row.get<sstring>("state");
        auto generation = row.get<utils::UUID>("generation");
        auto sstable_id = row.get<utils::UUID>("sstable_id");
        auto version = row.get<sstring>("version");
        auto format = row.get<sstring>("format");
        if (!status || !state || !generation || !sstable_id || !version || !format) {
            logger.warn("skipping incomplete {}.{} entry of table {}", db::system_keyspace::NAME,
                    db::system_keyspace::SSTABLES_REGISTRY, table);
            continue;
        }
        entries.emplace_back(std::move(*status), sstables::state_from_dir(*state),
                sstables::entry_descriptor(sstables::generation_type(*generation),
                        sstables::sstable_id(*sstable_id),
                        sstables::version_from_string(*version),
                        sstables::format_from_string(*format),
                        sstables::component_type::TOC));
    }
    return entries;
}

std::optional<tools::local_node_info> do_load_local_node_info(const db::config& dbcfg,
                                        std::filesystem::path scylla_data_path,
                                        reader_permit permit) {
    sharded<sstable_manager_service> sst_man;
    auto scf = make_sstable_compressor_factory_for_tests_in_thread();
    sst_man.start(std::ref(dbcfg), std::ref(*scf)).get();
    auto stop_sst_man_service = deferred_stop(sst_man);

    auto local_table_directory = get_table_directory(scylla_data_path,
                                                     db::system_keyspace::NAME,
                                                     db::system_keyspace::LOCAL).get();
    auto mut = read_mutation_from_table_offline(sst_man,
                                                permit,
                                                local_table_directory,
                                                db::system_keyspace::NAME,
                                                db::system_keyspace::local,
                                                data_value(sstring(db::system_keyspace::LOCAL)),
                                                {});
    if (!mut) {
        return std::nullopt;
    }
    query::result_set result_set{*mut};
    if (result_set.empty()) {
        return std::nullopt;
    }
    const auto& row = result_set.row(0);
    auto host_id = row.get<utils::UUID>("host_id");
    if (!host_id) {
        return std::nullopt;
    }
    tools::local_node_info info{.host_id = locator::host_id(*host_id)};

    // The sharding parameters of the node live in "system.topology", in the row
    // of its host id. The "scylla_nr_shards" and "scylla_msb_ignore" columns of
    // "system.local" are not an alternative: they are dropped columns, so
    // nothing has written them for a long time.
    auto topology_table_directory = get_table_directory(scylla_data_path,
                                                        db::system_keyspace::NAME,
                                                        db::system_keyspace::TOPOLOGY).get();
    auto topology_mut = read_mutation_from_table_offline(sst_man,
                                                permit,
                                                topology_table_directory,
                                                db::system_keyspace::NAME,
                                                db::system_keyspace::topology,
                                                data_value(sstring(db::system_keyspace::TOPOLOGY)),
                                                data_value(*host_id));
    if (!topology_mut) {
        return info;
    }
    query::result_set topology_result_set{*topology_mut};
    if (topology_result_set.empty()) {
        return info;
    }
    const auto& topology_row = topology_result_set.row(0);
    if (auto shard_count = topology_row.get<int32_t>("shard_count")) {
        info.shard_count = unsigned(*shard_count);
    }
    if (auto ignore_msb_bits = topology_row.get<int32_t>("ignore_msb")) {
        info.ignore_msb_bits = unsigned(*ignore_msb_bits);
    }
    return info;
}

// Reads the rows of "system.sstables_registry" the lister asks for, the way
// system_keyspace does on a running node, so that the sstables of a table on
// object storage can be enumerated from the data dir of a node which is down.
class offline_sstables_registry final : public sstables::sstables_registry {
    const db::config& _dbcfg;
    std::filesystem::path _scylla_data_path;
    reader_permit _permit;

    static future<> read_only() {
        return make_exception_future<>(std::runtime_error(
                "the sstables registry of a node which is not running is read-only"));
    }

public:
    offline_sstables_registry(const db::config& dbcfg, std::filesystem::path scylla_data_path, reader_permit permit)
        : _dbcfg(dbcfg)
        , _scylla_data_path(std::move(scylla_data_path))
        , _permit(std::move(permit))
    { }

    future<> create_entry(table_id, locator::host_id, sstring, sstables::sstable_state, sstables::entry_descriptor) override {
        return read_only();
    }
    future<> update_entry_status(table_id, locator::host_id, sstables::generation_type, sstring) override {
        return read_only();
    }
    future<> update_entry_state(table_id, locator::host_id, sstables::generation_type, sstables::sstable_state) override {
        return read_only();
    }
    future<> batch_update_entry_status(table_id, locator::host_id, const std::vector<sstables::generation_type>&, sstring) override {
        return read_only();
    }
    future<> delete_entry(table_id, locator::host_id, sstables::generation_type) override {
        return read_only();
    }
    future<> sstables_registry_list(table_id table, locator::host_id node_owner, entry_consumer consumer) override {
        auto entries = co_await tools::load_system_sstables_registry(_dbcfg, _scylla_data_path, table, node_owner, _permit);
        for (auto& entry : entries) {
            co_await consumer(std::move(entry.status), entry.state, std::move(entry.desc));
        }
    }
};

} // anonymous namespace

namespace tools {

future<tablets_t> load_system_tablets(const db::config &dbcfg,
                                      std::filesystem::path scylla_data_path,
                                      table_id table,
                                      reader_permit permit,
                                      std::optional<std::filesystem::path> tablets_directory) {
    return async([=, &dbcfg] {
        return do_load_system_tablets(dbcfg, scylla_data_path, table, permit, tablets_directory);
    });
}

future<std::optional<data_dictionary::storage_options>> load_keyspace_storage_options(const db::config& dbcfg,
                                      std::filesystem::path scylla_data_path,
                                      std::string_view keyspace,
                                      reader_permit permit) {
    return async([=, &dbcfg] {
        return do_load_keyspace_storage_options(dbcfg, scylla_data_path, keyspace, permit);
    });
}

future<std::vector<sstables_registry_entry>> load_system_sstables_registry(const db::config& dbcfg,
                                      std::filesystem::path scylla_data_path,
                                      table_id table,
                                      locator::host_id node_owner,
                                      reader_permit permit) {
    return async([=, &dbcfg] {
        return do_load_system_sstables_registry(dbcfg, scylla_data_path, table, node_owner, permit);
    });
}

std::unique_ptr<sstables::sstables_registry> make_offline_sstables_registry(const db::config& dbcfg,
                                      std::filesystem::path scylla_data_path,
                                      reader_permit permit) {
    return std::make_unique<offline_sstables_registry>(dbcfg, std::move(scylla_data_path), std::move(permit));
}

future<std::optional<local_node_info>> load_local_node_info(const db::config& dbcfg,
                                      std::filesystem::path scylla_data_path,
                                      reader_permit permit) {
    return async([=, &dbcfg] {
        return do_load_local_node_info(dbcfg, scylla_data_path, permit);
    });
}

} // namespace tools
