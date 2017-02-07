#include <boost/test/unit_test.hpp>
#include <memory>
#include <utility>

#include "core/sstring.hh"
#include "core/future-util.hh"
#include "core/do_with.hh"
#include "core/distributed.hh"
#include "sstables/sstables.hh"
#include "tests/test-utils.hh"
#include "schema.hh"
#include "database.hh"
#include "mutation_reader.hh"
#include "sstable_test.hh"
#include "tmpdir.hh"

#include "disk-error-handler.hh"

thread_local disk_error_signal_type commit_error;
thread_local disk_error_signal_type general_disk_error;

using namespace sstables;

class database_test {
public:
    static void add_keyspace(database& db, sstring name, keyspace k) {
        db.add_keyspace(name, std::move(k));
    }

    static void add_column_family(database& db, schema_ptr& schema, column_family::config cfg) {
        lw_shared_ptr<column_family> cf = make_lw_shared<column_family>(schema, std::move(cfg), column_family::no_commitlog(), db._compaction_manager);
        auto uuid = schema->id();
        db._column_families.emplace(uuid, std::move(cf));
        auto kscf = std::make_pair(schema->ks_name(), schema->cf_name());
        db._ks_cf_to_uuid.emplace(std::move(kscf), uuid);
    }
};

static inline dht::token create_token_from_key(sstring key) {
    sstables::key_view key_view = sstables::key_view(bytes_view(reinterpret_cast<const signed char*>(key.c_str()), key.size()));
    dht::token token = dht::global_partitioner().get_token(key_view);
    assert(token == dht::global_partitioner().get_token(key_view));
    return token;
}

static inline std::vector<std::pair<sstring, dht::token>> token_generation_for_shard(shard_id shard, unsigned tokens_to_generate) {
    unsigned tokens = 0;
    unsigned key_id = 0;
    std::vector<std::pair<sstring, dht::token>> key_and_token_pair;

    key_and_token_pair.reserve(tokens_to_generate);
    dht::set_global_partitioner(to_sstring("org.apache.cassandra.dht.Murmur3Partitioner"));

    while (tokens < tokens_to_generate) {
        sstring key = to_sstring(key_id++);
        dht::token token = create_token_from_key(key);
        if (shard != dht::global_partitioner().shard_of(token)) {
            continue;
        }
        tokens++;
        key_and_token_pair.emplace_back(key, token);
    }
    assert(key_and_token_pair.size() == tokens_to_generate);

    std::sort(key_and_token_pair.begin(),key_and_token_pair.end(), [] (auto& i, auto& j) {
        return i.second < j.second;
    });

    return key_and_token_pair;
}

static schema_ptr get_schema() {
    return schema_builder("tests", "sstable_resharding_test")
        .with_column("id", utf8_type, column_kind::partition_key)
        .with_column("value", int32_type).build();
}

void run_sstable_resharding_test() {
    auto tmp = make_lw_shared<tmpdir>();
    distributed<database> db;

    db.start().get();
    db.invoke_on_all([datadir = tmp->path] (database& db) {
        auto s = get_schema();
        auto ksm = make_lw_shared<keyspace_metadata>(s->ks_name(), "org.apache.cassandra.locator.LocalStrategy", std::map<sstring, sstring>{}, false);
        auto kscfg = db.make_keyspace_config(*ksm);
        kscfg.enable_disk_reads = kscfg.enable_disk_writes = true;
        kscfg.enable_cache = false;
        keyspace ks{ksm, std::move(kscfg)};
        database_test::add_keyspace(db, s->ks_name(), std::move(ks));

        column_family::config cfg;
        cfg.datadir = datadir;
        cfg.enable_commitlog = cfg.enable_incremental_backups = false;
        database_test::add_column_family(db, s, cfg);
        auto& cf = db.find_column_family(s->ks_name(), s->cf_name());
        column_family_test::update_sstables_known_generation(cf, 5 * smp::count);
    }).get();

    auto s = get_schema();

    // create sst shared by all shards
    {
        auto mt = make_lw_shared<memtable>(s);
        auto apply_key = [mt, s] (sstring key_to_write, auto value) {
            auto key = partition_key::from_exploded(*s, {to_bytes(key_to_write)});
            mutation m(key, s);
            m.set_clustered_cell(clustering_key::make_empty(), bytes("value"), data_value(int32_t(value)), api::timestamp_type(0));
            mt->apply(std::move(m));
        };
        for (auto i : boost::irange(0u, smp::count)) {
            auto key_token_pair = token_generation_for_shard(i, 1);
            BOOST_REQUIRE(key_token_pair.size() == 1);
            apply_key(key_token_pair[0].first, i);
        }
        auto sst = make_lw_shared<sstable>(s, tmp->path, 1, sstables::sstable::version_types::ka, sstables::sstable::format_types::big);
        sst->write_components(*mt).get();
    }
    auto sst = make_lw_shared<sstables::sstable>(s, tmp->path, 1, sstables::sstable::version_types::ka, sstables::sstable::format_types::big);
    sst->load().get();

    auto new_sstables = sstables::reshard_sstables(db, { sst }, s->ks_name(), s->cf_name(), std::numeric_limits<uint64_t>::max(), 0).get0();
    BOOST_REQUIRE(new_sstables.size() == smp::count);
    for (auto& sstable : new_sstables) {
        auto new_sst = make_lw_shared<sstables::sstable>(s, tmp->path, sstable->generation(),
            sstables::sstable::version_types::ka, sstables::sstable::format_types::big);
        new_sst->load().get();
        auto shards = new_sst->get_shards_for_this_sstable();
        BOOST_REQUIRE(shards.size() == 1); // check sstable is unshared.
        auto shard = shards.front();
        BOOST_REQUIRE(column_family_test::calculate_shard_from_sstable_generation(new_sst->generation()) == shard);

        auto key_token_pair = token_generation_for_shard(shard, 1);
        auto reader = make_lw_shared(as_mutation_reader(new_sst, new_sst->read_rows(s)));
        (*reader)().then([] (auto sm) {
            return mutation_from_streamed_mutation(std::move(sm));
        }).then([reader, s, key = key_token_pair[0].first, shard] (mutation_opt m) {
            BOOST_REQUIRE(m);
            BOOST_REQUIRE(m->key().equal(*s, partition_key::from_singular(*s, data_value(key))));
            BOOST_REQUIRE(!m->partition().partition_tombstone());
            auto &rows = m->partition().clustered_rows();
            auto &row = rows.begin()->row();
            BOOST_REQUIRE(!row.deleted_at());
            auto &cells = row.cells();
            BOOST_REQUIRE(cells.cell_at(s->get_column_definition("value")->id).as_atomic_cell().value() == bytes({0,0,0,int8_t(shard)}));
            return (*reader)();
        }).then([reader] (streamed_mutation_opt m) {
            BOOST_REQUIRE(!m);
        }).get();
    }

    db.stop().get();
}

SEASTAR_TEST_CASE(sstable_resharding_test) {
    return seastar::async([] {
        run_sstable_resharding_test();
    });
}

