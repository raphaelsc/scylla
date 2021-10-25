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

#pragma once

#include <string>
#include <map>
#include <seastar/core/sstring.hh>

struct storage_options {
    enum class storage_type {
        NATIVE, S3,
    };

    storage_type type = storage_type::NATIVE;
    sstring bucket;
    sstring key_id;
    sstring endpoint;
    storage_options() = default;

    std::map<sstring, sstring> to_map() const;

    static storage_type parse_type(std::string_view str);
    static sstring print_type(storage_type type);
};
