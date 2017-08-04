/*
 * Copyright (C) 2017 ScyllaDB
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

#include "seastar/core/file.hh"
#include "bytes.hh"
#include <algorithm>

namespace sstables {

extern logging::logger sstlog;

class integrity_checked_file_impl : public file_impl {
public:
    integrity_checked_file_impl(sstring fname, file f)
            : _fname(std::move(fname)), _file(f) {
        _memory_dma_alignment = f.memory_dma_alignment();
        _disk_read_dma_alignment = f.disk_read_dma_alignment();
        _disk_write_dma_alignment = f.disk_write_dma_alignment();
    }

    virtual future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len, const io_priority_class& pc) override {
        auto wbuf = temporary_buffer<int8_t>(static_cast<const int8_t*>(buffer), len);
        if (len && wbuf[0] == 0 && wbuf[len/2] == 0 && wbuf[len-1] == 0 && !memcmp(wbuf.get(), wbuf.get() + 1, len-1)) {
            sstlog.error("integrity check for {} detected that buffer of {} bytes to offset {} is all zeroed",
                _fname, len, pos);
        }

        auto f = get_file_impl(_file)->write_dma(pos, buffer, len, pc);
        return f.then([this, pos, wbuf = std::move(wbuf), buffer = static_cast<const int8_t*>(buffer), len, &pc] (size_t ret) mutable {
            if (ret != len) {
                sstlog.error("integrity check for {} detected that only {} bytes out of {} were written to offset {}",
                    _fname, ret, len, pos);
            }

            if (!std::equal(wbuf.get(), wbuf.get() + wbuf.size(), buffer, buffer + len)) {
                sstlog.error("integrity check for {} detected that buffer of {} bytes to offset {} was modified after write_dma() call\n" \
                    " unmodified buffer sample:\t{}\n" \
                    " modified buffer sample:  \t{}",
                    _fname, len, pos, bytes(wbuf.get(), std::min(16UL, wbuf.size())), bytes(buffer, std::min(16UL, len)));

                return make_ready_future<size_t>(ret);
            }

            return _file.dma_read_exactly<int8_t>(pos, len, pc).then([this, pos, wbuf = std::move(wbuf), len, ret] (auto rbuf) mutable {
                if (rbuf.size() != len) {
                    sstlog.error("integrity check for {} was only able to read {} bytes out of {} written to offset {}",
                        _fname, rbuf.size(), ret, pos);
                }

                if (!std::equal(rbuf.get(), rbuf.get() + rbuf.size(), wbuf.get(), wbuf.get() + wbuf.size())) {
                    sstlog.error("integrity check for {} failed on write of {} bytes to offset {}\n" \
                        " written buffer sample:\t{}\n" \
                        " read buffer sample:   \t{}",
                        _fname, len, pos, bytes(wbuf.get(), std::min(16UL, wbuf.size())), bytes(rbuf.get(), std::min(16UL, rbuf.size())));
                }
                return make_ready_future<size_t>(ret);
            });
        });
    }

    virtual future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override {
        // TODO: check integrity before and after file_impl::write_dma() like write_dma() above.
        return get_file_impl(_file)->write_dma(pos, iov, pc);
    }

    virtual future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, const io_priority_class& pc) override {
        return get_file_impl(_file)->read_dma(pos, buffer, len, pc);
    }

    virtual future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override {
        return get_file_impl(_file)->read_dma(pos, iov, pc);
    }

    virtual future<> flush(void) override {
        return get_file_impl(_file)->flush();
    }

    virtual future<struct stat> stat(void) override {
        return get_file_impl(_file)->stat();
    }

    virtual future<> truncate(uint64_t length) override {
        return get_file_impl(_file)->truncate(length);
    }

    virtual future<> discard(uint64_t offset, uint64_t length) override {
        return get_file_impl(_file)->discard(offset, length);
    }

    virtual future<> allocate(uint64_t position, uint64_t length) override {
        return get_file_impl(_file)->allocate(position, length);
    }

    virtual future<uint64_t> size(void) override {
        return get_file_impl(_file)->size();
    }

    virtual future<> close() override {
        return get_file_impl(_file)->close();
    }

    // returns a handle for plain file, so make_checked_file() should be called
    // on file returned by handle.
    virtual std::unique_ptr<seastar::file_handle_impl> dup() override {
        return get_file_impl(_file)->dup();
    }

    virtual subscription<directory_entry> list_directory(std::function<future<> (directory_entry de)> next) override {
        return get_file_impl(_file)->list_directory(next);
    }

    virtual future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t range_size, const io_priority_class& pc) override {
        return get_file_impl(_file)->dma_read_bulk(offset, range_size, pc);
    }
private:
    sstring _fname;
    file _file;
};

inline file make_integrity_checked_file(sstring name, file f) {
    return file(::make_shared<integrity_checked_file_impl>(std::move(name), f));
}

inline open_flags adjust_flags_for_integrity_checked_file(open_flags flags) {
    if (static_cast<unsigned int>(flags) & O_WRONLY) {
        flags = open_flags((static_cast<unsigned int>(flags) & ~O_WRONLY) | O_RDWR);
    }
    return flags;
}

future<file>
inline open_integrity_checked_file_dma(sstring name, open_flags flags, file_open_options options) {
    return open_file_dma(name, adjust_flags_for_integrity_checked_file(flags), options).then([name] (file f) {
        return make_ready_future<file>(make_integrity_checked_file(std::move(name), f));
    });
}

future<file>
inline open_integrity_checked_file_dma(sstring name, open_flags flags) {
    return open_file_dma(name, adjust_flags_for_integrity_checked_file(flags)).then([name] (file f) {
        return make_ready_future<file>(make_integrity_checked_file(std::move(name), f));
    });
}

}
