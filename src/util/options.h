#pragma once
#include <cstddef>
#include <string>

namespace lsmkv {

struct Options {
    std::string db_path = "./db";
    size_t write_buffer_size = 4 * 1024 * 1024; // 4MB
    size_t block_size = 4 * 1024; // 4KB
    size_t block_cache_capacity = 64 * 1024 * 1024; // 64MB
    unsigned bloom_bits_per_key = 10;
    size_t max_open_files = 500;
    int num_levels = 7;
    bool create_if_missing = true;
    bool error_if_exists = false;
};

struct ReadOptions {
    bool fill_cache = true;
};

struct WriteOptions {
    bool sync = true;
};

} // namespace lsmkv
