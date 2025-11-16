#pragma once
#include <string>
#include <fstream>
#include <memory>
#include <optional>
#include <cstring>
#include "format.h"
#include "block.h"
#include "index_block.h"
#include "../util/status.h"
#include "../util/slice.h"
#include "../util/bloom_filter.h"

namespace lsmkv {

class BlockCache;

class SSTableReader {
public:
    static Status Open(const std::string& file_path, std::shared_ptr<SSTableReader>* out);

    ~SSTableReader() { Close(); }

    Status Get(const Slice& key, std::optional<MemValue>& result, BlockCache* bc, bool fill_cache);
    void Close();

    class Iterator {
    public:
        explicit Iterator(SSTableReader* r) : r_(r) { Init(); }
        ~Iterator() { delete reader_; }
        bool Valid() const { return valid_; }
        void Next();
        Slice key() const { return key_; }
        MemValue value() const { return mv_; }
    private:
        void Init();
        void ReadBlock(int index);
        SSTableReader* r_;
        bool valid_ = false;
        int block_index_ = -1;
        std::string block_buf_;
        DataBlockReader* reader_ = nullptr;
        ParsedEntry e_;
        Slice key_;
        MemValue mv_;
    };

    std::unique_ptr<Iterator> NewIterator() { return std::unique_ptr<Iterator>(new Iterator(this)); }

    const IndexBlockReader& index() const { return *index_reader_; }
    const BloomFilterReader& filter() const { return *filter_reader_; }

private:
    SSTableReader() = default;
    Status Load();

    std::ifstream ifs_;
    std::string path_;
    Footer footer_;
    std::unique_ptr<IndexBlockReader> index_reader_;
    std::unique_ptr<BloomFilterReader> filter_reader_;
};

} // namespace lsmkv
