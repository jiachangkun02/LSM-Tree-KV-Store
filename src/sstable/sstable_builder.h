#pragma once
#include <string>
#include <fstream>
#include <memory>
#include "../util/status.h"
#include "../util/slice.h"
#include "../util/coding.h"
#include "../util/bloom_filter.h"
#include "format.h"
#include "block.h"
#include "index_block.h"
#include "../memtable/memtable.h"

namespace lsmkv {

struct SSTableMeta {
    uint64_t number = 0;
    int level = 0;
    std::string file_path;
    std::string smallest_key;
    std::string largest_key;
    uint64_t file_size = 0;
};

class SSTableBuilder {
public:
    SSTableBuilder(const std::string& file_path, size_t block_size, unsigned bloom_bits)
        : file_path_(file_path), block_size_(block_size), data_block_(block_size), filter_builder_(bloom_bits) {}

    Status Open() {
        ofs_.open(file_path_, std::ios::binary | std::ios::out | std::ios::trunc);
        if (!ofs_.good()) return Status::IOError("open sstable for write failed: " + file_path_);
        offset_ = 0; return Status::OK();
    }

    Status Add(const Slice& key, const MemValue& mv) {
        if (num_entries_ == 0) smallest_key_ = key.ToString();
        largest_key_ = key.ToString();
        if (data_block_.CurrentSize() == 0) pending_index_key_ = key.ToString();

        data_block_.Add(key, mv);
        filter_builder_.AddKey(key);
        ++num_entries_;
        if (data_block_.ShouldFlush()) {
            std::string block = data_block_.Finish();
            uint64_t off = offset_;
            ofs_.write(block.data(), block.size());
            offset_ += block.size();
            index_builder_.Add(Slice(pending_index_key_), off, block.size());
        }
        return Status::OK();
    }

    Status Finish(SSTableMeta* meta_out) {
        if (data_block_.CurrentSize() > 0) {
            std::string block = data_block_.Finish();
            uint64_t off = offset_;
            ofs_.write(block.data(), block.size());
            offset_ += block.size();
            index_builder_.Add(Slice(pending_index_key_), off, block.size());
        }

        std::string index_data = index_builder_.Finish();
        uint64_t index_off = offset_; ofs_.write(index_data.data(), index_data.size()); offset_ += index_data.size();

        std::string filter_data = filter_builder_.Finalize();
        uint64_t filter_off = offset_; ofs_.write(filter_data.data(), filter_data.size()); offset_ += filter_data.size();

        Footer f; f.index_offset=index_off; f.index_size=index_data.size(); f.filter_offset=filter_off; f.filter_size=filter_data.size();
        std::string footer; EncodeFooter(footer, f);
        ofs_.write(footer.data(), footer.size()); offset_ += footer.size();
        ofs_.flush(); ofs_.close();

        if (meta_out) {
            meta_out->file_path = file_path_;
            meta_out->smallest_key = smallest_key_;
            meta_out->largest_key = largest_key_;
            meta_out->file_size = offset_;
        }
        return Status::OK();
    }

private:
    std::string file_path_;
    size_t block_size_;
    std::ofstream ofs_;
    uint64_t offset_ = 0;

    DataBlockBuilder data_block_;
    IndexBlockBuilder index_builder_;
    BloomFilterBuilder filter_builder_;
    std::string pending_index_key_;
    size_t num_entries_ = 0;

    std::string smallest_key_;
    std::string largest_key_;
};

} // namespace lsmkv
