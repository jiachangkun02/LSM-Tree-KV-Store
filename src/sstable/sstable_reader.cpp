#include "sstable_reader.h"
#include "../table_cache/block_cache.h"

namespace lsmkv {

Status SSTableReader::Open(const std::string& file_path, std::shared_ptr<SSTableReader>* out) {
    std::shared_ptr<SSTableReader> r(new SSTableReader());
    r->path_ = file_path;
    r->ifs_.open(file_path, std::ios::binary | std::ios::in);
    if (!r->ifs_.good()) return Status::IOError("open sstable for read failed: " + file_path);
    Status s = r->Load(); if (!s.ok()) return s;
    *out = std::move(r);
    return Status::OK();
}

Status SSTableReader::Load() {
    ifs_.seekg(0, std::ios::end);
    std::streamoff sz = ifs_.tellg();
    if (sz < 56) return Status::Corruption("file too small");
    ifs_.seekg(sz - 56, std::ios::beg);
    std::string footer_block(56, '\0');
    if (!ifs_.read(&footer_block[0], 56)) return Status::IOError("read footer failed");
    if (!DecodeFooter(footer_block, &footer_)) return Status::Corruption("bad footer");

    ifs_.seekg(footer_.index_offset, std::ios::beg);
    std::string index_data(footer_.index_size, '\0');
    if (!ifs_.read(&index_data[0], footer_.index_size)) return Status::IOError("read index failed");
    index_reader_.reset(new IndexBlockReader(Slice(index_data)));

    ifs_.seekg(footer_.filter_offset, std::ios::beg);
    std::string filter_data(footer_.filter_size, '\0');
    if (!ifs_.read(&filter_data[0], footer_.filter_size)) return Status::IOError("read filter failed");
    filter_reader_.reset(new BloomFilterReader(Slice(filter_data)));

    return Status::OK();
}

void SSTableReader::Close() { if (ifs_.is_open()) ifs_.close(); }

Status SSTableReader::Get(const Slice& key, std::optional<MemValue>& result, BlockCache* bc, bool fill_cache) {
    if (!filter_reader_->KeyMayMatch(key)) { result.reset(); return Status::OK(); }
    int blk = index_reader_->FindBlock(key);
    if (blk < 0) { result.reset(); return Status::OK(); }
    const auto& e = index_reader_->entries()[blk];

    std::string block_data;
    std::string cache_key = path_ + ":" + std::to_string(e.off);
    if (bc) {
        if (bc->Get(cache_key, &block_data)) { /* cached */ }
        else {
            ifs_.seekg(e.off, std::ios::beg);
            block_data.resize(e.sz);
            if (!ifs_.read(&block_data[0], e.sz)) return Status::IOError("read data block failed");
            if (fill_cache) bc->Put(cache_key, block_data);
        }
    } else {
        ifs_.seekg(e.off, std::ios::beg);
        block_data.resize(e.sz);
        if (!ifs_.read(&block_data[0], e.sz)) return Status::IOError("read data block failed");
    }

    DataBlockReader dbr(Slice(block_data));
    ParsedEntry pe;
    while (dbr.Next(pe)) {
        int c = pe.key.compare(key);
        if (c == 0) {
            MemValue mv; mv.type = pe.type; mv.value = pe.value.ToString();
            result = mv; return Status::OK();
        }
        if (c > 0) break;
    }
    result.reset(); return Status::OK();
}

void SSTableReader::Iterator::Init() {
    if (r_->index().entries().empty()) { valid_ = false; return; }
    block_index_ = 0;
    ReadBlock(block_index_);
    ParsedEntry e;
    if (reader_ && reader_->Next(e)) {
        key_ = e.key;
        mv_.type = e.type; mv_.value = e.value.ToString();
        valid_ = true;
    } else { valid_ = false; }
}
void SSTableReader::Iterator::ReadBlock(int index) {
    const auto& ent = r_->index().entries()[index];
    r_->ifs_.seekg(ent.off, std::ios::beg);
    block_buf_.resize(ent.sz);
    if (!r_->ifs_.read(&block_buf_[0], ent.sz)) { valid_ = false; return; }
    delete reader_;
    reader_ = new DataBlockReader(Slice(block_buf_));
}
void SSTableReader::Iterator::Next() {
    if (!valid_) return;
    ParsedEntry e;
    if (reader_->Next(e)) {
        key_ = e.key;
        mv_.type = e.type; mv_.value = e.value.ToString();
        valid_ = true; return;
    }
    block_index_++;
    if (block_index_ >= (int)r_->index().entries().size()) { valid_ = false; return; }
    ReadBlock(block_index_);
    if (reader_->Next(e)) {
        key_ = e.key;
        mv_.type = e.type; mv_.value = e.value.ToString();
        valid_ = true;
    } else valid_ = false;
}

} // namespace lsmkv
