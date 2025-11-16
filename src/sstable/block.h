#pragma once
#include <string>
#include <cstdint>
#include "../util/coding.h"
#include "../util/slice.h"
#include "../memtable/memtable.h"

namespace lsmkv {

// DataBlock: [klen varint][vlen varint][key][value] ...
class DataBlockBuilder {
public:
    explicit DataBlockBuilder(size_t target) : target_size_(target) {}

    void Add(const Slice& key, const MemValue& mv) {
        PutVarint32(buf_, (uint32_t)key.size());
        PutVarint32(buf_, (uint32_t)mv.value.size() + 1); // include type
        buf_.append(key.data(), key.size());
        buf_.push_back((char)mv.type);
        buf_.append(mv.value.data(), mv.value.size());
        if (first_key_.empty()) first_key_ = key.ToString();
    }

    bool ShouldFlush() const { return buf_.size() >= target_size_; }
    std::string Finish() { std::string out; out.swap(buf_); first_key_.clear(); return out; }
    const std::string& first_key() const { return first_key_; }
    size_t CurrentSize() const { return buf_.size(); }

private:
    size_t target_size_;
    std::string buf_;
    std::string first_key_;
};

struct ParsedEntry {
    Slice key;
    ValueType type;
    Slice value;
};

class DataBlockReader {
public:
    explicit DataBlockReader(const Slice& contents) : data_(contents), p_(contents.data()), limit_(contents.data()+contents.size()) {}

    bool Next(ParsedEntry& e) {
        if (p_ >= limit_) return false;
        uint32_t klen=0, vlen=0;
        const char* p = GetVarint32Ptr(p_, limit_, &klen); if (!p) { p_ = limit_; return false; }
        p = GetVarint32Ptr(p, limit_, &vlen); if (!p) { p_ = limit_; return false; }
        if (p + klen + vlen > limit_) { p_ = limit_; return false; }
        e.key = Slice(p, klen); p += klen;
        ValueType t = (ValueType)(unsigned char)(*p); ++p;
        e.type = t;
        e.value = Slice(p, vlen-1);
        p_ = p + (vlen-1);
        return true;
    }

private:
    Slice data_;
    const char* p_;
    const char* limit_;
};

} // namespace lsmkv
