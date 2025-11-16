#pragma once
#include <string>
#include <vector>
#include <cstdint>
#include "../util/coding.h"
#include "../util/slice.h"

namespace lsmkv {

// Index block: [klen varint][key][offset u64][size u64] ...
class IndexBlockBuilder {
public:
    void Add(const Slice& key, uint64_t offset, uint64_t size) {
        PutVarint32(buf_, (uint32_t)key.size());
        buf_.append(key.data(), key.size());
        PutFixed64(buf_, offset);
        PutFixed64(buf_, size);
        entries_.push_back({key.ToString(), offset, size});
    }
    std::string Finish() { std::string out; out.swap(buf_); return out; }

    struct Entry { std::string key; uint64_t off; uint64_t sz; };
    std::vector<Entry> entries_;

private:
    std::string buf_;
};

class IndexBlockReader {
public:
    explicit IndexBlockReader(const Slice& contents) {
        const char* p = contents.data();
        const char* limit = contents.data()+contents.size();
        while (p < limit) {
            uint32_t klen=0;
            const char* np = GetVarint32Ptr(p, limit, &klen);
            if (!np || np + klen + 16 > limit) break;
            std::string k(np, klen);
            np += klen;
            uint64_t off = DecodeFixed64(np); np += 8;
            uint64_t sz = DecodeFixed64(np); np += 8;
            entries_.push_back({std::move(k), off, sz});
            p = np;
        }
    }

    // binary search
    int FindBlock(const Slice& key) const {
        int lo=0, hi=(int)entries_.size()-1, ans=-1;
        while (lo<=hi) {
            int mid=(lo+hi)/2;
            int c = Slice(entries_[mid].key).compare(key);
            if (c<=0) { ans=mid; lo=mid+1; } else { hi=mid-1; }
        }
        return ans;
    }

    const std::vector<Entry>& entries() const { return entries_; }

private:
    std::vector<Entry> entries_;
};

} // namespace lsmkv
