#pragma once
#include <string>
#include <mutex>
#include <cstdint>
#include <atomic>
#include <vector>
#include <unordered_map>
#include "skiplist.h"
#include "../util/slice.h"

namespace lsmkv {

enum ValueType : uint8_t { kTypeValue = 1, kTypeDeletion = 2 };

struct MemValue {
    ValueType type;
    std::string value;
};

class MemTable {
public:
    MemTable() : approximate_size_(0) {}

    void Add(const Slice& key, const Slice& value, ValueType type) {
        std::lock_guard<std::mutex> lg(mu_);
        MemValue mv{type, value.ToString()};
        bool inserted = table_.InsertOrAssign(key.ToString(), mv);
        if (inserted) {
            approximate_size_ += key.size() + value.size() + sizeof(MemValue);
        } else {
            approximate_size_ += value.size();
        }
    }

    bool Get(const Slice& key, MemValue* out) const {
        std::lock_guard<std::mutex> lg(mu_);
        // linear iteration (ordered), return exact key
        auto it = table_.NewIterator();
        while (it.Valid()) {
            if (Slice(it.key()).compare(key) == 0) { *out = it.value(); return true; }
            if (Slice(it.key()).compare(key) > 0) break;
            it.Next();
        }
        return false;
    }

    size_t ApproximateMemoryUsage() const { return approximate_size_.load(); }

    struct IterKV {
        std::string key;
        MemValue value;
    };
    std::vector<IterKV> SnapshotInOrder() const {
        std::lock_guard<std::mutex> lg(mu_);
        std::vector<IterKV> res;
        auto it = table_.NewIterator();
        while (it.Valid()) {
            res.push_back(IterKV{ it.key(), it.value() });
            it.Next();
        }
        return res;
    }

private:
    mutable std::mutex mu_;
    struct KeyCmp {
        int operator()(const std::string& a, const std::string& b) const {
            if (a < b) return -1; if (a > b) return +1; return 0;
        }
    };
    SkipList<std::string, MemValue, KeyCmp> table_;
    std::atomic<size_t> approximate_size_;
};

} // namespace lsmkv
