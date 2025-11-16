#pragma once
#include <unordered_map>
#include <string>
#include <memory>
#include <mutex>
#include "../sstable/sstable_reader.h"

namespace lsmkv {

class SSTableCache {
public:
    explicit SSTableCache(size_t max_open) : max_open_(max_open) {}

    bool Get(const std::string& path, std::shared_ptr<SSTableReader>& out) {
        std::lock_guard<std::mutex> lg(mu_);
        auto it = cache_.find(path);
        if (it != cache_.end()) { out = it->second; return true; }
        if (cache_.size() >= max_open_) cache_.erase(cache_.begin());
        std::shared_ptr<SSTableReader> r;
        Status s = SSTableReader::Open(path, &r);
        if (!s.ok()) return false;
        cache_[path] = r;
        out = r;
        return true;
    }

    void Erase(const std::string& path) {
        std::lock_guard<std::mutex> lg(mu_);
        cache_.erase(path);
    }

private:
    size_t max_open_;
    std::mutex mu_;
    std::unordered_map<std::string, std::shared_ptr<SSTableReader>> cache_;
};

} // namespace lsmkv
