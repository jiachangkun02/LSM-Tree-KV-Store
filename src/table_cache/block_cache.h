#pragma once
#include <unordered_map>
#include <list>
#include <string>
#include <mutex>

namespace lsmkv {

class BlockCache {
public:
    explicit BlockCache(size_t capacity_bytes) : capacity_(capacity_bytes), usage_(0) {}

    bool Get(const std::string& key, std::string* value_out) {
        std::lock_guard<std::mutex> lg(mu_);
        auto it = map_.find(key);
        if (it == map_.end()) return false;
        lru_.splice(lru_.begin(), lru_, it->second);
        *value_out = it->second->value;
        return true;
    }

    void Put(const std::string& key, const std::string& value) {
        std::lock_guard<std::mutex> lg(mu_);
        auto it = map_.find(key);
        if (it != map_.end()) {
            usage_ -= it->second->value.size();
            it->second->value = value;
            usage_ += value.size();
            lru_.splice(lru_.begin(), lru_, it->second);
            Evict();
            return;
        }
        lru_.push_front(Node{key, value});
        map_[key] = lru_.begin();
        usage_ += value.size();
        Evict();
    }

private:
    struct Node { std::string key; std::string value; };
    std::mutex mu_;
    size_t capacity_, usage_;
    std::list<Node> lru_;
    std::unordered_map<std::string, std::list<Node>::iterator> map_;
    void Evict() {
        while (usage_ > capacity_ && !lru_.empty()) {
            auto last = std::prev(lru_.end());
            usage_ -= last->value.size();
            map_.erase(last->key);
            lru_.pop_back();
        }
    }
};

} // namespace lsmkv
