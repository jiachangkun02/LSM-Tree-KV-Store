#pragma once
#include <vector>
#include <queue>
#include <memory>
#include <string>
#include "../sstable/sstable_reader.h"
#include "../memtable/memtable.h"

namespace lsmkv {

struct MergeSource {
    std::unique_ptr<SSTableReader::Iterator> it;
    int level;
    uint64_t file_number;
};

class KWayMerger {
public:
    explicit KWayMerger(std::vector<MergeSource>&& srcs) : sources_(std::move(srcs)) {
        for (size_t i=0;i<sources_.size();++i) {
            if (sources_[i].it && sources_[i].it->Valid()) heap_.push(Node{i, sources_[i].it->key().ToString()});
        }
    }
    bool Next(std::string& key_out, MemValue& mv_out) {
        if (heap_.empty()) return false;
        Node top = heap_.top(); heap_.pop();
        std::string cur = top.key;
        size_t idx = top.source_index;
        key_out = cur;
        mv_out = sources_[idx].it->value();
        // advance this source
        sources_[idx].it->Next();
        if (sources_[idx].it->Valid()) heap_.push(Node{idx, sources_[idx].it->key().ToString()});
        // skip other sources with same key
        while (!heap_.empty() && heap_.top().key == cur) {
            size_t j = heap_.top().source_index; heap_.pop();
            sources_[j].it->Next();
            if (sources_[j].it->Valid()) heap_.push(Node{j, sources_[j].it->key().ToString()});
        }
        return true;
    }

private:
    struct Node {
        size_t source_index;
        std::string key;
        bool operator<(const Node& o) const { return key > o.key; }
    };
    std::vector<MergeSource> sources_;
    std::priority_queue<Node> heap_;
};

} // namespace lsmkv
