#pragma once
#include <vector>
#include <string>
#include <mutex>
#include <algorithm>
#include <filesystem>
#include <optional>
#include "../util/slice.h"
#include "../util/status.h"
#include "../sstable/sstable_builder.h"
#include "../sstable/sstable_reader.h"

namespace lsmkv {

struct TableFile {
    int level;
    uint64_t number;
    std::string path;
    std::string smallest;
    std::string largest;
    uint64_t size;
};

class VersionSet {
public:
    explicit VersionSet(int num_levels) : levels_(num_levels) {}

    void AddFile(const TableFile& f) {
        std::lock_guard<std::mutex> lg(mu_);
        levels_[f.level].push_back(f);
        if (f.level == 0) {
            std::sort(levels_[0].begin(), levels_[0].end(), [](const TableFile& a, const TableFile& b){ return a.number > b.number; });
        } else {
            std::sort(levels_[f.level].begin(), levels_[f.level].end(), [](const TableFile& a, const TableFile& b){ return a.smallest < b.smallest; });
        }
        max_number_ = std::max(max_number_, f.number);
    }

    void RemoveFile(int level, uint64_t number) {
        std::lock_guard<std::mutex> lg(mu_);
        auto& v = levels_[level];
        v.erase(std::remove_if(v.begin(), v.end(), [&](const TableFile& t){ return t.number == number; }), v.end());
    }

    uint64_t NextFileNumber() {
        std::lock_guard<std::mutex> lg(mu_);
        return ++max_number_;
    }

    std::optional<int> PickCompactionLevel() {
        std::lock_guard<std::mutex> lg(mu_);
        if (levels_[0].size() > 4) return 0;
        return std::nullopt;
    }

    std::vector<TableFile> FilesInLevel(int l) const {
        std::lock_guard<std::mutex> lg(mu_);
        return levels_[l];
    }

    void PickCompactionInputs(int level, std::vector<TableFile>& level_files, std::vector<TableFile>& next_level_files) {
        std::lock_guard<std::mutex> lg(mu_);
        level_files = levels_[level];
        next_level_files.clear();
        if (level+1 >= (int)levels_.size()) return;
        std::string smallest, largest;
        if (!level_files.empty()) {
            smallest = level_files.front().smallest;
            largest = level_files.back().largest;
            for (auto& f : levels_[level+1]) {
                if (!(f.largest < smallest || f.smallest > largest)) next_level_files.push_back(f);
            }
        }
    }

    void GetCandidateFiles(const Slice& key, std::vector<TableFile>& out_ordered) const {
        std::lock_guard<std::mutex> lg(mu_);
        out_ordered.clear();
        for (auto& f : levels_[0]) {
            if (key.compare(Slice(f.smallest)) >= 0 && key.compare(Slice(f.largest)) <= 0) out_ordered.push_back(f);
        }
        for (int l=1; l<(int)levels_.size(); ++l) {
            const auto& v = levels_[l];
            int lo=0, hi=(int)v.size()-1, idx=-1;
            while (lo<=hi) {
                int mid=(lo+hi)/2;
                if (Slice(v[mid].smallest).compare(key) <=0 && Slice(v[mid].largest).compare(key)>=0) { idx=mid; break; }
                if (Slice(v[mid].smallest).compare(key) > 0) hi=mid-1; else lo=mid+1;
            }
            if (idx>=0) out_ordered.push_back(v[idx]);
        }
    }

    void LoadFromDir(const std::string& dir) {
        std::lock_guard<std::mutex> lg(mu_);
        namespace fs = std::filesystem;
        if (!fs::exists(dir)) return;
        for (auto& p : fs::directory_iterator(dir)) {
            if (!p.is_regular_file()) continue;
            auto filename = p.path().filename().string();
            if (filename.size() < 6) continue;
            if (filename[0] != 'L') continue;
            size_t dash = filename.find('-');
            size_t dot = filename.find(".sst");
            if (dash == std::string::npos || dot == std::string::npos) continue;
            int level = std::stoi(filename.substr(1, dash-1));
            uint64_t number = std::stoull(filename.substr(dash+1, dot - (dash+1)));
            std::shared_ptr<SSTableReader> r;
            Status s = SSTableReader::Open(p.path().string(), &r);
            if (!s.ok()) continue;
            const auto& idx = r->index();
            if (idx.entries().empty()) continue;
            std::string smallest = idx.entries().front().key;
            std::string largest = idx.entries().back().key;
            uint64_t sz = std::filesystem::file_size(p.path());
            levels_[level].push_back(TableFile{level, number, p.path().string(), smallest, largest, sz});
            max_number_ = std::max(max_number_, number);
        }
        if (!levels_[0].empty()) {
            std::sort(levels_[0].begin(), levels_[0].end(), [](const TableFile& a, const TableFile& b){ return a.number > b.number; });
        }
        for (int l=1; l<(int)levels_.size(); ++l) {
            std::sort(levels_[l].begin(), levels_[l].end(), [](const TableFile& a, const TableFile& b){ return a.smallest < b.smallest; });
        }
    }

private:
    mutable std::mutex mu_;
    std::vector<std::vector<TableFile>> levels_;
    uint64_t max_number_ = 0;
};

} // namespace lsmkv
