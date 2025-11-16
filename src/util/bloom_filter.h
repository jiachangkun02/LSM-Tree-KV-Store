#pragma once
#include <vector>
#include <string>
#include <cstdint>
#include "slice.h"
#include "coding.h"

namespace lsmkv {

class BloomFilterBuilder {
public:
    explicit BloomFilterBuilder(unsigned bits_per_key = 10) : bits_per_key_(bits_per_key) {}

    void AddKey(const Slice& key) { keys_.emplace_back(key.ToString()); }

    std::string Finalize() {
        size_t n = keys_.size();
        size_t bits = n * bits_per_key_;
        if (bits < 64) bits = 64;
        size_ = (bits + 7) / 8;
        data_.assign(size_, 0);
        k_ = static_cast<unsigned>(bits_per_key_ * 0.69);
        if (k_ < 1) k_ = 1;
        if (k_ > 30) k_ = 30;

        for (const auto& key : keys_) {
            uint64_t h = Hash64(key.data(), key.size());
            uint32_t delta = (h >> 17) | (h << 15);
            for (unsigned j=0;j<k_;++j) {
                uint32_t bitpos = (h % (size_ * 8));
                data_[bitpos/8] |= (1 << (bitpos % 8));
                h += delta;
            }
        }
        std::string out;
        out.append((const char*)data_.data(), data_.size());
        out.push_back(static_cast<char>(k_));
        return out;
    }

private:
    unsigned bits_per_key_;
    std::vector<std::string> keys_;
    std::vector<unsigned char> data_;
    unsigned size_ = 0;
    unsigned k_ = 0;
};

class BloomFilterReader {
public:
    BloomFilterReader() : data_(nullptr), len_(0), k_(0) {}
    explicit BloomFilterReader(const Slice& contents) { Reset(contents); }

    void Reset(const Slice& contents) {
        if (contents.size() < 2) { data_ = nullptr; len_ = 0; k_ = 0; return; }
        len_ = contents.size() - 1;
        k_ = (unsigned char)contents.data()[contents.size()-1];
        data_ = (const unsigned char*)contents.data();
    }

    bool KeyMayMatch(const Slice& key) const {
        if (len_ == 0 || k_ == 0) return true;
        uint64_t h = Hash64(key.data(), key.size());
        uint32_t delta = (h >> 17) | (h << 15);
        for (unsigned j=0;j<k_;++j) {
            uint32_t bitpos = (h % (len_ * 8));
            if ((data_[bitpos/8] & (1 << (bitpos % 8))) == 0) return false;
            h += delta;
        }
        return true;
    }

private:
    const unsigned char* data_;
    size_t len_;
    unsigned k_;
};

} // namespace lsmkv
