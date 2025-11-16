#pragma once
#include <cstddef>
#include <cstring>
#include <string>
#include <string_view>
#include <ostream>

namespace lsmkv {

class Slice {
public:
    Slice() : data_(nullptr), size_(0) {}
    Slice(const char* d, size_t n) : data_(d), size_(n) {}
    Slice(const std::string& s) : data_(s.data()), size_(s.size()) {}
    Slice(std::string_view sv) : data_(sv.data()), size_(sv.size()) {}

    const char* data() const { return data_; }
    size_t size() const { return size_; }
    bool empty() const { return size_ == 0; }

    char operator[](size_t n) const { return data_[n]; }

    void remove_prefix(size_t n) { data_ += n; size_ -= n; }

    std::string ToString() const { return std::string(data_, size_); }

    int compare(const Slice& b) const {
        const size_t min_len = size_ < b.size_ ? size_ : b.size_;
        int r = std::memcmp(data_, b.data_, min_len);
        if (r == 0) {
            if (size_ < b.size_) r = -1;
            else if (size_ > b.size_) r = +1;
        }
        return r;
    }

    bool starts_with(const Slice& x) const {
        return (size_ >= x.size_) && (std::memcmp(data_, x.data_, x.size_) == 0);
    }

private:
    const char* data_;
    size_t size_;
};

inline bool operator==(const Slice& a, const Slice& b) { return a.size() == b.size() && std::memcmp(a.data(), b.data(), a.size()) == 0; }
inline bool operator!=(const Slice& a, const Slice& b) { return !(a == b); }
inline bool operator<(const Slice& a, const Slice& b) { return a.compare(b) < 0; }

inline std::ostream& operator<<(std::ostream& os, const Slice& s) { return os << s.ToString(); }

} // namespace lsmkv
