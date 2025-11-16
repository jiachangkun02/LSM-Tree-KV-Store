#pragma once
#include "slice.h"

namespace lsmkv {

class Comparator {
public:
    virtual ~Comparator() = default;
    virtual int Compare(const Slice& a, const Slice& b) const = 0;
    virtual const char* Name() const = 0;
};

class BytewiseComparatorImpl : public Comparator {
public:
    int Compare(const Slice& a, const Slice& b) const override { return a.compare(b); }
    const char* Name() const override { return "lsmkv.BytewiseComparator"; }
};

inline const Comparator* BytewiseComparator() {
    static BytewiseComparatorImpl cmp;
    return &cmp;
}

} // namespace lsmkv
