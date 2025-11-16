#pragma once
#include <memory>
#include <string>
#include "src/util/status.h"
#include "src/util/slice.h"
#include "src/util/options.h"

namespace lsmkv {

class DB {
public:
    virtual ~DB() = default;

    static Status Open(const Options& options, const std::string& dbname, std::unique_ptr<DB>* dbptr);

    virtual Status Put(const WriteOptions& options, const Slice& key, const Slice& value) = 0;
    virtual Status Delete(const WriteOptions& options, const Slice& key) = 0;
    virtual Status Get(const ReadOptions& options, const Slice& key, std::string* value) = 0;
    virtual Status CompactRange(const Slice& begin, const Slice& end) = 0;
    virtual Status Flush() = 0;
};

} // namespace lsmkv
