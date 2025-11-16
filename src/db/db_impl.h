#pragma once
#include <string>
#include <memory>
#include <shared_mutex>
#include <atomic>
#include <filesystem>
#include "../../include/lsm_kv.h"
#include "../util/options.h"
#include "../util/slice.h"
#include "../memtable/memtable.h"
#include "wal.h"
#include "version.h"
#include "../table_cache/block_cache.h"
#include "../table_cache/sstable_cache.h"
#include "../compaction/compaction.h"
#include "../compaction/merger.h"
#include "../sstable/sstable_builder.h"
#include "../sstable/sstable_reader.h"

namespace lsmkv {

class DBImpl final : public DB {
public:
    DBImpl(const Options& opt, const std::string& dbpath);
    ~DBImpl() override;

    static Status OpenDB(const Options& options, const std::string& dbname, std::unique_ptr<DB>& dbptr);

    Status Put(const WriteOptions& options, const Slice& key, const Slice& value) override;
    Status Delete(const WriteOptions& options, const Slice& key) override;
    Status Get(const ReadOptions& options, const Slice& key, std::string* value) override;
    Status CompactRange(const Slice& begin, const Slice& end) override;
    Status Flush() override;

private:
    Status RecoverWALs();
    Status RotateMemTable();
    void MaybeScheduleCompaction();

    std::string L0FilePath(uint64_t number) const { return db_path_ + "/L0-" + std::to_string(number) + ".sst"; }
    std::string WALFilePath(uint64_t number) const { return db_path_ + "/wal-" + std::to_string(number) + ".log"; }

    Options options_;
    std::string db_path_;

    mutable std::shared_mutex mu_;
    std::unique_ptr<MemTable> mem_;
    std::unique_ptr<MemTable> imm_;

    std::unique_ptr<WALWriter> wal_;
    uint64_t wal_number_ = 0;

    VersionSet versions_;
    BlockCache block_cache_;
    SSTableCache table_cache_;

    CompactionManager bg_;

    std::atomic<bool> shutting_down_{false};
};

} // namespace lsmkv
