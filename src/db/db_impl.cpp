#include "db_impl.h"
#include <filesystem>
#include <cassert>

namespace fs = std::filesystem;

namespace lsmkv {

DBImpl::DBImpl(const Options& opt, const std::string& dbpath)
    : options_(opt), db_path_(dbpath), versions_(opt.num_levels),
      block_cache_(opt.block_cache_capacity), table_cache_(opt.max_open_files) {
    fs::create_directories(db_path_);
}

DBImpl::~DBImpl() { shutting_down_ = true; }

Status DBImpl::OpenDB(const Options& options, const std::string& dbname, std::unique_ptr<DB>& dbptr) {
    std::unique_ptr<DBImpl> impl(new DBImpl(options, dbname));
    impl->versions_.LoadFromDir(impl->db_path_);
    impl->mem_.reset(new MemTable());

    Status s = impl->RecoverWALs();
    if (!s.ok()) return s;

    impl->wal_number_ = impl->versions_.NextFileNumber();
    std::unique_ptr<WALWriter> w;
    s = WALWriter::Open(impl->WALFilePath(impl->wal_number_), w);
    if (!s.ok()) return s;
    impl->wal_ = std::move(w);

    dbptr.reset(impl.release());
    return Status::OK();
}

Status DBImpl::RecoverWALs() {
    std::vector<std::pair<uint64_t, std::string>> wals;
    for (auto& p : fs::directory_iterator(db_path_)) {
        if (!p.is_regular_file()) continue;
        auto filename = p.path().filename().string();
        if (filename.rfind("wal-", 0) == 0 && filename.find(".log") != std::string::npos) {
            size_t dash = filename.find('-');
            size_t dot = filename.find(".log");
            uint64_t num = std::stoull(filename.substr(dash+1, dot - (dash+1)));
            wals.emplace_back(num, p.path().string());
        }
    }
    std::sort(wals.begin(), wals.end(), [](auto& a, auto& b){ return a.first < b.first; });
    for (auto& [num, path] : wals) {
        std::unique_ptr<WALReader> r;
        Status s = WALReader::Open(path, r);
        if (!s.ok()) return s;
        uint8_t type; std::string key, value;
        while (r->ReadRecord(&type, key, value)) {
            if (type == kTypeValue) mem_->Add(Slice(key), Slice(value), kTypeValue);
            else if (type == kTypeDeletion) mem_->Add(Slice(key), Slice(""), kTypeDeletion);
        }
        r->Close();
        fs::remove(path);
    }
    return Status::OK();
}

Status DBImpl::Put(const WriteOptions& options, const Slice& key, const Slice& value) {
    std::unique_lock<std::shared_mutex> lk(mu_);
    if (!wal_) return Status::IOError("WAL not open");
    Status s = wal_->AddRecord(kTypeValue, key, value, options.sync);
    if (!s.ok()) return s;
    mem_->Add(key, value, kTypeValue);
    if (mem_->ApproximateMemoryUsage() >= options_.write_buffer_size) {
        s = RotateMemTable();
        if (!s.ok()) return s;
    }
    return Status::OK();
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
    std::unique_lock<std::shared_mutex> lk(mu_);
    if (!wal_) return Status::IOError("WAL not open");
    Status s = wal_->AddRecord(kTypeDeletion, key, Slice(""), options.sync);
    if (!s.ok()) return s;
    mem_->Add(key, Slice(""), kTypeDeletion);
    if (mem_->ApproximateMemoryUsage() >= options_.write_buffer_size) {
        s = RotateMemTable();
        if (!s.ok()) return s;
    }
    return Status::OK();
}

Status DBImpl::Get(const ReadOptions& options, const Slice& key, std::string* value) {
    {
        std::shared_lock<std::shared_mutex> lk(mu_);
        MemValue mv;
        if (mem_ && mem_->Get(key, &mv)) {
            if (mv.type == kTypeDeletion) return Status::NotFound("deleted");
            *value = mv.value; return Status::OK();
        }
        if (imm_ && imm_->Get(key, &mv)) {
            if (mv.type == kTypeDeletion) return Status::NotFound("deleted");
            *value = mv.value; return Status::OK();
        }
    }
    std::vector<TableFile> candidates;
    versions_.GetCandidateFiles(key, candidates);
    for (const auto& t : candidates) {
        std::shared_ptr<SSTableReader> r;
        if (!table_cache_.Get(t.path, r)) continue;
        std::optional<MemValue> res;
        Status s = r->Get(key, res, &block_cache_, options.fill_cache);
        if (!s.ok()) return s;
        if (res.has_value()) {
            if (res->type == kTypeDeletion) return Status::NotFound("deleted");
            *value = res->value; return Status::OK();
        }
    }
    return Status::NotFound("not found");
}

Status DBImpl::RotateMemTable() {
    if (imm_) return Status::OK();
    imm_.reset(mem_.release());
    mem_.reset(new MemTable());

    std::string old_wal_path;
    if (wal_) { old_wal_path = wal_->path(); wal_->Close(); wal_.reset(); }

    wal_number_ = versions_.NextFileNumber();
    std::unique_ptr<WALWriter> w;
    Status s = WALWriter::Open(WALFilePath(wal_number_), w);
    if (!s.ok()) return s;
    wal_ = std::move(w);

    auto imm_snapshot = std::make_shared<std::vector<MemTable::IterKV>>(imm_->SnapshotInOrder());
    uint64_t file_number = versions_.NextFileNumber();
    std::string out_path = L0FilePath(file_number);
    size_t block_size = options_.block_size;
    unsigned bloom_bits = options_.bloom_bits_per_key;
    VersionSet* vset = &versions_;
    std::string wal_to_delete = old_wal_path;

    bg_.Schedule(CompactionManager::Task{
        CompactionManager::kFlush,
        [imm_snapshot, out_path, block_size, bloom_bits, file_number, vset, wal_to_delete]() -> Status {
            SSTableBuilder builder(out_path, block_size, bloom_bits);
            Status s = builder.Open(); if (!s.ok()) return s;
            SSTableMeta meta;
            for (const auto& kv : *imm_snapshot) {
                s = builder.Add(Slice(kv.key), kv.value); if (!s.ok()) return s;
            }
            s = builder.Finish(&meta); if (!s.ok()) return s;

            TableFile tf; tf.level=0; tf.number=file_number; tf.path=out_path; tf.smallest=meta.smallest_key; tf.largest=meta.largest_key; tf.size=meta.file_size;
            vset->AddFile(tf);

            if (!wal_to_delete.empty()) { std::error_code ec; fs::remove(wal_to_delete, ec); }
            return Status::OK();
        }
    });

    imm_.reset();
    MaybeScheduleCompaction();
    return Status::OK();
}

void DBImpl::MaybeScheduleCompaction() {
    auto lvl = versions_.PickCompactionLevel();
    if (!lvl.has_value()) return;
    int level = *lvl;
    VersionSet* vset = &versions_;
    size_t block_size = options_.block_size;
    unsigned bloom_bits = options_.bloom_bits_per_key;
    std::string dbp = db_path_;

    bg_.Schedule(CompactionManager::Task{
        CompactionManager::kCompact,
        [vset, level, block_size, bloom_bits, dbp]() -> Status {
            std::vector<TableFile> level_files, next_files;
            vset->PickCompactionInputs(level, level_files, next_files);
            if (level_files.empty()) return Status::OK();

            std::vector<MergeSource> sources;
            auto open_iter = [&](const TableFile& tf)->std::unique_ptr<SSTableReader::Iterator> {
                std::shared_ptr<SSTableReader> r;
                Status s = SSTableReader::Open(tf.path, &r);
                if (!s.ok()) return nullptr;
                return r->NewIterator();
            };
            for (auto& tf : level_files) {
                auto it = open_iter(tf);
                if (it) sources.push_back(MergeSource{std::move(it), tf.level, tf.number});
            }
            for (auto& tf : next_files) {
                auto it = open_iter(tf);
                if (it) sources.push_back(MergeSource{std::move(it), tf.level, tf.number});
            }
            if (sources.empty()) return Status::OK();

            KWayMerger merger(std::move(sources));
            uint64_t new_number = vset->NextFileNumber();
            std::string out_path = dbp + "/L" + std::to_string(level+1) + "-" + std::to_string(new_number) + ".sst";
            SSTableBuilder builder(out_path, block_size, bloom_bits);
            Status s = builder.Open(); if (!s.ok()) return s;

            std::string last_key;
            while (true) {
                std::string key; MemValue mv;
                if (!merger.Next(key, mv)) break;
                if (!last_key.empty() && key == last_key) continue;
                builder.Add(Slice(key), mv);
                last_key = key;
            }
            SSTableMeta meta;
            s = builder.Finish(&meta); if (!s.ok()) return s;

            for (auto& tf : level_files) vset->RemoveFile(tf.level, tf.number);
            for (auto& tf : next_files) vset->RemoveFile(tf.level, tf.number);
            TableFile out; out.level=level+1; out.number=new_number; out.path=out_path; out.smallest=meta.smallest_key; out.largest=meta.largest_key; out.size=meta.file_size;
            vset->AddFile(out);
            for (auto& tf : level_files) { std::error_code ec; fs::remove(tf.path, ec); }
            for (auto& tf : next_files) { std::error_code ec; fs::remove(tf.path, ec); }
            return Status::OK();
        }
    });
}

Status DBImpl::CompactRange(const Slice& begin, const Slice& end) {
    MaybeScheduleCompaction();
    return Status::OK();
}

Status DBImpl::Flush() {
    std::unique_lock<std::shared_mutex> lk(mu_);
    return RotateMemTable();
}

} // namespace lsmkv
