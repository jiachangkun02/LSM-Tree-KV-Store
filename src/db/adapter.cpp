#include "../../include/lsm_kv.h"
#include "db_impl.h"

namespace lsmkv {
Status DB::Open(const Options& options, const std::string& dbname, std::unique_ptr<DB>* dbptr) {
    std::unique_ptr<DB> db;
    Status s = DBImpl::OpenDB(options, dbname, db);
    if (!s.ok()) return s;
    dbptr->reset(db.release());
    return Status::OK();
}
} // namespace lsmkv
