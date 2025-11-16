#pragma once
#include <string>
#include <fstream>
#include <mutex>
#include <memory>
#include <cstdint>
#include <cerrno>
#include <system_error>

#include "../util/status.h"
#include "../util/slice.h"
#include "../util/coding.h"

#if defined(_WIN32)
#include <io.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#else
#include <unistd.h>
#include <fcntl.h>
#endif

namespace lsmkv {

class WALWriter {
public:
    WALWriter() = default;
    ~WALWriter() { Close(); }

    static Status Open(const std::string& path, std::unique_ptr<WALWriter>& out) {
        std::unique_ptr<WALWriter> w(new WALWriter());
        w->path_ = path;
        w->ofs_.open(path, std::ios::binary | std::ios::out | std::ios::app);
        if (!w->ofs_.good()) return Status::IOError("open WAL for write failed: " + path);
        out = std::move(w);
        return Status::OK();
    }

    Status AddRecord(uint8_t type, const Slice& key, const Slice& value, bool sync) {
        std::lock_guard<std::mutex> lg(mu_);
        std::string rec;
        PutVarint32(rec, static_cast<uint32_t>(type));
        PutVarint32(rec, static_cast<uint32_t>(key.size()));
        PutVarint32(rec, static_cast<uint32_t>(value.size()));
        rec.append(key.data(), key.size());
        rec.append(value.data(), value.size());
        uint32_t checksum = static_cast<uint32_t>(Hash64(rec.data(), rec.size()));
        std::string header;
        PutVarint32(header, static_cast<uint32_t>(rec.size()));
        ofs_.write(header.data(), header.size());
        ofs_.write(rec.data(), rec.size());
        PutFixed32(header, checksum);
        ofs_.write(header.data()+header.size()-4, 4);
        ofs_.flush();
        if (sync) return Fsync();
        return Status::OK();
    }

    Status Fsync() {
#if defined(_WIN32)
        ofs_.flush();
        int fd = _open(path_.c_str(), _O_RDONLY);
        if (fd < 0) return Status::IOError("open for _commit failed");
        int rc = _commit(fd);
        _close(fd);
        if (rc != 0) return Status::IOError("fsync(_commit) failed");
        return Status::OK();
#else
        ofs_.flush();
        int fd = ::open(path_.c_str(), O_RDONLY);
        if (fd < 0) return Status::IOError("open for fsync failed");
        int rc = ::fsync(fd);
        ::close(fd);
        if (rc != 0) return Status::IOError("fsync failed");
        return Status::OK();
#endif
    }

    void Close() {
        std::lock_guard<std::mutex> lg(mu_);
        if (ofs_.is_open()) ofs_.close();
    }

    const std::string& path() const { return path_; }

private:
    std::mutex mu_;
    std::string path_;
    std::ofstream ofs_;
};

class WALReader {
public:
    WALReader() = default;
    ~WALReader() { Close(); }

    static Status Open(const std::string& path, std::unique_ptr<WALReader>& out) {
        std::unique_ptr<WALReader> r(new WALReader());
        r->ifs_.open(path, std::ios::binary | std::ios::in);
        if (!r->ifs_.good()) return Status::IOError("open WAL for read failed: " + path);
        out = std::move(r);
        return Status::OK();
    }

    bool ReadRecord(uint8_t* type, std::string& key, std::string& value) {
        uint32_t len = 0;
        if (!ReadVarint32(len)) return false;
        std::string rec(len, '\0');
        if (!ifs_.read(&rec[0], len)) return false;
        char csum[4];
        if (!ifs_.read(csum, 4)) return false;
        uint32_t checksum = DecodeFixed32(csum);
        uint32_t calc = static_cast<uint32_t>(Hash64(rec.data(), rec.size()));
        if (checksum != calc) return false;
        const char* p = rec.data();
        const char* limit = p + rec.size();
        uint32_t t=0, klen=0, vlen=0;
        p = GetVarint32Ptr(p, limit, &t); if (!p) return false;
        p = GetVarint32Ptr(p, limit, &klen); if (!p) return false;
        p = GetVarint32Ptr(p, limit, &vlen); if (!p) return false;
        if ((size_t)(limit - p) < klen + vlen) return false;
        *type = (uint8_t)t;
        key.assign(p, klen); p += klen;
        value.assign(p, vlen);
        return true;
    }

    void Close() { if (ifs_.is_open()) ifs_.close(); }

private:
    bool ReadVarint32(uint32_t& out) {
        unsigned char byte; uint32_t result = 0; int shift = 0;
        while (true) {
            if (!ifs_.read((char*)&byte, 1)) return false;
            if (byte & 0x80) { result |= ((byte & 0x7F) << shift); shift += 7; if (shift > 28) return false; }
            else { result |= (byte << shift); break; }
        }
        out = result; return true;
    }

    std::ifstream ifs_;
};

} // namespace lsmkv
