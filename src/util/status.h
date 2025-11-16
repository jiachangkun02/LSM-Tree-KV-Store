#pragma once
#include <string>

namespace lsmkv {

class Status {
public:
    Status() : code_(kOk) {}
    static Status OK() { return Status(); }
    static Status NotFound(const std::string& m) { return Status(kNotFound, m); }
    static Status IOError(const std::string& m) { return Status(kIOError, m); }
    static Status Corruption(const std::string& m) { return Status(kCorruption, m); }
    static Status InvalidArgument(const std::string& m) { return Status(kInvalidArgument, m); }

    bool ok() const { return code_ == kOk; }
    bool IsNotFound() const { return code_ == kNotFound; }

    std::string ToString() const {
        switch (code_) {
            case kOk: return "OK";
            case kNotFound: return "NotFound: " + msg_;
            case kIOError: return "IOError: " + msg_;
            case kCorruption: return "Corruption: " + msg_;
            case kInvalidArgument: return "InvalidArgument: " + msg_;
        }
        return "Unknown";
    }

private:
    enum Code { kOk=0, kNotFound, kIOError, kCorruption, kInvalidArgument };
    Code code_;
    std::string msg_;
    Status(Code c, std::string m) : code_(c), msg_(std::move(m)) {}
};

} // namespace lsmkv
