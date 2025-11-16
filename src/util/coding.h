#pragma once
#include <cstdint>
#include <cstring>
#include <string>

namespace lsmkv {

inline void PutFixed32(std::string& dst, uint32_t v) { char b[4]; std::memcpy(b,&v,4); dst.append(b,4); }
inline void PutFixed64(std::string& dst, uint64_t v) { char b[8]; std::memcpy(b,&v,8); dst.append(b,8); }
inline uint32_t DecodeFixed32(const char* p) { uint32_t r; std::memcpy(&r,p,4); return r; }
inline uint64_t DecodeFixed64(const char* p) { uint64_t r; std::memcpy(&r,p,8); return r; }

inline void PutVarint32(std::string& dst, uint32_t v) { unsigned char buf[5]; int len=0; while (v>=128){buf[len++]=v|128; v>>=7;} buf[len++]=v; dst.append((char*)buf,len); }
inline void PutVarint64(std::string& dst, uint64_t v) { unsigned char buf[10]; int len=0; while (v>=128){buf[len++]=v|128; v>>=7;} buf[len++]=v; dst.append((char*)buf,len); }

inline const char* GetVarint32Ptr(const char* p, const char* limit, uint32_t* value) {
    uint32_t result = 0;
    for (uint32_t shift=0; shift<=28 && p<limit; shift+=7) {
        uint32_t byte = *(const unsigned char*)p; ++p;
        if (byte & 128) result |= ((byte & 127) << shift);
        else { result |= (byte << shift); *value = result; return p; }
    }
    return nullptr;
}
inline const char* GetVarint64Ptr(const char* p, const char* limit, uint64_t* value) {
    uint64_t result = 0;
    for (uint32_t shift=0; shift<=63 && p<limit; shift+=7) {
        uint32_t byte = *(const unsigned char*)p; ++p;
        if (byte & 128) result |= (uint64_t(byte & 127) << shift);
        else { result |= (uint64_t(byte) << shift); *value = result; return p; }
    }
    return nullptr;
}

inline uint64_t Hash64(const char* data, size_t n, uint64_t seed=1469598103934665603ull) {
    uint64_t h = seed;
    for (size_t i=0;i<n;++i) { h ^= (unsigned char)data[i]; h *= 1099511628211ull; }
    return h;
}

} // namespace lsmkv
