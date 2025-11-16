#pragma once
#include <cstdint>
#include <string>
#include <cstring>

namespace lsmkv {

static const uint64_t kSSTableMagic = 0xdb4775248b80fb57ull;
static const uint32_t kSSTableVersion = 1;

// Footer: [index_off u64][index_sz u64][filter_off u64][filter_sz u64][version u32][pad u32][magic u64] = 56 bytes
struct Footer {
    uint64_t index_offset = 0;
    uint64_t index_size = 0;
    uint64_t filter_offset = 0;
    uint64_t filter_size = 0;
    uint32_t version = kSSTableVersion;
    uint32_t pad = 0;
    uint64_t magic = kSSTableMagic;
};

inline void EncodeFooter(std::string& dst, const Footer& f) {
    auto put64 = [&](uint64_t v){ char b[8]; std::memcpy(b,&v,8); dst.append(b,8); };
    auto put32 = [&](uint32_t v){ char b[4]; std::memcpy(b,&v,4); dst.append(b,4); };
    put64(f.index_offset); put64(f.index_size);
    put64(f.filter_offset); put64(f.filter_size);
    put32(f.version); put32(f.pad); put64(f.magic);
}
inline bool DecodeFooter(const std::string& data, Footer* f) {
    if (data.size() < 56) return false;
    const char* p = data.data();
    auto get64 = [&](uint64_t* v){ std::memcpy(v,p,8); p+=8; };
    auto get32 = [&](uint32_t* v){ std::memcpy(v,p,4); p+=4; };
    get64(&f->index_offset); get64(&f->index_size);
    get64(&f->filter_offset); get64(&f->filter_size);
    get32(&f->version); get32(&f->pad); get64(&f->magic);
    return f->magic == kSSTableMagic;
}

} // namespace lsmkv
