#include "src/sstable/sstable_builder.h"
#include "src/sstable/sstable_reader.h"
#include <iostream>
#include <optional>

int main() {
    using namespace lsmkv;
    SSTableBuilder b("/mnt/data/tmp.sst", 4*1024, 10);
    auto s = b.Open();
    if (!s.ok()) { std::cerr << s.ToString() << std::endl; return 1; }
    MemValue v1{ kTypeValue, "v1" };
    b.Add(Slice("a"), v1);
    b.Add(Slice("b"), v1);
    SSTableMeta m;
    s = b.Finish(&m);
    if (!s.ok()) { std::cerr << s.ToString() << std::endl; return 1; }
    std::shared_ptr<SSTableReader> r;
    s = SSTableReader::Open("/mnt/data/tmp.sst", &r);
    if (!s.ok()) { std::cerr << s.ToString() << std::endl; return 1; }
    std::optional<MemValue> res;
    s = r->Get(Slice("a"), res, nullptr, false);
    if (!res.has_value()) { std::cerr << "not found" << std::endl; return 1; }
    std::cout << "ok " << res->value << std::endl;
    return 0;
}
