#include "src/memtable/skiplist.h"
#include <iostream>
#include <string>

struct Cmp { int operator()(const std::string& a,const std::string& b) const { if (a<b) return -1; if (a>b) return 1; return 0; } };

int main() {
    lsmkv::SkipList<std::string, int, Cmp> sl;
    sl.InsertOrAssign("a", 1);
    sl.InsertOrAssign("b", 2);
    sl.InsertOrAssign("a", 3);
    auto it = sl.NewIterator();
    while (it.Valid()) {
        std::cout << it.key() << ":" << it.value() << "\n";
        it.Next();
    }
    return 0;
}
