#include "../include/lsm_kv.h"
#include <iostream>
#include <cassert>

using namespace lsmkv;

int main() {
    Options opt;
    opt.db_path = "./testdb";
    std::unique_ptr<DB> db;
    Status s = DB::Open(opt, opt.db_path, &db);
    if (!s.ok()) { std::cerr << "Open failed: " << s.ToString() << std::endl; return 1; }

    WriteOptions wopt; wopt.sync = true;
    s = db->Put(wopt, Slice("foo"), Slice("bar"));
    assert(s.ok());
    s = db->Put(wopt, Slice("hello"), Slice("world"));
    assert(s.ok());

    ReadOptions ro;
    std::string v;
    s = db->Get(ro, Slice("foo"), &v);
    if (s.ok()) std::cout << "foo=" << v << std::endl; else std::cout << s.ToString() << std::endl;

    s = db->Flush();
    if (!s.ok()) std::cerr << "Flush failed: " << s.ToString() << std::endl;

    s = db->Get(ro, Slice("hello"), &v);
    if (s.ok()) std::cout << "hello=" << v << std::endl; else std::cout << s.ToString() << std::endl;

    return 0;
}
