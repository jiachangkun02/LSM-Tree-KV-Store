# LSM-Tree KV Store

`LSM-Tree KV Store` 是一个用 C++ 实现的、简化的、持久化的键值 (KV) 存储引擎。

本项目完整地实现了一个基于 **Log-Structured Merge-Tree (LSM-Tree)** 架构的存储引擎。其核心设计思想是优化**写性能**（通过将写入转换为对内存和磁盘的顺序追加），并通过后台的**合并（Compaction）**来控制读放大，保证读性能。

这是一个功能完备的实现，包含了从内存写入、日志持久化、磁盘刷盘到后台合并的完整流程，是学习和理解工业级存储引擎（如 LevelDB, RocksDB）内部原理的绝佳范例。

---

## 核心特性

- **持久化保证**: 通过 **Write-Ahead Log (WAL)** 实现。任何写入操作在写入内存之前都会先追加到 WAL 并刷盘，确保在数据库崩溃重启后能完整恢复数据。
- **高速写入**: 写入操作仅涉及一次 WAL 顺序追加和一次对内存数据结构 **SkipList (跳表)** 的插入。
- **SSTable (Sorted String Table)**: 磁盘上的数据以不可变的、有序的 SSTable 文件格式存储。
- **专业的文件格式**: SSTable 采用经典的多部分设计，包括：
  - **Data Blocks**（数据块）
  - **Index Block**（索引块，二级索引）
  - **Bloom Filter**（布隆过滤器，用于快速判断 Key 是否*不*存在）
  - **Footer**（文件尾，包含元数据指针和 Magic Number）
- **异步刷盘 (Flush)**: 当 MemTable 写满后，会切换为不可变的 ImmutableMemTable，并由后台线程将其内容刷盘（Flush）为一个新的 Level-0 SSTable 文件。
- **后台合并 (Compaction)**: 由一个专用的后台线程池（`CompactionManager`）负责执行。使用 **K-Way Merge (K路归并)** 算法 将不同层级的 SSTable 合并，以：
  - 清理已删除或被覆盖的数据。
  - 减少文件数量，控制“读放大”。
- **多层缓存**:
  - **Block Cache**: 一个基于 LRU 策略的块缓存，用于缓存从 SSTable 读取的 Data Block，大幅提升读性能。
  - **SSTable Cache**: 用于缓存打开的 SSTable 文件句柄及元数据（索引、布隆过滤器），避免重复打开文件和解析元数据。
- **严格的读取路径**: `Get()` 操作会按照从新到旧的顺序查找数据：
  1. `MemTable`（最新数据）
  2. `ImmutableMemTable`（正在刷盘的数据）
  3. `Block Cache`（缓存的数据块）
  4. `SSTables`（从 Level-0 到 Level-N）

---

## 项目结构

```
lsm-kv-store/
│
├── include/                 # 公共头文件，给用户使用
│   └── lsm_kv.h             # 数据库主 API (DB::Open, Put, Get, Delete)
│
├── src/                     # 所有实现代码
│   │
│   ├── db/                  # 数据库核心实现
│   │   ├── db_impl.h        # 数据库实现类
│   │   ├── db_impl.cpp
│   │   ├── wal.h            # Write-Ahead Log
│   │   └── version.h        # 管理SSTable文件列表和层级
│   │
│   ├── memtable/            # 内存表
│   │   ├── memtable.h       # MemTable 接口
│   │   └── skiplist.h       # 跳表实现
│   │
│   ├── sstable/             # SSTable 文件
│   │   ├── sstable_builder.h  # SSTable 写入器
│   │   ├── sstable_builder.cpp
│   │   ├── sstable_reader.h # SSTable 读取器
│   │   ├── sstable_reader.cpp
│   │   ├── block.h          # 数据块 (Data Block)
│   │   ├── index_block.h    # 索引块 (Index Block)
│   │   └── format.h         # SSTable 文件格式定义
│   │
│   ├── compaction/          # 后台合并
│   │   ├── compaction.h     # 合并任务调度
│   │   └── merger.h         # K路合并迭代器
│   │
│   ├── table_cache/         # 缓存层
│   │   ├── block_cache.h    # 块缓存
│   │   └── sstable_cache.h  # SSTable 文件句柄缓存
│   │
│   ├── util/                # 公共工具
│   │   ├── slice.h          # 零拷贝字节视图
│   │   ├── comparator.h     # key 比较器
│   │   ├── bloom_filter.h   # 布隆过滤器
│   │   ├── status.h         # 状态/错误返回
│   │   └── options.h        # 数据库配置选项
│   │
│   └── main.cpp             # 用于测试的入口
│
├── test/                    # 单元测试
│   ├── test_skiplist.cpp
│   └── test_sstable.cpp
│
├── CMakeLists.txt           # CMake 编译文件
└── README.md                # 项目文档
```

---

## 构建与运行

项目使用 CMake 管理构建。需要 C++17 及以上标准的编译器（如 g++ 或 clang++）。

### 1. 构建项目

```bash
# 1. 创建构建目录
mkdir build
cd build

# 2. 运行 CMake（默认使用 C++17 标准）
cmake ..

# 3. 编译所有目标（包括主程序和测试）
cmake --build . -j $(nproc)
```

### 2. 运行主程序

编译后，`build/` 目录下会生成 `lsmkv_main` 可执行文件。

```bash
./lsmkv_main
```

这将运行 `src/main.cpp` 中的示例代码，在当前目录下创建一个 `testdb` 目录来存放数据库文件。

### 3. 运行测试

编译后，`build/` 目录下会生成单元测试程序：

```bash
# 运行 SkipList 单元测试
./test_skiplist

# 运行 SSTable 单元测试
./test_sstable
```

您也可以使用 `ctest` 来自动运行所有已定义的测试：

```bash
cd build
ctest
```

---

## API 使用示例

以下示例展示了如何打开数据库、写入数据、读取数据和手动刷盘。（示例代码改编自 `src/main.cpp`）

```cpp
#include "include/lsm_kv.h"
#include <iostream>
#include <cassert>

using namespace lsmkv;

int main() {
    // 1. 设置数据库选项
    Options opt;
    opt.db_path = "./testdb"; // 数据库文件将存储在此
    opt.create_if_missing = true;
    opt.write_buffer_size = 4 * 1024 * 1024; // 4MB MemTable

    std::unique_ptr<DB> db;

    // 2. 打开数据库
    Status s = DB::Open(opt, opt.db_path, &db);
    if (!s.ok()) {
        std::cerr << "Open failed: " << s.ToString() << std::endl;
        return 1;
    }
    std::cout << "Database opened successfully." << std::endl;

    // 3. 写入数据
    WriteOptions wopt;
    wopt.sync = true; // 保证高持久性

    s = db->Put(wopt, Slice("foo"), Slice("bar"));
    assert(s.ok());
    s = db->Put(wopt, Slice("hello"), Slice("world"));
    assert(s.ok());
    s = db->Put(wopt, Slice("key1"), Slice("value1"));
    assert(s.ok());

    // 4. 读取数据
    ReadOptions ro;
    std::string v;

    s = db->Get(ro, Slice("foo"), &v);
    if (s.ok()) {
        std::cout << "Get(foo) = " << v << std::endl;
    } else {
        std::cout << s.ToString() << std::endl;
    }

    s = db->Get(ro, Slice("hello"), &v);
    if (s.ok()) {
        std::cout << "Get(hello) = " << v << std::endl;
    } else {
        std::cout << s.ToString() << std::endl;
    }

    // 5. 手动刷盘 (Flush)
    s = db->Flush();
    if (!s.ok()) {
        std::cerr << "Flush failed: " << s.ToString() << std::endl;
    } else {
        std::cout << "Flush completed." << std::endl;
    }

    // 6. 再次读取 (数据现在位于 SSTable 中)
    s = db->Get(ro, Slice("key1"), &v);
    if (s.ok()) {
        std::cout << "Get(key1) after flush = " << v << std::endl;
    } else {
        std::cout << s.ToString() << std::endl;
    }
    
    // 7. 测试删除
    s = db->Delete(wopt, Slice("foo"));
    assert(s.ok());
    s = db->Get(ro, Slice("foo"), &v);
    assert(s.IsNotFound()); // 应该找不到了
    std::cout << "Get(foo) after delete = " << s.ToString() << std::endl;

    // 数据库将在 std::unique_ptr<DB> 析构时自动关闭
    return 0;
}
```
