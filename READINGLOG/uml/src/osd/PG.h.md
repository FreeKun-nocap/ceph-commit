# PG.h 笔记

## class PG

PG 负责一个 PG 的通用生命周期和状态机粘合；具体客户端 IO、PG Log、recovery 执行逻辑主要在子类 PrimaryLogPG 中。

### 属性说明

#### `object_stat_collection_t unstable_stats` — 多次 delta_stats 的延迟落盘累计

PG 用于批量缓存的统计增量，主要来自纯读或 no-op 操作产生的 `delta_stats`。它不会立刻写入 PGInfo，而是先在内存中累计；发布 PG stats 时临时加到 `info.stats` 上，让监控看到的值尽量接近实时。下一次真正准备写 PGInfo 时，`PG::prepare_write()` 才把它合并进 `info.stats.stats` 并清空。
