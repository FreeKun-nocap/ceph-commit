# PGLog.h 笔记

## class PGLog

PGLog 管理 PG 的内存日志、索引、missing 状态，以及这些状态待持久化到 PGMeta omap 的增量变更。

### 属性说明

#### `pg_missing_tracker_t missing` — 对象 missing 状态集合

记录 PG 中尚未恢复到目标版本的对象及其缺失版本、删除状态。它用于判断对象需要 log recovery 还是 backfill，并参与 `last_complete` 与日志裁剪的安全检查；持久化时按对象写入或删除 `missing/<object>` key。

#### `IndexedLog log` — 带索引的 PG 日志

PGLog 的核心内存日志对象。`IndexedLog` 继承 `pg_log_t`：`log.log` 保存实际日志条目，`log.dups` 保存请求重放记录，`head/tail` 标记日志边界；额外维护对象、请求 ID 和 dup 索引以及 recovery 迭代位置。`PGLog::trim()` 最终通过 `log.trim()` 裁剪它。

#### `std::set<eversion_t> trimmed` — 普通日志待删 key

记录已经从内存日志裁掉、但仍需要从 PGMeta omap 删除的普通日志条目。集合中保存的是条目版本，生成持久化事务时转换成对应的日志 key，再加入 `omap_rmkeys` 删除。

#### `std::set<std::string> trimmed_dups` — dup 待删 key

记录已经从内存 `dups` 删除、但仍需要从 PGMeta omap 删除的 duplicate request key。它直接保存待删除的 key，用于保持盘上的请求重放记录与内存状态一致。

#### `eversion_t write_from_dups` — dup 待重写起点

记录新增或发生变化的 dup 记录从哪个版本开始需要重新写入 PGMeta omap。持久化时会重写所有版本不小于该起点的 dup key；初始值为 `eversion_t::max()`，表示当前没有额外的 dup 区间需要重写。

## struct PGLog::IndexedLog

在 `pg_log_t` 的日志链表和水位之上增加对象、请求 ID、dup 索引，并维护 recovery 所需的日志遍历位置。

### 属性说明

#### `std::list<pg_log_entry_t>::iterator complete_to` — 日志完整边界

指向第一个尚未确认完整的日志条目，但不包含该条目本身；它之前的日志已经完整。`complete_to == log.end()` 表示保留日志均已完整。恢复 missing 对象时它会向前推进并同步 `info.last_complete`，日志裁剪越过该位置时则重置到剩余日志开头。
“日志条目未完整”不是指这条日志写坏或没有提交，而是指 PG 状态还不能确认完整推进到这个版本。
