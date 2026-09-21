# PrimaryLogPG.h 笔记

## struct OpContext

一个对象操作的执行上下文，保存请求、对象状态草稿、待提交事务和本次操作产生的统计增量。

### 属性说明

#### `object_stat_sum_t delta_stats` — 单次操作统计增量

表示当前请求相对原对象状态产生的变化。例如读操作会增加 `num_rd` 和 `num_rd_kb`，创建对象会增加 `num_objects` 和 `num_bytes`，删除、快照、cache evict/promote 则会增加或减少对应分类的统计。写事务路径会把它交给 backend，用于更新 PGInfo 和 backfill target 的统计投影；纯读或 no-op 路径则先累加到 PG 的 `unstable_stats`。
