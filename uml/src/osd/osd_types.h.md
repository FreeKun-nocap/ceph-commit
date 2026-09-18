# osd_types.h 笔记

## struct pg_log_t

PG 的增量日志结构


### 属性说明

#### `eversion_t head` — 最新条目版本

指向该 PG 日志中最新一条日志条目(update 或 delete)的版本，也就是这个 PG 已经写入到的最新进度。

#### `eversion_t tail` — 最旧保留条目的前一个版本



## struct pg_log_entry_t

PG 日志里的一条操作记录。

### 属性说明

#### `eversion_t version` — 写完后的版本

本次操作之后对象所在的版本，也就是这条日志对应的那次写操作产生的新 `eversion_t`(epoch + 序号)。它是日志条目的主键，恢复时靠它判断谁新谁旧、谁缺这条操作。

#### `eversion_t prior_version` — 写之前的版本

本次操作之前对象的上一个版本，即这条写所基于的旧版本。用来验证日志链的连续性：如果副本上该对象的当前版本不等于 `prior_version`，说明日志出现了分叉/缺口，需要靠 backfill 而不是 log recovery 来修复。对 MODIFY 类操作有效；对 CLONE/DELETE 等可能是空的。

#### `eversion_t reverting_to` — 丢失回滚的目标版本

只在 `LOST_REVERT` 操作中有意义。当 PG 发生分裂(stale merge)且判定某次写"丢失"需要本地回滚时，记录要回滚到哪个版本。`ObjectModDesc mod_desc` 里存的撤销操作(如回退某个字段)就是执行到 `reverting_to` 这个版本为止。普通读写场景下它一直是空的。
