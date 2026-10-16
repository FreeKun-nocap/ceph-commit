# scrub_queue_entry.h 笔记

## struct Scrub::SchedEntry

一条 scrub 排期条目：描述"哪个 PG 的哪一层 scrub"，即 `ScrubQueue` 排期队列中每个元素的类型（`initiate_scrub()` 里弹出的 `candidate`）。它是 `ScrubJob` 中 `SchedTarget` 调度部分的副本，从队列弹出后交给 PG 层发起 scrub。

### 属性说明

#### `spg_t pgid` — 目标 PG

要刷洗的 PG 标识。`initiate_a_scrub()` 用它给 PG 加锁，再转交 `start_scrubbing()`。

#### `scrub_level_t level` — scrub 层次

浅层（shallow）或深层（deep）。每个 PG 的浅层、深层各占一条排期条目。

#### `urgency_t urgency` — 紧急度

从 `periodic_regular` 到 `must_repair` 共六级（周期性、must_scrub、after_repair、repairing、operator_requested、must_repair）。既决定排队优先级（越紧急越先被选中），也决定该 scrub 的行为——低紧急度的周期性 scrub 会受时间窗、负载、并发上限等限制约束，管理员请求的则不受。

#### `Scrub::scrub_schedule_t schedule` — 调度时间

包含计划时间 `scheduled_at` 和最早可开始时间 `not_before`。队列按 `not_before` 排序，只有"到期"（`not_before <= now`）的条目才可能被 `pop_ready_entry()` 取出。

#### `delay_cause_t last_issue` — 上次失败/延迟原因

记录该排期最近一次失败或延迟的原因（无则为 `none`），仅用于日志与状态输出，不影响调度。
