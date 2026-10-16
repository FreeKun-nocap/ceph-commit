# osd_scrub.h 笔记

## class OsdScrub

OsdScrub 把 scrub 启动逻辑从 OSD 中剥离出来：OSD 周期性调用 `initiate_scrub()`，由它检查 OSD 级环境条件（资源、时间窗、负载、恢复状态）后从队列中挑选可 scrub 的 PG 并发起 scrub。

### 属性说明

#### `ScrubQueue m_queue` — 待 scrub 的 PG 调度队列

OSD 上所有 PG 的"刷洗排期表"。本 OSD 作为 primary 的每个 PG，其浅层和深层 scrub 各对应一条 `SchedEntry` 排期条目（即 `ScrubJob` 中 `SchedTarget` 调度信息的副本），记录各自最早可以开始刷洗的时间；条目存放在 `to_scrub` 中并按该时间排序。OSD 周期性调用 `pop_ready_entry()` 取出时间已到、且当前条件（负载、时段、并发数等）允许的条目来发起 scrub；配置变化时排期会被重算，PG 不再由本 OSD 负责（主副本迁移、PG 删除）时经 `remove_from_osd_queue()` 移除。并发刷洗数量有限，因此即使到了时间也可能继续排队。队列由 `jobs_lock` 保护，持有该锁时不能获取 PG 锁。
