# OSDMonitor.h 笔记

## class OSDMonitor

管理 OSDMap 的 PaxosService：接收 OSD 的状态报告（boot/beacon/failure）转成地图增量提案，经 Paxos 提交后重建并发布 OSDMap。

### 属性说明

#### `OSDMap::Incremental pending_inc` — 待提交的地图增量草稿

OSDMap::Incremental（增量）记录"相对上一版地图的变化"：成员增删、up/down 状态位、pool/CRUSH 变更等。所有 prepare_* 处理（如 prepare_failure 标 down、prepare_boot 加入新 OSD）都把变更写进它；攒够后经 propose_pending() 编码成 Paxos 值提交，提交通过即成为新一版 OSDMap 的内容。这就是"提案产生"的载体——Paxos 里存的一条条已提交值就是它的序列化。

#### `OSDMap osdmap` — 当前已生效的地图

内存里的最新 OSDMap（已提交、已应用增量后的版本），供读取校验用；与 pending_inc 的关系是"正式版 vs 草稿"。

#### `std::map<int, failure_info_t> failure_info` — 故障报告登记表

目标 OSD -> 各报告者的失联记录。prepare_failure 收到 MOSDFailure 时登记/撤销报告，check_failure 按 CRUSH 子树去重计票，够票才写入 pending_inc 标 down——防单点误判。

相关概念（check_failure 局部变量 reporter_loc）：`osdmap.crush->get_full_location(osd)` 返回的 CRUSH 位置表（`map<string,string>`，层级类型 -> 所在桶名，如 `{"root": "default", "rack": "rack2", "host": "osd-node7"}`）；按 mon_osd_reporter_subtree_level 在表中查桶名，同一桶内多份报告只算一票。

#### `std::map<int,utime_t> down_pending_out` — down 转 out 计时表

已被标 down 的 OSD -> down 的时刻。update_from_paxos 中维护，超 mon_osd_down_out_interval 后由 process_failures() 把它转为 out（踢出数据分布）。

#### `std::unique_ptr<ParallelPGMapper::Job> mapping_job` — 后台 PG 映射计算任务

计算 pg -> osd 映射的后台并行任务（发布地图统计/校验用），换新地图时未完成的旧任务会中止重算。
