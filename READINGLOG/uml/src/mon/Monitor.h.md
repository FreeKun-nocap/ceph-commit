# Monitor.h 笔记

## class Monitor

Monitor 是 mon 守护进程的核心类：持有 monmap、Paxos 各服务和选举器，负责维护集群状态（monmap/OSDMap/MDSMap 等）的一致性。

### 属性说明

#### `int rank` — 自身在 monmap 中的编号

本 mon 在 monmap 中的序号（0 ~ monmap->size()-1），决定消息实体名（`entity_name_t::MON(rank)`）和默认选举顺位；`-1` 表示当前不在 monmap 中（准备加入的新 mon）。monmap 变化（成员增删、重排）时 rank 可能改变。

#### `bool has_ever_joined` — 是否曾加入过 quorum

区分"老成员被移除"与"新成员未加入"：rank 被查不到时，该值为 true 表示曾是成员、现被 monmap 移除（自杀退出）；false 则清选举历史继续走加入流程。

#### `MonMap *monmap` — monmap 成员视图

当前 mon 对集群成员（各 mon 的名字/地址/rank）的认知，rank 的计算依据（`get_rank(addr)` / `get_addrs(rank)`）都来自它。
