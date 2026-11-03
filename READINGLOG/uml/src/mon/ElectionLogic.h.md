# ElectionLogic.h 笔记

## class ElectionLogic

Mon 选举的纯逻辑层（与 Elector 的消息收发分离）：决定自己是否参选/竞选，维护选票统计并判定当选，选举策略（rank 优先或连通性优先）也在这里执行。

### 属性说明

#### `std::set<int> acked_me` — 我的票箱

本选举周期里已经投票给我的 mon 集合（元素是 rank）。发起选举时清空并自投一票，之后每收到一个让贤的 OP_ACK 就加入对方 rank；票数达到 `paxos_size()`（法定人数）即当选，向所有 mon 发 OP_VICTORY 宣胜。

#### `bool electing_me` — 我是否为候选

发起选举时置 true（自己竞选）；收到别人的 PROPOSE 时，仅当认为自己有胜算（如对方 rank 更低）才会转为竞选自己，否则让贤。

#### `bool participating` — 是否参与选举

默认 true。经 `quorum exit` 置 false 后本 mon 既不竞选也不投票（进程存活、仍在 monmap，属优雅退场）；`quorum enter` 置回 true 并重新发起选举。

#### `epoch_t epoch` — 选举 epoch

选举周期编号，奇数表示"选举中"、偶数表示"稳定"；发起选举时若为偶数则 +1 进入选举周期。

#### `int leader_acked` — 我已 ack 的对象

记录本轮我向谁让贤（投出的那一票给哪个 rank），与 `acked_me` 互为镜像。

#### `election_strategy strategy` — 选举策略

`CLASSIC`（按 rank 定优先级）、`DISALLOW`（禁止部分成员当 leader）、`CONNECTIVITY`（在 DISALLOW 基础上按连通性得分优选，PROPOSE 携带连通性追踪表供对方评估）。
