# PaxosService.h 笔记

## class PaxosService

Mon 各状态服务（MonmapMonitor/OSDMonitor 等）的基类：维护各自的 pending 草稿状态，负责把变更编码成提案交给 Paxos 共识，并在提案通过后应用生效。每个服务各有一个 Paxos 实例承载其值序列。

### 属性说明

#### `Context *proposal_timer` — 延迟提案定时器

攒批闹钟：业务变更累积后不立即提案，起一个约 `paxos_propose_interval` 的定时器，到点再提交，让多个变更合并进同一轮 Paxos 共识。`propose_pending()` 提案前会取消它，防止"提前提交后闹钟再触发一次"造成重复提案。

#### `bool have_pending` — 是否有 pending 草稿

leader 激活时 `create_pending()` 生成草稿并置 true；`encode_pending()` 把草稿写入提案事务后置 false。`propose_pending()` 要求它为 true。

#### `bool proposing` — 提案进行中标志

`propose_pending()` 提交时置 true，提案走完 Paxos 后由 `C_Committed` 回调清 false。期间 `_active()` 等入口直接返回，防止重复提交。

#### `bool need_immediate_propose` — 需要立即提案标记

业务侧（如命令处理）设置，表示攒的变更不能等攒批定时器、下次激活时应立即 `propose_pending()`；每次提案时重置。

#### `std::vector<Context*> waiting_for_commit` — 等提案提交的回调队列

等待本服务提案值被 Paxos 提交（commit）后唤醒的回调；选举重启时以 -EAGAIN 结束。

#### `std::vector<Context*> waiting_for_finished_proposal` — 等提案结束的回调队列

等待提案流程结束（不管成败）后唤醒的回调；`_active()` 中以 0 唤醒。
