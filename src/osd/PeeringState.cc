// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "PeeringState.h"
#include "PGPeeringEvent.h"
#include "osd_perf_counters.h"
#include "common/ceph_releases.h"
#include "common/debug.h"
#include "common/ostream_temp.h"
#include "crush/crush.h" // for CRUSH_ITEM_NONE
#include "crush/CrushWrapper.h"

#include "messages/MOSDPGRemove.h"
#include "messages/MBackfillReserve.h"
#include "messages/MRecoveryReserve.h"
#include "messages/MOSDScrubReserve.h"
#include "messages/MOSDPGInfo2.h"
#include "messages/MOSDPGTrim.h"
#include "messages/MOSDPGLog.h"
#include "messages/MOSDPGNotify2.h"
#include "messages/MOSDPGQuery2.h"
#include "messages/MOSDPGLease.h"
#include "messages/MOSDPGLeaseAck.h"

#define dout_context cct
#define dout_subsys ceph_subsys_osd

using std::dec;
using std::hex;
using std::make_pair;
using std::map;
using std::ostream;
using std::ostringstream;
using std::pair;
using std::set;
using std::string;
using std::stringstream;
using std::vector;

using ceph::Formatter;
using ceph::make_message;

static int get_max_prio_for_base(int base) {
  static const std::map<int, int> max_prio_map = {
    {OSD_BACKFILL_PRIORITY_BASE, OSD_BACKFILL_DEGRADED_PRIORITY_BASE - 1},
    {OSD_BACKFILL_DEGRADED_PRIORITY_BASE, OSD_RECOVERY_PRIORITY_BASE - 1},
    {OSD_RECOVERY_PRIORITY_BASE, OSD_BACKFILL_INACTIVE_PRIORITY_BASE - 1},
    {OSD_RECOVERY_INACTIVE_PRIORITY_BASE, OSD_RECOVERY_PRIORITY_MAX},
    {OSD_BACKFILL_INACTIVE_PRIORITY_BASE, OSD_RECOVERY_PRIORITY_MAX}
  };
  auto it = max_prio_map.find(base);
  ceph_assert(it != max_prio_map.end());
  return it->second;
}

BufferedRecoveryMessages::BufferedRecoveryMessages(PeeringCtx &ctx)
  // steal messages from ctx
  : message_map{std::move(ctx.message_map)}
{}

void BufferedRecoveryMessages::send_notify(int to, const pg_notify_t &n)
{
  spg_t pgid(n.info.pgid.pgid, n.to);
  send_osd_message(to, TOPNSPC::make_message<MOSDPGNotify2>(pgid, n));
}

void BufferedRecoveryMessages::send_query(
  int to,
  spg_t to_spgid,
  const pg_query_t &q)
{
  send_osd_message(to, TOPNSPC::make_message<MOSDPGQuery2>(to_spgid, q));
}

void BufferedRecoveryMessages::send_info(
  int to,
  spg_t to_spgid,
  epoch_t min_epoch,
  epoch_t cur_epoch,
  const pg_info_t &info,
  std::optional<pg_lease_t> lease,
  std::optional<pg_lease_ack_t> lease_ack)
{
  send_osd_message(
    to,
    TOPNSPC::make_message<MOSDPGInfo2>(
      to_spgid,
      info,
      cur_epoch,
      min_epoch,
      lease,
      lease_ack)
  );
}

void PGPool::update(OSDMapRef map)
{
  const pg_pool_t *pi = map->get_pg_pool(id);
  if (!pi) {
    return; // pool has been deleted
  }
  info = *pi;
  name = map->get_pool_name(id);

  bool updated = false;
  if ((map->get_epoch() != cached_epoch + 1) ||
      (pi->get_snap_epoch() == map->get_epoch())) {
    updated = true;
  }

  if (info.is_pool_snaps_mode() && updated) {
    snapc = pi->get_snap_context();
  }
  cached_epoch = map->get_epoch();
}

/*-------------Peering State Helpers----------------*/
#undef dout_prefix
#define dout_prefix (dpp->gen_prefix(*_dout)) \
        << "PeeringState::" << __func__ << " "
#undef psdout
#define psdout(x) ldout(cct, x)

PeeringState::PeeringState(
  CephContext *cct,
  pg_shard_t pg_whoami,
  spg_t spgid,
  const PGPool &_pool,
  OSDMapRef curmap,
  pg_feature_vec_t supported_pg_acting_features,
  DoutPrefixProvider *dpp,
  PeeringListener *pl)
  : state_history(*pl),
    cct(cct),
    spgid(spgid),
    dpp(dpp),
    pl(pl),
    orig_ctx(0),
    osdmap_ref(curmap),
    pool(_pool),
    pg_whoami(pg_whoami),
    info(spgid),
    pg_log(cct),
    local_pg_acting_features(supported_pg_acting_features),
    pg_acting_features(local_pg_acting_features),
    last_require_osd_release(curmap->require_osd_release),
    missing_loc(spgid, this, dpp, cct),
    machine(this, cct, spgid, dpp, pl, &state_history)
{
  machine.initiate();
}

void PeeringState::start_handle(PeeringCtx *new_ctx) {
  // 开始处理一个状态机事件，建立本轮事件使用的上下文。
  // rctx 和 orig_ctx 必须为空，表示上一轮事件已经完成并清理。
  ceph_assert(!rctx);
  ceph_assert(!orig_ctx);
  orig_ctx = new_ctx;
  if (new_ctx) {
    // 如果当前存在被暂缓发送的出站消息，让新的 RecoveryCtx 继续使用该消息缓冲区；
    // 否则直接基于调用者提供的上下文创建本轮 RecoveryCtx。
    if (messages_pending_flush) {
      rctx.emplace(*messages_pending_flush, *new_ctx);
    } else {
      rctx.emplace(*new_ctx);
    }
    // 记录本轮状态机处理的起始时间，用于 peering/recovery 延迟统计。
    rctx->start_time = ceph_clock_now();
  }
}

void PeeringState::begin_block_outgoing() {
  ceph_assert(!messages_pending_flush);
  ceph_assert(orig_ctx);
  ceph_assert(rctx);
  messages_pending_flush.emplace();
  rctx.emplace(*messages_pending_flush, *orig_ctx);
}

void PeeringState::clear_blocked_outgoing() {
  ceph_assert(orig_ctx);
  ceph_assert(rctx);
  messages_pending_flush = std::optional<BufferedRecoveryMessages>();
}

void PeeringState::end_block_outgoing() {
  ceph_assert(messages_pending_flush);
  ceph_assert(orig_ctx);
  ceph_assert(rctx);

  orig_ctx->accept_buffered_messages(*messages_pending_flush);
  rctx.emplace(*orig_ctx);
  messages_pending_flush = std::optional<BufferedRecoveryMessages>();
}

void PeeringState::end_handle() {
  if (rctx) {
    // 只有调用者提供了 PeeringCtx，start_handle() 才会创建 rctx；
    // 这里累计本轮状态机事件的处理耗时。
    utime_t dur = ceph_clock_now() - rctx->start_time;
    machine.event_time += dur;
  }

  // 无论事件是否携带上下文，每次 process_event() 完成后都增加处理计数。
  machine.event_count++;
  // 与 start_handle() 配对，清除本轮临时上下文，使下一事件可以重新建立上下文。
  rctx = std::nullopt;
  orig_ctx = NULL;
}

void PeeringState::check_recovery_sources(const OSDMapRef& osdmap)
{
  /*
   * check that any peers we are planning to (or currently) pulling
   * objects from are dealt with.
   */
  missing_loc.check_recovery_sources(osdmap);
  pl->check_recovery_sources(osdmap);

  for (auto i = peer_log_requested.begin(); i != peer_log_requested.end();) {
    if (!osdmap->is_up(i->osd)) {
      psdout(10) << "peer_log_requested removing " << *i << dendl;
      peer_log_requested.erase(i++);
    } else {
      ++i;
    }
  }

  for (auto i = peer_missing_requested.begin();
       i != peer_missing_requested.end();) {
    if (!osdmap->is_up(i->osd)) {
      psdout(10) << "peer_missing_requested removing " << *i << dendl;
      peer_missing_requested.erase(i++);
    } else {
      ++i;
    }
  }
}

void PeeringState::update_history(const pg_history_t& new_history)
{
  auto mnow = pl->get_mnow();

  // history 中持久化的是旧 interval 读 lease 的剩余时长；
  // 先按当前单调时间刷新它，过期则清零，未过期则以当前时刻为基准重新记录剩余时间。
  info.history.refresh_prior_readable_until_ub(mnow, prior_readable_until_ub);

  // 合并副本上报的 history，只在本地 history 确实被推进时才标记为需要持久化。
  if (info.history.merge(new_history)) {
    psdout(20) << "advanced history from " << new_history << dendl;
    dirty_info = true;

    // 若已知 PG 在当前 interval 开始之后达到 clean，则此前的副本映射历史不再影响
    // 权威日志选择或缺失对象定位；可以裁剪 PastIntervals，并持久化这一较大元数据变更。
    if (info.history.last_epoch_clean >= info.history.same_interval_since) {
      psdout(20) << "clearing past_intervals" << dendl;
      past_intervals.clear();
      dirty_big_info = true;
    }

    // history 合并可能带来更晚的旧 lease 剩余时长；将其恢复为运行时使用的绝对上界，
    // 后续 activation 可据此等待旧 interval 的读许可完全失效。
    prior_readable_until_ub = info.history.get_prior_readable_until_ub(mnow);
    if (prior_readable_until_ub != ceph::signedspan::zero()) {
      dout(20) << "prior_readable_until_ub " << prior_readable_until_ub
	       << " (mnow " << mnow << " + "
	       << info.history.prior_readable_until_ub << ")" << dendl;
    }
  }
}

hobject_t PeeringState::earliest_backfill() const
{
  hobject_t e = hobject_t::get_max();
  for (const pg_shard_t& bt : get_backfill_targets()) {
    const pg_info_t &pi = get_peer_info(bt);
    e = std::min(pi.last_backfill, e);
  }
  return e;
}

void PeeringState::purge_strays()
{
  if (is_premerge()) {
    psdout(10) << "purge_strays " << stray_set << " but premerge, doing nothing"
	       << dendl;
    return;
  }
  if (cct->_conf.get_val<bool>("osd_debug_no_purge_strays")) {
    return;
  }
  psdout(10) << "purge_strays " << stray_set << dendl;

  bool removed = false;
  for (auto p = stray_set.begin(); p != stray_set.end(); ++p) {
    ceph_assert(!is_acting_recovery_backfill(*p));
    if (get_osdmap()->is_up(p->osd)) {
      psdout(10) << "sending PGRemove to osd." << *p << dendl;
      vector<spg_t> to_remove;
      to_remove.push_back(spg_t(info.pgid.pgid, p->shard));
      auto m = TOPNSPC::make_message<MOSDPGRemove>(
	get_osdmap_epoch(),
	to_remove);
      pl->send_cluster_message(p->osd, std::move(m), get_osdmap_epoch());
    } else {
      psdout(10) << "not sending PGRemove to down osd." << *p << dendl;
    }
    peer_missing.erase(*p);
    peer_info.erase(*p);
    missing_loc.remove_stray_recovery_sources(*p);
    peer_purged.insert(*p);
    removed = true;
  }

  // if we removed anyone, update peers (which include peer_info)
  if (removed)
    update_heartbeat_peers();

  stray_set.clear();

  // clear _requested maps; we may have to peer() again if we discover
  // (more) stray content
  peer_log_requested.clear();
  peer_missing_requested.clear();
}

void PeeringState::query_unfound(Formatter *f, string state)
{
  psdout(20) << "Enter PeeringState common QueryUnfound" << dendl;
  {
    f->dump_string("state", state);
    f->dump_bool("available_might_have_unfound", true);
    f->open_array_section("might_have_unfound");
    for (auto p = might_have_unfound.begin();
	 p != might_have_unfound.end();
	 ++p) {
      if (peer_missing.count(*p)) {
	; // Ignore already probed OSDs
      } else {
        f->open_object_section("osd");
        f->dump_stream("osd") << *p;
	if (peer_missing_requested.count(*p)) {
	  f->dump_string("status", "querying");
        } else if (!get_osdmap()->is_up(p->osd)) {
	  f->dump_string("status", "osd is down");
        } else {
	  f->dump_string("status", "not queried");
        }
        f->close_section();
      }
    }
    f->close_section();
  }
  psdout(20) << "Exit PeeringState common QueryUnfound" << dendl;
  return;
}

void PeeringState::apply_pwlc(const std::pair<eversion_t, eversion_t> pwlc,
			      const pg_shard_t &shard,
			      pg_info_t &info,
			      pg_log_t *log1,
			      PGLog *log2)
{
  // Check if last_complete and last_update can be advanced based on
  // knowledge of partial_writes
  const auto & [fromversion, toversion] = pwlc;
  if (toversion > info.last_update) {
    if (fromversion <= info.last_update) {
      if (info.last_complete == info.last_update) {
	psdout(10) << "osd." << shard << " has last_complete"
		   << "=last_update " << info.last_update
		   << " pwlc can advance both to " << toversion
		   << dendl;
	info.last_complete = toversion;
      } else {
	psdout(10) << "osd." << shard << " has last_complete "
		   << info.last_complete << " and last_update "
		   << info.last_update
		   << " pwlc can advance last_update to " << toversion
		   << dendl;
      }
      info.last_update = toversion;
      if (log1 && toversion > log1->head) {
	log1->head = toversion;
      }
      if (log2 && toversion > log2->get_head()) {
	log2->set_head(toversion);
      }
    } else {
      psdout(10) << "osd." << shard << " has last_complete "
		 << info.last_complete << " and last_update "
		 << info.last_update
		 << " cannot apply pwlc from " << fromversion
		 << " to " << toversion
		 << dendl;
    }
  }
}

void PeeringState::update_peer_info(const pg_shard_t &from,
				    const pg_info_t &oinfo)
{
  // Merge pwlc information from another shard into
  // info.partial_writes_last_complete keeping the newest
  // updates. Ignore pwlc from nonprimary shards.
  if (!oinfo.partial_writes_last_complete.empty()&&
      !pool.info.is_nonprimary_shard(from.shard)) {
    bool updated = false;
    // oinfo includes partial_writes_last_complete data.
    // Merge this with our copy keeping the most up to date versions
    for (const auto & [shard, versionrange] :
	   oinfo.partial_writes_last_complete) {
      auto & [ofromversion, otoversion] = versionrange;
      if (info.partial_writes_last_complete.contains(shard)) {
	auto & [fromversion, toversion] =
	  info.partial_writes_last_complete[shard];
	// Prefer pwlc with a newer epoch, then pwlc with a newer
	// toversion, then pwlc with an older fromversion.
	bool newer_epoch = (oinfo.partial_writes_last_complete_epoch >
			    info.partial_writes_last_complete_epoch);
	bool same_epoch = (oinfo.partial_writes_last_complete_epoch ==
			    info.partial_writes_last_complete_epoch);
	if (newer_epoch ||
	    (same_epoch && (otoversion > toversion)) ||
	    (same_epoch && (otoversion == toversion) && (ofromversion < fromversion))) {
	  if (!updated) {
	    updated = true;
	    psdout(10) << "osd." << from
		       << " has pwlc=" << oinfo.partial_writes_last_complete
		       << dendl;
	  }
          psdout(10) << "osd." << from << " updating shard " << shard << dendl;
	  info.partial_writes_last_complete[shard] = versionrange;
	}
      } else {
	if (!updated) {
	  updated = true;
	  psdout(10) << "osd." << from
		     << " has pwlc=" << oinfo.partial_writes_last_complete
		     << dendl;
	}
        psdout(10) << "osd." << from << " setting shard " << shard << dendl;
	info.partial_writes_last_complete[shard] = versionrange;
      }
    }
    if (updated) {
      // Update last updated epoch
      info.partial_writes_last_complete_epoch = std::max(
	    info.partial_writes_last_complete_epoch,
	    oinfo.partial_writes_last_complete_epoch);

      psdout(10) << "pwlc=e" << info.partial_writes_last_complete_epoch
		 << ":" << info.partial_writes_last_complete << dendl;
    }
  }
  // Primary shards might need to apply pwlc to non-primary peer_info's
  if (is_primary()) {
    for (auto & [shard, peer] : peer_info) {
      if (info.partial_writes_last_complete.contains(shard.shard)) {
	apply_pwlc(info.partial_writes_last_complete[shard.shard], shard, peer);
      }
    }
  }
  // Non-primary shards might need to apply pwlc to update info
  if (info.partial_writes_last_complete.contains(pg_whoami.shard)) {
    apply_pwlc(info.partial_writes_last_complete[pg_whoami.shard], pg_whoami,
	       info, &pg_log);
  }
}

bool PeeringState::proc_replica_notify(const pg_shard_t &from, const pg_notify_t &notify)
{
  // notify 中的 pg_info 是发送副本对自身 PGLog、缺失对象和 history 的本地摘要；
  // epoch_sent 用于确认该消息没有跨越发送者的 down/up 实例边界。
  const pg_info_t &oinfo = notify.info;
  const epoch_t send_epoch = notify.epoch_sent;

  // 同一 peer 的 last_update 未前进，说明其可用于 peering 的日志进度没有变化；
  // 不重复更新 peer_info，并返回 false 表示没有收到新的输入。
  auto p = peer_info.find(from);
  if (p != peer_info.end() && p->second.last_update == oinfo.last_update) {
    psdout(10) << " got dup osd." << from << " info "
	       << oinfo << ", identical to ours" << dendl;
    return false;
  }

  // 若该 OSD 自消息发送 epoch 起曾 down 过，则该 notify 可能来自旧 OSD 实例，
  // 不能用它更新当前 peering 的副本视图。
  if (!get_osdmap()->has_been_up_since(from.osd, send_epoch)) {
    psdout(10) << " got info " << oinfo << " from down osd." << from
	     << " discarding" << dendl;
    return false;
  }

  psdout(10) << " got osd." << from << " " << oinfo << dendl;
  ceph_assert(is_primary());

  // 只有 primary 汇总其他副本的信息：保存该副本的摘要，并更新由 pg_info 派生的 peer 状态。
  peer_info[from] = oinfo;
  update_peer_info(from, oinfo);  // 主要处理 EC 的 partial_writes_last_complete，属于部分写入完成边界的修正，暂时跳过

  // 该 OSD 报告过自身信息，保守地认为它可能持有尚未定位的对象；
  // 后续 peering/recovery 会据此决定是否还需要向它查询。
  might_have_unfound.insert(from);

  // 将副本 history 合并到本地 PG history，推进如 last_epoch_clean 等集群一致性边界。
  update_history(oinfo.history);

  // 不在当前 up/acting 集合却仍持有此 PG 数据的是 stray；若 PG 已 clean，
  // 可以立刻发起清理，否则先保留其信息，避免过早丢弃可能有用的副本内容。
  if (!is_up(from) && !is_acting(from)) {
    psdout(10) << " osd." << from << " has stray content: " << oinfo << dendl;
    stray_set.insert(from);
    if (is_clean()) {
      purge_strays();
    }
  }

  // 只有当前 acting 成员会参与本轮服务协议；
  // 取其 feature 交集，使 primary 后续发送的 PG 消息只使用所有 acting 副本都支持的能力。
  if (is_acting(from)) {
    pg_acting_features &= notify.pg_features;
  }

  // 第一次得知这个 peer 时，心跳集合可能需要纳入它。
  if (p == peer_info.end())
    update_heartbeat_peers();

  // 返回 true 表示本次 notify 未被当作重复或旧实例消息丢弃。
  return true;
}


void PeeringState::remove_down_peer_info(const OSDMapRef &osdmap)
{
  // Remove any downed osds from peer_info
  bool removed = false;
  auto p = peer_info.begin();
  while (p != peer_info.end()) {
    if (!osdmap->is_up(p->first.osd)) {
      psdout(10) << " dropping down osd." << p->first << " info " << p->second << dendl;
      peer_missing.erase(p->first);
      peer_log_requested.erase(p->first);
      peer_missing_requested.erase(p->first);
      peer_info.erase(p++);
      removed = true;
    } else
      ++p;
  }

  // Remove any downed osds from peer_purged so we can re-purge if necessary
  auto it = peer_purged.begin();
  while (it != peer_purged.end()) {
    if (!osdmap->is_up(it->osd)) {
      psdout(10) << " dropping down osd." << *it << " from peer_purged" << dendl;
      peer_purged.erase(it++);
    } else {
      ++it;
    }
  }

  // if we removed anyone, update peers (which include peer_info)
  if (removed)
    update_heartbeat_peers();

  check_recovery_sources(osdmap);
}

void PeeringState::update_heartbeat_peers()
{
  if (!is_primary())
    return;

  set<int> new_peers;
  for (unsigned i=0; i<acting.size(); i++) {
    if (acting[i] != CRUSH_ITEM_NONE)
      new_peers.insert(acting[i]);
  }
  for (unsigned i=0; i<up.size(); i++) {
    if (up[i] != CRUSH_ITEM_NONE)
      new_peers.insert(up[i]);
  }
  for (auto p = peer_info.begin(); p != peer_info.end(); ++p) {
    new_peers.insert(p->first.osd);
  }
  pl->update_heartbeat_peers(std::move(new_peers));
}

/**
 * 把 PGInfo、PastIntervals、PGLog 和 missing 的 dirty 状态编码进当前事务。
 * 这里只负责生成待持久化内容；事务真正提交时这些修改才随对象数据一起落盘。
 */
void PeeringState::write_if_dirty(ObjectStore::Transaction& t)
{
  pl->prepare_write(
    info,
    last_written_info,
    past_intervals,
    pg_log,
    dirty_info,
    dirty_big_info,
    last_persisted_osdmap < get_osdmap_epoch(),
    t);
  if (dirty_info || dirty_big_info) {
    // prepare_write 事务编码成功后，同步刷新“已写入快照”和脏标记；
    // 需要强制重写 info 时，也把当前 OSDMap epoch 记为已持久化。
    last_persisted_osdmap = get_osdmap_epoch();
    last_written_info = info;
    dirty_info = false;
    dirty_big_info = false;
  }
}

void PeeringState::advance_map(
  OSDMapRef osdmap, OSDMapRef lastmap,
  vector<int>& newup, int up_primary,
  vector<int>& newacting, int acting_primary,
  PeeringCtx &rctx)
{
  // 调用者按 epoch 顺序推进 PG；lastmap 必须是本状态机当前持有的地图，
  // 避免跳过或乱序比较 OSDMap。
  ceph_assert(lastmap == osdmap_ref);
  psdout(10) << "handle_advance_map "
	    << newup << "/" << newacting
	    << " -- " << up_primary << "/" << acting_primary
	    << dendl;

  // 先切换状态机使用的 OSDMap 和 pool 配置，
  // 使 AdvMap 的状态处理逻辑读取到的是新地图；lastmap 仍作为事件参数保留旧值供比较。
  update_osdmap_ref(osdmap);
  pool.update(osdmap);

  // 将新旧地图及新 up/acting 映射包装成 AdvMap 事件，
  // 交由当前状态（如 Reset、Primary、Active）决定是否重置、重新 peering 或保持状态。
  AdvMap evt(
    osdmap, lastmap, newup, up_primary,
    newacting, acting_primary);
  handle_event(evt, &rctx);
  if (pool.info.last_change == osdmap_ref->get_epoch()) {
    // pool 配置恰好在本 epoch 改变，通知 PG 外层更新依赖 pool 配置的行为。
    pl->on_pool_change();
  }
  // 这两个值不由状态机事件直接维护：根据最新 pool/OSDMap 刷新可读间隔，
  // 并记录当前地图要求的最低 OSD release，供后续功能兼容性判断使用。
  readable_interval = pool.get_readable_interval(cct->_conf);
  last_require_osd_release = osdmap->require_osd_release;
}

void PeeringState::activate_map(PeeringCtx &rctx)
{
  psdout(10) << dendl;
  ActMap evt;
  handle_event(evt, &rctx);
  if (osdmap_ref->get_epoch() - last_persisted_osdmap >
    cct->_conf->osd_pg_epoch_persisted_max_stale) {
    psdout(20) << ": Dirtying info: last_persisted is "
	      << last_persisted_osdmap
	      << " while current is " << osdmap_ref->get_epoch() << dendl;
    dirty_info = true;
  } else {
    psdout(20) << ": Not dirtying info: last_persisted is "
	      << last_persisted_osdmap
	      << " while current is " << osdmap_ref->get_epoch() << dendl;
  }
  write_if_dirty(rctx.transaction);

  if (get_osdmap()->check_new_blocklist_entries()) {
    pl->check_blocklisted_watchers();
  }
}

void PeeringState::set_last_peering_reset()
{
  psdout(20) << "set_last_peering_reset " << get_osdmap_epoch() << dendl;
  if (last_peering_reset != get_osdmap_epoch()) {
    last_peering_reset = get_osdmap_epoch();
    psdout(10) << "Clearing blocked outgoing recovery messages" << dendl;
    clear_blocked_outgoing();
    if (!pl->try_flush_or_schedule_async()) {
      psdout(10) << "Beginning to block outgoing recovery messages" << dendl;
      begin_block_outgoing();
    } else {
      psdout(10) << "Not blocking outgoing recovery messages" << dendl;
    }
  }
}

void PeeringState::complete_flush()
{
  flushes_in_progress--;
  if (flushes_in_progress == 0) {
    pl->on_flushed();
  }
}

void PeeringState::check_full_transition(OSDMapRef lastmap, OSDMapRef osdmap)
{
  const pg_pool_t *pi = osdmap->get_pg_pool(info.pgid.pool());
  if (!pi) {
    return; // pool deleted
  }
  bool changed = false;
  if (pi->has_flag(pg_pool_t::FLAG_FULL)) {
    const pg_pool_t *opi = lastmap->get_pg_pool(info.pgid.pool());
    if (!opi || !opi->has_flag(pg_pool_t::FLAG_FULL)) {
      psdout(10) << " pool was marked full in " << osdmap->get_epoch() << dendl;
      changed = true;
    }
  }
  if (changed) {
    info.history.last_epoch_marked_full = osdmap->get_epoch();
    dirty_info = true;
  }
}

bool PeeringState::should_restart_peering(
  int newupprimary,
  int newactingprimary,
  const vector<int>& newup,
  const vector<int>& newacting,
  OSDMapRef lastmap,
  OSDMapRef osdmap)
{
  if (PastIntervals::is_new_interval(
	primary.osd,
	newactingprimary,
	acting,
	newacting,
	up_primary.osd,
	newupprimary,
	up,
	newup,
	osdmap.get(),
	lastmap.get(),
	info.pgid.pgid)) {
    psdout(20) << "new interval newup " << newup
	       << " newacting " << newacting << dendl;
    return true;
  }
  if (!lastmap->is_up(pg_whoami.osd) && osdmap->is_up(pg_whoami.osd)) {
    psdout(10) << "osd transitioned from down -> up"
	       << dendl;
    return true;
  }
  return false;
}

/* Called before initializing peering during advance_map */
void PeeringState::start_peering_interval(
  const OSDMapRef lastmap,
  const vector<int>& newup, int new_up_primary,
  const vector<int>& newacting, int new_acting_primary,
  ObjectStore::Transaction &t)
{
  // 新 interval 的统一初始化入口：安装新 up/acting 映射，
  // 失效旧角色、 peer 状态和 lease，并把需要持久化的 PG 状态写入调用者事务。
  const OSDMapRef osdmap = get_osdmap();

  // 记录本次 reset，并按 collection flush 是否完成决定是否暂缓发送 recovery 消息。
  set_last_peering_reset();

  // 保存旧映射和旧角色，后续用于计算 PastIntervals、日志和角色变化处理。
  vector<int> oldacting, oldup;
  int oldrole = get_role();

  if (is_primary()) {
    // 原 primary 上的 merge 准备只对旧 interval 有效，进入新 interval 前清除。
    pl->clear_ready_to_merge();
  }


  pg_shard_t old_acting_primary = get_primary();
  pg_shard_t old_up_primary = up_primary;
  bool was_old_primary = is_primary();
  bool was_old_nonprimary = is_nonprimary();

  // 将现有 up/acting 移出状态机，再安装新地图计算出的映射和 primary。
  acting.swap(oldacting);
  up.swap(oldup);
  init_primary_up_acting(
    newup,
    newacting,
    new_up_primary,
    new_acting_primary);

  if (info.stats.up != up ||
      info.stats.acting != acting ||
      info.stats.up_primary != new_up_primary ||
      info.stats.acting_primary != new_acting_primary) {
    // PG 统计中保存当前映射快照，供状态上报和诊断使用。
    info.stats.up = up;
    info.stats.up_primary = new_up_primary;
    info.stats.acting = acting;
    info.stats.acting_primary = new_acting_primary;
    info.stats.mapping_epoch = osdmap->get_epoch();
  }

  pl->clear_publish_stats();

  // This will now be remapped during a backfill in cases
  // that it would not have been before.
  // up 与 acting 不同时，CRUSH 期望位置与当前实际副本集不同，标记 REMAPPED；
  if (up != acting)
    state_set(PG_STATE_REMAPPED);
  else
    state_clear(PG_STATE_REMAPPED);

  // 用新 acting 集合重算当前 OSD 是 primary、replica 还是 stray。
  int role = osdmap->calc_pg_role(pg_whoami, acting);
  set_role(role);

  // 根据旧/新映射判断是否形成新 interval，并维护历史边界与 PastIntervals。
  // did acting, up, primary|acker change?
  if (!lastmap) {
    // 新建 PG 没有可比较的旧地图；当前 epoch 即为第一个 interval 的起点。
    psdout(10) << " no lastmap" << dendl;
    dirty_info = true;
    dirty_big_info = true;
    info.history.same_interval_since = osdmap->get_epoch();
  } else {
    std::stringstream debug;
    ceph_assert(info.history.same_interval_since != 0);
    bool new_interval = PastIntervals::check_new_interval(
      old_acting_primary.osd,
      new_acting_primary,
      oldacting, newacting,
      old_up_primary.osd,
      new_up_primary,
      oldup, newup,
      info.history.same_interval_since,
      info.history.last_epoch_clean,
      osdmap.get(),
      lastmap.get(),
      info.pgid.pgid,
      missing_loc.get_recoverable_predicate(),
      &past_intervals,
      &debug);
    psdout(10) << ": check_new_interval output: "
	       << debug.str() << dendl;
    if (new_interval) {
      // PastIntervals 已记录前一个 interval 中可能持有更新的 OSD；
      // 它是后续 peering 选择权威日志、查询缺失对象时的历史依据。
      if (osdmap->get_epoch() == pl->cluster_osdmap_trim_lower_bound() &&
	  info.history.last_epoch_clean < osdmap->get_epoch()) {
	psdout(10) << " map gap, clearing past_intervals and faking" << dendl;
	// OSDMap 已被裁剪且本 PG 不够新，无法可靠重建历史 interval，只能丢弃。
	// our information is incomplete and useless; someone else was clean
	// after everything we know if osdmaps were trimmed.
	past_intervals.clear();
      } else {
	psdout(10) << " noting past " << past_intervals << dendl;
      }
      dirty_info = true;
      dirty_big_info = true;
      info.history.same_interval_since = osdmap->get_epoch();
      // pg_num 增大导致的 PG split 也作为历史边界记录。
      if (osdmap->have_pg_pool(info.pgid.pgid.pool()) &&
	  info.pgid.pgid.is_split(lastmap->get_pg_num(info.pgid.pgid.pool()),
				  osdmap->get_pg_num(info.pgid.pgid.pool()),
				  nullptr)) {
	info.history.last_epoch_split = osdmap->get_epoch();
      }
    }
  }

  if (old_up_primary != up_primary ||
      oldup != up) {
    // up 映射或其 primary 改变，记录新的 same_up_since 起点。
    info.history.same_up_since = osdmap->get_epoch();
  }
  // this comparison includes primary rank via pg_shard_t
  if (old_acting_primary != get_primary()) {
    // acting primary 改变，记录新的 same_primary_since 起点。
    info.history.same_primary_since = osdmap->get_epoch();
  }

  // 重置 feature 交集、missing-delete 语义、心跳时间戳和读 lease 边界。
  on_new_interval();

  psdout(1) << "up " << oldup << " -> " << up
	    << ", acting " << oldacting << " -> " << acting
	    << ", acting_primary " << old_acting_primary << " -> "
	    << new_acting_primary
	    << ", up_primary " << old_up_primary << " -> " << new_up_primary
	    << ", role " << oldrole << " -> " << role
	    << ", features acting " << acting_features
	    << " upacting " << upacting_features
	    << dendl;

  // 旧 interval 的 Active/Peered/Recovery 标志不再成立；
  // 新的 peering 必须重新证明其一致性后才会恢复这些状态。
  // deactivate.
  state_clear(PG_STATE_ACTIVE);
  state_clear(PG_STATE_PEERED);
  state_clear(PG_STATE_PREMERGE);
  state_clear(PG_STATE_DOWN);
  state_clear(PG_STATE_RECOVERY_WAIT);
  state_clear(PG_STATE_RECOVERY_TOOFULL);
  state_clear(PG_STATE_RECOVERING);

  // 与旧 acting 集合绑定的 peer 进度和 recovery/backfill 目标全部失效。
  peer_purged.clear();
  acting_recovery_backfill.clear();
  acting_recovery_backfill_shard_id_set.clear();

  // pg_temp 意图属于旧角色/旧映射，清除后由新的 primary/replica 流程重新决定。
  // reset primary/replica state?
  if (was_old_primary || is_primary()) {
    pl->clear_want_pg_temp();
  } else if (was_old_nonprimary || is_nonprimary()) {
    pl->clear_want_pg_temp();
  }
  // 清空 primary 专属的 peering 临时状态，再通知 PG 外层当前映射已变化。
  clear_primary_state();

  pl->on_change(t);

  ceph_assert(!deleting);

  // 非 primary 需要在 ActMap 阶段向新 primary 发送 notify；primary 不需要通知自己。
  // should we tell the primary we are here?
  send_notify = !is_primary();

  if (role != oldrole ||
      was_old_primary != is_primary()) {
    // 当前 OSD 的角色发生改变，原有 clean 结论与上层角色资源都不能继续沿用。
    // did primary change?
    if (was_old_primary != is_primary()) {
      state_clear(PG_STATE_CLEAN);
    }

    pl->on_role_change();
  } else {
    // no role change.
    // did primary change?
    if (get_primary() != old_acting_primary) {
      psdout(10) << oldacting << " -> " << acting
	       << ", acting primary "
	       << old_acting_primary << " -> " << get_primary()
	       << dendl;
    } else {
      // primary is the same.
      if (is_primary()) {
	// i am (still) primary. but my replica set changed.
	state_clear(PG_STATE_CLEAN);

	psdout(10) << oldacting << " -> " << acting
		 << ", replicas changed" << dendl;
      }
    }
  }

  // 特殊映射：当前 OSD 是 up primary，但暂时没有 acting；
  // 请求清除遗留 pg_temp，让 Monitor 重新依据当前映射计算 acting。
  if (acting.empty() && !up.empty() && up_primary == pg_whoami) {
    psdout(10) << " acting empty, but i am up[0], clearing pg_temp" << dendl;
    pl->queue_want_pg_temp(acting);
  }
}

void PeeringState::on_new_interval()
{
  // 此处只重置“依赖 interval”的派生运行状态；
  // up/acting 和 role 已由 start_peering_interval() 安装完毕。
  dout(20) << dendl;
  const OSDMapRef osdmap = get_osdmap();

  // 计算新 up/acting 成员共同支持的 feature；后续 PG 消息与行为只能使用交集。
  // initialize features
  acting_features = CEPH_FEATURES_SUPPORTED_DEFAULT;
  upacting_features = CEPH_FEATURES_SUPPORTED_DEFAULT;
  pg_acting_features = local_pg_acting_features;
  for (auto p = acting.begin(); p != acting.end(); ++p) {
    if (*p == CRUSH_ITEM_NONE)
      continue;
    uint64_t f = osdmap->get_xinfo(*p).features;
    acting_features &= f;
    upacting_features &= f;
  }
  for (auto p = up.begin(); p != up.end(); ++p) {
    if (*p == CRUSH_ITEM_NONE)
      continue;
    upacting_features &= osdmap->get_xinfo(*p).features;
  }
  psdout(20) << "upacting_features 0x" << std::hex
	     << upacting_features << std::dec
	     << " from " << acting << "+" << up << dendl;

  psdout(20) << "checking missing set deletes flag. missing = "
	     << get_pg_log().get_missing() << dendl;

  if (!pg_log.get_missing().may_include_deletes &&
      !perform_deletes_during_peering()) {
    // 当前流程不会在 peering 中处理删除时，重建 missing 集合以纳入历史删除记录。
    pl->rebuild_missing_set_with_deletes(pg_log);
  }
  ceph_assert(
    pg_log.get_missing().may_include_deletes ==
    !perform_deletes_during_peering());

  // 新 interval 的 peer 集合已变更，重新初始化 heartbeat 时间戳。
  init_hb_stamps();

  // 处理旧 interval 遗留的读 lease
  // readable_until_ub       当前 interval 中，旧 acting 副本可能仍允许读到的最晚时间
  // prior_readable_until_ub 更早 interval 留下的、尚未过期的最晚时间
  // update lease bounds for a new interval
  auto mnow = pl->get_mnow();
  prior_readable_until_ub = std::max(prior_readable_until_ub,
				     readable_until_ub);
  prior_readable_until_ub = info.history.refresh_prior_readable_until_ub(
    mnow, prior_readable_until_ub);
  psdout(10) << "prior_readable_until_ub "
	     << prior_readable_until_ub << " (mnow " << mnow << " + "
	     << info.history.prior_readable_until_ub << ")" << dendl;
  prior_readable_down_osds.clear(); // we populate this when we build the priorset

  readable_until =
    readable_until_ub =
    readable_until_ub_sent =
    readable_until_ub_from_primary = ceph::signedspan::zero();

  acting_readable_until_ub.clear();
  if (is_primary()) {
    // 仅 primary 收集各 acting 副本的 readable_until 上界。
    acting_readable_until_ub.resize(acting.size(), ceph::signedspan::zero());
  }

  // 让 PG 外层同步 interval 变化后的请求、恢复与统计相关状态。
  pl->on_new_interval();
}

void PeeringState::init_primary_up_acting(
  const vector<int> &newup,
  const vector<int> &newacting,
  int new_up_primary,
  int new_acting_primary)
{
  actingset.clear();
  acting = newacting;
  for (uint8_t i = 0; i < acting.size(); ++i) {
    if (acting[i] != CRUSH_ITEM_NONE)
      actingset.insert(
	pg_shard_t(
	  acting[i],
	  pool.info.is_erasure() ? shard_id_t(i) : shard_id_t::NO_SHARD));
  }
  upset.clear();
  up = newup;
  for (uint8_t i = 0; i < up.size(); ++i) {
    if (up[i] != CRUSH_ITEM_NONE)
      upset.insert(
	pg_shard_t(
	  up[i],
	  pool.info.is_erasure() ? shard_id_t(i) : shard_id_t::NO_SHARD));
  }
  if (!pool.info.is_erasure()) {
    // replicated
    up_primary = pg_shard_t(new_up_primary, shard_id_t::NO_SHARD);
    primary = pg_shard_t(new_acting_primary, shard_id_t::NO_SHARD);
  } else {
    // erasure
    up_primary = pg_shard_t();
    primary = pg_shard_t();
    for (uint8_t i = 0; i < up.size(); ++i) {
      if (up[i] == new_up_primary) {
	up_primary = pg_shard_t(up[i], shard_id_t(i));
	break;
      }
    }
    // Calcuating the shard of the acting_primary is tricky because in
    // error conditions the same osd can be in multiple positions in
    // the acting_set. Use pgtemp ordering (which places shards which
    // can become the primary first) to match the code in
    // OSDMap::_get_temp_osds
    const OSDMapRef osdmap = get_osdmap();
    bool has_pgtemp = osdmap->has_pgtemp(spgid.pgid);
    std::vector<int> pg_temp = acting;
    if (has_pgtemp) {
      pg_temp = osdmap->pgtemp_primaryfirst(pool.info, acting);
    }
    for (uint8_t i = 0; i < acting.size(); ++i) {
      if (pg_temp[i] == new_acting_primary) {
	if (has_pgtemp) {
	  primary = pg_shard_t(new_acting_primary,
			       osdmap->pgtemp_undo_primaryfirst(
				 pool.info, spgid.pgid, shard_id_t(i)));
	} else {
	  primary = pg_shard_t(new_acting_primary, shard_id_t(i));
	}
	break;
      }
    }
    ceph_assert(up_primary.osd == new_up_primary);
    ceph_assert(primary.osd == new_acting_primary);
  }
}

void PeeringState::init_hb_stamps()
{
  if (is_primary()) {
    // we care about all other osds in the acting set
    hb_stamps.resize(acting.size() - 1);
    unsigned i = 0;
    for (auto p : acting) {
      if (p == CRUSH_ITEM_NONE || p == get_primary().osd) {
	continue;
      }
      hb_stamps[i++] = pl->get_hb_stamps(p);
    }
    hb_stamps.resize(i);
  } else if (is_nonprimary()) {
    // we care about just the primary
    hb_stamps.resize(1);
    hb_stamps[0] = pl->get_hb_stamps(get_primary().osd);
  } else {
    hb_stamps.clear();
  }
  dout(10) << "now " << hb_stamps << dendl;
}


void PeeringState::clear_recovery_state()
{
  async_recovery_targets.clear();
  backfill_targets.clear();
  backfill_target_shard_id_set.clear();
}

void PeeringState::clear_primary_state()
{
  psdout(10) << "clear_primary_state" << dendl;

  // clear peering state
  stray_set.clear();
  peer_log_requested.clear();
  peer_missing_requested.clear();
  peer_info.clear();
  peer_bytes.clear();
  peer_missing.clear();
  peer_last_complete_ondisk.clear();
  peer_activated.clear();
  min_last_complete_ondisk = eversion_t();
  pg_trim_to = eversion_t();
  might_have_unfound.clear();
  need_up_thru = false;
  missing_loc.clear();
  pg_log.reset_recovery_pointers();

  clear_recovery_state();

  pg_committed_to = eversion_t();
  missing_loc.clear();
  pl->clear_primary_state();
}

/// return [start,end) bounds for required past_intervals
static pair<epoch_t, epoch_t> get_required_past_interval_bounds(
  const pg_info_t &info,
  epoch_t oldest_map) {
  epoch_t start = std::max(
    info.history.last_epoch_clean ? info.history.last_epoch_clean :
    info.history.epoch_pool_created,
    oldest_map);
  epoch_t end = std::max(
    info.history.same_interval_since,
    info.history.epoch_pool_created);
  return make_pair(start, end);
}


void PeeringState::check_past_interval_bounds() const
{
  // See: https://tracker.ceph.com/issues/64002
  if (cct->_conf.get_val<bool>("osd_skip_check_past_interval_bounds")) {
    return;
  }
  // cluster_osdmap_trim_lower_bound gives us a bound on needed
  // intervals, see doc/dev/osd_internals/past_intervals.rst
  auto oldest_epoch = pl->cluster_osdmap_trim_lower_bound();
  auto rpib = get_required_past_interval_bounds(
    info,
    oldest_epoch);
  if (rpib.first >= rpib.second) {
    // do not warn if the start bound is dictated by oldest_map; the
    // past intervals are presumably appropriate given the pg info.
    if (!past_intervals.empty() &&
	rpib.first > oldest_epoch) {
      pl->get_clog_error() << info.pgid << " required past_interval bounds are"
			     << " empty [" << rpib << ") but past_intervals is not: "
			     << past_intervals;
      derr << info.pgid << " required past_interval bounds are"
	   << " empty [" << rpib << ") but past_intervals is not: "
	   << past_intervals << dendl;
    }
  } else {
    if (past_intervals.empty()) {
      pl->get_clog_error() << info.pgid << " required past_interval bounds are"
			     << " not empty [" << rpib << ") but past_intervals "
			     << past_intervals << " is empty";
      derr << info.pgid << " required past_interval bounds are"
	   << " not empty [" << rpib << ") but past_intervals "
	   << past_intervals << " is empty" << dendl;
      ceph_assert(!past_intervals.empty());
    }

    auto apib = past_intervals.get_bounds();
    if (apib.first > rpib.first) {
      pl->get_clog_error() << info.pgid << " past_intervals [" << apib
			     << ") start interval does not contain the required"
			     << " bound [" << rpib << ") start";
      derr << info.pgid << " past_intervals [" << apib
	   << ") start interval does not contain the required"
	   << " bound [" << rpib << ") start" << dendl;
      ceph_abort_msg("past_interval start interval mismatch");
    }
    if (apib.second != rpib.second) {
      pl->get_clog_error() << info.pgid << " past_interal bound [" << apib
			     << ") end does not match required [" << rpib
			     << ") end";
      derr << info.pgid << " past_interal bound [" << apib
	   << ") end does not match required [" << rpib
	   << ") end" << dendl;
      ceph_abort_msg("past_interval end mismatch");
    }
  }
}

int PeeringState::clamp_recovery_priority(int priority, int pool_recovery_priority, int max)
{
  static_assert(OSD_RECOVERY_PRIORITY_MIN < OSD_RECOVERY_PRIORITY_MAX, "Invalid priority range");
  static_assert(OSD_RECOVERY_PRIORITY_MIN >= 0, "Priority range must match unsigned type");

  ceph_assert(max <= OSD_RECOVERY_PRIORITY_MAX);

  // User can't set this too high anymore, but might be a legacy value
  if (pool_recovery_priority > OSD_POOL_PRIORITY_MAX)
    pool_recovery_priority = OSD_POOL_PRIORITY_MAX;
  if (pool_recovery_priority < OSD_POOL_PRIORITY_MIN)
    pool_recovery_priority = OSD_POOL_PRIORITY_MIN;
  // Shift range from min to max to 0 to max - min
  pool_recovery_priority += (0 - OSD_POOL_PRIORITY_MIN);
  ceph_assert(pool_recovery_priority >= 0 && pool_recovery_priority <= (OSD_POOL_PRIORITY_MAX - OSD_POOL_PRIORITY_MIN));

  priority += pool_recovery_priority;

  // Clamp to valid range
  return std::clamp<int>(priority, OSD_RECOVERY_PRIORITY_MIN, max);
}

unsigned PeeringState::get_recovery_priority()
{
  // a higher value -> a higher priority
  int ret = OSD_RECOVERY_PRIORITY_BASE;
  int base = ret;

  if (state & PG_STATE_FORCED_RECOVERY) {
    ret = OSD_RECOVERY_PRIORITY_FORCED;
  } else {
    // XXX: This priority boost isn't so much about inactive, but about data-at-risk
    if (is_degraded() && info.stats.avail_no_missing.size() < pool.info.min_size) {
      base = OSD_RECOVERY_INACTIVE_PRIORITY_BASE;
      // inactive: no. of replicas < min_size, highest priority since it blocks IO
      ret = base + (pool.info.min_size - info.stats.avail_no_missing.size());
    }

    int64_t pool_recovery_priority = 0;
    pool.info.opts.get(pool_opts_t::RECOVERY_PRIORITY, &pool_recovery_priority);

    ret = clamp_recovery_priority(ret, pool_recovery_priority, get_max_prio_for_base(base));
  }
  psdout(20) << "recovery priority is " << ret << dendl;
  return static_cast<unsigned>(ret);
}

unsigned PeeringState::get_backfill_priority()
{
  // a higher value -> a higher priority
  int ret = OSD_BACKFILL_PRIORITY_BASE;
  int base = ret;

  if (state & PG_STATE_FORCED_BACKFILL) {
    ret = OSD_BACKFILL_PRIORITY_FORCED;
  } else {
    if (actingset.size() < pool.info.min_size) {
      base = OSD_BACKFILL_INACTIVE_PRIORITY_BASE;
      // inactive: no. of replicas < min_size, highest priority since it blocks IO
      ret = base + (pool.info.min_size - actingset.size());

    } else if (is_undersized()) {
      // undersized: OSD_BACKFILL_DEGRADED_PRIORITY_BASE + num missing replicas
      ceph_assert(pool.info.size > actingset.size());
      base = OSD_BACKFILL_DEGRADED_PRIORITY_BASE;
      ret = base + (pool.info.size - actingset.size());

    } else if (is_degraded()) {
      // degraded: baseline degraded
      base = ret = OSD_BACKFILL_DEGRADED_PRIORITY_BASE;
    }

    // Adjust with pool's recovery priority
    int64_t pool_recovery_priority = 0;
    pool.info.opts.get(pool_opts_t::RECOVERY_PRIORITY, &pool_recovery_priority);

    ret = clamp_recovery_priority(ret, pool_recovery_priority, get_max_prio_for_base(base));
  }

  psdout(20) << "backfill priority is " << ret << dendl;
  return static_cast<unsigned>(ret);
}

unsigned PeeringState::get_delete_priority()
{
  auto state = get_osdmap()->get_state(pg_whoami.osd);
  if (state & (CEPH_OSD_BACKFILLFULL |
               CEPH_OSD_FULL)) {
    return OSD_DELETE_PRIORITY_FULL;
  } else if (state & CEPH_OSD_NEARFULL) {
    return OSD_DELETE_PRIORITY_FULLISH;
  } else {
    return OSD_DELETE_PRIORITY_NORMAL;
  }
}

bool PeeringState::set_force_recovery(bool b)
{
  bool did = false;
  if (b) {
    if (!(state & PG_STATE_FORCED_RECOVERY) &&
	(state & (PG_STATE_DEGRADED |
		  PG_STATE_RECOVERY_WAIT |
		  PG_STATE_RECOVERING))) {
      psdout(20) << "set" << dendl;
      state_set(PG_STATE_FORCED_RECOVERY);
      pl->publish_stats_to_osd();
      did = true;
    }
  } else if (state & PG_STATE_FORCED_RECOVERY) {
    psdout(20) << "clear" << dendl;
    state_clear(PG_STATE_FORCED_RECOVERY);
    pl->publish_stats_to_osd();
    did = true;
  }
  if (did) {
    psdout(20) << "state " << get_current_state()
	     << dendl;
    pl->update_local_background_io_priority(get_recovery_priority());
  }
  return did;
}

bool PeeringState::set_force_backfill(bool b)
{
  bool did = false;
  if (b) {
    if (!(state & PG_STATE_FORCED_BACKFILL) &&
	(state & (PG_STATE_DEGRADED |
		  PG_STATE_BACKFILL_WAIT |
		  PG_STATE_BACKFILLING))) {
      psdout(10) << "set" << dendl;
      state_set(PG_STATE_FORCED_BACKFILL);
      pl->publish_stats_to_osd();
      did = true;
    }
  } else if (state & PG_STATE_FORCED_BACKFILL) {
    psdout(10) << "clear" << dendl;
    state_clear(PG_STATE_FORCED_BACKFILL);
    pl->publish_stats_to_osd();
    did = true;
  }
  if (did) {
    psdout(20) << "state " << get_current_state()
	     << dendl;
    pl->update_local_background_io_priority(get_backfill_priority());
  }
  return did;
}

void PeeringState::schedule_renew_lease()
{
  pl->schedule_renew_lease(
    last_peering_reset,
    readable_interval / 2);
}

void PeeringState::send_lease()
{
  epoch_t epoch = pl->get_osdmap_epoch();
  for (auto peer : actingset) {
    if (peer == pg_whoami) {
      continue;
    }
    pl->send_cluster_message(
      peer.osd,
      TOPNSPC::make_message<MOSDPGLease>(epoch,
		      spg_t(spgid.pgid, peer.shard),
		      get_lease()),
      epoch);
  }
}

void PeeringState::proc_lease(const pg_lease_t& l)
{
  ceph_assert(HAVE_FEATURE(upacting_features, SERVER_OCTOPUS));
  if (!is_nonprimary()) {
    psdout(20) << "no-op, !nonprimary" << dendl;
    return;
  }
  psdout(10) << l << dendl;
  if (l.readable_until_ub > readable_until_ub_from_primary) {
    readable_until_ub_from_primary = l.readable_until_ub;
  }

  ceph::signedspan ru = ceph::signedspan::zero();
  if (l.readable_until != ceph::signedspan::zero() &&
      hb_stamps[0]->peer_clock_delta_ub) {
    ru = l.readable_until - *hb_stamps[0]->peer_clock_delta_ub;
    psdout(20) << " peer_clock_delta_ub " << *hb_stamps[0]->peer_clock_delta_ub
	       << " -> ru " << ru << dendl;
  }
  if (ru > readable_until) {
    readable_until = ru;
    psdout(20) << "readable_until now " << readable_until << dendl;
    // NOTE: if we ever decide to block/queue ops on the replica,
    // we'll need to wake them up here.
  }

  ceph::signedspan ruub;
  if (hb_stamps[0]->peer_clock_delta_lb) {
    ruub = l.readable_until_ub - *hb_stamps[0]->peer_clock_delta_lb;
    psdout(20) << " peer_clock_delta_lb " << *hb_stamps[0]->peer_clock_delta_lb
	       << " -> ruub " << ruub << dendl;
  } else {
    ruub = pl->get_mnow() + l.interval;
    psdout(20) << " no peer_clock_delta_lb -> ruub " << ruub << dendl;
  }
  if (ruub > readable_until_ub) {
    readable_until_ub = ruub;
    psdout(20) << "readable_until_ub now " << readable_until_ub
	       << dendl;
  }
}

void PeeringState::proc_lease_ack(int from, const pg_lease_ack_t& a)
{
  ceph_assert(HAVE_FEATURE(upacting_features, SERVER_OCTOPUS));
  auto now = pl->get_mnow();
  bool was_min = false;
  for (unsigned i = 0; i < acting.size(); ++i) {
    if (from == acting[i]) {
      // the lease_ack value is based on the primary's clock
      if (a.readable_until_ub > acting_readable_until_ub[i]) {
	if (acting_readable_until_ub[i] == readable_until) {
	  was_min = true;
	}
	acting_readable_until_ub[i] = a.readable_until_ub;
	break;
      }
    }
  }
  if (was_min) {
    auto old_ru = readable_until;
    recalc_readable_until();
    if (now >= old_ru) {
      pl->recheck_readable();
    }
  }
}

void PeeringState::proc_renew_lease()
{
  ceph_assert(HAVE_FEATURE(upacting_features, SERVER_OCTOPUS));
  renew_lease(pl->get_mnow());
  if (actingset.size() > 1) {
    send_lease();
  } else {
    pl->recheck_readable();
  }
  schedule_renew_lease();
}

void PeeringState::recalc_readable_until()
{
  ceph_assert(is_primary());
  ceph::signedspan min = readable_until_ub_sent;
  for (unsigned i = 0; i < acting.size(); ++i) {
    if (acting[i] == pg_whoami.osd || acting[i] == CRUSH_ITEM_NONE) {
      continue;
    }
    dout(20) << "peer osd." << acting[i]
	     << " ruub " << acting_readable_until_ub[i] << dendl;
    if (acting_readable_until_ub[i] < min) {
      min = acting_readable_until_ub[i];
    }
  }
  readable_until = min;
  readable_until_ub = min;
  dout(20) << "readable_until[_ub] " << readable_until
	   << " (sent " << readable_until_ub_sent << ")" << dendl;
}

bool PeeringState::check_prior_readable_down_osds(const OSDMapRef& map)
{
  ceph_assert(HAVE_FEATURE(upacting_features, SERVER_OCTOPUS));
  bool changed = false;
  auto p = prior_readable_down_osds.begin();
  while (p != prior_readable_down_osds.end()) {
    if (map->is_dead(*p)) {
      dout(10) << "prior_readable_down_osds osd." << *p
	       << " is dead as of epoch " << map->get_epoch()
	       << dendl;
      p = prior_readable_down_osds.erase(p);
      changed = true;
    } else {
      ++p;
    }
  }
  if (changed && prior_readable_down_osds.empty()) {
    psdout(10) << " empty prior_readable_down_osds, clearing ub" << dendl;
    clear_prior_readable_until_ub();
    return true;
  }
  return false;
}

bool PeeringState::adjust_need_up_thru(const OSDMapRef osdmap)
{
  epoch_t up_thru = osdmap->get_up_thru(pg_whoami.osd);
  if (need_up_thru &&
      up_thru >= info.history.same_interval_since) {
    psdout(10) << "adjust_need_up_thru now "
	       << up_thru << ", need_up_thru now false" << dendl;
    need_up_thru = false;
    return true;
  }
  return false;
}

PastIntervals::PriorSet PeeringState::build_prior()
{
  if (1) {
    // sanity check
    for (auto it = peer_info.begin(); it != peer_info.end(); ++it) {
      ceph_assert(info.history.last_epoch_started >=
		  it->second.history.last_epoch_started);
    }
  }

  const OSDMap &osdmap = *get_osdmap();
  PastIntervals::PriorSet prior = past_intervals.get_prior_set(
    pool.info.is_erasure(),
    info.history.last_epoch_started,
    &missing_loc.get_recoverable_predicate(),
    [&](epoch_t start, int osd, epoch_t *lost_at) {
      const osd_info_t *pinfo = 0;
      if (osdmap.exists(osd)) {
	pinfo = &osdmap.get_info(osd);
	if (lost_at)
	  *lost_at = pinfo->lost_at;
      }

      if (osdmap.is_up(osd)) {
	return PastIntervals::UP;
      } else if (!pinfo) {
	return PastIntervals::DNE;
      } else if (pinfo->lost_at > start) {
	return PastIntervals::LOST;
      } else {
	return PastIntervals::DOWN;
      }
    },
    up,
    acting,
    dpp);

  if (prior.pg_down) {
    state_set(PG_STATE_DOWN);
  }

  if (get_osdmap()->get_up_thru(pg_whoami.osd) <
      info.history.same_interval_since) {
    psdout(10) << "up_thru " << get_osdmap()->get_up_thru(pg_whoami.osd)
	       << " < same_since " << info.history.same_interval_since
	       << ", must notify monitor" << dendl;
    need_up_thru = true;
  } else {
    psdout(10) << "up_thru " << get_osdmap()->get_up_thru(pg_whoami.osd)
	       << " >= same_since " << info.history.same_interval_since
	       << ", all is well" << dendl;
    need_up_thru = false;
  }
  pl->set_probe_targets(prior.probe);
  return prior;
}

bool PeeringState::needs_recovery() const
{
  ceph_assert(is_primary());

  auto &missing = pg_log.get_missing();

  if (missing.num_missing()) {
    psdout(10) << "primary has " << missing.num_missing()
	       << " missing" << dendl;
    return true;
  }

  ceph_assert(!acting_recovery_backfill.empty());
  for (const pg_shard_t& peer : acting_recovery_backfill) {
    if (peer == get_primary()) {
      continue;
    }
    auto pm = peer_missing.find(peer);
    if (pm == peer_missing.end()) {
      psdout(10) << "osd." << peer << " doesn't have missing set"
		 << dendl;
      continue;
    }
    if (pm->second.num_missing()) {
      std::ostringstream ss;
      ss << "osd." << peer << " has "
         << pm->second.num_missing() << " missing";

      const auto &items = pm->second.get_items();
      if (!items.empty()) {
        const auto &first = *items.begin();
        const hobject_t &oid = first.first;
        ss << ", first missing oid=" << oid;
      }

      psdout(10) << ss.str() << dendl;
      return true;
    }
  }

  psdout(10) << "is recovered" << dendl;
  return false;
}

bool PeeringState::needs_backfill() const
{
  ceph_assert(is_primary());

  // We can assume that only possible osds that need backfill
  // are on the backfill_targets vector nodes.
  for (const pg_shard_t& peer : backfill_targets) {
    auto pi = peer_info.find(peer);
    ceph_assert(pi != peer_info.end());
    if (!pi->second.last_backfill.is_max()) {
      psdout(10) << "osd." << peer
		 << " has last_backfill " << pi->second.last_backfill << dendl;
      return true;
    }
  }

  psdout(10) << "does not need backfill" << dendl;
  return false;
}

/**
* Returns whether a particular object can be safely read
*/
bool PeeringState::can_serve_read(const hobject_t &hoid)
{
  ceph_assert(!is_primary());
  std::string_view storage_object = "replica";
  if (pool.info.is_erasure()) {
    storage_object = "shard";
  }
  if (!pg_log.get_log().has_write_since(
      hoid, pg_committed_to)) {
    psdout(20) << "can be safely read on this " << storage_object << dendl;
    return true;
  } else {
    psdout(20) << "can't read object on this " << storage_object << dendl;
    return false;
  }
}

/*
 * Returns true unless there is a non-lost OSD in might_have_unfound.
 */
bool PeeringState::all_unfound_are_queried_or_lost(
  const OSDMapRef osdmap) const
{
  ceph_assert(is_primary());

  auto peer = might_have_unfound.begin();
  auto mend = might_have_unfound.end();
  for (; peer != mend; ++peer) {
    if (peer_missing.count(*peer))
      continue;
    auto iter = peer_info.find(*peer);
    if (iter != peer_info.end() &&
        (iter->second.is_empty() || iter->second.dne()))
      continue;
    if (!osdmap->exists(peer->osd))
      continue;
    const osd_info_t &osd_info(osdmap->get_info(peer->osd));
    if (osd_info.lost_at <= osd_info.up_from) {
      // If there is even one OSD in might_have_unfound that isn't lost, we
      // still might retrieve our unfound.
      return false;
    }
  }
  psdout(10) << "all_unfound_are_queried_or_lost all of might_have_unfound "
	     << might_have_unfound
	     << " have been queried or are marked lost" << dendl;
  return true;
}


void PeeringState::reject_reservation()
{
  pl->unreserve_recovery_space();
  pl->send_cluster_message(
    primary.osd,
    TOPNSPC::make_message<MBackfillReserve>(
      MBackfillReserve::REJECT_TOOFULL,
      spg_t(info.pgid.pgid, primary.shard),
      get_osdmap_epoch()),
    get_osdmap_epoch());
}

/**
 * calculate_maxlesf_and_minlua
 *
 * Calculate max_last_epoch_started and
 * min_last_update_acceptable
 */
void PeeringState::calculate_maxles_and_minlua( const map<pg_shard_t, pg_info_t> &infos,
						epoch_t& max_last_epoch_started,
						eversion_t& min_last_update_acceptable,
						bool exclude_nonprimary_shards,
						bool *history_les_bound) const
{
  /* See doc/dev/osd_internals/last_epoch_started.rst before attempting
   * to make changes to this process.  Also, make sure to update it
   * when you find bugs! */
  max_last_epoch_started = 0;
  for (auto i = infos.begin(); i != infos.end(); ++i) {
    if (exclude_nonprimary_shards &&
	pool.info.is_nonprimary_shard(shard_id_t(i->first.shard)))
      continue;
    if (!cct->_conf->osd_find_best_info_ignore_history_les &&
	max_last_epoch_started < i->second.history.last_epoch_started) {
      if (history_les_bound) {
	*history_les_bound = true;
      }
      max_last_epoch_started = i->second.history.last_epoch_started;
    }
    if (!i->second.is_incomplete() &&
	max_last_epoch_started < i->second.last_epoch_started) {
      if (history_les_bound) {
	*history_les_bound = false;
      }
      max_last_epoch_started = i->second.last_epoch_started;
    }
  }
  min_last_update_acceptable = eversion_t::max();
  for (auto i = infos.begin(); i != infos.end(); ++i) {
    if (max_last_epoch_started <= i->second.last_epoch_started) {
      if (min_last_update_acceptable > i->second.last_update)
	min_last_update_acceptable = i->second.last_update;
    }
  }
}

/**
 * find_best_info
 *
 * Returns an iterator to the best info in infos sorted by:
 *  1) Prefer newer last_update
 *  2) Prefer longer tail if it brings another info into contiguity
 *  3) Prefer current primary
 */
map<pg_shard_t, pg_info_t>::const_iterator PeeringState::find_best_info(
  const map<pg_shard_t, pg_info_t> &infos,
  bool restrict_to_up_acting,
  bool exclude_nonprimary_shards,
  bool *history_les_bound) const
{
  epoch_t max_last_epoch_started;
  eversion_t min_last_update_acceptable;
  calculate_maxles_and_minlua( infos,
			       max_last_epoch_started,
			       min_last_update_acceptable,
			       exclude_nonprimary_shards,
			       history_les_bound);

  if (min_last_update_acceptable == eversion_t::max())
    return infos.end();

  auto best = infos.end();
  // find osd with newest last_update (oldest for ec_pool).
  // if there are multiples, prefer
  //  - a longer tail, if it brings another peer into log contiguity
  //  - the current primary
  for (auto p = infos.begin(); p != infos.end(); ++p) {
    if (restrict_to_up_acting && !is_up(p->first) &&
	!is_acting(p->first))
      continue;
    // Only consider peers with last_update >= min_last_update_acceptable
    if (p->second.last_update < min_last_update_acceptable)
      continue;
    // Disqualify anyone with a too old last_epoch_started
    if (p->second.last_epoch_started < max_last_epoch_started)
      continue;
    // Disqualify anyone who is incomplete (not fully backfilled)
    if (p->second.is_incomplete())
      continue;
    // If requested disqualify nonprimary shards (may have a sparse log)
    if (exclude_nonprimary_shards &&
	pool.info.is_nonprimary_shard(shard_id_t(p->first.shard)))
      continue;
    if (best == infos.end()) {
      best = p;
      continue;
    }
    // Prefer newer last_update
    if (pool.info.require_rollback()) {
      if (p->second.last_update > best->second.last_update)
	continue;
      if (p->second.last_update < best->second.last_update) {
	best = p;
	continue;
      }
    } else {
      if (p->second.last_update < best->second.last_update)
	continue;
      if (p->second.last_update > best->second.last_update) {
	best = p;
	continue;
      }
    }

    // Prefer longer tail
    if (p->second.log_tail > best->second.log_tail) {
      continue;
    } else if (p->second.log_tail < best->second.log_tail) {
      best = p;
      continue;
    }

    if (!p->second.has_missing() && best->second.has_missing()) {
      psdout(10) << "prefer osd." << p->first
               << " because it is complete while best has missing"
               << dendl;
      best = p;
      continue;
    } else if (p->second.has_missing() && !best->second.has_missing()) {
      psdout(10) << "skipping osd." << p->first
               << " because it has missing while best is complete"
               << dendl;
      continue;
    } else {
      // both are complete or have missing
      // fall through
    }

    // prefer current primary (usually the caller), all things being equal
    if (p->first == pg_whoami) {
      psdout(10) << "calc_acting prefer osd." << p->first
		 << " because it is current primary" << dendl;
      best = p;
      continue;
    }
  }
  return best;
}

void PeeringState::calc_ec_acting(
  map<pg_shard_t, pg_info_t>::const_iterator auth_log_shard,
  unsigned size,
  const vector<int> &acting,
  const vector<int> &up,
  const map<pg_shard_t, pg_info_t> &all_info,
  bool restrict_to_up_acting,
  vector<int> *_want,
  set<pg_shard_t> *backfill,
  set<pg_shard_t> *acting_backfill,
  ostream &ss)
{
  vector<int> want(size, CRUSH_ITEM_NONE);
  map<shard_id_t, set<pg_shard_t> > all_info_by_shard;
  for (auto i = all_info.begin();
       i != all_info.end();
       ++i) {
    all_info_by_shard[i->first.shard].insert(i->first);
  }
  for (uint8_t i = 0; i < want.size(); ++i) {
    ss << "For position " << (unsigned)i << ": ";
    if (up.size() > (unsigned)i && up[i] != CRUSH_ITEM_NONE &&
	!all_info.find(pg_shard_t(up[i], shard_id_t(i)))->second.is_incomplete() &&
	all_info.find(pg_shard_t(up[i], shard_id_t(i)))->second.last_update >=
	auth_log_shard->second.log_tail) {
      ss << " selecting up[i]: " << pg_shard_t(up[i], shard_id_t(i)) << std::endl;
      want[i] = up[i];
      continue;
    }
    if (up.size() > (unsigned)i && up[i] != CRUSH_ITEM_NONE) {
      ss << " backfilling up[i]: " << pg_shard_t(up[i], shard_id_t(i))
	 << " and ";
      backfill->insert(pg_shard_t(up[i], shard_id_t(i)));
    }

    if (acting.size() > (unsigned)i && acting[i] != CRUSH_ITEM_NONE &&
	!all_info.find(pg_shard_t(acting[i], shard_id_t(i)))->second.is_incomplete() &&
	all_info.find(pg_shard_t(acting[i], shard_id_t(i)))->second.last_update >=
	auth_log_shard->second.log_tail) {
      ss << " selecting acting[i]: " << pg_shard_t(acting[i], shard_id_t(i)) << std::endl;
      want[i] = acting[i];
    } else if (!restrict_to_up_acting) {
      for (auto j = all_info_by_shard[shard_id_t(i)].begin();
	   j != all_info_by_shard[shard_id_t(i)].end();
	   ++j) {
	ceph_assert(static_cast<int>(j->shard) == i);
	if (!all_info.find(*j)->second.is_incomplete() &&
	    all_info.find(*j)->second.last_update >=
	    auth_log_shard->second.log_tail) {
	  ss << " selecting stray: " << *j << std::endl;
	  want[i] = j->osd;
	  break;
	}
      }
      if (want[i] == CRUSH_ITEM_NONE)
	ss << " failed to fill position " << (int)i << std::endl;
    }
  }

  for (uint8_t i = 0; i < want.size(); ++i) {
    if (want[i] != CRUSH_ITEM_NONE) {
      acting_backfill->insert(pg_shard_t(want[i], shard_id_t(i)));
    }
  }
  acting_backfill->insert(backfill->begin(), backfill->end());
  _want->swap(want);
}

std::pair<map<pg_shard_t, pg_info_t>::const_iterator, eversion_t>
PeeringState::select_replicated_primary(
  map<pg_shard_t, pg_info_t>::const_iterator auth_log_shard,
  uint64_t force_auth_primary_missing_objects,
  const std::vector<int> &up,
  pg_shard_t up_primary,
  const map<pg_shard_t, pg_info_t> &all_info,
  const OSDMapRef osdmap,
  ostream &ss)
{
  pg_shard_t auth_log_shard_id = auth_log_shard->first;

  ss << __func__ << " newest update on osd." << auth_log_shard_id
     << " with " << auth_log_shard->second << std::endl;

  // select primary
  auto primary = all_info.find(up_primary);
  if (up.size() &&
      !primary->second.is_incomplete() &&
      primary->second.last_update >=
        auth_log_shard->second.log_tail) {
    ceph_assert(HAVE_FEATURE(osdmap->get_up_osd_features(), SERVER_NAUTILUS));
    auto approx_missing_objects =
      primary->second.stats.stats.sum.num_objects_missing;
    auto auth_version = auth_log_shard->second.last_update.version;
    auto primary_version = primary->second.last_update.version;
    if (auth_version > primary_version) {
      approx_missing_objects += auth_version - primary_version;
    } else {
      approx_missing_objects += primary_version - auth_version;
    }
    if ((uint64_t)approx_missing_objects >
        force_auth_primary_missing_objects) {
      primary = auth_log_shard;
      ss << "up_primary: " << up_primary << ") has approximate "
         << approx_missing_objects
         << "(>" << force_auth_primary_missing_objects <<") "
         << "missing objects, osd." << auth_log_shard_id
         << " selected as primary instead"
         << std::endl;
    } else {
      ss << "up_primary: " << up_primary << ") selected as primary"
         << std::endl;
    }
  } else {
    ceph_assert(!auth_log_shard->second.is_incomplete());
    ss << "up[0] needs backfill, osd." << auth_log_shard_id
       << " selected as primary instead" << std::endl;
    primary = auth_log_shard;
  }

  ss << __func__ << " primary is osd." << primary->first
     << " with " << primary->second << std::endl;

  /* We include auth_log_shard->second.log_tail because in GetLog,
   * we will request logs back to the min last_update over our
   * acting_backfill set, which will result in our log being extended
   * as far backwards as necessary to pick up any peers which can
   * be log recovered by auth_log_shard's log */
  eversion_t oldest_auth_log_entry =
    std::min(primary->second.log_tail, auth_log_shard->second.log_tail);

  return std::make_pair(primary, oldest_auth_log_entry);
}


/**
 * calculate the desired acting set.
 *
 * Choose an appropriate acting set.  Prefer up[0], unless it is
 * incomplete, or another osd has a longer tail that allows us to
 * bring other up nodes up to date.
 */
void PeeringState::calc_replicated_acting(
  map<pg_shard_t, pg_info_t>::const_iterator primary,
  eversion_t oldest_auth_log_entry,
  unsigned size,
  const vector<int> &acting,
  const vector<int> &up,
  pg_shard_t up_primary,
  const map<pg_shard_t, pg_info_t> &all_info,
  bool restrict_to_up_acting,
  vector<int> *want,
  set<pg_shard_t> *backfill,
  set<pg_shard_t> *acting_backfill,
  const OSDMapRef osdmap,
  const PGPool& pool,
  ostream &ss)
{
  ss << __func__ << (restrict_to_up_acting ? " restrict_to_up_acting" : "")
     << std::endl;

  want->push_back(primary->first.osd);
  acting_backfill->insert(primary->first);

  // select replicas that have log contiguity with primary.
  // prefer up, then acting, then any peer_info osds
  for (auto i : up) {
    pg_shard_t up_cand = pg_shard_t(i, shard_id_t::NO_SHARD);
    if (up_cand == primary->first)
      continue;
    const pg_info_t &cur_info = all_info.find(up_cand)->second;
    if (cur_info.is_incomplete() ||
        cur_info.last_update < oldest_auth_log_entry) {
      ss << " shard " << up_cand << " (up) backfill " << cur_info << std::endl;
      backfill->insert(up_cand);
      acting_backfill->insert(up_cand);
    } else {
      want->push_back(i);
      acting_backfill->insert(up_cand);
      ss << " osd." << i << " (up) accepted " << cur_info << std::endl;
    }
  }

  if (want->size() >= size) {
    return;
  }

  std::vector<std::pair<eversion_t, int>> candidate_by_last_update;
  candidate_by_last_update.reserve(acting.size());
  // This no longer has backfill OSDs, but they are covered above.
  for (auto i : acting) {
    pg_shard_t acting_cand(i, shard_id_t::NO_SHARD);
    // skip up osds we already considered above
    if (acting_cand == primary->first)
      continue;
    auto up_it = find(up.begin(), up.end(), i);
    if (up_it != up.end())
      continue;

    const pg_info_t &cur_info = all_info.find(acting_cand)->second;
    if (cur_info.is_incomplete() ||
	cur_info.last_update < oldest_auth_log_entry) {
      ss << " shard " << acting_cand << " (acting) REJECTED "
	 << cur_info << std::endl;
    } else {
      candidate_by_last_update.emplace_back(cur_info.last_update, i);
    }
  }

  auto sort_by_eversion =[](const std::pair<eversion_t, int> &lhs,
                            const std::pair<eversion_t, int> &rhs) {
    return lhs.first > rhs.first;
  };
  // sort by last_update, in descending order.
  std::sort(candidate_by_last_update.begin(),
            candidate_by_last_update.end(), sort_by_eversion);
  for (auto &p: candidate_by_last_update) {
    ceph_assert(want->size() < size);
    want->push_back(p.second);
    pg_shard_t s = pg_shard_t(p.second, shard_id_t::NO_SHARD);
    acting_backfill->insert(s);
    ss << " shard " << s << " (acting) accepted "
       << all_info.find(s)->second << std::endl;
    if (want->size() >= size) {
      return;
    }
  }

  if (restrict_to_up_acting) {
    return;
  }
  candidate_by_last_update.clear();
  candidate_by_last_update.reserve(all_info.size()); // overestimate but fine
  // continue to search stray to find more suitable peers
  for (auto &i : all_info) {
    // skip up osds we already considered above
    if (i.first == primary->first)
      continue;
    auto up_it = find(up.begin(), up.end(), i.first.osd);
    if (up_it != up.end())
      continue;
    auto acting_it = find(
      acting.begin(), acting.end(), i.first.osd);
    if (acting_it != acting.end())
      continue;

    if (i.second.is_incomplete() ||
	i.second.last_update < oldest_auth_log_entry) {
      ss << " shard " << i.first << " (stray) REJECTED " << i.second
         << std::endl;
    } else {
      candidate_by_last_update.emplace_back(
        i.second.last_update, i.first.osd);
    }
  }

  if (candidate_by_last_update.empty()) {
    // save us some effort
    return;
  }

  // sort by last_update, in descending order.
  std::sort(candidate_by_last_update.begin(),
            candidate_by_last_update.end(), sort_by_eversion);

  for (auto &p: candidate_by_last_update) {
    ceph_assert(want->size() < size);
    want->push_back(p.second);
    pg_shard_t s = pg_shard_t(p.second, shard_id_t::NO_SHARD);
    acting_backfill->insert(s);
    ss << " shard " << s << " (stray) accepted "
       << all_info.find(s)->second << std::endl;
    if (want->size() >= size) {
      return;
    }
  }
}

// Defines osd preference order: acting set, then larger last_update
using osd_ord_t = std::tuple<bool, eversion_t>; // <acting, last_update>
using osd_id_t = int;

class bucket_candidates_t {
  std::deque<std::pair<osd_ord_t, osd_id_t>> osds;
  int selected = 0;

public:
  void add_osd(osd_ord_t ord, osd_id_t osd) {
    // osds will be added in smallest to largest order
    ceph_assert(osds.empty() || osds.back().first <= ord);
    osds.push_back(std::make_pair(ord, osd));
  }
  osd_id_t pop_osd() {
    ceph_assert(!is_empty());
    auto ret = osds.front();
    osds.pop_front();
    return ret.second;
  }

  void inc_selected() { selected++; }
  unsigned get_num_selected() const { return selected; }

  osd_ord_t get_ord() const {
    return osds.empty() ? std::make_tuple(false, eversion_t())
      : osds.front().first;
  }

  bool is_empty() const { return osds.empty(); }

  bool operator<(const bucket_candidates_t &rhs) const {
    return std::make_tuple(-selected, get_ord()) <
      std::make_tuple(-rhs.selected, rhs.get_ord());
  }

  friend std::ostream &operator<<(std::ostream &, const bucket_candidates_t &);
};

std::ostream &operator<<(std::ostream &lhs, const bucket_candidates_t &cand)
{
  return lhs << "candidates[" << cand.osds << "]";
}

class bucket_heap_t {
  using elem_t = std::reference_wrapper<bucket_candidates_t>;
  std::vector<elem_t> heap;

  // Max heap -- should emit buckets in order of preference
  struct comp {
    bool operator()(const elem_t &lhs, const elem_t &rhs) {
      return lhs.get() < rhs.get();
    }
  };
public:
  void push_if_nonempty(elem_t e) {
    if (!e.get().is_empty()) {
      heap.push_back(e);
      std::push_heap(heap.begin(), heap.end(), comp());
    }
  }
  elem_t pop() {
    std::pop_heap(heap.begin(), heap.end(), comp());
    auto ret = heap.back();
    heap.pop_back();
    return ret;
  }

  bool is_empty() const { return heap.empty(); }
};

/**
 * calc_replicated_acting_stretch
 *
 * Choose an acting set using as much of the up set as possible; filling 
 * in the remaining slots so as to maximize the number of crush buckets at
 * level pool.info.peering_crush_bucket_barrier represented.
 *
 * Stretch clusters are a bit special: while they have a "size" the
 * same way as normal pools, if we happen to lose a data center
 * (we call it a "stretch bucket", but really it'll be a data center or
 * a cloud availability zone), we don't actually want to shove
 * 2 DC's worth of replication into a single site -- it won't fit!
 * So we locally calculate a bucket_max, based
 * on the targeted number of stretch buckets for the pool and
 * its size. Then we won't pull more than bucket_max from any
 * given ancestor even if it leaves us undersized.

 * There are two distinct phases: (commented below)
 */
void PeeringState::calc_replicated_acting_stretch(
  map<pg_shard_t, pg_info_t>::const_iterator primary,
  eversion_t oldest_auth_log_entry,
  unsigned size,
  const vector<int> &acting,
  const vector<int> &up,
  pg_shard_t up_primary,
  const map<pg_shard_t, pg_info_t> &all_info,
  bool restrict_to_up_acting,
  vector<int> *want,
  set<pg_shard_t> *backfill,
  set<pg_shard_t> *acting_backfill,
  const OSDMapRef osdmap,
  const PGPool& pool,
  ostream &ss)
{
  ceph_assert(want);
  ceph_assert(acting_backfill);
  ceph_assert(backfill);
  ss << __func__ << (restrict_to_up_acting ? " restrict_to_up_acting" : "")
     << std::endl;

  auto used = [want](int osd) {
    return std::find(want->begin(), want->end(), osd) != want->end();
  };

  auto usable_info = [&](const auto &cur_info) mutable {
    return !(cur_info.is_incomplete() ||
	     cur_info.last_update < oldest_auth_log_entry);
  };

  auto osd_info = [&](int osd) mutable -> const pg_info_t & {
    pg_shard_t cand = pg_shard_t(osd, shard_id_t::NO_SHARD);
    const pg_info_t &cur_info = all_info.find(cand)->second;
    return cur_info;
  };

  auto usable_osd = [&](int osd) mutable {
    return usable_info(osd_info(osd));
  };

  std::map<int, bucket_candidates_t> ancestors;
  auto get_ancestor = [&](int osd) mutable {
    int ancestor = osdmap->crush->get_parent_of_type(
      osd,
      pool.info.peering_crush_bucket_barrier,
      pool.info.crush_rule);
    return &ancestors[ancestor];
  };

  unsigned bucket_max = pool.info.size / pool.info.peering_crush_bucket_target;
  if (bucket_max * pool.info.peering_crush_bucket_target < pool.info.size) {
    ++bucket_max; 
  }

  /* 1) Select all usable osds from the up set as well as the primary
   *
   * We also stash any unusable osds from up into backfill.
   */
  auto add_required = [&](int osd) {
    if (!used(osd)) {
      want->push_back(osd);
      acting_backfill->insert(
	pg_shard_t(osd, shard_id_t::NO_SHARD));
      get_ancestor(osd)->inc_selected();
    }
  };
  add_required(primary->first.osd);
  ss << " osd " << primary->first.osd << " primary accepted "
     << osd_info(primary->first.osd) << std::endl;
  for (auto upcand: up) {
    auto upshard = pg_shard_t(upcand, shard_id_t::NO_SHARD);
    auto &curinfo = osd_info(upcand);
    if (usable_osd(upcand)) {
      ss << " osd " << upcand << " (up) accepted " << curinfo << std::endl;
      add_required(upcand);
    } else {
      ss << " osd " << upcand << " (up) backfill " << curinfo << std::endl;
      backfill->insert(upshard);
      acting_backfill->insert(upshard);
    }
  }

  if (want->size() >= pool.info.size) { // non-failed CRUSH mappings are valid
    ss << " up set sufficient" << std::endl;
    return;
  }
  ss << " up set insufficient, considering remaining osds" << std::endl;

  /* 2) Fill out remaining slots from usable osds in all_info
   *    while maximizing the number of ancestor nodes at the
   *    barrier_id crush level.
   */
  {
    std::vector<std::pair<osd_ord_t, osd_id_t>> candidates;
    /* To do this, we first filter the set of usable osd into an ordered
     * list of usable osds
     */
    auto get_osd_ord = [&](bool is_acting, const pg_info_t &info) -> osd_ord_t {
      return std::make_tuple(
	!is_acting /* acting should sort first */,
	info.last_update);
    };
    for (auto &cand : acting) {
      auto &cand_info = osd_info(cand);
      if (!used(cand) && usable_info(cand_info)) {
	ss << " acting candidate " << cand << " " << cand_info << std::endl;
	candidates.push_back(std::make_pair(get_osd_ord(true, cand_info), cand));
      }
    }
    if (!restrict_to_up_acting) {
      for (auto &[cand, info] : all_info) {
	if (!used(cand.osd) && usable_info(info) &&
	    (std::find(acting.begin(), acting.end(), cand.osd)
	     == acting.end())) {
	  ss << " other candidate " << cand << " " << info << std::endl;
	  candidates.push_back(
	    std::make_pair(get_osd_ord(false, info), cand.osd));
	}
      }
    }
    std::sort(candidates.begin(), candidates.end());

    // We then filter these candidates by ancestor
    std::for_each(candidates.begin(), candidates.end(), [&](auto cand) {
      get_ancestor(cand.second)->add_osd(cand.first, cand.second);
    });
  }

  auto pop_ancestor = [&](auto &ancestor) {
    ceph_assert(!ancestor.is_empty());
    auto osd = ancestor.pop_osd();

    ss << " accepting candidate " << osd << std::endl;

    ceph_assert(!used(osd));
    ceph_assert(usable_osd(osd));

    want->push_back(osd);
    acting_backfill->insert(
      pg_shard_t(osd, shard_id_t::NO_SHARD));
    ancestor.inc_selected();
  };

  /* Next, we use the ancestors map to grab a descendant of the
   * peering_crush_mandatory_member if not already represented.
   *
   * TODO: using 0 here to match other users.  Prior to merge, I
   * expect that this and other users should instead check against
   * CRUSH_ITEM_NONE.
   */
  if (pool.info.peering_crush_mandatory_member != CRUSH_ITEM_NONE) {
    auto aiter = ancestors.find(pool.info.peering_crush_mandatory_member);
    if (aiter != ancestors.end() &&
	!aiter->second.get_num_selected()) {
      ss << " adding required ancestor " << aiter->first << std::endl;
      ceph_assert(!aiter->second.is_empty()); // wouldn't exist otherwise
      pop_ancestor(aiter->second);
    }
  }

  /* We then place the ancestors in a heap ordered by fewest selected
   * and then by the ordering token of the next osd */
  bucket_heap_t aheap;
  std::for_each(ancestors.begin(), ancestors.end(), [&](auto &anc) {
    if (anc.second.get_num_selected() < bucket_max) {
      aheap.push_if_nonempty(anc.second);
    }
  });

  /* and pull from this heap until it's empty or we have enough.
   * "We have enough" is a sufficient check here for
   * stretch_set_can_peer() because our heap sorting always
   * pulls from ancestors with the least number of included OSDs,
   * so if it is possible to satisfy the bucket_count constraints we
   * will do so.
   */
  while (!aheap.is_empty() && want->size() < pool.info.size) {
    auto next = aheap.pop();
    pop_ancestor(next.get());
    if (next.get().get_num_selected() < bucket_max) {
      aheap.push_if_nonempty(next);
    }
  }

  /* The end result is that we should have as many buckets covered as
   * possible while respecting up, the primary selection,
   * the pool size (given bucket count constraints),
   * and the mandatory member.
   */
}


bool PeeringState::recoverable(const vector<int> &want) const
{
  unsigned num_want_acting = 0;
  set<pg_shard_t> have;
  for (int i = 0; i < (int)want.size(); ++i) {
    if (want[i] != CRUSH_ITEM_NONE) {
      ++num_want_acting;
      have.insert(
        pg_shard_t(
          want[i],
          pool.info.is_erasure() ? shard_id_t(i) : shard_id_t::NO_SHARD));
    }
  }

  if (num_want_acting < pool.info.min_size) {
    if (!cct->_conf.get_val<bool>("osd_allow_recovery_below_min_size")) {
      psdout(10) << "failed, recovery below min size not enabled" << dendl;
      return false;
    }
  }
  if (missing_loc.get_recoverable_predicate()(have)) {
    return true;
  } else {
    psdout(10) << "failed, not recoverable " << dendl;
    return false;
  }
}

void PeeringState::choose_async_recovery_ec(
  const map<pg_shard_t, pg_info_t> &all_info,
  const pg_info_t &auth_info,
  vector<int> *want,
  set<pg_shard_t> *async_recovery,
  const OSDMapRef osdmap) const
{
  set<pair<int, pg_shard_t> > candidates_by_cost;
  unsigned want_acting_size = 0;
  for (uint8_t i = 0; i < want->size(); ++i) {
    if ((*want)[i] == CRUSH_ITEM_NONE)
      continue;
    ++want_acting_size;

    // Considering log entries to recover is accurate enough for
    // now. We could use minimum_to_decode_with_cost() later if
    // necessary.
    pg_shard_t shard_i((*want)[i], shard_id_t(i));
    // do not include strays
    if (stray_set.find(shard_i) != stray_set.end())
      continue;
    // Do not include an osd that is not up, since choosing it as
    // an async_recovery_target will move it out of the acting set.
    // This results in it being identified as a stray during peering,
    // because it is no longer in the up or acting set.
    if (!is_up(shard_i))
      continue;
    auto shard_info = all_info.find(shard_i)->second;
    // for ec pools we rollback all entries past the authoritative
    // last_update *before* activation. This is relatively inexpensive
    // compared to recovery, since it is purely local, so treat shards
    // past the authoritative last_update the same as those equal to it.
    version_t auth_version = auth_info.last_update.version;
    version_t candidate_version = shard_info.last_update.version;
    ceph_assert(HAVE_FEATURE(osdmap->get_up_osd_features(), SERVER_NAUTILUS));
    auto approx_missing_objects =
      shard_info.stats.stats.sum.num_objects_missing;
    if (auth_version > candidate_version) {
      approx_missing_objects += auth_version - candidate_version;
    }
    if (static_cast<uint64_t>(approx_missing_objects) >
       cct->_conf.get_val<uint64_t>("osd_async_recovery_min_cost")) {
      candidates_by_cost.emplace(approx_missing_objects, shard_i);
    }
  }
  ceph_assert(candidates_by_cost.size() <= want_acting_size);

  psdout(20) << "candidates by cost are: " << candidates_by_cost
	     << dendl;

  // take out as many osds as we can for async recovery, in order of cost
  for (auto rit = candidates_by_cost.rbegin();
       rit != candidates_by_cost.rend(); ++rit) {
    pg_shard_t cur_shard = rit->second;
    vector<int> candidate_want(*want);
    candidate_want[cur_shard.shard.id] = CRUSH_ITEM_NONE;
    ceph_assert(want_acting_size > 0);
    --want_acting_size;
    if ((want_acting_size >= pool.info.min_size) &&
	recoverable(candidate_want)) {
      want->swap(candidate_want);
      async_recovery->insert(cur_shard);
    }
  }
  psdout(20) << "result want=" << *want
	     << " async_recovery=" << *async_recovery << dendl;
}

void PeeringState::choose_async_recovery_replicated(
  const map<pg_shard_t, pg_info_t> &all_info,
  const pg_info_t &auth_info,
  vector<int> *want,
  set<pg_shard_t> *async_recovery,
  const OSDMapRef osdmap) const
{
  set<pair<int, pg_shard_t> > candidates_by_cost;
  for (auto osd_num : *want) {
    pg_shard_t shard_i(osd_num, shard_id_t::NO_SHARD);
    // do not include strays
    if (stray_set.find(shard_i) != stray_set.end())
      continue;
    // Do not include an osd that is not up, since choosing it as
    // an async_recovery_target will move it out of the acting set.
    // This results in it being identified as a stray during peering,
    // because it is no longer in the up or acting set.
    if (!is_up(shard_i))
      continue;
    auto shard_info = all_info.find(shard_i)->second;
    // use the approximate magnitude of the difference in length of
    // logs plus historical missing objects as the cost of recovery
    version_t auth_version = auth_info.last_update.version;
    version_t candidate_version = shard_info.last_update.version;
    ceph_assert(HAVE_FEATURE(osdmap->get_up_osd_features(), SERVER_NAUTILUS));
    auto approx_missing_objects =
      shard_info.stats.stats.sum.num_objects_missing;
    if (auth_version > candidate_version) {
      approx_missing_objects += auth_version - candidate_version;
    } else {
      approx_missing_objects += candidate_version - auth_version;
    }
    if (static_cast<uint64_t>(approx_missing_objects)  >
       cct->_conf.get_val<uint64_t>("osd_async_recovery_min_cost")) {
      candidates_by_cost.emplace(approx_missing_objects, shard_i);
    }
  }

  psdout(20) << "candidates by cost are: " << candidates_by_cost
	     << dendl;
  // take out as many osds as we can for async recovery, in order of cost
  for (auto rit = candidates_by_cost.rbegin();
       rit != candidates_by_cost.rend(); ++rit) {
    if (want->size() <= pool.info.min_size) {
      break;
    }
    pg_shard_t cur_shard = rit->second;
    vector<int> candidate_want(*want);
    for (auto it = candidate_want.begin(); it != candidate_want.end(); ++it) {
      if (*it == cur_shard.osd) {
	candidate_want.erase(it);
	if (pool.info.stretch_set_can_peer(candidate_want, *osdmap, NULL)) {
	  // if we're in stretch mode, we can only remove the osd if it doesn't
	  // break peering limits.
	  want->swap(candidate_want);
	  async_recovery->insert(cur_shard);
	}
	break;
      }
    }
  }

  psdout(20) << "result want=" << *want
	     << " async_recovery=" << *async_recovery << dendl;
}

/**
 * choose acting
 *
 * calculate the desired acting, and request a change with the monitor
 * if it differs from the current acting.
 *
 * if restrict_to_up_acting=true, we filter out anything that's not in
 * up/acting.  in order to lift this restriction, we need to
 *  1) check whether it's worth switching the acting set any time we get
 *     a new pg info (not just here, when recovery finishes)
 *  2) check whether anything in want_acting went down on each new map
 *     (and, if so, calculate a new want_acting)
 *  3) remove the assertion in PG::PeeringState::Active::react(const AdvMap)
 * TODO!
 */
bool PeeringState::choose_acting(pg_shard_t &get_log_shard_id,
				 bool restrict_to_up_acting,
				 bool request_pg_temp_change_only,
				 bool *history_les_bound,
				 bool *repeat_getlog)
{
  map<pg_shard_t, pg_info_t> all_info(peer_info.begin(), peer_info.end());
  all_info[pg_whoami] = info;

  if (cct->_conf->subsys.should_gather<dout_subsys, 10>()) {
    for (auto p = all_info.begin(); p != all_info.end(); ++p) {
      psdout(10) << "all_info osd." << p->first << " "
		 << p->second << dendl;
    }
  }

  auto auth_log_shard = find_best_info(all_info, restrict_to_up_acting,
				       true, history_les_bound);
  auto get_log_shard = find_best_info(all_info, restrict_to_up_acting,
				    false, history_les_bound);

  if ((auth_log_shard == all_info.end()) ||
      (get_log_shard == all_info.end())) {
    if (up != acting) {
      psdout(10) << "no suitable info found (incomplete backfills?),"
		 << " reverting to up" << dendl;
      want_acting = up;
      vector<int> empty;
      pl->queue_want_pg_temp(empty);
    } else {
      psdout(10) << "failed" << dendl;
      ceph_assert(want_acting.empty());
    }
    return false;
  }

  if ((repeat_getlog != nullptr) &&
      (info.last_update < auth_log_shard->second.last_update) &&
      pool.info.is_nonprimary_shard(get_log_shard->first.shard)) {
    // Only EC pools with ec_optimizations enabled:
    // Our log is behind that of the auth_log_shard and
    // the get log shard is a non-primary shard and hence may have
    // a sparse log, get a complete log from the auth_log_shard
    // first then repeat this step in the state machine to work
    // out what has to be rolled backwards
    psdout(10) << "get_log_shard " << get_log_shard->first
	       << " is ahead but is a non_primary shard" << dendl;
    psdout(10) << "auth_log_shard " << auth_log_shard->first
	       << " selected instead" << dendl;
    get_log_shard = auth_log_shard;
    *repeat_getlog = true;
  }

  ceph_assert(!auth_log_shard->second.is_incomplete());
  get_log_shard_id = get_log_shard->first;

  set<pg_shard_t> want_backfill, want_acting_backfill;
  vector<int> want;
  stringstream ss;
  if (pool.info.is_replicated()) {
    auto [primary_shard, oldest_log] = select_replicated_primary(
      auth_log_shard,
      cct->_conf.get_val<uint64_t>(
	"osd_force_auth_primary_missing_objects"),
      up,
      up_primary,
      all_info,
      get_osdmap(),
      ss);
    if (pool.info.is_stretch_pool()) {
      calc_replicated_acting_stretch(
	primary_shard,
	oldest_log,
	get_osdmap()->get_pg_size(info.pgid.pgid),
	acting,
	up,
	up_primary,
	all_info,
	restrict_to_up_acting,
	&want,
	&want_backfill,
	&want_acting_backfill,
	get_osdmap(),
	pool,
	ss);
    } else {
      calc_replicated_acting(
	primary_shard,
	oldest_log,
	get_osdmap()->get_pg_size(info.pgid.pgid),
	acting,
	up,
	up_primary,
	all_info,
	restrict_to_up_acting,
	&want,
	&want_backfill,
	&want_acting_backfill,
	get_osdmap(),
	pool,
	ss);
    }
  } else {
    calc_ec_acting(
      auth_log_shard,
      get_osdmap()->get_pg_size(info.pgid.pgid),
      acting,
      up,
      all_info,
      restrict_to_up_acting,
      &want,
      &want_backfill,
      &want_acting_backfill,
      ss);
  }
  psdout(10) << ss.str() << dendl;

  if (!recoverable(want)) {
    want_acting.clear();
    return false;
  }

  set<pg_shard_t> want_async_recovery;
  if (HAVE_FEATURE(get_osdmap()->get_up_osd_features(), SERVER_MIMIC)) {
    if (pool.info.is_erasure()) {
      choose_async_recovery_ec(
	all_info, auth_log_shard->second, &want, &want_async_recovery,
	get_osdmap());
    } else {
      choose_async_recovery_replicated(
	all_info, auth_log_shard->second, &want, &want_async_recovery,
	get_osdmap());
    }
  }
  while (want.size() > pool.info.size) {
    // async recovery should have taken out as many osds as it can.
    // if not, then always evict the last peer
    // (will get synchronously recovered later)
    psdout(10) << "evicting osd." << want.back()
               << " from oversized want " << want << dendl;
    want.pop_back();
  }
  if ((want != acting) ||
      pool.info.is_nonprimary_shard(pg_whoami.shard)) {
    if (pool.info.is_nonprimary_shard(pg_whoami.shard)) {
      psdout(10) << "shard " << pg_whoami.shard << " cannot be primary, want "
	       << pg_vector_string(want)
	       << " acting " << pg_vector_string(acting)
	       << ", requesting pg_temp change" << dendl;
    } else {
      psdout(10) << "want " << pg_vector_string(want)
	       << " != acting " << pg_vector_string(acting)
	       << ", requesting pg_temp change" << dendl;
    }
    want_acting = want;

    if (!cct->_conf->osd_debug_no_acting_change) {
      if ((want_acting == up) &&
	  !pool.info.is_nonprimary_shard(pg_whoami.shard)) {
	// There can't be any pending backfill if
	// want is the same as crush map up OSDs.
	ceph_assert(want_backfill.empty());
	vector<int> empty;
	pl->queue_want_pg_temp(empty);
      } else
	pl->queue_want_pg_temp(want);
    }
    return false;
  }

  if (request_pg_temp_change_only)
    return true;
  want_acting.clear();
  acting_recovery_backfill = want_acting_backfill;
  acting_recovery_backfill_shard_id_set.clear();
  for (auto &ps : acting_recovery_backfill) {
    if (ps.shard != shard_id_t::NO_SHARD) {
      acting_recovery_backfill_shard_id_set.insert(ps.shard);
    }
  }
  psdout(10) << "acting_recovery_backfill is "
	     << acting_recovery_backfill << dendl;
  ceph_assert(
    backfill_targets.empty() ||
    backfill_targets == want_backfill);
  if (backfill_targets.empty()) {
    // Caller is GetInfo
    backfill_targets = want_backfill;
    backfill_target_shard_id_set.clear();
    for (auto &&i : want_backfill) {
      if (i.shard != shard_id_t::NO_SHARD) {
        backfill_target_shard_id_set.insert(i.shard);
      }
    }
  }
  // Adding !needs_recovery() to let the async_recovery_targets reset after recovery is complete
  ceph_assert(
    async_recovery_targets.empty() ||
    async_recovery_targets == want_async_recovery ||
    !needs_recovery());
  if (async_recovery_targets.empty() || !needs_recovery()) {
    async_recovery_targets = want_async_recovery;
  }
  // Will not change if already set because up would have had to change
  // Verify that nothing in backfill is in stray_set
  for (auto i = want_backfill.begin(); i != want_backfill.end(); ++i) {
    ceph_assert(stray_set.find(*i) == stray_set.end());
  }
  psdout(10) << "choose_acting want=" << want << " backfill_targets="
           << want_backfill << " async_recovery_targets="
           << async_recovery_targets << dendl;
  return true;
}

void PeeringState::log_weirdness()
{
  if (pg_log.get_tail() != info.log_tail)
    pl->get_clog_error() << info.pgid
			   << " info mismatch, log.tail " << pg_log.get_tail()
			   << " != info.log_tail " << info.log_tail;
  if (pg_log.get_head() != info.last_update)
    pl->get_clog_error() << info.pgid
			   << " info mismatch, log.head " << pg_log.get_head()
			   << " != info.last_update " << info.last_update;

  if (!pg_log.get_log().empty()) {
    // sloppy check
    if ((pg_log.get_log().log.begin()->version <= pg_log.get_tail()))
      pl->get_clog_error() << info.pgid
			     << " log bound mismatch, info (tail,head] ("
			     << pg_log.get_tail() << ","
			     << pg_log.get_head() << "]"
			     << " actual ["
			     << pg_log.get_log().log.begin()->version << ","
			     << pg_log.get_log().log.rbegin()->version << "]";
  }

  if (pg_log.get_log().caller_ops.size() > pg_log.get_log().log.size()) {
    pl->get_clog_error() << info.pgid
			   << " caller_ops.size "
			   << pg_log.get_log().caller_ops.size()
			   << " > log size " << pg_log.get_log().log.size();
  }
}

/*
 * Process information from a replica to determine if it could have any
 * objects that i need.
 *
 * TODO: if the missing set becomes very large, this could get expensive.
 * Instead, we probably want to just iterate over our unfound set.
 */
bool PeeringState::search_for_missing(
  const pg_info_t &oinfo, const pg_missing_t &omissing,
  pg_shard_t from,
  PeeringCtxWrapper &ctx)
{
  uint64_t num_unfound_before = missing_loc.num_unfound();
  bool found_missing = missing_loc.add_source_info(
    from, oinfo, omissing, ctx.handle);
  if (found_missing && num_unfound_before != missing_loc.num_unfound())
    pl->publish_stats_to_osd();
  // avoid doing this if the peer is empty.  This is abit of paranoia
  // to avoid doing something rash if add_source_info() above
  // incorrectly decided we found something new. (if the peer has
  // last_update=0'0 that's impossible.)
  if (found_missing &&
      oinfo.last_update != eversion_t()) {
    pg_info_t tinfo(oinfo);
    tinfo.pgid.shard = pg_whoami.shard;
    // add partial write from our info
    tinfo.partial_writes_last_complete = info.partial_writes_last_complete;
    tinfo.partial_writes_last_complete_epoch = info.partial_writes_last_complete_epoch;
    if (info.partial_writes_last_complete.contains(from.shard)) {
      apply_pwlc(info.partial_writes_last_complete[from.shard], from, tinfo);
    }
    if (!tinfo.partial_writes_last_complete.empty()) {
      psdout(20) << "sending info to " << from
		 << " pwlc=e" << tinfo.partial_writes_last_complete_epoch
		 << ":" << tinfo.partial_writes_last_complete
		 << " info=" << tinfo
		 << dendl;
    }
    ctx.send_info(
      from.osd,
      spg_t(info.pgid.pgid, from.shard),
      get_osdmap_epoch(),  // fixme: use lower epoch?
      get_osdmap_epoch(),
      tinfo);
  }
  return found_missing;
}

bool PeeringState::discover_all_missing(
  BufferedRecoveryMessages &rctx)
{
  auto &missing = pg_log.get_missing();
  uint64_t unfound = get_num_unfound();
  bool any = false;  // did we start any queries

  psdout(10) << missing.num_missing() << " missing, "
             << unfound << " unfound"
             << dendl;

  auto m = might_have_unfound.begin();
  auto mend = might_have_unfound.end();
  for (; m != mend; ++m) {
    pg_shard_t peer(*m);

    if (!get_osdmap()->is_up(peer.osd)) {
      psdout(20) << "skipping down osd." << peer << dendl;
      continue;
    }

    if (peer_purged.count(peer)) {
      psdout(20) << "skipping purged osd." << peer << dendl;
      continue;
    }

    auto iter = peer_info.find(peer);
    if (iter != peer_info.end() &&
        (iter->second.is_empty() || iter->second.dne())) {
      // ignore empty peers
      continue;
    }

    // If we've requested any of this stuff, the pg_missing_t information
    // should be on its way.
    // TODO: coalsce requested_* into a single data structure
    if (peer_missing.find(peer) != peer_missing.end()) {
      psdout(20) << ": osd." << peer
		 << ": we already have pg_missing_t" << dendl;
      continue;
    }
    if (peer_log_requested.find(peer) != peer_log_requested.end()) {
      psdout(20) << ": osd." << peer
		 << ": in peer_log_requested" << dendl;
      continue;
    }
    if (peer_missing_requested.find(peer) != peer_missing_requested.end()) {
      psdout(20) << ": osd." << peer
		 << ": in peer_missing_requested" << dendl;
      continue;
    }

    // Request missing
    psdout(10) << ": osd." << peer << ": requesting pg_missing_t"
	       << dendl;
    peer_missing_requested.insert(peer);
    rctx.send_query(
      peer.osd,
      spg_t(info.pgid.pgid, peer.shard),
      pg_query_t(
	pg_query_t::FULLLOG,
	peer.shard, pg_whoami.shard,
	info.history, get_osdmap_epoch()));
    any = true;
  }
  return any;
}

/* Build the might_have_unfound set.
 *
 * This is used by the primary OSD during recovery.
 *
 * This set tracks the OSDs which might have unfound objects that the primary
 * OSD needs. As we receive pg_missing_t from each OSD in might_have_unfound, we
 * will remove the OSD from the set.
 */
void PeeringState::build_might_have_unfound()
{
  ceph_assert(might_have_unfound.empty());
  ceph_assert(is_primary());

  psdout(10) << dendl;

  check_past_interval_bounds();

  might_have_unfound = past_intervals.get_might_have_unfound(
    pg_whoami,
    pool.info.is_erasure());

  // include any (stray) peers
  for (auto p = peer_info.begin(); p != peer_info.end(); ++p)
    might_have_unfound.insert(p->first);

  psdout(15) << ": built " << might_have_unfound << dendl;
}

/**
 * 将本轮 peering 结果转为激活中的 PG：更新待持久化的 PGInfo/PGLog，
 * 为本地事务提交登记 ActivateCommitted，并由 primary 按副本进度发送
 * PGInfo 或 PGLog、建立 missing 的恢复来源；副本侧只完成本地激活收尾。
 */
void PeeringState::activate(
  ObjectStore::Transaction& t,
  epoch_t activation_epoch,
  PeeringCtxWrapper &ctx)
{
  // 只能从尚未 peered 的阶段进入；真正设置 ACTIVE 还要等所有参与方提交确认且 acting 集可写。
  ceph_assert(!is_peered());

  // 本轮已选出可用的 acting 集，不再以 DOWN 状态对外报告。
  state_clear(PG_STATE_DOWN);

  send_notify = false;

  if (is_primary()) {
    // primary 将 peering 期间观察到的 partial-write 完成点定为权威值。
    // Update the epoch so that pwlc used by the primary during
    // peering becomes the definitive copy of pwlc
    info.partial_writes_last_complete_epoch = get_osdmap_epoch();

    // 只有 acting 集可写，才推进本 PG primary 的 started epoch/interval。
    if (acting_set_writeable()) {
      ceph_assert(cct->_conf->osd_find_best_info_ignore_history_les ||
	     info.last_epoch_started <= activation_epoch);
      info.last_epoch_started = activation_epoch;
      info.last_interval_started = info.history.same_interval_since;

      // updating last_epoch_started ensures that last_update will not
      // become divergent after activation completes.
      pg_committed_to = info.last_update;
    }
  } else if (is_acting(pg_whoami)) {
    // acting 副本以 primary 发来的 activation_epoch 为准；不能倒退本地记录。
    /* update last_epoch_started on acting replica to whatever the primary sent
     * unless it's smaller (could happen if we are going peered rather than
     * active, see doc/dev/osd_internals/last_epoch_started.rst) */
    if (info.last_epoch_started < activation_epoch) {
      info.last_epoch_started = activation_epoch;
      info.last_interval_started = info.history.same_interval_since;

      // updating last_epoch_started ensures that last_update will not
      // become divergent after activation completes.
      pg_committed_to = info.last_update;
    }
  }

  // 重新建立 activation 后的运行时恢复基线；这些值尚未表示数据已恢复。
  auto &missing = pg_log.get_missing();

  min_last_complete_ondisk = eversion_t(0,0);  // we don't know (yet)!
  last_update_applied = info.last_update;
  last_rollback_info_trimmed_to_applied = pg_log.get_can_rollback_to();

  need_up_thru = false;

  // 标记 PGInfo/大元数据将随当前事务写入 ObjectStore。
  dirty_info = true;
  dirty_big_info = true; // maybe

  // 不在此处直接把 primary 计入 peer_activated：必须等 t 真正提交后，
  // 才异步投递 ActivateCommitted 事件。
  pl->schedule_event_on_commit(
    t,
    std::make_shared<PGPeeringEvent>(
      get_osdmap_epoch(),
      get_osdmap_epoch(),
      ActivateCommitted(
	get_osdmap_epoch(),
	activation_epoch)));

  // 根据本地 missing 初始化 complete 指针和后续 recovery 的扫描位置。
  if (missing.num_missing() == 0) {
    psdout(10) << "activate - no missing, moving last_complete " << info.last_complete
	     << " -> " << info.last_update << dendl;
    info.last_complete = info.last_update;
    info.stats.stats.sum.num_objects_missing = 0;
    pg_log.reset_recovery_pointers();
  } else {
    psdout(10) << "activate - not complete, " << missing << dendl;
    info.stats.stats.sum.num_objects_missing = missing.num_missing();
    pg_log.activate_not_complete(info, pool.info.allows_ecoptimizations());
  }

  log_weirdness();

  if (is_primary()) {
    // 汇总 OSDMap 中待删除的快照，并排除本 PG 已记录为 purged 的部分；
    // 激活完成后由 on_activate() 启动实际的 snap trim。
    interval_set<snapid_t> to_trim;
    auto& removed_snaps_queue = get_osdmap()->get_removed_snaps_queue();
    auto p = removed_snaps_queue.find(info.pgid.pgid.pool());
    if (p != removed_snaps_queue.end()) {
      dout(20) << "activate - purged_snaps " << info.purged_snaps
	       << " removed_snaps " << p->second
	       << dendl;
      for (auto q : p->second) {
	to_trim.insert(q.first, q.second);
      }
    }
    interval_set<snapid_t> purged;
    purged.intersection_of(to_trim, info.purged_snaps);
    to_trim.subtract(purged);

    ceph_assert(HAVE_FEATURE(upacting_features, SERVER_OCTOPUS));
    // 先生成新 lease，但在所有副本完成 activation 前不安排下一次续租。
    renew_lease(pl->get_mnow());
    // do not schedule until we are actually activated

    // adjust purged_snaps: PG may have been inactive while snaps were pruned
    // from the removed_snaps_queue in the osdmap.  update local purged_snaps
    // reflect only those snaps that we thought were pruned and were still in
    // the queue.
    info.purged_snaps.swap(purged);

    // 旧可读租约不再受已下线 OSD 约束时，清除其时间上界；随后刷新历史中的上界。
    if (prior_readable_down_osds.empty()) {
      dout(10) << "no prior_readable_down_osds to wait on, clearing ub"
	       << dendl;
      clear_prior_readable_until_ub();
    }
    info.history.refresh_prior_readable_until_ub(pl->get_mnow(),
						 prior_readable_until_ub);

    // 逐个初始化参与 shard。根据副本的 last_update 和 backfill 进度，
    // 选择仅发送 PGInfo、发送追赶日志，或从头开始 backfill。
    ceph_assert(!acting_recovery_backfill.empty());
    for (auto i = acting_recovery_backfill.begin();
	 i != acting_recovery_backfill.end();
	 ++i) {
      if (*i == pg_whoami) continue;
      pg_shard_t peer = *i;
      auto pi_it = peer_info.find(peer);
      ceph_assert(pi_it != peer_info.end());
      pg_info_t& pi = pi_it->second;

      psdout(10) << "activate peer osd." << peer << " " << pi << dendl;

      #ifdef WITH_CRIMSON
      MURef<MOSDPGLog> m;
      #else
      MRef<MOSDPGLog> m;
      #endif
      auto pm_it = peer_missing.find(peer);
      ceph_assert(pm_it != peer_missing.end());
      pg_missing_t& pm = pm_it->second;

      // 在修改 pi 前记录它是否不存在；新建 PG 的副本还需要完整 past_intervals。
      bool needs_past_intervals = pi.dne();

      // 保存副本当前数据量，后续申请 backfill 资源时用于空间估算，不能为负。
      peer_bytes[peer] = std::max<int64_t>(0, pi.stats.stats.sum.num_bytes);

      if (pi.last_update == info.last_update) {
        // 副本版本已追平 primary
	if (!pi.last_backfill.is_max())
    // 只是记录日志：该副本的版本已追平，但对象 backfill 尚未结束，因此 activation 后还会继续 backfill。
	  pl->get_clog_info() << info.pgid << " continuing backfill to osd."
				<< peer
				<< " from (" << pi.log_tail << "," << pi.last_update
				<< "] " << pi.last_backfill
				<< " to " << info.last_update;
	if (!pi.is_empty()) {
    // pi 非空：副本已有 PG，只需发送最新 PGInfo + lease。
	  psdout(10) << "activate peer osd." << peer
		     << " is up to date, queueing in pending_activators" << dendl;
          if (!info.partial_writes_last_complete.empty()) {
	    psdout(20) << "sending info to " << peer
		       << " pwlc=e" << info.partial_writes_last_complete_epoch
		       << ":" << info.partial_writes_last_complete
		       << " info=" << info
		       << dendl;
	  }
	  ctx.send_info(
	    peer.osd,
	    spg_t(info.pgid.pgid, peer.shard),
	    get_osdmap_epoch(), // fixme: use lower epoch?
	    get_osdmap_epoch(),
	    info,
	    get_lease());
	} else {
    // pi 为空：它虽然在版本号意义上“追平”（通常两边都是初始版本），但副本端连 PG 实体都还不存在。
    // 此时必须发送 MOSDPGLog，让副本创建/初始化该 PG；不能只发 PGInfo。
	  psdout(10) << "activate peer osd." << peer
		     << " is up to date, but sending pg_log anyway" << dendl;
	  m = TOPNSPC::make_message<MOSDPGLog>(
	    i->shard, pg_whoami.shard,
	    get_osdmap_epoch(), info,
	    last_peering_reset);
	}
      } else if (
	pg_log.get_tail() > pi.last_update ||
	pi.last_backfill == hobject_t() ||
	(backfill_targets.count(*i) && pi.last_backfill.is_max())) {
	/* ^ This last case covers a situation where a replica is not contiguous
	 * with the auth_log, but is contiguous with this replica.  Reshuffling
	 * the active set to handle this would be tricky, so instead we just go
	 * ahead and backfill it anyway.  This is probably preferrable in any
	 * case since the replica in question would have to be significantly
	 * behind.
	 */
	// 副本无法由现有连续 PGLog 追上，重置其进度并从对象 backfill 起点开始。
	pl->get_clog_debug() << info.pgid << " starting backfill to osd." << peer
			       << " from (" << pi.log_tail << "," << pi.last_update
			       << "] " << pi.last_backfill
			       << " to " << info.last_update;

	pi.partial_writes_last_complete = info.partial_writes_last_complete;
	pi.partial_writes_last_complete_epoch = info.partial_writes_last_complete_epoch;
	pi.last_update = info.last_update;
	pi.last_complete = info.last_update;
	pi.set_last_backfill(hobject_t());
	pi.last_epoch_started = info.last_epoch_started;
	pi.last_interval_started = info.last_interval_started;
	pi.history = info.history;
	pi.hit_set = info.hit_set;
        pi.stats.stats.clear();
        pi.stats.stats.sum.num_bytes = peer_bytes[peer];

	// initialize peer with our purged_snaps.
	pi.purged_snaps = info.purged_snaps;

	m = TOPNSPC::make_message<MOSDPGLog>(
	  i->shard, pg_whoami.shard,
	  get_osdmap_epoch(), pi,
	  last_peering_reset /* epoch to create pg at */);

	// send some recent log, so that op dup detection works well.
	m->log.copy_up_to(cct, pg_log.get_log(),
			  cct->_conf->osd_max_pg_log_entries);
	m->info.log_tail = m->log.tail;
	pi.log_tail = m->log.tail;  // sigh...

	pm.clear();
      } else {
	// 副本尚可由 primary 保留的日志连续追上，只发送 last_update 之后的条目。
	ceph_assert(pg_log.get_tail() <= pi.last_update);
	m = TOPNSPC::make_message<MOSDPGLog>(
	  i->shard, pg_whoami.shard,
	  get_osdmap_epoch(), info,
	  last_peering_reset /* epoch to create pg at */);
	// send new stuff to append to replicas log
	m->log.copy_after(cct, pg_log.get_log(), pi.last_update);
      }

      // 新建副本 PG 时附带 past_intervals，使其拥有后续 peering 所需的区间历史。
      // based on whether our info for that peer was dne() *before*
      // updating pi.history in the backfill block above.
      if (m && needs_past_intervals)
	m->past_intervals = past_intervals;

      // primary 同步维护它认为该副本的 missing；这只是内存中的恢复规划，
      // 不是副本已经完成数据同步的确认。
      if (m && pi.last_backfill != hobject_t()) {
        for (auto p = m->log.log.begin(); p != m->log.log.end(); ++p) {
	  if (p->soid <= pi.last_backfill &&
	      !p->is_error()) {
	    if (perform_deletes_during_peering() && p->is_delete()) {
	      pm.rm(p->soid, p->version);
	    } else {
	      pm.add_next_event(*p, pool.info, peer.shard);
	    }
	  }
	}
      }

      if (m) {
	// 带上当前 lease 后异步发送；副本本地提交完成会在之后回传激活信息。
	dout(10) << "activate peer osd." << peer << " sending " << m->log
		 << dendl;
	m->lease = get_lease();
	pl->send_cluster_message(peer.osd, std::move(m), get_osdmap_epoch());
      }

      // 消息携带的日志已使副本逻辑版本追至 primary；last_complete 仍取决于 missing。
      pi.last_update = info.last_update;

      // update our missing
      if (pm.num_missing() == 0) {
	pi.last_complete = pi.last_update;
        psdout(10) << "activate peer osd." << peer << " " << pi
		   << " uptodate" << dendl;
      } else {
        psdout(10) << "activate peer osd." << peer << " " << pi
		   << " missing " << pm << dendl;
      }
    }

    // 汇总 acting/recovery/backfill 各 shard 的 missing，建立“哪个 shard 可能拥有对象”的索引。
    set<pg_shard_t> complete_shards;
    for (auto i = acting_recovery_backfill.begin();
	 i != acting_recovery_backfill.end();
	 ++i) {
      psdout(20) << "setting up missing_loc from shard " << *i
		 << " " << dendl;
      if (*i == get_primary()) {
	missing_loc.add_active_missing(missing);
        if (!missing.have_missing())
          complete_shards.insert(*i);
      } else {
	auto peer_missing_entry = peer_missing.find(*i);
	ceph_assert(peer_missing_entry != peer_missing.end());
	missing_loc.add_active_missing(peer_missing_entry->second);
        if (!peer_missing_entry->second.have_missing() &&
	    peer_info[*i].last_backfill.is_max())
	  complete_shards.insert(*i);
      }
    }

    // 对仍缺失的对象登记可作为恢复源的 shard，并找出可能存在但尚未定位的 unfound 对象。
    // NOTE: It's important that we build might_have_unfound before trimming the
    // past intervals.
    might_have_unfound.clear();
    if (needs_recovery()) {
      // If only one shard has missing, we do a trick to add all others as recovery
      // source, this is considered safe since the PGLogs have been merged locally,
      // and covers vast majority of the use cases, like one OSD/host is down for
      // a while for hardware repairing
      if (complete_shards.size() + 1 == acting_recovery_backfill.size()) {
        missing_loc.add_batch_sources_info(complete_shards, ctx.handle);
      } else {
        missing_loc.add_source_info(pg_whoami, info, pg_log.get_missing(),
				    ctx.handle);
        for (auto i = acting_recovery_backfill.begin();
	     i != acting_recovery_backfill.end();
	     ++i) {
	  if (*i == pg_whoami) continue;
	  psdout(10) << ": adding " << *i << " as a source" << dendl;
	  auto pi_it = peer_info.find(*i);
	  ceph_assert(pi_it != peer_info.end());
	  auto pm_it = peer_missing.find(*i);
	  ceph_assert(pm_it != peer_missing.end());
	  missing_loc.add_source_info(
	    *i,
	    pi_it->second,
	    pm_it->second,
            ctx.handle);
        }
      }
      for (auto i = peer_missing.begin(); i != peer_missing.end(); ++i) {
	if (is_acting_recovery_backfill(i->first))
	  continue;
	auto pi_it = peer_info.find(i->first);
	ceph_assert(pi_it != peer_info.end());
	search_for_missing(
	  pi_it->second,
	  i->second,
	  i->first,
	  ctx);
      }

      build_might_have_unfound();

      // 现在就查询/标记 unfound，使随后计算的 PG degraded 统计准确。
      discover_all_missing(ctx.msgs);

    }

    // acting 数少于 pool 期望副本数时，即使可激活也应报告 UNDERSIZED。
    if (get_osdmap()->get_pg_size(info.pgid.pgid) > actingset.size()) {
      state_set(PG_STATE_UNDERSIZED);
    }

    // primary 进入“等待所有 activation 提交确认”的中间状态；
    // Active::react(AllReplicasActivated) 再按 acting 集是否可写设置 ACTIVE 或 PEERED。
    state_set(PG_STATE_ACTIVATING);
    pl->on_activate(std::move(to_trim));
  } else {
    // 副本直接激活自己
    pl->on_replica_activate();
  }
  if (acting_set_writeable()) {
    // 对可写 acting 集前滚 PGLog；所需对象回滚/删除动作写入同一个事务 t。
    PGLog::LogEntryHandlerRef rollbacker{pl->get_log_handler(t)};
    pg_log.roll_forward(&info, rollbacker.get());
  }
}

void PeeringState::share_pg_info()
{
  psdout(10) << "share_pg_info" << dendl;

  info.history.refresh_prior_readable_until_ub(pl->get_mnow(),
					       prior_readable_until_ub);

  // share new pg_info_t with replicas
  ceph_assert(!acting_recovery_backfill.empty());
  for (auto pg_shard : acting_recovery_backfill) {
    if (pg_shard == pg_whoami) continue;
    if (auto peer = peer_info.find(pg_shard); peer != peer_info.end()) {
      peer->second.last_epoch_started = info.last_epoch_started;
      peer->second.last_interval_started = info.last_interval_started;
      peer->second.history.merge(info.history);
    }
    auto m = TOPNSPC::make_message<MOSDPGInfo2>(spg_t{info.pgid.pgid, pg_shard.shard},
			  info,
			  get_osdmap_epoch(),
			  get_osdmap_epoch(),
			  std::optional<pg_lease_t>{get_lease()},
			  std::nullopt);
    pl->send_cluster_message(pg_shard.osd, std::move(m), get_osdmap_epoch());
  }
}

void PeeringState::merge_log(
  ObjectStore::Transaction& t, pg_info_t &oinfo, pg_log_t&& olog,
  pg_shard_t from)
{
  PGLog::LogEntryHandlerRef rollbacker{pl->get_log_handler(t)};
  pg_log.merge_log(
    oinfo, std::move(olog), from, info, pool.info, pg_whoami,
    rollbacker.get(), dirty_info, dirty_big_info,
    pool.info.allows_ecoptimizations());
}

void PeeringState::rewind_divergent_log(
  ObjectStore::Transaction& t, eversion_t newhead)
{
  PGLog::LogEntryHandlerRef rollbacker{pl->get_log_handler(t)};
  pg_log.rewind_divergent_log(
    newhead, info, rollbacker.get(), dirty_info, dirty_big_info,
    pool.info.allows_ecoptimizations(), pg_whoami);
}


void PeeringState::proc_primary_info(
  ObjectStore::Transaction &t, const pg_info_t &oinfo)
{
  ceph_assert(!is_primary());

  update_history(oinfo.history);
  if (!info.stats.stats_invalid && info.stats.stats.sum.num_scrub_errors) {
    info.stats.stats.sum.num_scrub_errors = 0;
    info.stats.stats.sum.num_shallow_scrub_errors = 0;
    info.stats.stats.sum.num_deep_scrub_errors = 0;
    dirty_info = true;
  }

  if (!(info.purged_snaps == oinfo.purged_snaps)) {
    psdout(10) << "updating purged_snaps to "
	       << oinfo.purged_snaps
	       << dendl;
    info.purged_snaps = oinfo.purged_snaps;
    dirty_info = true;
    dirty_big_info = true;
  }
}

void PeeringState::consider_adjusting_pwlc(eversion_t last_complete)
{
  for (const auto & [shard, versionrange] :
	 info.partial_writes_last_complete) {
    auto [fromversion, toversion] = versionrange;
    if (last_complete > toversion) {
      // Full writes are being rolled forward, eventually
      // partial_write will be called to advance pwlc, but we need
      // to preempt that here before proc_master_log considers
      // rolling forward partial writes
      info.partial_writes_last_complete[shard] = std::pair(last_complete,
							   last_complete);
      psdout(10) << "shard " << shard << " pwlc rolled forward to "
		 << info.partial_writes_last_complete[shard] << dendl;
    } else if (last_complete < toversion) {
      // A divergent update has advanced pwlc adhead of last_complete,
      // roll backwards to the last completed full write and then
      // let proc_master_log roll forward partial writes
      info.partial_writes_last_complete[shard] = std::pair(last_complete,
							   last_complete);
      psdout(10) << "shard " << shard << " pwlc rolled backward to "
		 << info.partial_writes_last_complete[shard] << dendl;
    }
  }
}

void PeeringState::consider_rollback_pwlc(eversion_t last_complete)
{
  for (const auto & [shard, versionrange] :
	 info.partial_writes_last_complete) {
    auto [fromversion, toversion] = versionrange;
    if (last_complete.version < fromversion.version) {
      // It is possible that we need to rollback pwlc, this can happen if
      // peering is attempted with an OSD missing but does not manage to
      // activate (typically because of a wait upthru) before the missing
      // OSD returns
      info.partial_writes_last_complete[shard] = std::pair(last_complete,
							   last_complete);
      psdout(10) << "shard " << shard << " pwlc rolled back to "
		 << info.partial_writes_last_complete[shard] << dendl;
    } else if (last_complete < toversion) {
      info.partial_writes_last_complete[shard].second = last_complete;
      psdout(10) << "shard " << shard << " pwlc rolled back to "
		 << info.partial_writes_last_complete[shard] << dendl;
    }
  }
}

void PeeringState::proc_master_log(
  ObjectStore::Transaction& t, pg_info_t &oinfo,
  pg_log_t&& olog, pg_missing_t&& omissing, pg_shard_t from)
{
  psdout(10) << "proc_master_log for osd." << from << ": "
	     << olog << " " << omissing << dendl;
  ceph_assert(!is_peered() && is_primary());

  if (info.partial_writes_last_complete.contains(from.shard)) {
    apply_pwlc(info.partial_writes_last_complete[from.shard], from, oinfo,
	       &olog);
  }

  bool invalidate_stats = false;

  // For partial writes we may be able to keep some of the divergent entries
  if (pool.info.allows_ecoptimizations() && (olog.head < pg_log.get_head())) {
    // Iterate backwards to divergence
    auto p = pg_log.get_log().log.end();
    while (p != pg_log.get_log().log.begin()) {
      --p;
      if (p->version <= olog.head) {
	break;
      }
    }
    if (p == pg_log.get_log().log.end()) {
      // Empty log - probably due to a PG split - nothing to do
    } else {
      // After a PG split there may be gaps in the log where entries were
      // split to the other PG. This can result in olog.head being ahead
      // of p->version. So long as there are no entries in olog between
      // p->version and olog.head we can still try to wind forward
      // partially written entries
      auto op = olog.log.end();
      if (op == olog.log.begin()) {
	// Other log is emtpy
	if (p->version <= olog.head) {
	  consider_adjusting_pwlc(p->version);
	  ++p;
	} else {
	  consider_adjusting_pwlc(pg_log.get_tail());
	}
      } else if (op->version == p->version) {
	// Normal case - both logs have this entry
	consider_adjusting_pwlc(p->version);
	++p;
      } else if (op->version < p->version) {
	// Last entry in other log is before this entry
	consider_adjusting_pwlc(pg_log.get_tail());
      } else {
	// Other log is ahead of the primary log - give up
	p = pg_log.get_log().log.end();
      }
    }
    // See if we can wind forward partially written entries
    map<pg_shard_t, pg_info_t> all_info(peer_info.begin(), peer_info.end());
    all_info[pg_whoami] = info;
    epoch_t max_last_epoch_started;
    eversion_t min_last_update_acceptable;
    calculate_maxles_and_minlua(all_info,
				max_last_epoch_started,
				min_last_update_acceptable);
    PGLog::LogEntryHandlerRef rollbacker{pl->get_log_handler(t)};
    shard_id_set shards_currently_present;
    for (auto&& [pg_shard, pi] : all_info) {
      if ((pi.last_update >= min_last_update_acceptable) &&
	  (pi.last_epoch_started >= max_last_epoch_started)) {
	shards_currently_present.insert(pg_shard.shard);
      }
    }
    while (p != pg_log.get_log().log.end()) {
      if (p->is_written_shard(from.shard)) {
        psdout(10) << "entry " << p->version << " has written shards "
		   << p->written_shards << " so is divergent" << dendl;
	// This entry was meant to be written on from, this is the first
	// divergent entry
	break;
      }
      // Test if enough shards have the update
      shard_id_set shards_with_update;
      shard_id_set shards_without_update;
      for (auto&& [pg_shard, pi] : all_info) {
	psdout(20) << "version " << p->version
		   << " testing osd " << pg_shard
		   << " written=" << p->written_shards << dendl;
	if (p->is_written_shard(pg_shard.shard)) {
	  if (pi.last_update < p->version) {
	    if (!shards_with_update.contains(pg_shard.shard)) {
	      shards_without_update.insert(pg_shard.shard);
	    }
	  } else {
	    shards_with_update.insert(pg_shard.shard);
	    shards_without_update.erase(pg_shard.shard);
	  }
	}
      }
      psdout(20) << "shards_with_update=" << shards_with_update
		 << " shards_without_update=" << shards_without_update
		 << " shards_currently_present=" << shards_currently_present
		 << dendl;
      if (!shard_id_set::intersection(shards_without_update,
				      shards_currently_present).empty()) {
	// One or more of the currently present shards is missing this write - this
	// is the first divergent entry
	break;
      }
      // This entry can be kept, only shards that didn't participate in
      // the partial write or are not currently present missed the update.
      // If shards are not currently present there are still enough remaining
      // shards to reconstruct the data.
      psdout(20) << "keeping entry " << p->version << dendl;
      invalidate_stats = true;
      eversion_t previous_version;
      if (p == pg_log.get_log().log.begin()) {
	previous_version = pg_log.get_tail();
      } else {
	previous_version = std::prev(p)->version;
      }
      rollbacker.get()->partial_write(&info, previous_version, *p);
      olog.head = p->version;

      // Process the next entry
      ++p;
    }
  }
  // merge log into our own log to build master log.  no need to
  // make any adjustments to their missing map; we are taking their
  // log to be authoritative (i.e., their entries are by definitely
  // non-divergent).
  merge_log(t, oinfo, std::move(olog), from);
  if (info.last_backfill.is_max() &&
      pool.info.is_nonprimary_shard(from.shard)) {
    invalidate_stats = true;
  }
  info.stats.stats_invalid |= invalidate_stats;
  increment_stats_invalidations_counter(invalidate_stats);
  peer_info[from] = oinfo;
  psdout(10) << " peer osd." << from << " now " << oinfo
	     << " " << omissing << dendl;
  might_have_unfound.insert(from);

  // our log is now authoritative - update pwlc information based
  // on the log head
  consider_rollback_pwlc(pg_log.get_head());

  // See doc/dev/osd_internals/last_epoch_started
  if (oinfo.last_epoch_started > info.last_epoch_started) {
    info.last_epoch_started = oinfo.last_epoch_started;
    dirty_info = true;
  }
  if (oinfo.last_interval_started > info.last_interval_started) {
    info.last_interval_started = oinfo.last_interval_started;
    dirty_info = true;
  }
  update_history(oinfo.history);
  ceph_assert(cct->_conf->osd_find_best_info_ignore_history_les ||
	 info.last_epoch_started >= info.history.last_epoch_started);

  peer_missing[from].claim(std::move(omissing));
}

/**
 * 处理副本在 GetMissing 阶段返回的 PGInfo、PG log 和 missing 集合。
 *
 * 权威 PG log 已在 GetLog 阶段确定，本函数不再选择或合并权威历史；
 * 它仅按副本日志修正必要的 EC partial-write 元数据，更新该副本的 pg_info，
 * 并保存其 missing 集合，供后续选择 recovery 来源和判断恢复需求。
 */
void PeeringState::proc_replica_log(
  pg_info_t &oinfo,
  pg_log_t &olog,
  pg_missing_t&& omissing,
  pg_shard_t from)
{
  psdout(10) << "proc_replica_log for osd." << from << ": "
	     << oinfo << " " << olog << " " << omissing << dendl;

  // EC partial write 可能使不同 shard 的 last_complete 不同；
  // 先按本地记录修正 peer 信息和日志，再交给 PGLog 更新副本日志相关状态。
  if (info.partial_writes_last_complete.contains(from.shard)) {
    apply_pwlc(info.partial_writes_last_complete[from.shard], from, oinfo,
	       &olog);
  }

  // 更新 PGLog 对该副本日志的认知；primary 的权威日志不会在此路径被替换。
  pg_log.proc_replica_log(oinfo, olog, omissing, from, pg_whoami, pool.info.allows_ecoptimizations());

  // 保存最新 PGInfo，并更新 peer 的统计、状态和相关元数据。
  peer_info[from] = oinfo;
  update_peer_info(from, oinfo);
  psdout(10) << " peer osd." << from << " now "
	     << oinfo << " " << omissing << dendl;

  // 该副本的日志/missing 已被确认，它可能拥有后续 unfound 对象的某个版本。
  might_have_unfound.insert(from);

  // 输出副本缺失对象的版本关系，便于诊断后续 recovery 的来源选择。
  for (auto i = omissing.get_items().begin();
       i != omissing.get_items().end();
       ++i) {
    psdout(20) << " after missing " << i->first
	       << " need " << i->second.need
	       << " have " << i->second.have << dendl;
  }

  // 转移 missing 集合所有权，形成 peer_missing[from]，
  // 供 needs_recovery() 和 MissingLoc 构建恢复来源时使用。
  peer_missing[from].claim(std::move(omissing));
}

/**
* Update min_last_complete_ondisk to the minimum
* last_complete_ondisk version informed by each peer.
*/
void PeeringState::calc_min_last_complete_ondisk() {
  ceph_assert(!acting_recovery_backfill.empty());
  eversion_t min = last_complete_ondisk;
  for (const auto& pg_shard : acting_recovery_backfill) {
    if (pg_shard == get_primary()) {
      continue;
    }
    if (peer_last_complete_ondisk.count(pg_shard) == 0) {
      psdout(20) <<  "no complete info on: "
                 << pg_shard << dendl;
      return;
    }
    if (peer_last_complete_ondisk[pg_shard] < min) {
      min = peer_last_complete_ondisk[pg_shard];
    }
  }
  if (min != min_last_complete_ondisk) {
    psdout(20) << "last_complete_ondisk is "
               << "updated to: " << min
               << " from: " << min_last_complete_ondisk
               << dendl;
    min_last_complete_ondisk = min;
  }
}

void PeeringState::fulfill_info(
  pg_shard_t from, const pg_query_t &query,
  pair<pg_shard_t, pg_info_t> &notify_info)
{
  ceph_assert(from == primary);
  ceph_assert(query.type == pg_query_t::INFO);

  // info
  psdout(10) << "sending info" << dendl;
  notify_info = make_pair(from, info);
}

void PeeringState::fulfill_log(
  pg_shard_t from, const pg_query_t &query, epoch_t query_epoch)
{
  psdout(10) << "log request from " << from << dendl;
  ceph_assert(from == primary);
  ceph_assert(query.type != pg_query_t::INFO);

  auto mlog = TOPNSPC::make_message<MOSDPGLog>(
    from.shard, pg_whoami.shard,
    get_osdmap_epoch(),
    info, query_epoch);
  mlog->missing = pg_log.get_missing();

  // primary -> other, when building master log
  if (query.type == pg_query_t::LOG) {
    psdout(10) << " sending info+missing+log since " << query.since
	       << dendl;
    if (query.since != eversion_t() && query.since < pg_log.get_tail()) {
      pl->get_clog_error() << info.pgid << " got broken pg_query_t::LOG since "
			     << query.since
			     << " when my log.tail is " << pg_log.get_tail()
			     << ", sending full log instead";
      mlog->log = pg_log.get_log();           // primary should not have requested this!!
    } else
      mlog->log.copy_after(cct, pg_log.get_log(), query.since);
  }
  else if (query.type == pg_query_t::FULLLOG) {
    psdout(10) << " sending info+missing+full log" << dendl;
    mlog->log = pg_log.get_log();
  }

  psdout(10) << " sending " << mlog->log << " " << mlog->missing << dendl;

  pl->send_cluster_message(from.osd, std::move(mlog), get_osdmap_epoch(), true);
}

void PeeringState::fulfill_query(const MQuery& query, PeeringCtxWrapper &rctx)
{
  if (query.query.type == pg_query_t::INFO) {
    pair<pg_shard_t, pg_info_t> notify_info;
    // note this refreshes our prior_readable_until_ub value
    update_history(query.query.history);
    fulfill_info(query.from, query.query, notify_info);
    rctx.send_notify(
      notify_info.first.osd,
      pg_notify_t(
	notify_info.first.shard, pg_whoami.shard,
	query.query_epoch,
	get_osdmap_epoch(),
	notify_info.second,
	past_intervals,
	local_pg_acting_features));
  } else {
    update_history(query.query.history);
    fulfill_log(query.from, query.query, query.query_epoch);
  }
}

void PeeringState::try_mark_clean()
{
  if (actingset.size() == get_osdmap()->get_pg_size(info.pgid.pgid)) {
    state_clear(PG_STATE_FORCED_BACKFILL | PG_STATE_FORCED_RECOVERY);
    state_set(PG_STATE_CLEAN);
    info.history.last_epoch_clean = get_osdmap_epoch();
    info.history.last_interval_clean = info.history.same_interval_since;
    past_intervals.clear();
    dirty_big_info = true;
    dirty_info = true;
  }

  if (!is_active() && is_peered()) {
    if (is_clean()) {
      bool target;
      if (pool.info.is_pending_merge(info.pgid.pgid, &target)) {
	if (target) {
	  psdout(10) << "ready to merge (target)" << dendl;
	  pl->set_ready_to_merge_target(
	    info.last_update,
	    info.history.last_epoch_started,
	    info.history.last_epoch_clean);
	} else {
	  psdout(10) << "ready to merge (source)" << dendl;
	  pl->set_ready_to_merge_source(info.last_update);
	}
      }
    } else {
      psdout(10) << "not clean, not ready to merge" << dendl;
      // we should have notified OSD in Active state entry point
    }
  }

  state_clear(PG_STATE_FORCED_RECOVERY | PG_STATE_FORCED_BACKFILL);

  share_pg_info();
  pl->publish_stats_to_osd();
  clear_recovery_state();
}

void PeeringState::split_into(
  pg_t child_pgid, PeeringState *child, unsigned split_bits)
{
  bool ec_optimizations_enabled = pool.info.allows_ecoptimizations();

  child->update_osdmap_ref(get_osdmap());
  child->pool = pool;

  // Log
  pg_log.split_into(child_pgid, split_bits, &(child->pg_log));
  child->info.last_complete = info.last_complete;

  info.last_update = pg_log.get_head();
  child->info.last_update = child->pg_log.get_head();

  child->info.last_user_version = info.last_user_version;

  // fix up pwlc - it may refer to log entries that are no longer in the log
  child->info.partial_writes_last_complete = info.partial_writes_last_complete;
  child->info.partial_writes_last_complete_epoch = info.partial_writes_last_complete_epoch;
  pg_log.split_pwlc(info);
  child->pg_log.split_pwlc(child->info);

  info.log_tail = pg_log.get_tail();
  child->info.log_tail = child->pg_log.get_tail();

  // reset last_complete, we might have modified pg_log & missing above
  pg_log.reset_complete_to(&info, ec_optimizations_enabled);
  child->pg_log.reset_complete_to(&child->info, ec_optimizations_enabled);

  // Info
  child->info.history = info.history;
  child->info.history.epoch_created = get_osdmap_epoch();
  child->info.purged_snaps = info.purged_snaps;

  if (info.last_backfill.is_max()) {
    child->info.set_last_backfill(hobject_t::get_max());
  } else {
    // restart backfill on parent and child to be safe.  we could
    // probably do better in the bitwise sort case, but it's more
    // fragile (there may be special work to do on backfill completion
    // in the future).
    info.set_last_backfill(hobject_t());
    child->info.set_last_backfill(hobject_t());
    // restarting backfill implies that the missing set is empty,
    // since it is only used for objects prior to last_backfill
    pg_log.reset_backfill();
    child->pg_log.reset_backfill();
  }

  child->info.stats = info.stats;
  child->info.stats.parent_split_bits = split_bits;
  info.stats.stats_invalid = true;
  child->info.stats.stats_invalid = true;
  child->info.stats.objects_trimmed = 0;
  child->info.stats.snaptrim_duration = 0.0;
  child->info.last_epoch_started = info.last_epoch_started;
  child->info.last_interval_started = info.last_interval_started;

  increment_stats_invalidations_counter(info.stats.stats_invalid);
  increment_stats_invalidations_counter(child->info.stats.stats_invalid);

  // There can't be recovery/backfill going on now
  int primary, up_primary;
  vector<int> newup, newacting;
  get_osdmap()->pg_to_up_acting_osds(
    child->info.pgid.pgid, &newup, &up_primary, &newacting, &primary);
  child->init_primary_up_acting(
    newup,
    newacting,
    up_primary,
    primary);
  child->role = OSDMap::calc_pg_role(pg_whoami, child->acting);

  // this comparison includes primary rank via pg_shard_t
  if (get_primary() != child->get_primary())
    child->info.history.same_primary_since = get_osdmap_epoch();

  child->info.stats.up = newup;
  child->info.stats.up_primary = up_primary;
  child->info.stats.acting = newacting;
  child->info.stats.acting_primary = primary;
  child->info.stats.mapping_epoch = get_osdmap_epoch();

  // History
  child->past_intervals = past_intervals;

  child->on_new_interval();

  child->send_notify = !child->is_primary();

  child->dirty_info = true;
  child->dirty_big_info = true;
  dirty_info = true;
  dirty_big_info = true;
}

void PeeringState::merge_from(
  map<spg_t,PeeringState *>& sources,
  PeeringCtx &rctx,
  unsigned split_bits,
  const pg_merge_meta_t& last_pg_merge_meta)
{
  bool incomplete = false;
  if (info.last_complete != info.last_update ||
      info.is_incomplete() ||
      info.dne()) {
    psdout(10) << "target incomplete" << dendl;
    incomplete = true;
  }
  if (last_pg_merge_meta.source_pgid != pg_t()) {
    if (info.pgid.pgid != last_pg_merge_meta.source_pgid.get_parent()) {
      psdout(10) << "target doesn't match expected parent "
		 << last_pg_merge_meta.source_pgid.get_parent()
		 << " of source_pgid " << last_pg_merge_meta.source_pgid
		 << dendl;
      incomplete = true;
    }
    if (info.last_update != last_pg_merge_meta.target_version) {
      psdout(10) << "target version doesn't match expected "
	       << last_pg_merge_meta.target_version << dendl;
      incomplete = true;
    }
  }

  PGLog::LogEntryHandlerRef handler{pl->get_log_handler(rctx.transaction)};
  pg_log.roll_forward(&info, handler.get());

  info.last_complete = info.last_update;  // to fake out trim()
  pg_log.reset_recovery_pointers();
  pg_log.trim(info.last_update, info);

  vector<PGLog*> log_from;
  for (auto& i : sources) {
    auto& source = i.second;
    if (!source) {
      psdout(10) << "source " << i.first << " missing" << dendl;
      incomplete = true;
      continue;
    }
    if (source->info.last_complete != source->info.last_update ||
	source->info.is_incomplete() ||
	source->info.dne()) {
      psdout(10) << "source " << source->pg_whoami
		 << " incomplete"
		 << dendl;
      incomplete = true;
    }
    if (last_pg_merge_meta.source_pgid != pg_t()) {
      if (source->info.pgid.pgid != last_pg_merge_meta.source_pgid) {
	dout(10) << "source " << source->info.pgid.pgid
		 << " doesn't match expected source pgid "
		 << last_pg_merge_meta.source_pgid << dendl;
	incomplete = true;
      }
      if (source->info.last_update != last_pg_merge_meta.source_version) {
	dout(10) << "source version doesn't match expected "
		 << last_pg_merge_meta.target_version << dendl;
	incomplete = true;
      }
    }

    // prepare log
    PGLog::LogEntryHandlerRef handler{
      source->pl->get_log_handler(rctx.transaction)};
    source->pg_log.roll_forward(&info, handler.get());
    source->info.last_complete = source->info.last_update;  // to fake out trim()
    source->pg_log.reset_recovery_pointers();
    source->pg_log.trim(source->info.last_update, source->info);
    log_from.push_back(&source->pg_log);

    // combine stats
    info.stats.add(source->info.stats);

    // pull up last_update
    info.last_update = std::max(info.last_update, source->info.last_update);

    // adopt source's PastIntervals if target has none.  we can do this since
    // pgp_num has been reduced prior to the merge, so the OSD mappings for
    // the PGs are identical.
    if (past_intervals.empty() && !source->past_intervals.empty()) {
      psdout(10) << "taking source's past_intervals" << dendl;
      past_intervals = source->past_intervals;
    }

    // merge pwlc - reset
    if (!info.partial_writes_last_complete.empty()) {
      for (auto &&[shard, versionrange] :
	   info.partial_writes_last_complete) {
	auto &&[old_v,  new_v] = versionrange;
	old_v = new_v = info.last_update;
      }
      info.partial_writes_last_complete_epoch = get_osdmap_epoch();
      psdout(10) << "merged pwlc=e" << info.partial_writes_last_complete_epoch
		 << ":" << info.partial_writes_last_complete << dendl;
    }
  }

  info.last_complete = info.last_update;
  info.log_tail = info.last_update;
  if (incomplete) {
    info.last_backfill = hobject_t();
  }

  // merge logs
  pg_log.merge_from(log_from, info.last_update);

  // make sure we have a meaningful last_epoch_started/clean (if we were a
  // placeholder)
  if (info.history.epoch_created == 0) {
    // start with (a) source's history, since these PGs *should* have been
    // remapped in concert with each other...
    info.history = sources.begin()->second->info.history;

    // we use the last_epoch_{started,clean} we got from
    // the caller, which are the epochs that were reported by the PGs were
    // found to be ready for merge.
    info.history.last_epoch_clean = last_pg_merge_meta.last_epoch_clean;
    info.history.last_epoch_started = last_pg_merge_meta.last_epoch_started;
    info.last_epoch_started = last_pg_merge_meta.last_epoch_started;
    psdout(10) << "set les/c to "
	       << last_pg_merge_meta.last_epoch_started << "/"
	       << last_pg_merge_meta.last_epoch_clean
	       << " from pool last_dec_*, source pg history was "
	       << sources.begin()->second->info.history
	       << dendl;

    // above we have pulled down source's history and we need to check
    // history.epoch_created again to confirm that source is not a placeholder
    // too. (peering requires a sane history.same_interval_since value for any
    // non-newly created pg and below here we know we are basically iterating
    // back a series of past maps to fake a merge process, hence we need to
    // fix history.same_interval_since first so that start_peering_interval()
    // will not complain)
    if (info.history.epoch_created == 0) {
      dout(10) << "both merge target and source are placeholders,"
               << " set sis to lec " << info.history.last_epoch_clean
               << dendl;
      info.history.same_interval_since = info.history.last_epoch_clean;
    }

    // if the past_intervals start is later than last_epoch_clean, it
    // implies the source repeered again but the target didn't, or
    // that the source became clean in a later epoch than the target.
    // avoid the discrepancy but adjusting the interval start
    // backwards to match so that check_past_interval_bounds() will
    // not complain.
    auto pib = past_intervals.get_bounds();
    if (info.history.last_epoch_clean < pib.first) {
      psdout(10) << "last_epoch_clean "
		 << info.history.last_epoch_clean << " < past_interval start "
		 << pib.first << ", adjusting start backwards" << dendl;
      past_intervals.adjust_start_backwards(info.history.last_epoch_clean);
    }

    // Similarly, if the same_interval_since value is later than
    // last_epoch_clean, the next interval change will result in a
    // past_interval start that is later than last_epoch_clean.  This
    // can happen if we use the pg_history values from the merge
    // source.  Adjust the same_interval_since value backwards if that
    // happens.  (We trust the les and lec values more because they came from
    // the real target, whereas the history value we stole from the source.)
    if (info.history.last_epoch_started < info.history.same_interval_since) {
      psdout(10) << "last_epoch_started "
		 << info.history.last_epoch_started << " < same_interval_since "
		 << info.history.same_interval_since
		 << ", adjusting pg_history backwards" << dendl;
      info.history.same_interval_since = info.history.last_epoch_clean;
      // make sure same_{up,primary}_since are <= same_interval_since
      info.history.same_up_since = std::min(
	info.history.same_up_since, info.history.same_interval_since);
      info.history.same_primary_since = std::min(
	info.history.same_primary_since, info.history.same_interval_since);
    }
  }

  dirty_info = true;
  dirty_big_info = true;
}

void PeeringState::start_split_stats(
  const set<spg_t>& childpgs, vector<object_stat_sum_t> *out)
{
  out->resize(childpgs.size() + 1);
  info.stats.stats.sum.split(*out);
}

void PeeringState::finish_split_stats(
  const object_stat_sum_t& stats, ObjectStore::Transaction &t)
{
  info.stats.stats.sum = stats;
  write_if_dirty(t);
}

void PeeringState::update_blocked_by()
{
  // set a max on the number of blocking peers we report. if we go
  // over, report a random subset.  keep the result sorted.
  unsigned keep = std::min<unsigned>(
    blocked_by.size(), cct->_conf->osd_max_pg_blocked_by);
  unsigned skip = blocked_by.size() - keep;
  info.stats.blocked_by.clear();
  info.stats.blocked_by.resize(keep);
  unsigned pos = 0;
  for (auto p = blocked_by.begin(); p != blocked_by.end() && keep > 0; ++p) {
    if (skip > 0 && (rand() % (skip + keep) < skip)) {
      --skip;
    } else {
      info.stats.blocked_by[pos++] = *p;
      --keep;
    }
  }
}

static bool find_shard(const set<pg_shard_t> & pgs, shard_id_t shard)
{
    for (auto&p : pgs)
      if (p.shard == shard)
        return true;
    return false;
}

static pg_shard_t get_another_shard(const set<pg_shard_t> & pgs, pg_shard_t skip, shard_id_t shard)
{
    for (auto&p : pgs) {
      if (p == skip)
        continue;
      if (p.shard == shard)
        return p;
    }
    return pg_shard_t();
}

void PeeringState::update_calc_stats()
{
  info.stats.version = info.last_update;
  info.stats.created = info.history.epoch_created;
  info.stats.last_scrub = info.history.last_scrub;
  info.stats.last_scrub_stamp = info.history.last_scrub_stamp;
  info.stats.last_deep_scrub = info.history.last_deep_scrub;
  info.stats.last_deep_scrub_stamp = info.history.last_deep_scrub_stamp;
  info.stats.last_clean_scrub_stamp = info.history.last_clean_scrub_stamp;
  info.stats.last_epoch_clean = info.history.last_epoch_clean;

  info.stats.log_size = pg_log.get_head().version - pg_log.get_tail().version;
  info.stats.log_dups_size = pg_log.get_log().dups.size();
  info.stats.ondisk_log_size = info.stats.log_size;
  info.stats.log_start = pg_log.get_tail();
  info.stats.ondisk_log_start = pg_log.get_tail();
  info.stats.snaptrimq_len = pl->get_snap_trimq_size();

  unsigned num_shards = get_osdmap()->get_pg_size(info.pgid.pgid);

  // In rare case that upset is too large (usually transient), use as target
  // for calculations below.
  unsigned target = std::max(num_shards, (unsigned)upset.size());
  // For undersized actingset may be larger with OSDs out
  unsigned nrep = std::max(actingset.size(), upset.size());
  // calc num_object_copies
  info.stats.stats.calc_copies(std::max(target, nrep));
  info.stats.stats.sum.num_objects_degraded = 0;
  info.stats.stats.sum.num_objects_unfound = 0;
  info.stats.stats.sum.num_objects_misplaced = 0;
  info.stats.avail_no_missing.clear();
  info.stats.object_location_counts.clear();

  // We should never hit this condition, but if end up hitting it,
  // make sure to update num_objects and set PG_STATE_INCONSISTENT.
  if (info.stats.stats.sum.num_objects < 0) {
    psdout(0) << "negative num_objects = "
              << info.stats.stats.sum.num_objects << " setting it to 0 "
              << dendl;
    info.stats.stats.sum.num_objects = 0;
    state_set(PG_STATE_INCONSISTENT);
  }

  if ((is_remapped() || is_undersized() || !is_clean()) &&
      (is_peered()|| is_activating())) {
    psdout(20) << "actingset " << actingset << " upset "
	       << upset << " acting_recovery_backfill " << acting_recovery_backfill << dendl;

    ceph_assert(!acting_recovery_backfill.empty());

    bool estimate = false;

    // NOTE: we only generate degraded, misplaced and unfound
    // values for the summation, not individual stat categories.
    int64_t num_objects = info.stats.stats.sum.num_objects;

    // Objects missing from up nodes, sorted by # objects.
    boost::container::flat_set<pair<int64_t,pg_shard_t>> missing_target_objects;
    // Objects missing from nodes not in up, sort by # objects
    boost::container::flat_set<pair<int64_t,pg_shard_t>> acting_source_objects;

    // Fill missing_target_objects/acting_source_objects

    {
      int64_t missing;

      // Primary first
      missing = pg_log.get_missing().num_missing();
      ceph_assert(acting_recovery_backfill.count(pg_whoami));
      if (upset.count(pg_whoami)) {
        missing_target_objects.emplace(missing, pg_whoami);
      } else {
        acting_source_objects.emplace(missing, pg_whoami);
      }
      info.stats.stats.sum.num_objects_missing_on_primary = missing;
      if (missing == 0)
        info.stats.avail_no_missing.push_back(pg_whoami);
      psdout(20) << "shard " << pg_whoami
		 << " primary objects " << num_objects
		 << " missing " << missing
		 << dendl;
    }

    // All other peers
    for (auto& peer : peer_info) {
      // Primary should not be in the peer_info, skip if it is.
      if (peer.first == pg_whoami) continue;
      int64_t missing = 0;
      int64_t peer_num_objects =
        std::max((int64_t)0, peer.second.stats.stats.sum.num_objects);
      // Backfill targets always track num_objects accurately
      // all other peers track missing accurately.
      if (is_backfill_target(peer.first)) {
        missing = std::max((int64_t)0, num_objects - peer_num_objects);
      } else {
	auto pm_it = peer_missing.find(peer.first);
        if (pm_it != peer_missing.end()) {
          missing = pm_it->second.num_missing();
        } else {
          psdout(20) << "no peer_missing found for "
		     << peer.first << dendl;
          if (is_recovering()) {
            estimate = true;
          }
          missing = std::max((int64_t)0, num_objects - peer_num_objects);
        }
      }
      if (upset.count(peer.first)) {
	missing_target_objects.emplace(missing, peer.first);
      } else if (actingset.count(peer.first)) {
	acting_source_objects.emplace(missing, peer.first);
      }
      peer.second.stats.stats.sum.num_objects_missing = missing;
      if (missing == 0)
        info.stats.avail_no_missing.push_back(peer.first);
      psdout(20) << "shard " << peer.first
		 << " objects " << peer_num_objects
		 << " missing " << missing
		 << dendl;
    }

    // Compute object_location_counts
    for (auto& ml: missing_loc.get_missing_locs()) {
      info.stats.object_location_counts[ml.second]++;
      psdout(30) << ml.first << " object_location_counts["
		 << ml.second << "]=" << info.stats.object_location_counts[ml.second]
		 << dendl;
    }
    int64_t not_missing = num_objects - missing_loc.get_missing_locs().size();
    if (not_missing) {
	// During recovery we know upset == actingset and is being populated
	// During backfill we know that all non-missing objects are in the actingset
        info.stats.object_location_counts[actingset] = not_missing;
    }
    psdout(30) << "object_location_counts["
	       << upset << "]=" << info.stats.object_location_counts[upset]
	       << dendl;
    psdout(20) << "object_location_counts "
	       << info.stats.object_location_counts << dendl;

    // A misplaced object is not stored on the correct OSD
    int64_t misplaced = 0;
    // a degraded objects has fewer replicas or EC shards than the pool specifies.
    int64_t degraded = 0;

    if (is_recovering()) {
      for (auto& sml: missing_loc.get_missing_by_count()) {
        for (auto& ml: sml.second) {
          int missing_shards;
          if (sml.first == shard_id_t::NO_SHARD) {
            psdout(20) << "ml " << ml.second
		       << " upset size " << upset.size()
		       << " up " << ml.first.up << dendl;
            missing_shards = (int)upset.size() - ml.first.up;
          } else {
	    // Handle shards not even in upset below
            if (!find_shard(upset, sml.first))
	      continue;
	    missing_shards = std::max(0, 1 - ml.first.up);
            psdout(20) << "shard " << sml.first
		       << " ml " << ml.second
		       << " missing shards " << missing_shards << dendl;
          }
          int odegraded = ml.second * missing_shards;
          // Copies on other osds but limited to the possible degraded
          int more_osds = std::min(missing_shards, ml.first.other);
          int omisplaced = ml.second * more_osds;
          ceph_assert(omisplaced <= odegraded);
          odegraded -= omisplaced;

          misplaced += omisplaced;
          degraded += odegraded;
        }
      }

      psdout(20) << "missing based degraded "
		 << degraded << dendl;
      psdout(20) << "missing based misplaced "
		 << misplaced << dendl;

      // Handle undersized case
      if (pool.info.is_replicated()) {
        // Add degraded for missing targets (num_objects missing)
        ceph_assert(target >= upset.size());
        unsigned needed = target - upset.size();
        degraded += num_objects * needed;
      } else {
        for (unsigned i = 0 ; i < num_shards; ++i) {
          shard_id_t shard(i);

          if (!find_shard(upset, shard)) {
            pg_shard_t pgs = get_another_shard(actingset, pg_shard_t(), shard);

            if (pgs != pg_shard_t()) {
              int64_t missing;

              if (pgs == pg_whoami)
                missing = info.stats.stats.sum.num_objects_missing_on_primary;
              else
                missing = peer_info[pgs].stats.stats.sum.num_objects_missing;

              degraded += missing;
              misplaced += std::max((int64_t)0, num_objects - missing);
            } else {
              // No shard anywhere
              degraded += num_objects;
            }
          }
        }
      }
      goto out;
    }

    // Handle undersized case
    if (pool.info.is_replicated()) {
      // Add to missing_target_objects
      ceph_assert(target >= missing_target_objects.size());
      unsigned needed = target - missing_target_objects.size();
      if (needed)
        missing_target_objects.emplace(num_objects * needed, pg_shard_t(pg_shard_t::NO_OSD));
    } else {
      for (unsigned i = 0 ; i < num_shards; ++i) {
        shard_id_t shard(i);
	bool found = false;
	for (const auto& t : missing_target_objects) {
	  if (std::get<1>(t).shard == shard) {
	    found = true;
	    break;
	  }
	}
	if (!found)
	  missing_target_objects.emplace(num_objects, pg_shard_t(pg_shard_t::NO_OSD,shard));
      }
    }

    for (const auto& item : missing_target_objects)
      psdout(20) << "missing shard " << std::get<1>(item)
		 << " missing= " << std::get<0>(item) << dendl;
    for (const auto& item : acting_source_objects)
      psdout(20) << "acting shard " << std::get<1>(item)
		 << " missing= " << std::get<0>(item) << dendl;

    // Handle all objects not in missing for remapped
    // or backfill
    for (auto m = missing_target_objects.rbegin();
        m != missing_target_objects.rend(); ++m) {

      int64_t extra_missing = -1;

      if (pool.info.is_replicated()) {
	if (!acting_source_objects.empty()) {
	  auto extra_copy = acting_source_objects.begin();
	  extra_missing = std::get<0>(*extra_copy);
          acting_source_objects.erase(extra_copy);
	}
      } else {	// Erasure coded
	// Use corresponding shard
	for (const auto& a : acting_source_objects) {
	  if (std::get<1>(a).shard == std::get<1>(*m).shard) {
	    extra_missing = std::get<0>(a);
	    acting_source_objects.erase(a);
	    break;
	  }
	}
      }

      if (extra_missing >= 0 && std::get<0>(*m) >= extra_missing) {
	// We don't know which of the objects on the target
	// are part of extra_missing so assume are all degraded.
	misplaced += std::get<0>(*m) - extra_missing;
	degraded += extra_missing;
      } else {
	// 1. extra_missing == -1, more targets than sources so degraded
	// 2. extra_missing > std::get<0>(m), so that we know that some extra_missing
	//    previously degraded are now present on the target.
	degraded += std::get<0>(*m);
      }
    }
    // If there are still acting that haven't been accounted for
    // then they are misplaced
    for (const auto& a : acting_source_objects) {
      int64_t extra_misplaced = std::max((int64_t)0, num_objects - std::get<0>(a));
      psdout(20) << "extra acting misplaced " << extra_misplaced
		 << dendl;
      misplaced += extra_misplaced;
    }
out:
    // NOTE: Tests use these messages to verify this code
    psdout(20) << "degraded " << degraded
	       << (estimate ? " (est)": "") << dendl;
    psdout(20) << "misplaced " << misplaced
	       << (estimate ? " (est)": "")<< dendl;

    info.stats.stats.sum.num_objects_degraded = degraded;
    info.stats.stats.sum.num_objects_unfound = get_num_unfound();
    info.stats.stats.sum.num_objects_misplaced = misplaced;
  }
}

std::optional<pg_stat_t> PeeringState::prepare_stats_for_publish(
  const std::optional<pg_stat_t> &pg_stats_publish,
  const object_stat_collection_t &unstable_stats)
{
  if (info.stats.stats.sum.num_scrub_errors) {
    psdout(10) << "inconsistent due to " <<
      info.stats.stats.sum.num_scrub_errors << " scrub errors" << dendl;
    state_set(PG_STATE_INCONSISTENT);
  } else {
    state_clear(PG_STATE_INCONSISTENT);
    state_clear(PG_STATE_FAILED_REPAIR);
  }

  utime_t now = ceph_clock_now();
  if (info.stats.state != state) {
    info.stats.last_change = now;
    // Optimistic estimation, if we just find out an inactive PG,
    // assume it is active till now.
    if (!(state & PG_STATE_ACTIVE) &&
	(info.stats.state & PG_STATE_ACTIVE))
      info.stats.last_active = now;

    if ((state & PG_STATE_ACTIVE) &&
	!(info.stats.state & PG_STATE_ACTIVE))
      info.stats.last_became_active = now;
    if ((state & (PG_STATE_ACTIVE|PG_STATE_PEERED)) &&
	!(info.stats.state & (PG_STATE_ACTIVE|PG_STATE_PEERED)))
      info.stats.last_became_peered = now;
    info.stats.state = state;
  }

  update_calc_stats();
  if (info.stats.stats.sum.num_objects_degraded) {
    state_set(PG_STATE_DEGRADED);
  } else {
    state_clear(PG_STATE_DEGRADED);
  }
  update_blocked_by();

  pg_stat_t pre_publish = info.stats;
  pre_publish.stats.add(unstable_stats);

  // share (some of) our purged_snaps via the pg_stats. limit # of intervals
  // because we don't want to make the pg_stat_t structures too expensive.
  unsigned max = cct->_conf->osd_max_snap_prune_intervals_per_epoch;
  unsigned num = 0;
  auto i = info.purged_snaps.begin();
  while (num < max && i != info.purged_snaps.end()) {
    pre_publish.purged_snaps.insert(i.get_start(), i.get_len());
    ++num;
    ++i;
  }
  psdout(20) << "reporting purged_snaps "
	     << pre_publish.purged_snaps << dendl;

  // when there is no change in osdmap,
  // update info.stats.reported_epoch by the number of time seconds.
  utime_t cutoff_time = now;
  cutoff_time -= *osd_pg_stat_report_interval_max_seconds;
  const bool is_time_expired = cutoff_time > info.stats.last_fresh;

  // 500 epoch osdmaps are also the minimum number of osdmaps that mon must retain.
  // if info.stats.reported_epoch less than current osdmap epoch exceeds 500 osdmaps,
  // it can be considered that the one reported by pgid is too old and needs to be updated.
  // to facilitate mon trim osdmaps
  epoch_t cutoff_epoch = info.stats.reported_epoch;
  cutoff_epoch += *osd_pg_stat_report_interval_max_epochs;
  const bool is_epoch_behind = cutoff_epoch < get_osdmap_epoch();

  if (pg_stats_publish && pre_publish == *pg_stats_publish &&
      (!is_epoch_behind && !is_time_expired)) {
    psdout(15) << "publish_stats_to_osd " << pg_stats_publish->reported_epoch
	       << ": no change since " << info.stats.last_fresh << dendl;
    return std::nullopt;
  } else {
    // update our stat summary and timestamps
    info.stats.reported_epoch = get_osdmap_epoch();
    ++info.stats.reported_seq;

    info.stats.last_fresh = now;

    if (info.stats.state & PG_STATE_CLEAN)
      info.stats.last_clean = now;
    if (info.stats.state & PG_STATE_ACTIVE)
      info.stats.last_active = now;
    if (info.stats.state & (PG_STATE_ACTIVE|PG_STATE_PEERED))
      info.stats.last_peered = now;
    if ((info.stats.state & PG_STATE_STALE) == 0)
      info.stats.last_unstale = now;
    if ((info.stats.state & PG_STATE_DEGRADED) == 0)
      info.stats.last_undegraded = now;
    if ((info.stats.state & PG_STATE_UNDERSIZED) == 0)
      info.stats.last_fullsized = now;

    psdout(15) << "publish_stats_to_osd " << pre_publish.reported_epoch
	       << ":" << pre_publish.reported_seq << dendl;
    return std::make_optional(std::move(pre_publish));
  }
}

void PeeringState::increment_stats_invalidations_counter(bool invalidation_state) {
  if (invalidation_state) {
    pl->get_peering_perf().inc(rs_stats_invalidated);
  }
}

void PeeringState::init(
  int role,
  const vector<int>& newup, int new_up_primary,
  const vector<int>& newacting, int new_acting_primary,
  const pg_history_t& history,
  const PastIntervals& pi,
  ObjectStore::Transaction &t)
{
  psdout(10) << "init role " << role << " up "
	     << newup << " acting " << newacting
	     << " history " << history
	     << " past_intervals " << pi
	     << dendl;

  set_role(role);
  init_primary_up_acting(
    newup,
    newacting,
    new_up_primary,
    new_acting_primary);

  info.history = history;
  past_intervals = pi;

  info.stats.up = up;
  info.stats.up_primary = new_up_primary;
  info.stats.acting = acting;
  info.stats.acting_primary = new_acting_primary;
  info.stats.mapping_epoch = info.history.same_interval_since;

  if (!perform_deletes_during_peering()) {
    pg_log.set_missing_may_contain_deletes();
  }

  on_new_interval();

  dirty_info = true;
  dirty_big_info = true;
  write_if_dirty(t);
}

void PeeringState::dump_peering_state(Formatter *f)
{
  f->dump_string("state", get_pg_state_string());
  f->dump_unsigned("epoch", get_osdmap_epoch());
  f->open_array_section("up");
  for (auto p = up.begin(); p != up.end(); ++p)
    f->dump_unsigned("osd", *p);
  f->close_section();
  f->open_array_section("acting");
  for (auto p = acting.begin(); p != acting.end(); ++p)
    f->dump_unsigned("osd", *p);
  f->close_section();
  if (!backfill_targets.empty()) {
    f->open_array_section("backfill_targets");
    for (auto p = backfill_targets.begin(); p != backfill_targets.end(); ++p)
      f->dump_stream("shard") << *p;
    f->close_section();
  }
  if (!async_recovery_targets.empty()) {
    f->open_array_section("async_recovery_targets");
    for (auto p = async_recovery_targets.begin();
	 p != async_recovery_targets.end();
	 ++p)
      f->dump_stream("shard") << *p;
    f->close_section();
  }
  if (!acting_recovery_backfill.empty()) {
    f->open_array_section("acting_recovery_backfill");
    for (auto p = acting_recovery_backfill.begin();
	 p != acting_recovery_backfill.end();
	 ++p)
      f->dump_stream("shard") << *p;
    f->close_section();
  }
  f->open_object_section("info");
  update_calc_stats();
  info.dump(f);
  f->close_section();

  f->open_array_section("peer_info");
  for (auto p = peer_info.begin(); p != peer_info.end(); ++p) {
    f->open_object_section("info");
    f->dump_stream("peer") << p->first;
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();
}

void PeeringState::update_stats(
  std::function<bool(pg_history_t &, pg_stat_t &)> f,
  ObjectStore::Transaction *t) {
  bool previous_stats_invalidation = info.stats.stats_invalid;
  if (f(info.history, info.stats)) {
    pl->publish_stats_to_osd();
  }

  if (previous_stats_invalidation != info.stats.stats_invalid) {
    increment_stats_invalidations_counter(info.stats.stats_invalid);
  }

  if (t) {
    dirty_info = true;
    write_if_dirty(*t);
  }
}

void PeeringState::update_stats_wo_resched(
  std::function<void(pg_history_t &, pg_stat_t &)> f)
{
  f(info.history, info.stats);
}

bool PeeringState::append_log_entries_update_missing(
  const mempool::osd_pglog::list<pg_log_entry_t> &entries,
  ObjectStore::Transaction &t, std::optional<eversion_t> trim_to,
  std::optional<eversion_t> pg_committed_to)
{
  ceph_assert(!entries.empty());
  ceph_assert(entries.begin()->version > info.last_update);

  PGLog::LogEntryHandlerRef rollbacker{pl->get_log_handler(t)};
  bool invalidate_stats =
    pg_log.append_new_log_entries(
      info.last_backfill,
      entries,
      &info,
      rollbacker.get(),
      pool.info,
      pg_whoami.shard,
      pool.info.allows_ecoptimizations());

  if (pg_committed_to && entries.rbegin()->soid > info.last_backfill) {
    pg_log.roll_forward(&info, rollbacker.get());
  }
  if (pg_committed_to && *pg_committed_to > pg_log.get_can_rollback_to()) {
    pg_log.roll_forward_to(*pg_committed_to, &info, rollbacker.get());
    last_rollback_info_trimmed_to_applied = *pg_committed_to;
  }

  info.last_update = pg_log.get_head();

  if (pg_log.get_missing().num_missing() == 0) {
    // advance last_complete since nothing else is missing!
    info.last_complete = info.last_update;
  }
  info.stats.stats_invalid = info.stats.stats_invalid || invalidate_stats;
  increment_stats_invalidations_counter(invalidate_stats);
  psdout(20) << "trim_to bool = " << bool(trim_to)
	     << " trim_to = " << (trim_to ? *trim_to : eversion_t()) << dendl;
  if (trim_to) {
    eversion_t trim = *trim_to;
    if (pool.info.allows_ecoptimizations() &&
	(trim > pg_log.get_can_rollback_to())) {
      // An exceptionally long sequence of partial writes followed by a full
      // write can result in trim_to being ahead of crt
      trim = pg_log.get_can_rollback_to();
    }
    pg_log.trim(trim, info);
  }
  dirty_info = true;
  write_if_dirty(t);
  return invalidate_stats;
}

void PeeringState::merge_new_log_entries(
  const mempool::osd_pglog::list<pg_log_entry_t> &entries,
  ObjectStore::Transaction &t,
  std::optional<eversion_t> trim_to,
  std::optional<eversion_t> pg_committed_to)
{
  psdout(10) << entries << dendl;
  ceph_assert(is_primary());

  bool rebuild_missing = append_log_entries_update_missing(
    entries, t, trim_to, pg_committed_to);
  for (auto i = acting_recovery_backfill.begin();
       i != acting_recovery_backfill.end();
       ++i) {
    pg_shard_t peer(*i);
    if (peer == pg_whoami) continue;
    auto pm_it = peer_missing.find(peer);
    ceph_assert(pm_it != peer_missing.end());
    auto pi_it = peer_info.find(peer);
    ceph_assert(pi_it != peer_info.end());
    pg_missing_t& pmissing(pm_it->second);
    psdout(20) << "peer_missing for " << peer
	       << " = " << pmissing << dendl;
    pg_info_t& pinfo = pi_it->second;
    bool invalidate_stats = PGLog::append_log_entries_update_missing(
      pinfo.last_backfill,
      entries,
      true,
      NULL,
      pmissing,
      NULL,
      pool.info,
      peer.shard,
      dpp);
    pinfo.last_update = info.last_update;
    pinfo.stats.stats_invalid = pinfo.stats.stats_invalid || invalidate_stats;
    increment_stats_invalidations_counter(invalidate_stats);
    rebuild_missing = rebuild_missing || invalidate_stats;
  }

  if (!rebuild_missing) {
    return;
  }

  for (auto &&i: entries) {
    missing_loc.rebuild(
      i.soid,
      pg_whoami,
      acting_recovery_backfill,
      info,
      pg_log.get_missing(),
      peer_missing,
      peer_info);
  }
}

/**
 * 将单条 log entry 追加到 PGLog（由 append_log 的循环逐条调用）。
 *
 * 前半部分推进 pg_info 的三条水位：last_complete
 * （仅当本地原已完全追平才随之推进——否则中间有 missing 对象，不能跳过它们）、
 * last_update（必须严格递增，代表本 PG 已知的最写版本）、
 * last_user_version（客户端视角的版本，可能不随每条日志递增）。
 * 最后交给 PGLog::add 完成内存日志/missing/dups 的更新与脏区间标记。
 */
void PeeringState::add_log_entry(const pg_log_entry_t& e, ObjectStore::Transaction &t, bool applied)
{
  // raise last_complete only if we were previously up to date
  // 仅当本地此前已完全追平（没有 missing）时才推进 last_complete；
  // 若本地本来就有缺口，缺口不会因为收到更新的日志而消失，
  // last_complete 必须保持原位，否则会掩盖 missing。
  if (info.last_complete == info.last_update)
    info.last_complete = e.version;

  // raise last_update.
  // last_update 必须严格递增：每条日志代表一个新版本。
  ceph_assert(e.version > info.last_update);
  info.last_update = e.version;

  // raise user_version, if it increased (it may have not get bumped
  // by all logged updates)
  // user_version 是客户端视角的版本（如 rbd 侧），部分操作不递增它，所以只在变大时推进。
  if (e.user_version > info.last_user_version)
    info.last_user_version = e.user_version;

  // log mutation
  // nonprimary 标记本 shard 是否为 EC 的非 primary 分片，PGLog::add
  // 据此调整 missing 的处理方式（【EC 相关】影响分片条目）。
  enum PGLog::NonPrimary nonprimary{pool.info.is_nonprimary_shard(info.pgid.shard)};
  PGLog::LogEntryHandlerRef handler{pl->get_log_handler(t)};
  // 核心：追加到内存日志、更新 missing/dups、标记 dirty 区间。
  pg_log.add(e, nonprimary, applied, &info, handler.get());  // 重点
}


/**
 * 将一批 log entry（logv）追加到 PGLog 内存状态，并把日志/missing/trim 相关修改编码进事务 t，随数据事务一起落盘；
 * 写路径（primary 的 submit_transaction、副本的 do_repop）经 log_operation 进入这里。
 *
 * 关键分支：transaction_applied 为 false 时（backfill/async recovery peer 收到超前日志），
 * 只推进 crt 不做实际前滚（skip_rollforward），避免破坏对象等后续 _merge_object_divergent_entries() 处理。
 *
 * 注：【EC 相关】标注的段仅适用于纠删码 pool（partial write、各分片水位等）
 */
void PeeringState::append_log(
  vector<pg_log_entry_t>&& logv,
  eversion_t trim_to,
  eversion_t roll_forward_to,
  eversion_t pct,
  ObjectStore::Transaction &t,
  bool transaction_applied,
  bool async)
{
  /* With EC optimisations on, it is possible that we are told to commit a
   * version we don't have. This happens when the multiple transactions were
   * in flight and the last was a partial write.
   * While this is technically valid, there are a number of asserts which can
   * be avoided by refusing to roll forward beyond the head of the log.
   */
  // 【EC 相关】EC 优化开启时，多个事务并发中最后一个可能是 partial write，会导致被告知的提交版本超过本地日志头；
  // 这里钳制到日志头，避免后面前滚/提交边界的断言失败。（副本 pool 走不到这个分支）
  if (pool.info.allows_ecoptimizations()) {
    if (roll_forward_to > pg_log.get_head()) {
      roll_forward_to = pg_log.get_head();
    }
    if (pct > pg_log.get_head()) {
      pct = pg_log.get_head();
    }
  }
  /* The primary has sent an info updating the history, but it may not
   * have arrived yet.  We want to make sure that we cannot remember this
   * write without remembering that it happened in an interval which went
   * active in epoch history.last_epoch_started.
   */
  // primary 更新 history 的 info 消息可能尚未到达。
  // 这里先把 last_epoch_started/last_interval_started 同步进 history，
  // 保证"记住这次写"的同时也记住它发生在哪个已 active 的 interval——
  // 否则 peering 时无法判断该写是否需要参与回退判定。
  if (info.last_epoch_started != info.history.last_epoch_started) {
    info.history.last_epoch_started = info.last_epoch_started;
  }
  if (info.last_interval_started != info.history.last_interval_started) {
    info.history.last_interval_started = info.last_interval_started;
  }
  psdout(10) << "append_log " << pg_log.get_log() << " " << logv << dendl;

  // 日志条目的对象级副作用（回滚/前滚/删除）通过 handler 回调到 PGBackend（PG::PGLogEntryHandler）。
  PGLog::LogEntryHandlerRef handler{pl->get_log_handler(t)};
  if (!transaction_applied) {
     /* We must be a backfill or async recovery peer, so it's ok if we apply
      * out-of-turn since we won't be considered when
      * determining a min possible last_update.
      *
      * We skip_rollforward() here, which advances the crt, without
      * doing an actual rollforward. This avoids cleaning up entries
      * from the backend and we do not end up in a situation, where the
      * object is deleted before we can _merge_object_divergent_entries().
      */
    // 本 shard 是 backfill/async recovery 目标：日志超前于本地数据，允许乱序 apply（不会参与 min last_update 的计算）。
    // 只推进 crt 而不实际前滚对象，避免对象被提前清理、影响之后的日志合并。
    pg_log.skip_rollforward(&info, handler.get());
    /* Invalidate pwlc for this shard until the next interval when
     * it will be updated with the pwlc from another shard
     */
    // 【EC 相关】乱序 apply 后本 shard 的 partial write 水位失效，置为无效区间，等下个 interval 从其他 shard 重新获取。
    // （pwlc 记录 EC 各分片的 partial write 水位，副本 pool 不使用）
    for (auto & [shard, versionrange] :
	   info.partial_writes_last_complete) {
      auto & [fromversion, toversion] = versionrange;
      fromversion.epoch = 0;
      fromversion.version = eversion_t::max().version;
      toversion = fromversion;
    }
    info.partial_writes_last_complete_epoch = 0;
  }

  for (auto p = logv.begin(); p != logv.end(); ++p) {
    // 逐条追加到内存日志（更新 log/dups/missing、标记 dirty 区间）。
    add_log_entry(*p, t, transaction_applied);

    /* We don't want to leave the rollforward artifacts around
     * here past last_backfill.  It's ok for the same reason as
     * above */
    // 事务直接生效、且对象已越过 last_backfill（数据确定保留）时，
    // 把该条目前滚到 backend：crt 之前未前滚的旧条目也一并清理，避免残留回滚产物。
    if (transaction_applied &&
	p->soid > info.last_backfill) {
      pg_log.roll_forward(&info, handler.get());
    }
  }
  // primary 随消息带来的前滚边界：把 crt 推进到 roll_forward_to，
  // 将其之前所有可前滚条目一次性 apply 到 backend。
  // （前滚/回滚主要服务于 EC partial write 和副本 stash 场景，
  // replica pool 的普通 write 条目不可回滚，rollforward 大多是空操作）
  if (transaction_applied && roll_forward_to > pg_log.get_can_rollback_to()) {
    pg_log.roll_forward_to(
      roll_forward_to,
      &info,
      handler.get());
    last_rollback_info_trimmed_to_applied = roll_forward_to;
  }

  psdout(10) << "approx pg log length =  "
	     << pg_log.get_log().approx_size() << dendl;
  psdout(10) << "dups pg log length =  "
	     << pg_log.get_log().dups.size() << dendl;
  psdout(10) << "transaction_applied = "
	     << transaction_applied << dendl;
  if (!transaction_applied || async)
    psdout(10) << pg_whoami
	       << " is async_recovery or backfill target" << dendl;
  if (pool.info.allows_ecoptimizations() &&
      (trim_to > pg_log.get_can_rollback_to())) {
    // An exceptionally long sequence of partial writes followed by a full
    // write can result in trim_to being ahead of crt
    // 【EC 相关】同开头的 EC 钳制：partial write 序列可能导致 trim_to 超过 crt。（副本 pool 走不到这个分支）
    trim_to = pg_log.get_can_rollback_to();
  }
  // 内存裁剪日志（被裁条目进 trimmed/trimmed_dups），受 trim_to 约束。
  pg_log.trim(trim_to, info, transaction_applied, async);

  // update the local pg, pg log info 已变化，
  // write_if_dirty 会把 pg info 和日志的 dirty 区间编码进事务 t 持久化（PG::write_log_and_missing）。
  dirty_info = true;
  write_if_dirty(t);

  // 副本记录 primary 告知的本次写提交水位，之后据此上报 last_complete_ondisk / 参与日志裁剪。
  if (!is_primary())
    pg_committed_to = pct;
}

void PeeringState::recover_got(
  const hobject_t &oid, eversion_t v,
  bool is_delete,
  ObjectStore::Transaction &t)
{
  if (v > pg_log.get_can_rollback_to()) {
    /* This can only happen during a repair, and even then, it would
     * be one heck of a race.  If we are repairing the object, the
     * write in question must be fully committed, so it's not valid
     * to roll it back anyway (and we'll be rolled forward shortly
     * anyway) */
    PGLog::LogEntryHandlerRef handler{pl->get_log_handler(t)};
    pg_log.roll_forward_to(v, &info, handler.get());
  }

  psdout(10) << "got missing " << oid << " v " << v << dendl;
  pg_log.recover_got(oid, v, info);
  if (pg_log.get_log().log.empty()) {
    psdout(10) << "last_complete now " << info.last_complete
               << " while log is empty" << dendl;
  } else if (pg_log.get_log().complete_to != pg_log.get_log().log.end()) {
    psdout(10) << "last_complete now " << info.last_complete
	       << " log.complete_to " << pg_log.get_log().complete_to->version
	       << dendl;
  } else {
    psdout(10) << "last_complete now " << info.last_complete
	       << " log.complete_to at end" << dendl;
    //below is not true in the repair case.
    //ceph_assert(missing.num_missing() == 0);  // otherwise, complete_to was wrong.
    ceph_assert(info.last_complete == info.last_update);
  }

  if (is_primary()) {
    ceph_assert(missing_loc.needs_recovery(oid));
    if (!is_delete)
      missing_loc.add_location(oid, pg_whoami);
  }

  // update pg
  dirty_info = true;
  write_if_dirty(t);
}

void PeeringState::update_backfill_progress(
  const hobject_t &updated_backfill,
  const pg_stat_t &updated_stats,
  bool preserve_local_num_bytes,
  ObjectStore::Transaction &t) {
  info.set_last_backfill(updated_backfill);
  if (preserve_local_num_bytes) {
    psdout(25) << "primary " << updated_stats.stats.sum.num_bytes
	       << " local " << info.stats.stats.sum.num_bytes << dendl;
    int64_t bytes = info.stats.stats.sum.num_bytes;
    info.stats = updated_stats;
    info.stats.stats.sum.num_bytes = bytes;
  } else {
    psdout(20) << "final " << updated_stats.stats.sum.num_bytes
	       << " replaces local " << info.stats.stats.sum.num_bytes << dendl;
    info.stats = updated_stats;
  }

  dirty_info = true;
  write_if_dirty(t);
}

void PeeringState::adjust_purged_snaps(
  std::function<void(interval_set<snapid_t> &snaps)> f) {
  f(info.purged_snaps);
  dirty_info = true;
  dirty_big_info = true;
}

void PeeringState::on_peer_recover(
  pg_shard_t peer,
  const hobject_t &soid,
  const eversion_t &version)
{
  pl->publish_stats_to_osd();
  // done!
  peer_missing[peer].got(soid, version);
  missing_loc.add_location(soid, peer);
}

void PeeringState::begin_peer_recover(
  pg_shard_t peer,
  const hobject_t soid)
{
  peer_missing[peer].revise_have(soid, eversion_t());
}

void PeeringState::force_object_missing(
  const set<pg_shard_t> &peers,
  const hobject_t &soid,
  eversion_t version)
{
  for (auto &&peer : peers) {
    if (peer != primary) {
      peer_missing[peer].add(soid, version, eversion_t(), false);
    } else {
      pg_log.missing_add(soid, version, eversion_t());
      pg_log.reset_complete_to(&info, pool.info.allows_ecoptimizations());
      pg_log.set_last_requested(0);
    }
  }

  missing_loc.rebuild(
    soid,
    pg_whoami,
    acting_recovery_backfill,
    info,
    pg_log.get_missing(),
    peer_missing,
    peer_info);
}

void PeeringState::pre_submit_op(
  const hobject_t &hoid,
  const vector<pg_log_entry_t>& logv,
  eversion_t at_version)
{
  if (at_version > eversion_t()) {
    for (auto &&i : get_acting_recovery_backfill()) {
      if (i == primary) continue;
      pg_info_t &pinfo = peer_info[i];
      // keep peer_info up to date
      if (pinfo.last_complete == pinfo.last_update)
	pinfo.last_complete = at_version;
      pinfo.last_update = at_version;
    }
  }

  bool requires_missing_loc = false;
  for (auto &&i : get_async_recovery_targets()) {
    if (i == primary || !get_peer_missing(i).is_missing(hoid))
      continue;
    requires_missing_loc = true;
    for (auto &&entry: logv) {
      peer_missing[i].add_next_event(entry, pool.info, i.shard);
    }
  }

  if (requires_missing_loc) {
    for (auto &&entry: logv) {
      psdout(30) << "missing_loc before: "
		 << missing_loc.get_locations(entry.soid) << dendl;
      missing_loc.add_missing(entry.soid, entry.version,
                              eversion_t(), entry.is_delete());
      // clear out missing_loc
      missing_loc.clear_location(entry.soid);
      for (auto &i: get_actingset()) {
        if (!get_peer_missing(i).is_missing(entry.soid))
          missing_loc.add_location(entry.soid, i);
      }
      psdout(30) << "missing_loc after: "
		 << missing_loc.get_locations(entry.soid) << dendl;
    }
  }
}

void PeeringState::recovery_committed_to(eversion_t version)
{
  psdout(10) << "version " << version
	     << " now ondisk" << dendl;
  last_complete_ondisk = version;

  if (last_complete_ondisk == info.last_update) {
    if (!is_primary()) {
      // Either we are a replica or backfill target.
      // we are fully up to date.  tell the primary!
      pl->send_cluster_message(
	get_primary().osd,
	TOPNSPC::make_message<MOSDPGTrim>(
	  get_osdmap_epoch(),
	  spg_t(info.pgid.pgid, primary.shard),
	  last_complete_ondisk),
	get_osdmap_epoch());
    } else {
      calc_min_last_complete_ondisk();
    }
  }
}

void PeeringState::complete_write(eversion_t v, eversion_t lc)
{
  pg_committed_to = v;
  last_complete_ondisk = lc;
  calc_min_last_complete_ondisk();
}

void PeeringState::calc_trim_to()
{
  size_t target = pl->get_target_pg_log_entries();

  eversion_t limit = std::min(
    min_last_complete_ondisk,
    pg_log.get_can_rollback_to());
  if (limit != eversion_t() &&
      limit != pg_trim_to &&
      pg_log.get_log().approx_size() > target) {
    size_t num_to_trim = std::min(pg_log.get_log().approx_size() - target,
                             cct->_conf->osd_pg_log_trim_max);
    if (num_to_trim < cct->_conf->osd_pg_log_trim_min &&
        cct->_conf->osd_pg_log_trim_max >= cct->_conf->osd_pg_log_trim_min) {
      return;
    }
    auto it = pg_log.get_log().log.begin();
    eversion_t new_trim_to;
    for (size_t i = 0; i < num_to_trim; ++i) {
      new_trim_to = it->version;
      ++it;
      if (new_trim_to > limit) {
        new_trim_to = limit;
        psdout(10) << "calc_trim_to trimming to min_last_complete_ondisk" << dendl;
        break;
      }
    }
    psdout(10) << "calc_trim_to " << pg_trim_to << " -> " << new_trim_to << dendl;
    pg_trim_to = new_trim_to;
    ceph_assert(pg_trim_to <= pg_log.get_head());
    ceph_assert(pg_trim_to <= min_last_complete_ondisk);
  }
}

void PeeringState::calc_trim_to_aggressive()
{
  size_t target = pl->get_target_pg_log_entries();

  // limit pg log trimming up to the can_rollback_to value
  eversion_t limit = std::min({
    pg_log.get_head(),
    pg_log.get_can_rollback_to(),
    pg_committed_to});
  psdout(10) << "limit = " << limit << dendl;

  if (limit != eversion_t() &&
      limit != pg_trim_to &&
      pg_log.get_log().approx_size() > target) {
    psdout(10) << "approx pg log length =  "
             << pg_log.get_log().approx_size() << dendl;
    uint64_t num_to_trim = std::min<uint64_t>(pg_log.get_log().approx_size() - target,
                                              cct->_conf->osd_pg_log_trim_max);
    psdout(10) << "num_to_trim =  " << num_to_trim << dendl;
    if (num_to_trim < cct->_conf->osd_pg_log_trim_min &&
	cct->_conf->osd_pg_log_trim_max >= cct->_conf->osd_pg_log_trim_min) {
      return;
    }
    auto it = pg_log.get_log().log.begin(); // oldest log entry
    auto rit = pg_log.get_log().log.rbegin();
    eversion_t by_n_to_keep; // start from tail
    eversion_t by_n_to_trim = eversion_t::max(); // start from head
    for (size_t i = 0; it != pg_log.get_log().log.end(); ++it, ++rit) {
      i++;
      if (i > target && by_n_to_keep == eversion_t()) {
        by_n_to_keep = rit->version;
      }
      if (i >= num_to_trim && by_n_to_trim == eversion_t::max()) {
        by_n_to_trim = it->version;
      }
      if (by_n_to_keep != eversion_t() &&
          by_n_to_trim != eversion_t::max()) {
        break;
      }
    }

    if (by_n_to_keep == eversion_t()) {
      return;
    }

    pg_trim_to = std::min({by_n_to_keep, by_n_to_trim, limit});
    psdout(10) << "pg_trim_to now " << pg_trim_to << dendl;
    ceph_assert(pg_trim_to <= pg_log.get_head());
  }
}

void PeeringState::apply_op_stats(
  const hobject_t &soid,
  const object_stat_sum_t &delta_stats)
{
  info.stats.stats.add(delta_stats);
  info.stats.stats.floor(0);

  for (auto i = get_backfill_targets().begin();
       i != get_backfill_targets().end();
       ++i) {
    pg_shard_t bt = *i;
    pg_info_t& pinfo = peer_info[bt];
    if (soid <= pinfo.last_backfill)
      pinfo.stats.stats.add(delta_stats);
  }
}

void PeeringState::update_complete_backfill_object_stats(
  const hobject_t &hoid,
  const pg_stat_t &stats)
{
  for (auto &&bt: get_backfill_targets()) {
    pg_info_t& pinfo = peer_info[bt];
    //Add stats to all peers that were missing object
    if (hoid > pinfo.last_backfill)
      pinfo.stats.add(stats);
  }
}

void PeeringState::update_peer_last_backfill(
  pg_shard_t peer,
  const hobject_t &new_last_backfill)
{
  pg_info_t &pinfo = peer_info[peer];
  pinfo.last_backfill = new_last_backfill;
  if (new_last_backfill.is_max()) {
    /* pinfo.stats might be wrong if we did log-based recovery on the
     * backfilled portion in addition to continuing backfill.
     */
    pinfo.stats = info.stats;
  }
}

void PeeringState::set_revert_with_targets(
  const hobject_t &soid,
  const set<pg_shard_t> &good_peers)
{
  for (auto &&peer: good_peers) {
    missing_loc.add_location(soid, peer);
  }
}

void PeeringState::update_peer_last_complete_ondisk(
  pg_shard_t fromosd,
  eversion_t lcod) {
  psdout(20) << "updating peer_last_complete_ondisk"
             << " of osd: "<< fromosd << " to: "
             << lcod << dendl;
  peer_last_complete_ondisk[fromosd] = lcod;
}

void PeeringState::update_last_complete_ondisk(
  eversion_t lcod) {
  psdout(20) << "updating last_complete_ondisk"
             << " to: " << lcod << dendl;
  last_complete_ondisk = lcod;
}

void PeeringState::prepare_backfill_for_missing(
  const hobject_t &soid,
  const eversion_t &version,
  const vector<pg_shard_t> &targets) {
  for (auto &&peer: targets) {
    peer_missing[peer].add(soid, version, eversion_t(), false);
  }
}

void PeeringState::update_hset(const pg_hit_set_history_t &hset_history)
{
  info.hit_set = hset_history;
}

/*------------ Peering State Machine----------------*/
#undef dout_prefix
#define dout_prefix (context< PeeringMachine >().dpp->gen_prefix(*_dout) \
                    << "state<" << get_state_name() << ">: ")
#undef psdout
#define psdout(x) ldout(context< PeeringMachine >().cct, x)

#define DECLARE_LOCALS                                  \
  PeeringState *ps = context< PeeringMachine >().state; \
  std::ignore = ps;                                     \
  PeeringListener *pl = context< PeeringMachine >().pl; \
  std::ignore = pl


/*------Crashed-------*/
PeeringState::Crashed::Crashed(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Crashed")
{
  context< PeeringMachine >().log_enter(state_name);
  ceph_abort_msg("we got a bad state machine event");
}


/*------Initial-------*/
PeeringState::Initial::Initial(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Initial")
{
  context< PeeringMachine >().log_enter(state_name);
}

boost::statechart::result PeeringState::Initial::react(const MNotifyRec& notify)
{
  DECLARE_LOCALS;
  ps->proc_replica_notify(notify.from, notify.notify);
  ps->set_last_peering_reset();
  return transit< Primary >();
}

boost::statechart::result PeeringState::Initial::react(const MInfoRec& i)
{
  DECLARE_LOCALS;
  ceph_assert(!ps->is_primary());
  post_event(i);
  return transit< Stray >();
}

boost::statechart::result PeeringState::Initial::react(const MLogRec& i)
{
  DECLARE_LOCALS;
  ceph_assert(!ps->is_primary());
  post_event(i);
  return transit< Stray >();
}

void PeeringState::Initial::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_initial_latency, dur);
}

/*------Started-------*/
PeeringState::Started::Started(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started")
{
  context< PeeringMachine >().log_enter(state_name);
}

boost::statechart::result
PeeringState::Started::react(const IntervalFlush&)
{
  psdout(10) << "Ending blocked outgoing recovery messages" << dendl;
  context< PeeringMachine >().state->end_block_outgoing();
  return discard_event();
}

boost::statechart::result PeeringState::Started::react(const AdvMap& advmap)
{
  // Active/Peering 等子状态对 AdvMap 返回 forward_event() 后，事件会传播到 Started；
  // 这里统一决定这次地图变化是否必须重新开始 peering。
  DECLARE_LOCALS;
  psdout(10) << "Started advmap" << dendl;
  // pool 的 FULL 标志变化不一定改变 PG interval，但需要更新 PG 的 full 状态。
  ps->check_full_transition(advmap.lastmap, advmap.osdmap);
  if (ps->should_restart_peering(
	advmap.up_primary,
	advmap.acting_primary,
	advmap.newup,
	advmap.newacting,
	advmap.lastmap,
	advmap.osdmap)) {
    psdout(10) << "should_restart_peering, transitioning to Reset"
		       << dendl;
    // 先把同一个 AdvMap 重新投递；transit<Reset>() 完成状态切换后，
    // Reset::react(AdvMap) 会用新状态重新建立 peering interval。
    post_event(advmap);
    return transit< Reset >();
  }
  // 映射未改变 interval，不必重置；丢弃已经失效的 down peer 信息即可。
  ps->remove_down_peer_info(advmap.osdmap);
  // 当前事件结束，状态不变，回到调用栈
  return discard_event();
}

boost::statechart::result PeeringState::Started::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->close_section();
  return discard_event();
}

boost::statechart::result PeeringState::Started::react(const QueryUnfound& q)
{
  q.f->dump_string("state", "Started");
  q.f->dump_bool("available_might_have_unfound", false);
  return discard_event();
}

void PeeringState::Started::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_started_latency, dur);
  ps->state_clear(PG_STATE_WAIT | PG_STATE_LAGGY);
}

/*--------Reset---------*/
PeeringState::Reset::Reset(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Reset")
{
  context< PeeringMachine >().log_enter(state_name);
  DECLARE_LOCALS;

  ps->flushes_in_progress = 0;
  ps->set_last_peering_reset();
  ps->log_weirdness();
}

boost::statechart::result
PeeringState::Reset::react(const IntervalFlush&)
{
  psdout(10) << "Ending blocked outgoing recovery messages" << dendl;
  context< PeeringMachine >().state->end_block_outgoing();
  return discard_event();
}

boost::statechart::result PeeringState::Reset::react(const AdvMap& advmap)
{
  // Started 在转入 Reset 前重新投递的 AdvMap 会到达这里；
  // Reset 期间后续 OSDMap 更新也复用同一处理路径。
  DECLARE_LOCALS;
  psdout(10) << "Reset advmap" << dendl;

  // FULL 标志变化独立于 interval，先同步 PG 的 full 状态。
  ps->check_full_transition(advmap.lastmap, advmap.osdmap);

  // 只有新 up/acting 映射确实开启新的 peering interval 时，
  // 才重新初始化 PG 的角色、映射、past_intervals 等 interval 相关状态。
  if (ps->should_restart_peering(
	advmap.up_primary,
	advmap.acting_primary,
	advmap.newup,
	advmap.newacting,
	advmap.lastmap,
	advmap.osdmap)) {
    psdout(10) << "should restart peering, calling start_peering_interval again"
		       << dendl;
    // 传入状态机本轮上下文持有的事务；interval 初始化产生的持久化修改
    // 会与本次 peering 事件的其他输出一并由 PeeringCtx 派发。
    ps->start_peering_interval(
      advmap.lastmap,
      advmap.newup, advmap.up_primary,
      advmap.newacting, advmap.acting_primary,
      context< PeeringMachine >().get_cur_transaction());
  }
  // 无论是否开启新 interval，都删除新地图中已 down 的 peer 信息，
  // 并校验 past_intervals 的边界仍与 PG history 一致。
  ps->remove_down_peer_info(advmap.osdmap);
  ps->check_past_interval_bounds();
  return discard_event();
}

boost::statechart::result PeeringState::Reset::react(const ActMap&)
{
  DECLARE_LOCALS;

  // start_peering_interval() 已根据新角色设置 send_notify：只有非 primary 的本地副本
  // 需要把自己的 peering 信息告诉当前 acting primary；primary 不需要向自己发送。
  if (ps->should_send_notify() && ps->get_primary().osd >= 0) {
    // 将旧 interval 尚未过期的读 lease 上界换算/保存到 info.history；
    // 这样 primary 收到 notify 后也能知道旧副本可能仍可读到何时。
    ps->info.history.refresh_prior_readable_until_ub(
      pl->get_mnow(),
      ps->prior_readable_until_ub);

    // notify 携带目标 primary/local replica 的 shard、当前 map epoch、本地 pg_info、
    // PastIntervals 以及本地支持的 PG feature；primary 用它建立本轮 peering 的 peer 视图。
    // 消息类型：MOSDPGNotify2
    context< PeeringMachine >().send_notify(
      ps->get_primary().osd,
      pg_notify_t(
	ps->get_primary().shard, ps->pg_whoami.shard,
	ps->get_osdmap_epoch(),
	ps->get_osdmap_epoch(),
	ps->info,
	ps->past_intervals,
	ps->local_pg_acting_features));
  }

  // acting 角色和副本集已按新 map 安装，更新心跳监测对象。
  ps->update_heartbeat_peers();

  // ActMap 是本批 map 推进后的激活边界；Reset 的工作至此完成，回到 Started
  // 等待后续 Notify、Query 等 peering 事件驱动下一阶段。
  return transit< Started >();
}

boost::statechart::result PeeringState::Reset::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->close_section();
  return discard_event();
}

boost::statechart::result PeeringState::Reset::react(const QueryUnfound& q)
{
  q.f->dump_string("state", "Reset");
  q.f->dump_bool("available_might_have_unfound", false);
  return discard_event();
}

void PeeringState::Reset::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_reset_latency, dur);
}

/*-------Start---------*/
PeeringState::Start::Start(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Start")
{
  // context<PeeringMachine>() 取当前 Start 所属的状态机对象；log_enter(state_name) 记录“进入 Start 状态”。
  context< PeeringMachine >().log_enter(state_name);

  DECLARE_LOCALS;
  // Start 是 Started 的初始子状态，只负责按当前 PG 角色选择分支。
  // post_event() 将内部事件交给 statechart 随后分发；
  // 它并不在这里直接调用 Primary 或 Stray 的构造函数。
  if (ps->is_primary()) {
    // Start::reactions 中 MakePrimary -> Primary；
    psdout(1) << "transitioning to Primary" << dendl;
    post_event(MakePrimary());
  } else { // is_stray
    // Start::reactions 中 MakeStray -> Stray。
    // 此 OSD 不再担任当前 acting 集的 primary，转入副本/stray 路径。
    psdout(1) << "transitioning to Stray" << dendl;
    post_event(MakeStray());
  }
}

void PeeringState::Start::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_start_latency, dur);
}

/*---------Primary--------*/
PeeringState::Primary::Primary(my_context ctx)
  : my_base(ctx),
    // state_history 用于记录状态层级，供 PG 状态查询和调试观察。
    NamedState(context< PeeringMachine >().state_history, "Started/Primary")
{
  context< PeeringMachine >().log_enter(state_name);
  DECLARE_LOCALS;

  // 新 interval 已在 Reset 中清除上一次 primary 尝试的候选 acting 集。
  // 进入 Primary 时必须从空集合开始，后续 peering 才能重新选择权威副本集。
  ceph_assert(ps->want_acting.empty());

  // last_epoch_started 为 0 表示这个 PG 尚未成功开始过任何一次 peering，
  // 即新建 PG 的首次 primary peering。
  if (ps->info.history.last_epoch_started == 0) {
    // 在首次 peering 成功前保持 CREATING 状态；离开 Primary 或完成 activation 后会清除。
    ps->state_set(PG_STATE_CREATING);

    // 新建 PG 尚无实际的状态转换时刻，使用创建时由 Monitor 最终写入 history 的
    // 时间戳作为统计基线，避免 last_active、last_clean 等字段为未初始化时间。
    utime_t t = ps->info.history.last_scrub_stamp;

    // 这些是 PG 状态与 scrub 统计的时间戳，不表示 PG 已 fresh、active 或 clean；
    // 它们仅以创建时间初始化，后续相应状态转换会写入真实发生时间。
    ps->info.stats.last_fresh = t;
    ps->info.stats.last_active = t;
    ps->info.stats.last_change = t;
    ps->info.stats.last_peered = t;
    ps->info.stats.last_clean = t;
    ps->info.stats.last_unstale = t;
    ps->info.stats.last_undegraded = t;
    ps->info.stats.last_fullsized = t;
    ps->info.stats.last_scrub_stamp = t;
    ps->info.stats.last_deep_scrub_stamp = t;
    ps->info.stats.last_clean_scrub_stamp = t;
  }
}

boost::statechart::result PeeringState::Primary::react(const MNotifyRec& notevt)
{
  DECLARE_LOCALS;

  // MOSDPGNotify2 已被接收线程转换为 MNotifyRec 并投递到本 PG；
  // 状态机确认当前角色为 Primary 后，才由这个状态处理副本上报。
  psdout(7) << "handle_pg_notify from osd." << notevt.from << dendl;

  // 实际处理包括去重、校验发送者仍有效，以及更新 peer_info、历史和 feature 交集。
  ps->proc_replica_notify(notevt.from, notevt.notify);

  // notify 已消费；Primary 状态本身不因单条 notify 直接发生状态迁移。
  return discard_event();
}

boost::statechart::result PeeringState::Primary::react(const ActMap&)
{
  DECLARE_LOCALS;
  psdout(7) << "handle ActMap primary" << dendl;
  pl->publish_stats_to_osd();
  return discard_event();
}

boost::statechart::result PeeringState::Primary::react(
  const SetForceRecovery&)
{
  DECLARE_LOCALS;
  ps->set_force_recovery(true);
  return discard_event();
}

boost::statechart::result PeeringState::Primary::react(
  const UnsetForceRecovery&)
{
  DECLARE_LOCALS;
  ps->set_force_recovery(false);
  return discard_event();
}

boost::statechart::result PeeringState::Primary::react(
  const RequestScrub& evt)
{
  DECLARE_LOCALS;
  if (ps->is_primary()) {
    pl->scrub_requested(evt.deep, evt.repair);
    psdout(10) << "marking for scrub" << dendl;
  }
  return discard_event();
}

boost::statechart::result PeeringState::Primary::react(
  const SetForceBackfill&)
{
  DECLARE_LOCALS;
  ps->set_force_backfill(true);
  return discard_event();
}

boost::statechart::result PeeringState::Primary::react(
  const UnsetForceBackfill&)
{
  DECLARE_LOCALS;
  ps->set_force_backfill(false);
  return discard_event();
}

void PeeringState::Primary::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;
  ps->want_acting.clear();
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_primary_latency, dur);
  pl->clear_primary_state();
  ps->state_clear(PG_STATE_CREATING);
}

/*---------Peering--------*/
PeeringState::Peering::Peering(my_context ctx)
  : my_base(ctx),
    // Peering 是 Primary 的子状态；完整名称用于状态历史和诊断输出。
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Peering"),
    // 后续 choose_acting() 若发现 history.last_epoch_started 限制了可选副本，
    // 会置此标记并在状态查询中说明 peering 的阻塞原因。
    history_les_bound(false)
{
  context< PeeringMachine >().log_enter(state_name);
  DECLARE_LOCALS;

  // Peering 只能由尚未完成 peering 的 primary 进入。
  // 若旧状态仍是 peered/peering，说明状态切换或 interval 重置没有正确清理，直接终止以避免状态混用。
  ceph_assert(!ps->is_peered());
  ceph_assert(!ps->is_peering());
  ceph_assert(ps->is_primary());

  // 对外发布 PG 正处于 peering；具体的副本信息收集由默认子状态 GetInfo 开始。
  ps->state_set(PG_STATE_PEERING);
}

boost::statechart::result PeeringState::Peering::react(const AdvMap& advmap)
{
  DECLARE_LOCALS;
  psdout(10) << "Peering advmap" << dendl;
  if (prior_set.affected_by_map(*(advmap.osdmap), ps->dpp)) {
    psdout(1) << "Peering, affected_by_map, going to Reset" << dendl;
    post_event(advmap);
    return transit< Reset >();
  }

  ps->adjust_need_up_thru(advmap.osdmap);
  ps->check_prior_readable_down_osds(advmap.osdmap);

  return forward_event();
}

boost::statechart::result PeeringState::Peering::react(const QueryState& q)
{
  DECLARE_LOCALS;

  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;

  q.f->open_array_section("past_intervals");
  ps->past_intervals.dump(q.f);
  q.f->close_section();

  q.f->open_array_section("probing_osds");
  for (auto p = prior_set.probe.begin(); p != prior_set.probe.end(); ++p)
    q.f->dump_stream("osd") << *p;
  q.f->close_section();

  if (prior_set.pg_down)
    q.f->dump_string("blocked", "peering is blocked due to down osds");

  q.f->open_array_section("down_osds_we_would_probe");
  for (auto p = prior_set.down.begin(); p != prior_set.down.end(); ++p)
    q.f->dump_int("osd", *p);
  q.f->close_section();

  q.f->open_array_section("peering_blocked_by");
  for (auto p = prior_set.blocked_by.begin();
       p != prior_set.blocked_by.end();
       ++p) {
    q.f->open_object_section("osd");
    q.f->dump_int("osd", p->first);
    q.f->dump_int("current_lost_at", p->second);
    q.f->dump_string("comment", "starting or marking this osd lost may let us proceed");
    q.f->close_section();
  }
  q.f->close_section();

  if (history_les_bound) {
    q.f->open_array_section("peering_blocked_by_detail");
    q.f->open_object_section("item");
    q.f->dump_string("detail","peering_blocked_by_history_les_bound");
    q.f->close_section();
    q.f->close_section();
  }

  q.f->close_section();
  return forward_event();
}

boost::statechart::result PeeringState::Peering::react(const QueryUnfound& q)
{
  q.f->dump_string("state", "Peering");
  q.f->dump_bool("available_might_have_unfound", false);
  return discard_event();
}

void PeeringState::Peering::exit()
{

  DECLARE_LOCALS;
  psdout(10) << "Leaving Peering" << dendl;
  context< PeeringMachine >().log_exit(state_name, enter_time);
  ps->state_clear(PG_STATE_PEERING);
  pl->clear_probe_targets();

  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_peering_latency, dur);
}


/*------Backfilling-------*/
PeeringState::Backfilling::Backfilling(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Active/Backfilling")
{
  context< PeeringMachine >().log_enter(state_name);


  DECLARE_LOCALS;
  ps->backfill_reserved = true;
  ps->state_clear(PG_STATE_BACKFILL_TOOFULL);
  ps->state_clear(PG_STATE_BACKFILL_WAIT);
  ps->state_set(PG_STATE_BACKFILLING);
  pl->on_backfill_reserved();
  pl->publish_stats_to_osd();
}

void PeeringState::Backfilling::backfill_release_reservations()
{
  DECLARE_LOCALS;
  // 释放 primary 在本地为本轮 backfill 申请的后台 IO 资源。
  pl->cancel_local_background_io_reservation();

  // backfill target 的远端 reservation 由 primary 逐个申请，结束或暂停时也要逐个释放。
  for (auto it = ps->backfill_targets.begin();
       it != ps->backfill_targets.end();
       ++it) {
    // primary 只负责向其他 OSD backfill，不会把自己作为远端 target。
    ceph_assert(*it != ps->pg_whoami);

    // 通知 target OSD 撤销该 PG 的 backfill reservation；消息 epoch 用于丢弃过期请求。
    pl->send_cluster_message(
      it->osd,
      TOPNSPC::make_message<MBackfillReserve>(
	MBackfillReserve::RELEASE,
	spg_t(ps->info.pgid.pgid, it->shard),
	ps->get_osdmap_epoch()),
      ps->get_osdmap_epoch());
  }
}

void PeeringState::Backfilling::suspend_backfill()
{
  DECLARE_LOCALS;
  backfill_release_reservations();
  pl->on_backfill_suspended();
}

boost::statechart::result
PeeringState::Backfilling::react(const Backfilled &c)
{
  backfill_release_reservations();
  return transit<Recovered>();
}

boost::statechart::result
PeeringState::Backfilling::react(const DeferBackfill &c)
{
  DECLARE_LOCALS;
  if (ps->needs_backfill()) {
    psdout(10) << "defer backfill, retry delay " << c.delay << dendl;
    ps->state_set(PG_STATE_BACKFILL_WAIT);
    ps->state_clear(PG_STATE_BACKFILLING);
    suspend_backfill();

    pl->schedule_event_after(
      std::make_shared<PGPeeringEvent>(
	ps->get_osdmap_epoch(),
	ps->get_osdmap_epoch(),
	RequestBackfill()),
      c.delay);
    return transit<NotBackfilling>();
  } else {
    // raced with MOSDPGBackfill::OP_BACKFILL_FINISH, ignore
    psdout(10) << "discarding stale DeferBackfill event , pg does not need "
		  "backfill anymore"
	       << dendl;
    return discard_event();
  }
}

boost::statechart::result
PeeringState::Backfilling::react(const UnfoundBackfill &c)
{
  DECLARE_LOCALS;
  psdout(10) << "backfill has unfound, can't continue" << dendl;
  ps->state_set(PG_STATE_BACKFILL_UNFOUND);
  ps->state_clear(PG_STATE_BACKFILLING);
  suspend_backfill();
  return transit<NotBackfilling>();
}

boost::statechart::result
PeeringState::Backfilling::react(const RemoteReservationRevokedTooFull &)
{
  DECLARE_LOCALS;

  ps->state_set(PG_STATE_BACKFILL_TOOFULL);
  ps->state_clear(PG_STATE_BACKFILLING);
  suspend_backfill();

  pl->schedule_event_after(
    std::make_shared<PGPeeringEvent>(
      ps->get_osdmap_epoch(),
      ps->get_osdmap_epoch(),
      RequestBackfill()),
    ps->cct->_conf->osd_backfill_retry_interval);

  return transit<NotBackfilling>();
}

boost::statechart::result
PeeringState::Backfilling::react(const RemoteReservationRevoked &)
{
  DECLARE_LOCALS;
  ps->state_set(PG_STATE_BACKFILL_WAIT);
  suspend_backfill();
  if (ps->needs_backfill()) {
    return transit<WaitLocalBackfillReserved>();
  } else {
    // raced with MOSDPGBackfill::OP_BACKFILL_FINISH, ignore
    return discard_event();
  }
}

void PeeringState::Backfilling::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;
  ps->backfill_reserved = false;
  ps->state_clear(PG_STATE_BACKFILLING);
  ps->state_clear(PG_STATE_FORCED_BACKFILL | PG_STATE_FORCED_RECOVERY);
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_backfilling_latency, dur);
}

/*--WaitRemoteBackfillReserved--*/

PeeringState::WaitRemoteBackfillReserved::WaitRemoteBackfillReserved(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Active/WaitRemoteBackfillReserved"),
    backfill_osd_it(context< Active >().remote_shards_to_reserve_backfill.begin())
{
  context< PeeringMachine >().log_enter(state_name);
  DECLARE_LOCALS;

  ps->state_set(PG_STATE_BACKFILL_WAIT);
  pl->publish_stats_to_osd();
  post_event(RemoteBackfillReserved());
}

boost::statechart::result
PeeringState::WaitRemoteBackfillReserved::react(const RemoteBackfillReserved &evt)
{
  DECLARE_LOCALS;

  int64_t num_bytes = ps->info.stats.stats.sum.num_bytes;
  psdout(10) << __func__ << " num_bytes " << num_bytes << dendl;
  if (backfill_osd_it !=
      context< Active >().remote_shards_to_reserve_backfill.end()) {
    // The primary never backfills itself
    ceph_assert(*backfill_osd_it != ps->pg_whoami);
    pl->send_cluster_message(
      backfill_osd_it->osd,
      TOPNSPC::make_message<MBackfillReserve>(
	MBackfillReserve::REQUEST,
	spg_t(context< PeeringMachine >().spgid.pgid, backfill_osd_it->shard),
	ps->get_osdmap_epoch(),
	ps->get_backfill_priority(),
        num_bytes,
        ps->peer_bytes[*backfill_osd_it]),
      ps->get_osdmap_epoch());
    ++backfill_osd_it;
  } else {
    ps->peer_bytes.clear();
    post_event(AllBackfillsReserved());
  }
  return discard_event();
}

void PeeringState::WaitRemoteBackfillReserved::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;

  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_waitremotebackfillreserved_latency, dur);
}

void PeeringState::WaitRemoteBackfillReserved::retry()
{
  DECLARE_LOCALS;
  pl->cancel_local_background_io_reservation();

  // Send CANCEL to all previously acquired reservations
  set<pg_shard_t>::const_iterator it, begin, end;
  begin = context< Active >().remote_shards_to_reserve_backfill.begin();
  end = context< Active >().remote_shards_to_reserve_backfill.end();
  ceph_assert(begin != end);
  for (it = begin; it != backfill_osd_it; ++it) {
    // The primary never backfills itself
    ceph_assert(*it != ps->pg_whoami);
    pl->send_cluster_message(
      it->osd,
      TOPNSPC::make_message<MBackfillReserve>(
	MBackfillReserve::RELEASE,
	spg_t(context< PeeringMachine >().spgid.pgid, it->shard),
	ps->get_osdmap_epoch()),
      ps->get_osdmap_epoch());
  }

  ps->state_clear(PG_STATE_BACKFILL_WAIT);
  pl->publish_stats_to_osd();

  pl->schedule_event_after(
    std::make_shared<PGPeeringEvent>(
      ps->get_osdmap_epoch(),
      ps->get_osdmap_epoch(),
      RequestBackfill()),
    ps->cct->_conf->osd_backfill_retry_interval);
}

boost::statechart::result
PeeringState::WaitRemoteBackfillReserved::react(const RemoteReservationRejectedTooFull &evt)
{
  DECLARE_LOCALS;
  ps->state_set(PG_STATE_BACKFILL_TOOFULL);
  retry();
  return transit<NotBackfilling>();
}

boost::statechart::result
PeeringState::WaitRemoteBackfillReserved::react(const RemoteReservationRevoked &evt)
{
  retry();
  return transit<NotBackfilling>();
}

/*--WaitLocalBackfillReserved--*/
PeeringState::WaitLocalBackfillReserved::WaitLocalBackfillReserved(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Active/WaitLocalBackfillReserved")
{
  context< PeeringMachine >().log_enter(state_name);
  DECLARE_LOCALS;

  ps->state_set(PG_STATE_BACKFILL_WAIT);
  pl->request_local_background_io_reservation(
    ps->get_backfill_priority(),
    std::make_unique<PGPeeringEvent>(
      ps->get_osdmap_epoch(),
      ps->get_osdmap_epoch(),
      LocalBackfillReserved()),
    std::make_unique<PGPeeringEvent>(
      ps->get_osdmap_epoch(),
      ps->get_osdmap_epoch(),
      DeferBackfill(0.0)));
  pl->publish_stats_to_osd();
}

void PeeringState::WaitLocalBackfillReserved::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_waitlocalbackfillreserved_latency, dur);
}

/*----NotBackfilling------*/
PeeringState::NotBackfilling::NotBackfilling(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Active/NotBackfilling")
{
  context< PeeringMachine >().log_enter(state_name);
  DECLARE_LOCALS;
  ps->state_clear(PG_STATE_REPAIR);
  pl->publish_stats_to_osd();
}

boost::statechart::result PeeringState::NotBackfilling::react(const QueryUnfound& q)
{
  DECLARE_LOCALS;

  ps->query_unfound(q.f, "NotBackfilling");
  return discard_event();
}

boost::statechart::result
PeeringState::NotBackfilling::react(const RemoteBackfillReserved &evt)
{
  return discard_event();
}

boost::statechart::result
PeeringState::NotBackfilling::react(const RemoteReservationRejectedTooFull &evt)
{
  return discard_event();
}

void PeeringState::NotBackfilling::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);

  DECLARE_LOCALS;
  ps->state_clear(PG_STATE_BACKFILL_UNFOUND);
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_notbackfilling_latency, dur);
}

/*----NotRecovering------*/
PeeringState::NotRecovering::NotRecovering(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Active/NotRecovering")
{
  context< PeeringMachine >().log_enter(state_name);
  DECLARE_LOCALS;
  ps->state_clear(PG_STATE_REPAIR);
  pl->publish_stats_to_osd();
}

boost::statechart::result PeeringState::NotRecovering::react(const QueryUnfound& q)
{
  DECLARE_LOCALS;

  ps->query_unfound(q.f, "NotRecovering");
  return discard_event();
}

void PeeringState::NotRecovering::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);

  DECLARE_LOCALS;
  ps->state_clear(PG_STATE_RECOVERY_UNFOUND);
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_notrecovering_latency, dur);
}

/*---RepNotRecovering----*/
PeeringState::RepNotRecovering::RepNotRecovering(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/ReplicaActive/RepNotRecovering")
{
  context< PeeringMachine >().log_enter(state_name);
}

boost::statechart::result
PeeringState::RepNotRecovering::react(const RejectTooFullRemoteReservation &evt)
{
  DECLARE_LOCALS;
  ps->reject_reservation();
  post_event(RemoteReservationRejectedTooFull());
  return discard_event();
}

void PeeringState::RepNotRecovering::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_repnotrecovering_latency, dur);
}

/*---RepWaitRecoveryReserved--*/
PeeringState::RepWaitRecoveryReserved::RepWaitRecoveryReserved(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/ReplicaActive/RepWaitRecoveryReserved")
{
  context< PeeringMachine >().log_enter(state_name);
}

boost::statechart::result
PeeringState::RepWaitRecoveryReserved::react(const RemoteRecoveryReserved &evt)
{
  DECLARE_LOCALS;
  pl->send_cluster_message(
    ps->primary.osd,
    TOPNSPC::make_message<MRecoveryReserve>(
      MRecoveryReserve::GRANT,
      spg_t(ps->info.pgid.pgid, ps->primary.shard),
      ps->get_osdmap_epoch()),
    ps->get_osdmap_epoch());
  return transit<RepRecovering>();
}

boost::statechart::result
PeeringState::RepWaitRecoveryReserved::react(
  const RemoteReservationCanceled &evt)
{
  DECLARE_LOCALS;
  pl->unreserve_recovery_space();

  pl->cancel_remote_recovery_reservation();
  return transit<RepNotRecovering>();
}

void PeeringState::RepWaitRecoveryReserved::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_repwaitrecoveryreserved_latency, dur);
}

/*-RepWaitBackfillReserved*/
PeeringState::RepWaitBackfillReserved::RepWaitBackfillReserved(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/ReplicaActive/RepWaitBackfillReserved")
{
  context< PeeringMachine >().log_enter(state_name);
}

boost::statechart::result
PeeringState::RepNotRecovering::react(const RequestBackfillPrio &evt)
{

  DECLARE_LOCALS;

  if (!pl->try_reserve_recovery_space(
	evt.primary_num_bytes, evt.local_num_bytes)) {
    post_event(RejectTooFullRemoteReservation());
  } else {
    PGPeeringEventURef preempt;
    if (HAVE_FEATURE(ps->upacting_features, RECOVERY_RESERVATION_2)) {
      // older peers will interpret preemption as TOOFULL
      preempt = std::make_unique<PGPeeringEvent>(
	pl->get_osdmap_epoch(),
	pl->get_osdmap_epoch(),
	RemoteBackfillPreempted());
    }
    pl->request_remote_recovery_reservation(
      evt.priority,
      std::make_unique<PGPeeringEvent>(
	pl->get_osdmap_epoch(),
	pl->get_osdmap_epoch(),
        RemoteBackfillReserved()),
      std::move(preempt));
  }
  return transit<RepWaitBackfillReserved>();
}

boost::statechart::result
PeeringState::RepNotRecovering::react(const RequestRecoveryPrio &evt)
{
  DECLARE_LOCALS;

  // fall back to a local reckoning of priority of primary doesn't pass one
  // (pre-mimic compat)
  int prio = evt.priority ? evt.priority : ps->get_recovery_priority();

  PGPeeringEventURef preempt;
  if (HAVE_FEATURE(ps->upacting_features, RECOVERY_RESERVATION_2)) {
    // older peers can't handle this
    preempt = std::make_unique<PGPeeringEvent>(
      ps->get_osdmap_epoch(),
      ps->get_osdmap_epoch(),
      RemoteRecoveryPreempted());
  }

  pl->request_remote_recovery_reservation(
    prio,
    std::make_unique<PGPeeringEvent>(
      ps->get_osdmap_epoch(),
      ps->get_osdmap_epoch(),
      RemoteRecoveryReserved()),
    std::move(preempt));
  return transit<RepWaitRecoveryReserved>();
}

void PeeringState::RepWaitBackfillReserved::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_repwaitbackfillreserved_latency, dur);
}

boost::statechart::result
PeeringState::RepWaitBackfillReserved::react(const RemoteBackfillReserved &evt)
{
  DECLARE_LOCALS;


  pl->send_cluster_message(
      ps->primary.osd,
      TOPNSPC::make_message<MBackfillReserve>(
	MBackfillReserve::GRANT,
	spg_t(ps->info.pgid.pgid, ps->primary.shard),
	ps->get_osdmap_epoch()),
      ps->get_osdmap_epoch());
  return transit<RepRecovering>();
}

boost::statechart::result
PeeringState::RepWaitBackfillReserved::react(
  const RejectTooFullRemoteReservation &evt)
{
  DECLARE_LOCALS;
  ps->reject_reservation();
  post_event(RemoteReservationRejectedTooFull());
  return discard_event();
}

boost::statechart::result
PeeringState::RepWaitBackfillReserved::react(
  const RemoteReservationRejectedTooFull &evt)
{
  DECLARE_LOCALS;
  pl->unreserve_recovery_space();

  pl->cancel_remote_recovery_reservation();
  return transit<RepNotRecovering>();
}

boost::statechart::result
PeeringState::RepWaitBackfillReserved::react(
  const RemoteReservationCanceled &evt)
{
  DECLARE_LOCALS;
  pl->unreserve_recovery_space();

  pl->cancel_remote_recovery_reservation();
  return transit<RepNotRecovering>();
}

/*---RepRecovering-------*/
PeeringState::RepRecovering::RepRecovering(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/ReplicaActive/RepRecovering")
{
  context< PeeringMachine >().log_enter(state_name);
}

boost::statechart::result
PeeringState::RepRecovering::react(const RemoteRecoveryPreempted &)
{
  DECLARE_LOCALS;


  pl->unreserve_recovery_space();
  pl->send_cluster_message(
    ps->primary.osd,
    TOPNSPC::make_message<MRecoveryReserve>(
      MRecoveryReserve::REVOKE,
      spg_t(ps->info.pgid.pgid, ps->primary.shard),
      ps->get_osdmap_epoch()),
    ps->get_osdmap_epoch());
  return discard_event();
}

boost::statechart::result
PeeringState::RepRecovering::react(const BackfillTooFull &)
{
  DECLARE_LOCALS;


  pl->unreserve_recovery_space();
  pl->send_cluster_message(
    ps->primary.osd,
    TOPNSPC::make_message<MBackfillReserve>(
      MBackfillReserve::REVOKE_TOOFULL,
      spg_t(ps->info.pgid.pgid, ps->primary.shard),
      ps->get_osdmap_epoch()),
    ps->get_osdmap_epoch());
  return discard_event();
}

boost::statechart::result
PeeringState::RepRecovering::react(const RemoteBackfillPreempted &)
{
  DECLARE_LOCALS;


  pl->unreserve_recovery_space();
  pl->send_cluster_message(
    ps->primary.osd,
    TOPNSPC::make_message<MBackfillReserve>(
      MBackfillReserve::REVOKE,
      spg_t(ps->info.pgid.pgid, ps->primary.shard),
      ps->get_osdmap_epoch()),
    ps->get_osdmap_epoch());
  return discard_event();
}

void PeeringState::RepRecovering::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;
  pl->unreserve_recovery_space();

  pl->cancel_remote_recovery_reservation();
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_reprecovering_latency, dur);
}

/*------Activating--------*/
PeeringState::Activating::Activating(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Active/Activating")
{
  context< PeeringMachine >().log_enter(state_name);
}

void PeeringState::Activating::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_activating_latency, dur);
}

/**
 * 等待本地 recovery IO 资源预留：只有本地和远端资源都准备好后，
 * PG 才会进入 Recovering 状态并由 OSD recovery 队列执行恢复操作。
 */
PeeringState::WaitLocalRecoveryReserved::WaitLocalRecoveryReserved(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Active/WaitLocalRecoveryReserved")
{
  context< PeeringMachine >().log_enter(state_name);
  DECLARE_LOCALS;

  // recovery 涉及的 acting/recovery/backfill OSD 不能处于 full 状态；
  // 若空间不足，投递 RecoveryTooFull，让状态机安排延迟重试。
  if (!ps->cct->_conf->osd_debug_skip_full_check_in_recovery &&
      ps->get_osdmap()->check_full(ps->acting_recovery_backfill)) {
    post_event(RecoveryTooFull());
    return;
  }

  ps->state_clear(PG_STATE_RECOVERY_TOOFULL);
  // 已开始等待 recovery 资源；资源尚未批准前，recovery 不会真正启动。
  ps->state_set(PG_STATE_RECOVERY_WAIT);
  // 预留成功时重新排队 LocalRecoveryReserved，驱动状态机转到 WaitRemoteRecoveryReserved；
  // 预留被抢占时排队 DeferRecovery，稍后重试。
  pl->request_local_background_io_reservation(
    ps->get_recovery_priority(),
    std::make_unique<PGPeeringEvent>(
      ps->get_osdmap_epoch(),
      ps->get_osdmap_epoch(),
      LocalRecoveryReserved()),
    std::make_unique<PGPeeringEvent>(
      ps->get_osdmap_epoch(),
      ps->get_osdmap_epoch(),
      DeferRecovery(0.0)));
  // 对外发布 PG 当前处于等待 recovery 资源的状态。
  pl->publish_stats_to_osd();
}

boost::statechart::result
PeeringState::WaitLocalRecoveryReserved::react(const RecoveryTooFull &evt)
{
  DECLARE_LOCALS;
  ps->state_set(PG_STATE_RECOVERY_TOOFULL);
  pl->schedule_event_after(
    std::make_shared<PGPeeringEvent>(
      ps->get_osdmap_epoch(),
      ps->get_osdmap_epoch(),
      DoRecovery()),
    ps->cct->_conf->osd_recovery_retry_interval);
  return transit<NotRecovering>();
}

boost::statechart::result
PeeringState::WaitLocalRecoveryReserved::react(const AdvMap& ev)
{
  DECLARE_LOCALS;
  if (!ps->cct->_conf->osd_debug_skip_full_check_in_recovery &&
      ps->get_osdmap()->check_full(ps->acting_recovery_backfill)) {
    post_event(RecoveryTooFull());
    return discard_event();
  }
  return forward_event();
}

void PeeringState::WaitLocalRecoveryReserved::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_waitlocalrecoveryreserved_latency, dur);
}

/**
 * 等待参与 recovery 的远端 OSD 预留资源；构造时从第一个远端 shard 开始，
 * 逐个发送 recovery reservation 请求，全部完成后再进入 Recovering。
 */
PeeringState::WaitRemoteRecoveryReserved::WaitRemoteRecoveryReserved(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Active/WaitRemoteRecoveryReserved"),
    // 记录下一个需要发送 reservation 请求的远端 shard。
    remote_recovery_reservation_it(context< Active >().remote_shards_to_reserve_recovery.begin())
{
  // 记录进入状态；RemoteRecoveryReserved 是启动逐个请求流程的内部事件，
  // 此时并不表示任何远端 OSD 已经完成资源预留。
  context< PeeringMachine >().log_enter(state_name);
  post_event(RemoteRecoveryReserved());
}

/**
 * 推进远端 recovery 资源预留：每次处理一个事件就向下一个远端 shard 发送请求；
 * 所有远端都完成后，通知状态机进入 Recovering。
 */
boost::statechart::result
PeeringState::WaitRemoteRecoveryReserved::react(const RemoteRecoveryReserved &evt) {
  DECLARE_LOCALS;

  if (remote_recovery_reservation_it !=
      context< Active >().remote_shards_to_reserve_recovery.end()) {
    // recovery 资源预留只针对远端 shard；primary 自身已经在本地预留阶段处理。
    ceph_assert(*remote_recovery_reservation_it != ps->pg_whoami);
    // 向当前迭代位置对应的 OSD 请求 recovery 资源，
    // 并携带 PG、shard、epoch 和 recovery 优先级，供对端执行资源检查和预留。
    pl->send_cluster_message(
      remote_recovery_reservation_it->osd,
      TOPNSPC::make_message<MRecoveryReserve>(
	MRecoveryReserve::REQUEST,
	spg_t(context< PeeringMachine >().spgid.pgid,
	      remote_recovery_reservation_it->shard),
	ps->get_osdmap_epoch(),
	ps->get_recovery_priority()),
      ps->get_osdmap_epoch());
    ++remote_recovery_reservation_it;
  } else {
    // 所有远端 reservation 请求都已完成，转入统一处理全部远端已预留事件。
    post_event(AllRemotesReserved());
  }
  return discard_event();
}

boost::statechart::result
PeeringState::WaitRemoteRecoveryReserved::react(const AdvMap& ev)
{
  DECLARE_LOCALS;
  if (!ps->cct->_conf->osd_debug_skip_full_check_in_recovery &&
      ps->get_osdmap()->check_full(ps->acting_recovery_backfill)) {
    post_event(RecoveryTooFull());
    return discard_event();
  }
  return forward_event();
}

void PeeringState::WaitRemoteRecoveryReserved::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_waitremoterecoveryreserved_latency, dur);
}

/**
 * 本地和所有远端 recovery 资源都已预留，进入实际 recovery 阶段；
 * 同时把 PG 加入 OSD 的 recovery 调度队列。
 */
PeeringState::Recovering::Recovering(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Active/Recovering")
{
  // 记录进入 Recovering 状态。
  context< PeeringMachine >().log_enter(state_name);

  DECLARE_LOCALS;
  // 已不再等待资源，也不再处于此前因 full 而暂停的状态。
  ps->state_clear(PG_STATE_RECOVERY_WAIT);
  ps->state_clear(PG_STATE_RECOVERY_TOOFULL);
  // 对外标记 PG 正在执行 recovery；具体对象操作由后续 recovery worker 执行。
  ps->state_set(PG_STATE_RECOVERING);
  // 通知 PrimaryLogPG reservation 已完成；该回调会把 PG 放入 OSD recovery 队列。
  pl->on_recovery_reserved();
  // activation 必须已经结束，recovery 不能与 activation 并行启动。
  ceph_assert(!ps->state_test(PG_STATE_ACTIVATING));
  // 发布更新后的 PG 状态和 recovery 等待信息。
  pl->publish_stats_to_osd();
}

void PeeringState::Recovering::release_reservations(bool cancel)
{
  DECLARE_LOCALS;
  ceph_assert(cancel || !ps->pg_log.get_missing().have_missing());

  // release remote reservations
  for (auto i = context< Active >().remote_shards_to_reserve_recovery.begin();
       i != context< Active >().remote_shards_to_reserve_recovery.end();
       ++i) {
    if (*i == ps->pg_whoami) // skip myself
      continue;
    pl->send_cluster_message(
      i->osd,
      TOPNSPC::make_message<MRecoveryReserve>(
	MRecoveryReserve::RELEASE,
	spg_t(ps->info.pgid.pgid, i->shard),
	ps->get_osdmap_epoch()),
      ps->get_osdmap_epoch());
  }
}

boost::statechart::result
PeeringState::Recovering::react(const AllReplicasRecovered &evt)
{
  DECLARE_LOCALS;
  ps->state_clear(PG_STATE_FORCED_RECOVERY);
  release_reservations();
  pl->cancel_local_background_io_reservation();
  return transit<Recovered>();
}

boost::statechart::result
PeeringState::Recovering::react(const RequestBackfill &evt)
{
  DECLARE_LOCALS;

  release_reservations();

  ps->state_clear(PG_STATE_FORCED_RECOVERY);
  pl->cancel_local_background_io_reservation();
  pl->publish_stats_to_osd();
  // transit any async_recovery_targets back into acting
  // so pg won't have to stay undersized for long
  // as backfill might take a long time to complete..
  if (!ps->async_recovery_targets.empty()) {
    pg_shard_t get_log_shard;
    // FIXME: Uh-oh we have to check this return value; choose_acting can fail!
    ps->choose_acting(get_log_shard, true);
  }
  return transit<WaitLocalBackfillReserved>();
}

boost::statechart::result
PeeringState::Recovering::react(const DeferRecovery &evt)
{
  DECLARE_LOCALS;
  if (!ps->state_test(PG_STATE_RECOVERING)) {
    // we may have finished recovery and have an AllReplicasRecovered
    // event queued to move us to the next state.
    psdout(10) << "got defer recovery but not recovering" << dendl;
    return discard_event();
  }
  psdout(10) << "defer recovery, retry delay " << evt.delay << dendl;
  ps->state_set(PG_STATE_RECOVERY_WAIT);
  pl->cancel_local_background_io_reservation();
  release_reservations(true);
  pl->on_recovery_cancelled();
  pl->schedule_event_after(
    std::make_shared<PGPeeringEvent>(
      ps->get_osdmap_epoch(),
      ps->get_osdmap_epoch(),
      DoRecovery()),
    evt.delay);
  return transit<NotRecovering>();
}

boost::statechart::result
PeeringState::Recovering::react(const UnfoundRecovery &evt)
{
  DECLARE_LOCALS;
  psdout(10) << "recovery has unfound, can't continue" << dendl;
  ps->state_set(PG_STATE_RECOVERY_UNFOUND);
  pl->cancel_local_background_io_reservation();
  release_reservations(true);
  pl->on_recovery_cancelled();
  return transit<NotRecovering>();
}

void PeeringState::Recovering::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);

  DECLARE_LOCALS;
  utime_t dur = ceph_clock_now() - enter_time;
  ps->state_clear(PG_STATE_RECOVERING);
  pl->get_peering_perf().tinc(rs_recovering_latency, dur);
}

/**
 * 进入 Recovered 状态，表示本轮普通 recovery 或 backfill 已完成。
 *
 * 这里重新检查 PG 是否仍需要 recovery，并根据 backfill 完成后的 acting 集合变化调整 acting；
 * 只有所有副本都完成 activation 且不存在异步 recovery target 时，才继续投递 GoClean 进入 Clean 状态。
 */
PeeringState::Recovered::Recovered(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Active/Recovered")
{
  // choose_acting() 通过该参数返回需要获取 PG log 的 shard。
  pg_shard_t get_log_shard;

  context< PeeringMachine >().log_enter(state_name);

  DECLARE_LOCALS;

  // 进入 Recovered 时，PG log 中不应再存在待恢复对象。
  ceph_assert(!ps->needs_recovery());

  // backfill 完成后所有 acting shard 都应已激活；
  // 根据当前 acting 数量重新判断是否还需要保留 DEGRADED 或 UNDERSIZED 状态。
  ceph_assert(!ps->acting_recovery_backfill.empty());
  if (ps->get_osdmap()->get_pg_size(context< PeeringMachine >().spgid.pgid) <=
      ps->acting_recovery_backfill.size()) {
    ps->state_clear(PG_STATE_FORCED_BACKFILL | PG_STATE_FORCED_RECOVERY);
    pl->publish_stats_to_osd();
  }

  // backfill 完成后，之前仅作为 recovery/backfill target 的 OSD 可能需要
  // 正式加入 acting 集合，因此重新计算 acting。
  if (ps->acting != ps->up &&
      !ps->choose_acting(get_log_shard, true)) {
    ceph_assert(ps->want_acting.size());
  } else if (!ps->async_recovery_targets.empty()) {
    // FIXME: Uh-oh we have to check this return value; choose_acting can fail!
    ps->choose_acting(get_log_shard, true);
  }

  // 只有本轮 activation 已完成且没有异步 recovery target，PG 才能继续
  // 进入 Clean 状态；否则仍需等待后续 acting 调整或异步恢复。
  if (context< Active >().all_replicas_activated  &&
      ps->async_recovery_targets.empty())
    post_event(GoClean());
}

void PeeringState::Recovered::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;

  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_recovered_latency, dur);
}

/**
 * 进入 Clean 状态，表示 PG 的 recovery/backfill 已完成且 PG log 已追平。
 *
 * 这里确认 last_complete 已经追上 last_update，将 PG 标记为 clean；
 * on_clean() 会立即执行必要的收尾准备，并返回一个在事务提交后执行的 Context。
 */
PeeringState::Clean::Clean(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Active/Clean")
{
  context< PeeringMachine >().log_enter(state_name);

  DECLARE_LOCALS;

  // Clean 状态要求 PG 的最后完整版本已经追上最后更新版本。
  if (ps->info.last_complete != ps->info.last_update) {
    ceph_abort();
  }

  // 设置 PG_STATE_CLEAN，并更新 PG 状态统计。
  ps->try_mark_clean();

  // on_clean() 先立即准备 recovery 收尾，并将返回的 Context 注册到事务提交回调。
  context< PeeringMachine >().get_cur_transaction().register_on_commit(
    pl->on_clean());
}

void PeeringState::Clean::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);

  DECLARE_LOCALS;
  ps->state_clear(PG_STATE_CLEAN);
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_clean_latency, dur);
}

template <typename T>
set<pg_shard_t> unique_osd_shard_set(const pg_shard_t & skip, const T &in)
{
  set<int> osds_found;
  set<pg_shard_t> out;
  for (auto i = in.begin(); i != in.end(); ++i) {
    if (*i != skip && !osds_found.count(i->osd)) {
      osds_found.insert(i->osd);
      out.insert(*i);
    }
  }
  return out;
}

/*---------Active---------*/
/**
 * 进入 primary 的 Active 状态：确定本轮 recovery/backfill 需向哪些远端 OSD
 * 申请资源预留，
 * 发起本地及副本激活，并等待参与副本提交确认后才完成真正的 active。
 */
PeeringState::Active::Active(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Active"),
    // 去掉本地 shard，并按 OSD 去重。一个 OSD 即使承载多个 shard，
    // 也只发送一次 recovery 资源预留请求。
    remote_shards_to_reserve_recovery(
      unique_osd_shard_set(
	context< PeeringMachine >().state->pg_whoami,
	context< PeeringMachine >().state->acting_recovery_backfill)),
    // backfill 的远端预留集合独立计算：它只针对 backfill target，未必等于 recovery 集合。
    remote_shards_to_reserve_backfill(
      unique_osd_shard_set(
	context< PeeringMachine >().state->pg_whoami,
	context< PeeringMachine >().state->backfill_targets)),
    // 只有收到所有参与副本的激活提交确认后才置为 true。
    all_replicas_activated(false)
{
  context< PeeringMachine >().log_enter(state_name);


  DECLARE_LOCALS;

  // Active 只能由 primary 进入，且此时还未持有 backfill 的本地资源预留。
  ceph_assert(!ps->backfill_reserved);
  ceph_assert(ps->is_primary());

  // 将 activation 产生的 PGInfo/PGLog 修改绑定到当前事务，并登记一次 flush；
  // 后续 activation 完成条件会等待该 flush 的持久化回调。
  psdout(10) << "In Active, about to call activate" << dendl;
  ps->start_flush(context< PeeringMachine >().get_cur_transaction());

  // 建立本 interval 的已 peered/activating 运行状态，向参与副本发送激活相关消息，
  // 并准备 missing/recovery 状态；具体状态修改由 activate() 完成。
  ps->activate(context< PeeringMachine >().get_cur_transaction(),
	       ps->get_osdmap_epoch(),
	       context< PeeringMachine >().get_recovery_ctx());

  // everyone has to commit/ack before we are truly active
  // // 将尚未完成 activation commit 的参与方显示为 blocker
  ps->blocked_by.clear();
  for (auto p = ps->acting_recovery_backfill.begin();
       p != ps->acting_recovery_backfill.end();
       ++p) {
    if (p->shard != ps->pg_whoami.shard) {
      ps->blocked_by.insert(static_cast<int>(p->shard));
    }
  }
  pl->publish_stats_to_osd();
  psdout(10) << "Activate Finished" << dendl;
}

boost::statechart::result PeeringState::Active::react(const AdvMap& advmap)
{
  DECLARE_LOCALS;

  // up/acting primary、成员或 interval 的关键变化会使现有 peering 结论失效。
  // 此时 Active 不自行收尾，转发 AdvMap 给外层状态，由其进入 Reset 并重新 peering。
  if (ps->should_restart_peering(
	advmap.up_primary,
	advmap.acting_primary,
	advmap.newup,
	advmap.newacting,
	advmap.lastmap,
	advmap.osdmap)) {
    psdout(10) << "Active advmap interval change, fast return" << dendl;
    return forward_event();
  }
  psdout(10) << "Active advmap" << dendl;

  // interval 未变化，PG 可以保持 Active；通知 PG 外层处理 active 期间的地图更新。
  pl->on_active_advmap(advmap.osdmap);
  if (ps->dirty_big_info) {
    // share updated purged_snaps to mgr/mon so that we (a) stop reporting
    // purged snaps and (b) perhaps share more snaps that we have purged
    // but didn't fit in pg_stat_t.
    ps->share_pg_info();
  }

  bool need_acting_change = false;
  for (size_t i = 0; i < ps->want_acting.size(); i++) {
    int osd = ps->want_acting[i];
    if (!advmap.osdmap->is_up(osd)) {
      pg_shard_t osd_with_shard(osd, shard_id_t(i));
      // 该 OSD 已 down，且不属于当前 up/acting 集合：它只是 want_acting 中的旧候选，需要重新选择 acting
      if (!ps->is_acting(osd_with_shard) && !ps->is_up(osd_with_shard)) {
        psdout(10) << "Active stray osd." << osd << " in want_acting is down"
                   << dendl;
        need_acting_change = true;
      }
    }
  }
  if (need_acting_change) {
     psdout(10) << "Active need acting change, call choose_acting again"
                << dendl;
    // possibly because we re-add some strays into the acting set and
    // some of them then go down in a subsequent map before we could see
    // the map changing the pg temp.
    // call choose_acting again to clear them out.
    // note that we leave restrict_to_up_acting to false in order to
    // not overkill any chosen stray that is still alive.
    pg_shard_t get_log_shard;
    // 清理其 peer 信息并重新选择 acting
    ps->remove_down_peer_info(advmap.osdmap);
    ps->choose_acting(get_log_shard, false, true);
  }

  /* Check for changes in pool size (if the acting set changed as a result,
   * this does not matter) */
  // 副本数配置变化时，仅按当前 actingset 是否满足新 size 更新 UNDERSIZED 标志；
  if (advmap.lastmap->get_pg_size(ps->info.pgid.pgid) !=
      ps->get_osdmap()->get_pg_size(ps->info.pgid.pgid)) {
    if (ps->get_osdmap()->get_pg_size(ps->info.pgid.pgid) <=
	ps->actingset.size()) {
      ps->state_clear(PG_STATE_UNDERSIZED);
    } else {
      ps->state_set(PG_STATE_UNDERSIZED);
    }
    // degraded changes will be detected by call from publish_stats_to_osd()
  }

  // degraded 状态由 publish_stats_to_osd 发布 PG 统计时重新计算。
  pl->publish_stats_to_osd();

  // 新地图可能使此前可读的 OSD down；需要重新判断 PG 的可读性并唤醒/阻塞读请求。
  if (ps->check_prior_readable_down_osds(advmap.osdmap)) {
    pl->recheck_readable();
  }

  // Active 处理完自身逻辑后，继续向父状态传播 AdvMap，使外层状态也能处理该地图事件。
  return forward_event();
}

boost::statechart::result PeeringState::Active::react(const ActMap&)
{
  DECLARE_LOCALS;
  psdout(10) << "Active: handling ActMap" << dendl;
  ceph_assert(ps->is_primary());

  pl->on_active_actmap();

  if (ps->have_unfound()) {
    // object may have become unfound
    ps->discover_all_missing(context<PeeringMachine>().get_recovery_ctx().msgs);
  }

  uint64_t unfound = ps->missing_loc.num_unfound();
  if (unfound > 0 &&
      ps->all_unfound_are_queried_or_lost(ps->get_osdmap())) {
    if (ps->cct->_conf->osd_auto_mark_unfound_lost) {
      pl->get_clog_error() << context< PeeringMachine >().spgid.pgid << " has " << unfound
			    << " objects unfound and apparently lost, would automatically "
			    << "mark these objects lost but this feature is not yet implemented "
			    << "(osd_auto_mark_unfound_lost)";
    } else
      pl->get_clog_error() << context< PeeringMachine >().spgid.pgid << " has "
                             << unfound << " objects unfound and apparently lost";
  }

  return forward_event();
}

boost::statechart::result PeeringState::Active::react(const MNotifyRec& notevt)
{

  DECLARE_LOCALS;
  ceph_assert(ps->is_primary());
  if (ps->peer_info.count(notevt.from)) {
    psdout(10) << "Active: got notify from " << notevt.from
		       << ", already have info from that osd, ignoring"
		       << dendl;
  } else if (ps->peer_purged.count(notevt.from)) {
    psdout(10) << "Active: got notify from " << notevt.from
		       << ", already purged that peer, ignoring"
		       << dendl;
  } else {
    psdout(10) << "Active: got notify from " << notevt.from
		       << ", calling proc_replica_notify and discover_all_missing"
		       << dendl;
    ps->proc_replica_notify(notevt.from, notevt.notify);
    if (ps->have_unfound() || (ps->is_degraded() && ps->might_have_unfound.count(notevt.from))) {
      ps->discover_all_missing(
	context<PeeringMachine>().get_recovery_ctx().msgs);
    }
    // check if it is a previous down acting member that's coming back.
    // if so, request pg_temp change to trigger a new interval transition
    pg_shard_t get_log_shard;
    // FIXME: Uh-oh we have to check this return value; choose_acting can fail!
    ps->choose_acting(get_log_shard, false, true);
    if (!ps->want_acting.empty() && ps->want_acting != ps->acting) {
      psdout(10) << "Active: got notify from previous acting member "
                 << notevt.from << ", requesting pg_temp change"
                 << dendl;
    }
  }
  return discard_event();
}

boost::statechart::result PeeringState::Active::react(const MTrim& trim)
{
  DECLARE_LOCALS;
  ceph_assert(ps->is_primary());

  // peer is informing us of their last_complete_ondisk
  ldout(ps->cct,10) << " replica osd." << trim.from << " lcod " << trim.trim_to << dendl;
  ps->update_peer_last_complete_ondisk(pg_shard_t{trim.from, trim.shard},
                                       trim.trim_to);
  // trim log when the pg is recovered
  ps->calc_min_last_complete_ondisk();
  return discard_event();
}

/**
 * 处理 acting 副本返回的 PGInfo：记录其 activation 事务已经提交，更新 lease 确认和 PG 统计；
 * 当 primary 及所有参与副本都完成提交后，触发 all_activated_and_committed()，进入真正激活的收尾流程。
 */
boost::statechart::result PeeringState::Active::react(const MInfoRec& infoevt)
{
  DECLARE_LOCALS;
  // Active 的 MInfoRec 只能由 primary 处理；副本回报会从 ReplicaActive 路径处理。
  ceph_assert(ps->is_primary());

  // 没有 acting/recovery/backfill 参与者就没有可等待的 activation 回报。
  ceph_assert(!ps->acting_recovery_backfill.empty());
  if (infoevt.lease_ack) {
    // 副本可能同时携带 lease ack；先把该确认交给 lease 管理逻辑。
    ps->proc_lease_ack(infoevt.from.osd, *infoevt.lease_ack);
  }

  // 只有当前 acting/recovery/backfill 集中的副本才参与本轮激活确认。
  // peer_activated 是去重集合，重复到达的 MInfoRec 不会重复计数。
  if (ps->is_acting_recovery_backfill(infoevt.from) &&
      ps->peer_activated.insert(infoevt.from).second) {
    psdout(10) << " peer osd." << infoevt.from
	       << " activated and committed" << dendl;

    // 该副本已不再阻塞 PG 激活；blocked_by 主要用于对外发布当前等待状态。
    ps->blocked_by.erase(static_cast<int>(infoevt.from.shard));
    pl->publish_stats_to_osd();

    // 收到最后一个参与副本的确认后，与 primary 自己的 ActivateCommitted 汇合。
    if (ps->peer_activated.size() == ps->acting_recovery_backfill.size()) {
      all_activated_and_committed();
    }
  }

  // MInfoRec 已在当前状态消费；不发生状态转换，后续事件继续由 Active 处理。
  return discard_event();
}

boost::statechart::result PeeringState::Active::react(const MLogRec& logevt)
{
  DECLARE_LOCALS;
  psdout(10) << "searching osd." << logevt.from
		     << " log for unfound items" << dendl;
  ps->proc_replica_log(
    logevt.msg->info, logevt.msg->log, std::move(logevt.msg->missing), logevt.from);
  bool got_missing = ps->search_for_missing(
    ps->peer_info[logevt.from],
    ps->peer_missing[logevt.from],
    logevt.from,
    context< PeeringMachine >().get_recovery_ctx());
  // If there are missing AND we are "fully" active then start recovery now
  if (got_missing && ps->state_test(PG_STATE_ACTIVE)) {
    post_event(DoRecovery());
  }
  return discard_event();
}

boost::statechart::result PeeringState::Active::react(const QueryState& q)
{
  DECLARE_LOCALS;

  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;

  {
    q.f->open_array_section("might_have_unfound");
    for (auto p = ps->might_have_unfound.begin();
	 p != ps->might_have_unfound.end();
	 ++p) {
      q.f->open_object_section("osd");
      q.f->dump_stream("osd") << *p;
      if (ps->peer_missing.count(*p)) {
	q.f->dump_string("status", "already probed");
      } else if (ps->peer_missing_requested.count(*p)) {
	q.f->dump_string("status", "querying");
      } else if (!ps->get_osdmap()->is_up(p->osd)) {
	q.f->dump_string("status", "osd is down");
      } else {
	q.f->dump_string("status", "not queried");
      }
      q.f->close_section();
    }
    q.f->close_section();
  }
  {
    q.f->open_object_section("recovery_progress");
    q.f->open_array_section("backfill_targets");
    for (auto p = ps->backfill_targets.begin();
	 p != ps->backfill_targets.end(); ++p)
      q.f->dump_stream("replica") << *p;
    q.f->close_section();
    pl->dump_recovery_info(q.f);
    q.f->close_section();
  }

  q.f->close_section();
  return forward_event();
}

boost::statechart::result PeeringState::Active::react(const QueryUnfound& q)
{
  DECLARE_LOCALS;

  ps->query_unfound(q.f, "Active");
  return discard_event();
}

/**
 * 处理 primary 自己的 activation 事务提交事件：将本地 primary 加入 peer_activated，
 * 并在它与所有 acting/recovery/backfill 副本都确认后，触发 all_activated_and_committed()。
 */
boost::statechart::result PeeringState::Active::react(
  const ActivateCommitted &evt)
{
  DECLARE_LOCALS;

  // ActivateCommitted 由 activate() 注册在 ObjectStore 事务提交回调中，
  // 因此此时只能把 primary 自己标记为已激活且已提交。
  auto p = ps->peer_activated.insert(ps->pg_whoami);
  // 同一 activation 事务不应重复产生本地提交事件。
  ceph_assert(p.second);
  psdout(10) << "_activate_committed " << evt.epoch
	     << " peer_activated now " << ps->peer_activated
	     << " last_interval_started "
	     << ps->info.history.last_interval_started
	     << " last_epoch_started "
	     << ps->info.history.last_epoch_started
	     << " same_interval_since "
	     << ps->info.history.same_interval_since
	     << dendl;
  // acting/recovery/backfill 至少包含 primary；它定义本轮必须完成确认的参与者总数。
  ceph_assert(!ps->acting_recovery_backfill.empty());
  // 副本确认由 Active::react(MInfoRec) 加入 peer_activated；数量齐全后统一收尾。
  if (ps->peer_activated.size() == ps->acting_recovery_backfill.size())
    all_activated_and_committed();

  // 当前 ActivateCommitted 已消费；若尚有副本未确认，继续等待后续 MInfoRec。
  return discard_event();
}

/**
 * 处理所有参与 shard 都完成 activation 提交后的事件：结束 ACTIVATING，
 * 根据 acting 集是否可写设置 ACTIVE/PEERED，发布最终 PGInfo，并通知 OSD 层 activation 已完成。
 */
boost::statechart::result PeeringState::Active::react(const AllReplicasActivated &evt)
{

  DECLARE_LOCALS;
  pg_t pgid = context< PeeringMachine >().spgid.pgid;

  // 该事件表示 primary 和所有参与副本都已完成 activation；后续不再等待副本确认。
  all_replicas_activated = true;

  // 清理本轮 activation/创建/合并准备阶段的临时状态位。
  ps->state_clear(PG_STATE_ACTIVATING);
  ps->state_clear(PG_STATE_CREATING);
  ps->state_clear(PG_STATE_PREMERGE);

  bool merge_target;
  if (ps->pool.info.is_pending_merge(pgid, &merge_target)) {
    // 合并尚未真正执行时，PG 只能先标记为 PEERED，并保留 PREMERGE。
    ps->state_set(PG_STATE_PEERED);
    ps->state_set(PG_STATE_PREMERGE);

    // acting 数量不足 pool 期望的 PG size 时，通知合并逻辑暂不可继续。
    if (ps->actingset.size() != ps->get_osdmap()->get_pg_size(pgid)) {
      if (merge_target) {
	// 当前 PG 是合并目标：记录其源 PG 尚未准备好。
	pg_t src = pgid;
	src.set_ps(ps->pool.info.get_pg_num_pending());
	ceph_assert(src.get_parent() == pgid);
	pl->set_not_ready_to_merge_target(pgid, src);
      } else {
	// 当前 PG 是合并源：记录它尚未满足合并目标的就绪条件。
	pl->set_not_ready_to_merge_source(pgid);
      }
    }
  } else if (!ps->acting_set_writeable()) {
    // acting 集不足 min_size 或不满足 stretch 约束，只能 peered，不能对外写入。
    ps->state_set(PG_STATE_PEERED);
  } else {
    // acting 集满足写入条件，本轮 PG 正式进入 ACTIVE。
    ps->state_set(PG_STATE_ACTIVE);
  }

  auto mnow = pl->get_mnow();
  // prior_readable_until_ub 是旧 interval 中某些 OSD 的读 lease 最晚有效时间
  if (ps->prior_readable_until_ub > mnow) {
    // 旧 interval 的可读租约尚未到期；先保留 WAIT，直到该时间上界后再检查可读性。
    psdout(10) << " waiting for prior_readable_until_ub "
	       << ps->prior_readable_until_ub << " > mnow " << mnow << dendl;
    ps->state_set(PG_STATE_WAIT);
    pl->queue_check_readable(
      ps->last_peering_reset,
      ps->prior_readable_until_ub - mnow);
  } else {
    // 没有未到期的旧租约约束，可以继续提供当前状态。
    psdout(10) << " mnow " << mnow << " >= prior_readable_until_ub "
	       << ps->prior_readable_until_ub << dendl;
  }

  if (ps->pool.info.has_flag(pg_pool_t::FLAG_CREATING)) {
    // pool 仍处于 creating 流程时，向上层确认该 PG 已完成创建/激活。
    pl->send_pg_created(pgid);
  }

  psdout(1) << __func__ << " AllReplicasActivated Activating complete" << dendl;

  // activation 提交完成后推进 history 的 started 位置，并标记 PGInfo 需要持久化。
  ps->info.history.last_epoch_started = ps->info.last_epoch_started;
  ps->info.history.last_interval_started = ps->info.last_interval_started;
  ps->dirty_info = true;

  // 把最新 PGInfo/lease 分享给副本，并发布统计；最后通知 OSD 后端完成 activation。
  ps->share_pg_info();
  pl->publish_stats_to_osd();

  pl->on_activate_complete();

  return discard_event();
}

boost::statechart::result PeeringState::Active::react(const RenewLease& rl)
{
  DECLARE_LOCALS;
  ps->proc_renew_lease();
  return discard_event();
}

boost::statechart::result PeeringState::Active::react(const MLeaseAck& la)
{
  DECLARE_LOCALS;
  ps->proc_lease_ack(la.from, la.lease_ack);
  return discard_event();
}


boost::statechart::result PeeringState::Active::react(const CheckReadable &evt)
{
  DECLARE_LOCALS;
  pl->recheck_readable();
  return discard_event();
}

boost::statechart::result PeeringState::Active::react(const PgCreateEvt &evt)
{
  DECLARE_LOCALS;
  pg_t pgid = context< PeeringMachine >().spgid.pgid;

  psdout(10) << __func__ << " receive PgCreateEvt"
             << " is_peered=" << ps->is_peered() << dendl;

  if (ps->is_peered()) {
    psdout(10) << __func__ << " pg is peered, reply pg_created" << dendl;
    pl->send_pg_created(pgid);
  }

  return discard_event();
}

/**
 * 在 primary 及所有参与副本都完成 activation 并提交事务后，执行激活收尾：
 * 重新覆盖 lease、更新 degraded 统计，并投递 AllReplicasActivated，
 * 由该事件处理函数推进 PG_STATE_ACTIVE/PG_STATE_PEERED 等最终状态。
 *
 * 只有此时 peering 结果才已经稳定持久化，才能推进 info.history.last_epoch_started。
 */
void PeeringState::Active::all_activated_and_committed()
{
  DECLARE_LOCALS;
  psdout(10) << "all_activated_and_committed" << dendl;

  // 该收尾只由 primary 执行，且 peer_activated 必须覆盖本轮所有参与 shard。
  ceph_assert(ps->is_primary());
  ceph_assert(ps->peer_activated.size() == ps->acting_recovery_backfill.size());
  ceph_assert(!ps->acting_recovery_backfill.empty());

  // blocked_by 为空表示没有仍在等待 activation 提交的副本。
  ceph_assert(ps->blocked_by.empty());

  // 当前版本要求所有参与 OSD 支持 lease 机制。
  ceph_assert(HAVE_FEATURE(ps->upacting_features, SERVER_OCTOPUS));

  // activation 期间可能耗时较长；activate() 生成的旧 lease 可能即将过期，
  // 因此此处立即续租、发送给副本，并重新安排后续续租，避免可读性保护出现空洞。
  ps->renew_lease(pl->get_mnow());
  ps->send_lease();
  ps->schedule_renew_lease();

  // 按当前 missing/recovery 结果重新计算统计，并同步 PG_STATE_DEGRADED。
  ps->update_calc_stats();
  if (ps->info.stats.stats.sum.num_objects_degraded) {
    ps->state_set(PG_STATE_DEGRADED);
  } else {
    ps->state_clear(PG_STATE_DEGRADED);
  }

  // 将“所有参与者已激活并提交”转换为状态机事件；
  // 真正设置 ACTIVE/PEERED 的逻辑在 Active::react(AllReplicasActivated) 中执行。
  post_event(PeeringState::AllReplicasActivated());
}


void PeeringState::Active::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);


  DECLARE_LOCALS;
  pl->cancel_local_background_io_reservation();

  ps->blocked_by.clear();
  ps->backfill_reserved = false;
  ps->state_clear(PG_STATE_ACTIVATING);
  ps->state_clear(PG_STATE_DEGRADED);
  ps->state_clear(PG_STATE_UNDERSIZED);
  ps->state_clear(PG_STATE_BACKFILL_TOOFULL);
  ps->state_clear(PG_STATE_BACKFILL_WAIT);
  ps->state_clear(PG_STATE_RECOVERY_WAIT);
  ps->state_clear(PG_STATE_RECOVERY_TOOFULL);
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_active_latency, dur);
  pl->on_active_exit();
}

/*------ReplicaActive-----*/
PeeringState::ReplicaActive::ReplicaActive(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/ReplicaActive")
{
  context< PeeringMachine >().log_enter(state_name);

  DECLARE_LOCALS;
  ps->start_flush(context< PeeringMachine >().get_cur_transaction());
}


boost::statechart::result PeeringState::ReplicaActive::react(
  const Activate& actevt) {
  DECLARE_LOCALS;
  psdout(10) << "In ReplicaActive, about to call activate" << dendl;
  ps->activate(
    context< PeeringMachine >().get_cur_transaction(),
    actevt.activation_epoch,
    context< PeeringMachine >().get_recovery_ctx());
  psdout(10) << "Activate Finished" << dendl;
  return discard_event();
}

boost::statechart::result PeeringState::ReplicaActive::react(
  const ActivateCommitted &evt)
{
  DECLARE_LOCALS;
  psdout(10) << __func__ << " " << evt.epoch << " telling primary" << dendl;

  auto &rctx = context<PeeringMachine>().get_recovery_ctx();
  auto epoch = ps->get_osdmap_epoch();
  pg_info_t i = ps->info;
  i.history.last_epoch_started = evt.activation_epoch;
  i.history.last_interval_started = i.history.same_interval_since;
  if (!i.partial_writes_last_complete.empty()) {
    psdout(20) << "sending info to " << ps->get_primary() << " pwlc=e"
	       << i.partial_writes_last_complete_epoch
	       << ":" << i.partial_writes_last_complete
	       << " info=" << i << dendl;
  }
  rctx.send_info(
    ps->get_primary().osd,
    spg_t(ps->info.pgid.pgid, ps->get_primary().shard),
    epoch,
    epoch,
    i,
    {}, /* lease */
    ps->get_lease_ack());

  if (ps->acting_set_writeable()) {
    ps->state_set(PG_STATE_ACTIVE);
  } else {
    ps->state_set(PG_STATE_PEERED);
  }
  /* Update the state in the stats. This will normally get overwritten when the next write
   * transaction is applied, but if the PG is idle then these stats may get copied to the
   * next primary (see PG::merge_log). The state update ensures that prepare_stats_for_publish
   * will update last_active when the primary changes and this stops health check generating
   * false positive stuck in peering alerts.
   */
  if (ps->info.stats.state != ps->state) {
    ps->info.stats.state = ps->state;
  }
  pl->on_activate_committed();

  return discard_event();
}

boost::statechart::result PeeringState::ReplicaActive::react(const MLease& l)
{
  DECLARE_LOCALS;
  spg_t spgid = context< PeeringMachine >().spgid;
  epoch_t epoch = pl->get_osdmap_epoch();

  ps->proc_lease(l.lease);
  pl->send_cluster_message(
    ps->get_primary().osd,
    TOPNSPC::make_message<MOSDPGLeaseAck>(epoch,
		       spg_t(spgid.pgid, ps->get_primary().shard),
		       ps->get_lease_ack()),
    epoch);
  return discard_event();
}

boost::statechart::result PeeringState::ReplicaActive::react(const MInfoRec& infoevt)
{
  DECLARE_LOCALS;
  ps->proc_primary_info(context<PeeringMachine>().get_cur_transaction(),
			infoevt.info);
  return discard_event();
}

boost::statechart::result PeeringState::ReplicaActive::react(const MLogRec& logevt)
{
  DECLARE_LOCALS;
  psdout(10) << "received log from " << logevt.from << dendl;
  MOSDPGLog *msg = logevt.msg.get();
  ObjectStore::Transaction &t = context<PeeringMachine>().get_cur_transaction();
  if (msg->info.partial_writes_last_complete.contains(ps->pg_whoami.shard)) {
    ps->apply_pwlc(msg->info.partial_writes_last_complete[ps->pg_whoami.shard],
		   ps->pg_whoami, ps->info, &ps->pg_log);
  }
  ps->merge_log(t, logevt.msg->info, std::move(logevt.msg->log), logevt.from);
  ps->update_peer_info(logevt.from, logevt.msg->info);
  ceph_assert(ps->pg_log.get_head() == ps->info.last_update);
  if (logevt.msg->lease) {
    ps->proc_lease(*logevt.msg->lease);
  }

  return discard_event();
}

boost::statechart::result PeeringState::ReplicaActive::react(const MTrim& trim)
{
  DECLARE_LOCALS;
  // primary is instructing us to trim
  eversion_t trim_to = trim.trim_to;
  if (ps->pool.info.allows_ecoptimizations() &&
      (trim_to > ps->pg_log.get_can_rollback_to())) {
    // An exceptionally long sequence of partial writes followed by a full
    // write can result in trim_to being ahead of crt
    trim_to = ps->pg_log.get_can_rollback_to();
  }
  ps->pg_log.trim(trim.trim_to, ps->info);
  ps->dirty_info = true;
  return discard_event();
}

boost::statechart::result PeeringState::ReplicaActive::react(const ActMap&)
{
  DECLARE_LOCALS;
  if (ps->should_send_notify() && ps->get_primary().osd >= 0) {
    ps->info.history.refresh_prior_readable_until_ub(
      pl->get_mnow(), ps->prior_readable_until_ub);
    context< PeeringMachine >().send_notify(
      ps->get_primary().osd,
      pg_notify_t(
	ps->get_primary().shard, ps->pg_whoami.shard,
	ps->get_osdmap_epoch(),
	ps->get_osdmap_epoch(),
	ps->info,
	ps->past_intervals,
	ps->local_pg_acting_features));
  }
  return discard_event();
}

boost::statechart::result PeeringState::ReplicaActive::react(
  const MQuery& query)
{
  DECLARE_LOCALS;
  ps->fulfill_query(query, context<PeeringMachine>().get_recovery_ctx());
  return discard_event();
}

boost::statechart::result PeeringState::ReplicaActive::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->close_section();
  return forward_event();
}

boost::statechart::result PeeringState::ReplicaActive::react(const QueryUnfound& q)
{
  q.f->dump_string("state", "ReplicaActive");
  q.f->dump_bool("available_might_have_unfound", false);
  return discard_event();
}

void PeeringState::ReplicaActive::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;
  pl->unreserve_recovery_space();

  pl->cancel_remote_recovery_reservation();
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_replicaactive_latency, dur);

  ps->min_last_complete_ondisk = eversion_t();
}

/*-------Stray---*/
PeeringState::Stray::Stray(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Stray")
{
  context< PeeringMachine >().log_enter(state_name);


  DECLARE_LOCALS;
  ceph_assert(!ps->is_peered());
  ceph_assert(!ps->is_peering());
  ceph_assert(!ps->is_primary());

  if (!ps->get_osdmap()->have_pg_pool(ps->info.pgid.pgid.pool())) {
    ldout(ps->cct,10) << __func__ << " pool is deleted" << dendl;
    post_event(DeleteStart());
  } else {
    ps->start_flush(context< PeeringMachine >().get_cur_transaction());
  }
}

boost::statechart::result PeeringState::Stray::react(const MLogRec& logevt)
{
  DECLARE_LOCALS;
  MOSDPGLog *msg = logevt.msg.get();
  psdout(10) << "got info+log from osd." << logevt.from << " " << msg->info << " " << msg->log << dendl;

  ObjectStore::Transaction &t = context<PeeringMachine>().get_cur_transaction();
  if (msg->info.last_backfill == hobject_t()) {
    // restart backfill
    ps->info = msg->info;
    ps->dirty_info = true;
    ps->dirty_big_info = true;  // maybe.

    PGLog::LogEntryHandlerRef rollbacker{pl->get_log_handler(t)};
    ps->pg_log.reset_backfill_claim_log(msg->log, &ps->info, rollbacker.get());

    ps->pg_log.reset_backfill();
  } else {
    if (msg->info.partial_writes_last_complete.contains(ps->pg_whoami.shard)) {
      ps->apply_pwlc(msg->info.partial_writes_last_complete[ps->pg_whoami.shard],
		     ps->pg_whoami, ps->info, &ps->pg_log);
    }
    ps->merge_log(t, msg->info, std::move(msg->log), logevt.from);
    ps->update_peer_info(logevt.from, msg->info);
  }
  if (logevt.msg->lease) {
    ps->proc_lease(*logevt.msg->lease);
  }

  ceph_assert(ps->pg_log.get_head() == ps->info.last_update);

  post_event(Activate(logevt.msg->info.last_epoch_started));
  return transit<ReplicaActive>();
}

boost::statechart::result PeeringState::Stray::react(const MInfoRec& infoevt)
{
  DECLARE_LOCALS;
  psdout(10) << "got info from osd." << infoevt.from << " " << infoevt.info << dendl;

  if (ps->info.last_update > infoevt.info.last_update) {
    // rewind divergent log entries
    ObjectStore::Transaction &t = context<PeeringMachine>().get_cur_transaction();
    ps->rewind_divergent_log(t, infoevt.info.last_update);
    ps->info.stats = infoevt.info.stats;
    ps->info.hit_set = infoevt.info.hit_set;
  }

  if (infoevt.lease) {
    ps->proc_lease(*infoevt.lease);
  }

  if (infoevt.info.last_update > ps->info.last_update) {
    // Log is missing entries, this is only allowed if the
    // missing entries are all partial writes that did not
    // update this shard

    // Must be a non-primary shard (which implies it is an EC pool
    // with ec_optimizations_main set)
    ceph_assert(ps->pool.info.is_nonprimary_shard(ps->pg_whoami.shard));
    // There must be a partial write last_complete entry for this shard
    ceph_assert(infoevt.info.partial_writes_last_complete.contains(
						   ps->pg_whoami.shard));
    auto pwlc = infoevt.info.partial_writes_last_complete.at(
						   ps->pg_whoami.shard);
    psdout(20) << "info from osd." << infoevt.from
	       << " last_update=" << infoevt.info.last_update
	       << " last_complete=" << infoevt.info.last_complete
	       << " pwlc=e" << infoevt.info.partial_writes_last_complete_epoch
	       << ":" << pwlc
	       << " our last_update=" << ps->info.last_update << dendl;
    // Our last update must be in the range described by partial write
    // last_complete
    ceph_assert(ps->info.last_update >= pwlc.first);
    // Last complete must match the partial write last_update
    ceph_assert(pwlc.second == infoevt.info.last_update);
  } else {
    // Log must match after any divergent entries were rewound
    ceph_assert(infoevt.info.last_update == ps->info.last_update);
  }
  // Log must be consistent with info
  ceph_assert(ps->pg_log.get_head() == ps->info.last_update);
  // Update pwlc
  ps->update_peer_info(infoevt.from, infoevt.info);
  post_event(Activate(infoevt.info.last_epoch_started));
  return transit<ReplicaActive>();
}

boost::statechart::result PeeringState::Stray::react(const MQuery& query)
{
  DECLARE_LOCALS;
  ps->fulfill_query(query, context<PeeringMachine>().get_recovery_ctx());
  return discard_event();
}

boost::statechart::result PeeringState::Stray::react(const ActMap&)
{
  DECLARE_LOCALS;
  if (ps->should_send_notify() && ps->get_primary().osd >= 0) {
    ps->info.history.refresh_prior_readable_until_ub(
      pl->get_mnow(), ps->prior_readable_until_ub);
    context< PeeringMachine >().send_notify(
      ps->get_primary().osd,
      pg_notify_t(
	ps->get_primary().shard, ps->pg_whoami.shard,
	ps->get_osdmap_epoch(),
	ps->get_osdmap_epoch(),
	ps->info,
	ps->past_intervals,
	ps->local_pg_acting_features));
  }
  return discard_event();
}

void PeeringState::Stray::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_stray_latency, dur);
}


/*--------ToDelete----------*/
PeeringState::ToDelete::ToDelete(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/ToDelete")
{
  context< PeeringMachine >().log_enter(state_name);
  DECLARE_LOCALS;
  pl->get_perf_logger().inc(l_osd_pg_removing);
}

void PeeringState::ToDelete::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;
  // note: on a successful removal, this path doesn't execute. see
  // do_delete_work().
  pl->get_perf_logger().dec(l_osd_pg_removing);

  pl->cancel_local_background_io_reservation();
}

/*----WaitDeleteReserved----*/
PeeringState::WaitDeleteReserved::WaitDeleteReserved(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history,
	       "Started/ToDelete/WaitDeleteReserved")
{
  context< PeeringMachine >().log_enter(state_name);
  DECLARE_LOCALS;
  context< ToDelete >().priority = ps->get_delete_priority();

  pl->cancel_local_background_io_reservation();
  pl->request_local_background_io_reservation(
    context<ToDelete>().priority,
    std::make_unique<PGPeeringEvent>(
      ps->get_osdmap_epoch(),
      ps->get_osdmap_epoch(),
      DeleteReserved()),
    std::make_unique<PGPeeringEvent>(
      ps->get_osdmap_epoch(),
      ps->get_osdmap_epoch(),
      DeleteInterrupted()));
}

boost::statechart::result PeeringState::ToDelete::react(
  const ActMap& evt)
{
  DECLARE_LOCALS;
  if (ps->get_delete_priority() != priority) {
    psdout(10) << __func__ << " delete priority changed, resetting"
		   << dendl;
    return transit<ToDelete>();
  }
  return discard_event();
}

void PeeringState::WaitDeleteReserved::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
}

/*----Deleting-----*/
PeeringState::Deleting::Deleting(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/ToDelete/Deleting")
{
  context< PeeringMachine >().log_enter(state_name);

  DECLARE_LOCALS;
  ps->deleting = true;
  ObjectStore::Transaction &t = context<PeeringMachine>().get_cur_transaction();

  // clear log
  PGLog::LogEntryHandlerRef rollbacker{pl->get_log_handler(t)};
  ps->pg_log.roll_forward(&ps->info, rollbacker.get());
  // invalidate pwlc
  ps->info.partial_writes_last_complete.clear();
  // adjust info to backfill
  ps->info.set_last_backfill(hobject_t());
  ps->pg_log.reset_backfill();
  ps->dirty_info = true;

  pl->on_removal(t);
}

boost::statechart::result PeeringState::Deleting::react(
  const DeleteSome& evt)
{
  DECLARE_LOCALS;
  std::pair<ghobject_t, bool> p;
  p = pl->do_delete_work(context<PeeringMachine>().get_cur_transaction(),
    next);
  next = p.first;
  return p.second ? discard_event() : terminate();
}

void PeeringState::Deleting::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;
  ps->deleting = false;
  pl->cancel_local_background_io_reservation();
}

/*--------GetInfo---------*/
PeeringState::GetInfo::GetInfo(my_context ctx)
  : my_base(ctx),
    // GetInfo 是 Peering 的初始子状态，负责收集选择权威日志所需的副本摘要。
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Peering/GetInfo")
{
  context< PeeringMachine >().log_enter(state_name);

  DECLARE_LOCALS;

  // 校验 PastIntervals 与当前 history 的边界，并记录异常的 PGLog/缺失状态供诊断。
  ps->check_past_interval_bounds();
  ps->log_weirdness();

  // prior_set： “本轮 peering 必须考虑的 OSD 集合”
  PastIntervals::PriorSet &prior_set = context< Peering >().prior_set;

  // 新一轮 GetInfo 尚未发出查询或等待回复，不能遗留上一轮的阻塞 OSD。
  ceph_assert(ps->blocked_by.empty());

  // 算出的“本轮 peering 必须考虑的 OSD 集合”
  prior_set = ps->build_prior();
  ps->prior_readable_down_osds = prior_set.down;

  if (ps->prior_readable_down_osds.empty()) {
    psdout(10) << " no prior_set down osds, will clear prior_readable_until_ub before activating"
	       << dendl;
  }

  // 初始时只知道本地能力；收到各副本信息后会重新求 acting 副本的 feature 交集。
  ps->reset_min_peer_features();

  // 对 prior_set.probe 中尚无摘要且仍 up 的副本发送 INFO 查询，
  // 并将已发请求的副本记入 peer_info_requested。
  get_infos();
  if (prior_set.pg_down) {
    // PriorSet 已判定本轮无法继续，转入 Down；不是单纯某一条查询尚未返回。
    post_event(IsDown());
  } else if (peer_info_requested.empty()) {
    // 无需等待任何远端摘要（例如所需信息都已存在或只需本地信息），
    // 投递 GotInfo 进入下一 peering 阶段；这不表示 PG 已经 peered。
    post_event(GotInfo());
  }
}

void PeeringState::GetInfo::get_infos()
{
  DECLARE_LOCALS;
  // 复用进入 GetInfo 时构建的探测集合；收到新信息后若重建 prior_set，
  // 本函数也会再次调用以补发或保留必要的查询。
  PastIntervals::PriorSet &prior_set = context< Peering >().prior_set;

  // blocked_by 只描述当前仍在等待 pg_info 的 peer，先清空后按本轮遍历重建。
  ps->blocked_by.clear();
  for (auto it = prior_set.probe.begin(); it != prior_set.probe.end(); ++it) {
    pg_shard_t peer = *it;

    // 本地 pg_info 已直接可用，无需向自己发送网络查询。
    if (peer == ps->pg_whoami) {
      continue;
    }

    // 之前的 notify 已经提供该副本的 pg_info，避免重复探测。
    if (ps->peer_info.count(peer)) {
      psdout(10) << " have osd." << peer << " info " << ps->peer_info[peer] << dendl;
      continue;
    }

    if (peer_info_requested.count(peer)) {
      // 请求已在途：继续把该 OSD 记为阻塞者，等待其 notify 到达。
      psdout(10) << " already requested info from osd." << peer << dendl;
      ps->blocked_by.insert(peer.osd);
    } else if (!ps->get_osdmap()->is_up(peer.osd)) {
      // down OSD 无法响应；是否因此使 PG down 由 prior_set.pg_down 决定。
      psdout(10) << " not querying info from down osd." << peer << dendl;
    } else {
      // INFO 查询请求对端返回自身 pg_info；
      // 请求中带目标/本地 shard、本地 history 和当前 map epoch，供对端验证并组织回复。
      psdout(10) << " querying info from osd." << peer << dendl;
      context< PeeringMachine >().send_query(
	peer.osd,
	pg_query_t(pg_query_t::INFO,
		   it->shard, ps->pg_whoami.shard,
		   ps->info.history,
		   ps->get_osdmap_epoch()));

      // 发送是异步的：记录等待项和阻塞 OSD，收到 MNotifyRec 后再移除。
      peer_info_requested.insert(peer);
      ps->blocked_by.insert(peer.osd);
    }
  }

  // 更新历史副本 down 对旧 interval 读 lease 的影响，必要时调整可读性判断。
  ps->check_prior_readable_down_osds(ps->get_osdmap());

  // 将新的 DOWN/PEERING 等状态和阻塞信息发布给 OSD 统计。
  pl->publish_stats_to_osd();
}

boost::statechart::result PeeringState::GetInfo::react(const MNotifyRec& infoevt)
{
  DECLARE_LOCALS;

  // 此 notify 可能是对本轮 INFO 查询的回复。先停止等待该 peer；
  // 即使是主动到达的 notify（不在集合中），后面仍会尝试将其作为新的 peering 信息处理。
  auto p = peer_info_requested.find(infoevt.from);
  if (p != peer_info_requested.end()) {
    peer_info_requested.erase(p);
    ps->blocked_by.erase(infoevt.from.osd);
  }

  // 处理前记录本地 last_epoch_started；
  // 副本 history 若将它推进，历史 probe 集合的安全边界随之变化，不能继续沿用旧 prior_set。
  epoch_t old_start = ps->info.history.last_epoch_started;
  if (ps->proc_replica_notify(infoevt.from, infoevt.notify)) {
    // 仅在该 notify 不是重复消息、也不来自已失效 OSD 实例时才采纳其 pg_info。
    PastIntervals::PriorSet &prior_set = context< Peering >().prior_set;
    if (old_start < ps->info.history.last_epoch_started) {
      // 更晚的 last_epoch_started 表示可信历史区间发生变化；
      // 重建 prior_set，使接下来的日志选择和信息探测不遗漏可能持有更新的副本。
      psdout(10) << " last_epoch_started moved forward, rebuilding prior" << dendl;
      prior_set = ps->build_prior();
      ps->prior_readable_down_osds = prior_set.down;

      // 新 prior_set 可能不再需要部分在途查询；
      // 移除这些等待项即可，不必重启整个 peering 并重新探测所有副本。
      auto p = peer_info_requested.begin();
      while (p != peer_info_requested.end()) {
	if (prior_set.probe.count(*p) == 0) {
	  psdout(20) << " dropping osd." << *p << " from info_requested, no longer in probe set" << dendl;
	  peer_info_requested.erase(p++);
	} else {
	  ++p;
	}
      }

      // 对重建后的 probe 集合补发尚未拥有、尚未请求的 INFO 查询。
      get_infos();
    }

    // 这是消息发送方的通用 Ceph feature 位；与前面 reset 的 peer_features 求交集，
    // 后续 PG 消息只能使用所有已采纳 peer 都支持的能力。
    // 它不同于 notify.pg_features，后者用于计算 acting PG feature 交集。
    psdout(20) << "Adding osd: " << infoevt.from.osd << " peer features: "
	       << hex << infoevt.features << dec << dendl;
    ps->apply_peer_features(infoevt.features);

    // 所有必需 INFO 回复均已到达，且 prior_set 未判定 PG down 时，
    // 才能进入 GetLog 选择权威日志。GotInfo 不表示整个 peering 已完成。
    if (peer_info_requested.empty() && !prior_set.pg_down) {
      psdout(20) << "Common peer features: " << hex << ps->get_min_peer_features() << dec << dendl;
      psdout(20) << "Common acting features: " << hex << ps->get_min_acting_features() << dec << dendl;
      psdout(20) << "Common upacting features: " << hex << ps->get_min_upacting_features() << dec << dendl;
      psdout(20) << "Common pg_acting_features: " << hex << ps->get_pg_acting_features() << dec << dendl;
      post_event(GotInfo());
    }
  }

  // 本条 notify 已消费；状态转换若有，由已投递的 GotInfo 在随后触发。
  return discard_event();
}

boost::statechart::result PeeringState::GetInfo::react(const QueryState& q)
{
  DECLARE_LOCALS;
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;

  q.f->open_array_section("requested_info_from");
  for (auto p = peer_info_requested.begin();
       p != peer_info_requested.end();
       ++p) {
    q.f->open_object_section("osd");
    q.f->dump_stream("osd") << *p;
    if (ps->peer_info.count(*p)) {
      q.f->open_object_section("got_info");
      ps->peer_info[*p].dump(q.f);
      q.f->close_section();
    }
    q.f->close_section();
  }
  q.f->close_section();

  q.f->close_section();
  return forward_event();
}

boost::statechart::result PeeringState::GetInfo::react(const QueryUnfound& q)
{
  q.f->dump_string("state", "GetInfo");
  q.f->dump_bool("available_might_have_unfound", false);
  return discard_event();
}

void PeeringState::GetInfo::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);

  DECLARE_LOCALS;
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_getinfo_latency, dur);
  ps->blocked_by.clear();
}

/*------GetLog------------*/
// GetLog 是 Peering 的子状态：在已有 pg_info 摘要后，选择日志源并取得实际 PGLog。
PeeringState::GetLog::GetLog(my_context ctx)
  : my_base(ctx),
    NamedState(
      context< PeeringMachine >().state_history,
      "Started/Primary/Peering/GetLog"),
    msg(0)
{
  context< PeeringMachine >().log_enter(state_name);

  DECLARE_LOCALS;

  // 输出 PGLog/missing 的异常诊断，便于识别无法形成连续日志的情况。
  ps->log_weirdness();

  // 基于 GetInfo 收集的 pg_info 选择可恢复的 acting 候选，并给出应取得日志的 auth_log_shard。
  // auth_log_shard，这次 primary 实际向其发送 LOG 查询、取得日志的副本 shard
  if (!ps->choose_acting(auth_log_shard, false, false,
			 &context< Peering >().history_les_bound,
                         &repeat_getlog)) {
    if (!ps->want_acting.empty()) {
      // want_acting 非空表示需要请求新的 acting 映射
      // 转入 WaitActingChange，等待 OSDMap/pg_temp 接纳候选集。
      post_event(NeedActingChange());
    } else {
      // 没有可恢复的组合，只能进入 Incomplete，不能安全地继续本轮 peering
      post_event(IsIncomplete());
    }
    return;
  }

  // 当前 primary 自己就是所选日志源时，本地已有全部需要的日志，无需网络请求。
  if (auth_log_shard == ps->pg_whoami) {
    post_event(GotLog());
    return;
  }

  // 远端日志源的摘要已在 GetInfo 阶段写入 peer_info。
  const pg_info_t& best = ps->peer_info[auth_log_shard];

  // ps->info.last_update 是本地 primary 已应用到的最新版本
  // best.log_tail 是权威日志源仍保留的日志起点边界，早于它的日志已经被裁剪
  // 本地 last_update 早于权威日志的 log_tail，二者之间的日志已被权威源裁剪，
  // 无法通过日志补齐这段缺口，因此当前 PG 不能继续正常 peering。
  if (ps->info.last_update < best.log_tail) {
    psdout(10) << " not contiguous with osd." << auth_log_shard << ", down" << dendl;
    repeat_getlog = false;
    post_event(IsIncomplete());
    return;
  }

  // 默认只需从本地最后更新之后取得日志；随后为可加入 acting_recovery_backfill 的
  // 较旧副本将起点回退，使权威日志能够覆盖它们的日志恢复范围。
  // acting_recovery_backfill 是本轮 peering 选出的“数据一致性参与者”集合，类型是 std::set<pg_shard_t>。
  eversion_t request_log_from = ps->info.last_update;
  ceph_assert(!ps->acting_recovery_backfill.empty());
  for (auto p = ps->acting_recovery_backfill.begin();
       p != ps->acting_recovery_backfill.end();
       ++p) {
    if (*p == ps->pg_whoami) continue;
    pg_info_t& ri = ps->peer_info[*p];
    if (ri.last_update < ps->info.log_tail && ri.last_update >= best.log_tail &&
        ri.last_update < request_log_from)
      request_log_from = ri.last_update;
  }

  // 向所选日志源发送异步 LOG 查询，回复以 MLogRec 到达 GetLog::react()。
  psdout(10) << " requesting log from osd." << auth_log_shard << dendl;
  context<PeeringMachine>().send_query(
    auth_log_shard.osd,
    pg_query_t(
      pg_query_t::LOG,
      auth_log_shard.shard, ps->pg_whoami.shard,
	  request_log_from, ps->info.history,
	  ps->get_osdmap_epoch()));

  // GetLog 同时只等待这个权威日志源；其 down/回复会驱动后续状态处理。
  ceph_assert(ps->blocked_by.empty());
  ps->blocked_by.insert(auth_log_shard.osd);
  pl->publish_stats_to_osd();
}

boost::statechart::result PeeringState::GetLog::react(const AdvMap& advmap)
{
  // make sure our log source didn't go down.  we need to check
  // explicitly because it may not be part of the prior set, which
  // means the Peering state check won't catch it going down.
  if (!advmap.osdmap->is_up(auth_log_shard.osd)) {
    psdout(10) << "GetLog: auth_log_shard osd."
		       << auth_log_shard.osd << " went down" << dendl;
    post_event(advmap);
    return transit< Reset >();
  }

  // let the Peering state do its checks.
  return forward_event();
}

/**
 * 接收本轮 LOG 查询的回复：只接受选定日志获取 shard 的 MLogRec，
 * 暂存其 MOSDPGLog 后投递 GotLog，由统一路径合并日志并推进到 GetMissing。
 */
boost::statechart::result PeeringState::GetLog::react(const MLogRec& logevt)
{
  // GetLog 同时只接受一份有效日志回复；msg 已存在表示重复处理或状态机顺序异常。
  ceph_assert(!msg);

  // 只信任本轮 choose_acting() 选定的日志获取 shard，忽略迟到或无关 OSD 的日志。
  if (logevt.from != auth_log_shard) {
    psdout(10) << "GetLog: discarding log from "
		       << "non-auth_log_shard osd." << logevt.from << dendl;
    return discard_event();
  }

  // 不在当前事件中直接合并日志：先保留消息，再由 GotLog 处理本地、远端两种
  // 日志来源共用的后续逻辑。
  psdout(10) << "GetLog: received master log from osd."
		     << logevt.from << dendl;
  msg = logevt.msg;

  // 内部事件稍后触发 GetLog::react(GotLog)。
  post_event(GotLog());

  // MLogRec 已消费；状态转换由刚投递的 GotLog 完成。
  return discard_event();
}

/**
 * 统一完成 GetLog：远端日志到达时合并其 PGLog/missing；本地就是日志源时直接继续。
 * 必要时处理 EC 稀疏日志的二次选择，否则登记本轮事务的 flush 并进入 GetMissing。
 */
boost::statechart::result PeeringState::GetLog::react(const GotLog&)
{
  DECLARE_LOCALS;
  psdout(10) << "leaving GetLog" << dendl;

  // msg 仅在远端 MLogRec 到达后存在；为空表示本地本身就是日志获取 shard，因而没有远端日志需要合并。
  if (msg) {
    // 将远端 pg_info、PGLog 和 missing 合并到本地 PeeringState，
    // 并把元数据修改记录到本轮 PeeringCtx 持有的事务中。
    psdout(10) << "processing master log" << dendl;
    ps->proc_master_log(context<PeeringMachine>().get_cur_transaction(),
			msg->info, std::move(msg->log), std::move(msg->missing),
			auth_log_shard);

    if (repeat_getlog) {
      // 仅限启用 EC 优化的特殊情况：先从 primary shard 取得完整日志以追平后，
      // 需要重新选择日志 shard，判断是否还须依据原来的非 primary 稀疏日志回滚。
      // 刚收到的 missing 不能作为该次重新选择的依据，先删除其 peer_missing 记录。
      ps->peer_missing.erase(auth_log_shard);
      psdout(10) << "repeating auth_log_shard selection" << dendl;

      // reaction 表将 RepeatGetLog 转回新的 GetLog 实例；本轮不进入 GetMissing。
      post_event(RepeatGetLog());
      return discard_event();
    }
  }

  // 登记当前事务的 flush。状态机可继续收集 missing，但后续 activation 会等待
  // 这次 flush 完成，避免用未完成的 PG 状态进入 active。
  ps->start_flush(context< PeeringMachine >().get_cur_transaction());

  // 已得到用于确定权威历史的日志，下一步查询各参与副本的 missing 集合。
  return transit< GetMissing >();
}

boost::statechart::result PeeringState::GetLog::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->dump_stream("auth_log_shard") << auth_log_shard;
  q.f->close_section();
  return forward_event();
}

boost::statechart::result PeeringState::GetLog::react(const QueryUnfound& q)
{
  q.f->dump_string("state", "GetLog");
  q.f->dump_bool("available_might_have_unfound", false);
  return discard_event();
}

void PeeringState::GetLog::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);

  DECLARE_LOCALS;
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_getlog_latency, dur);
  ps->blocked_by.clear();
}

/*------WaitActingChange--------*/
PeeringState::WaitActingChange::WaitActingChange(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/WaitActingChange")
{
  context< PeeringMachine >().log_enter(state_name);
}

boost::statechart::result PeeringState::WaitActingChange::react(const AdvMap& advmap)
{
  DECLARE_LOCALS;
  OSDMapRef osdmap = advmap.osdmap;

  psdout(10) << "verifying no want_acting " << ps->want_acting << " targets didn't go down" << dendl;
  for (auto p = ps->want_acting.begin(); p != ps->want_acting.end(); ++p) {
    if (!osdmap->is_up(*p)) {
      psdout(10) << " want_acting target osd." << *p << " went down, resetting" << dendl;
      post_event(advmap);
      return transit< Reset >();
    }
  }
  return forward_event();
}

boost::statechart::result PeeringState::WaitActingChange::react(const MLogRec& logevt)
{
  psdout(10) << "In WaitActingChange, ignoring MLocRec" << dendl;
  return discard_event();
}

boost::statechart::result PeeringState::WaitActingChange::react(const MInfoRec& evt)
{
  psdout(10) << "In WaitActingChange, ignoring MInfoRec" << dendl;
  return discard_event();
}

boost::statechart::result PeeringState::WaitActingChange::react(const MNotifyRec& evt)
{
  psdout(10) << "In WaitActingChange, ignoring MNotifyRec" << dendl;
  return discard_event();
}

boost::statechart::result PeeringState::WaitActingChange::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->dump_string("comment", "waiting for pg acting set to change");
  q.f->close_section();
  return forward_event();
}

boost::statechart::result PeeringState::WaitActingChange::react(const QueryUnfound& q)
{
  q.f->dump_string("state", "WaitActingChange");
  q.f->dump_bool("available_might_have_unfound", false);
  return discard_event();
}

void PeeringState::WaitActingChange::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_waitactingchange_latency, dur);
}

/*------Down--------*/
PeeringState::Down::Down(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Peering/Down")
{
  context< PeeringMachine >().log_enter(state_name);
  DECLARE_LOCALS;

  ps->state_clear(PG_STATE_PEERING);
  ps->state_set(PG_STATE_DOWN);

  auto &prior_set = context< Peering >().prior_set;
  ceph_assert(ps->blocked_by.empty());
  ps->blocked_by.insert(prior_set.down.begin(), prior_set.down.end());
  pl->publish_stats_to_osd();
}

void PeeringState::Down::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);

  DECLARE_LOCALS;

  ps->state_clear(PG_STATE_DOWN);
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_down_latency, dur);

  ps->blocked_by.clear();
}

boost::statechart::result PeeringState::Down::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->dump_string("comment",
		   "not enough up instances of this PG to go active");
  q.f->close_section();
  return forward_event();
}

boost::statechart::result PeeringState::Down::react(const QueryUnfound& q)
{
  q.f->dump_string("state", "Down");
  q.f->dump_bool("available_might_have_unfound", false);
  return discard_event();
}

boost::statechart::result PeeringState::Down::react(const MNotifyRec& infoevt)
{
  DECLARE_LOCALS;

  ceph_assert(ps->is_primary());
  epoch_t old_start = ps->info.history.last_epoch_started;
  if (!ps->peer_info.count(infoevt.from) &&
      ps->get_osdmap()->has_been_up_since(infoevt.from.osd, infoevt.notify.epoch_sent)) {
    ps->update_history(infoevt.notify.info.history);
  }
  // if we got something new to make pg escape down state
  if (ps->info.history.last_epoch_started > old_start) {
      psdout(10) << " last_epoch_started moved forward, re-enter getinfo" << dendl;
    ps->state_clear(PG_STATE_DOWN);
    ps->state_set(PG_STATE_PEERING);
    return transit< GetInfo >();
  }

  return discard_event();
}


/*------Incomplete--------*/
PeeringState::Incomplete::Incomplete(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Peering/Incomplete")
{
  context< PeeringMachine >().log_enter(state_name);
  DECLARE_LOCALS;

  ps->state_clear(PG_STATE_PEERING);
  ps->state_set(PG_STATE_INCOMPLETE);

  PastIntervals::PriorSet &prior_set = context< Peering >().prior_set;
  ceph_assert(ps->blocked_by.empty());
  ps->blocked_by.insert(prior_set.down.begin(), prior_set.down.end());
  pl->publish_stats_to_osd();
}

boost::statechart::result PeeringState::Incomplete::react(const AdvMap &advmap) {
  DECLARE_LOCALS;
  int64_t poolnum = ps->info.pgid.pool();

  // Reset if min_size turn smaller than previous value, pg might now be able to go active
  if (!advmap.osdmap->have_pg_pool(poolnum) ||
      advmap.lastmap->get_pools().find(poolnum)->second.min_size >
      advmap.osdmap->get_pools().find(poolnum)->second.min_size) {
    post_event(advmap);
    return transit< Reset >();
  }

  return forward_event();
}

boost::statechart::result PeeringState::Incomplete::react(const MNotifyRec& notevt) {
  DECLARE_LOCALS;
  psdout(7) << "handle_pg_notify from osd." << notevt.from << dendl;
  if (ps->proc_replica_notify(notevt.from, notevt.notify)) {
    // We got something new, try again!
    return transit< GetLog >();
  } else {
    return discard_event();
  }
}

boost::statechart::result PeeringState::Incomplete::react(
  const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->dump_string("comment", "not enough complete instances of this PG");
  q.f->close_section();
  return forward_event();
}

boost::statechart::result PeeringState::Incomplete::react(const QueryUnfound& q)
{
  q.f->dump_string("state", "Incomplete");
  q.f->dump_bool("available_might_have_unfound", false);
  return discard_event();
}

void PeeringState::Incomplete::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);

  DECLARE_LOCALS;

  ps->state_clear(PG_STATE_INCOMPLETE);
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_incomplete_latency, dur);

  ps->blocked_by.clear();
}

/*------GetMissing--------*/
/**
 * 收集参与本轮 peering 的副本 missing 集合，并决定请求增量日志还是完整日志。
 *
 * 权威 PG log 已在 GetLog 阶段确定。这里逐个检查远端 acting_recovery_backfill shard 的 pg_info：
 * 无法通过连续 PG log 追平、或已确定需要全量 backfill 的副本无需再请求 missing；
 * 其余副本返回的日志和 missing 将用于建立 peer_missing 与后续 recovery 来源判断。
 */
PeeringState::GetMissing::GetMissing(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Peering/GetMissing")
{
  context< PeeringMachine >().log_enter(state_name);

  DECLARE_LOCALS;
  // 输出当前 PG log/missing 的异常诊断，帮助发现不连续或异常的 peering 状态。
  ps->log_weirdness();

  // 本轮至少应有 primary 自身参与 acting/recovery/backfill 集合。
  ceph_assert(!ps->acting_recovery_backfill.empty());

  // `since` 指定向 peer 获取增量日志和 missing 时的起始版本。
  eversion_t since;
  for (auto i = ps->acting_recovery_backfill.begin();
       i != ps->acting_recovery_backfill.end();
       ++i) {
    // primary 的本地 missing 已在状态机中持有，不需要向自己发查询。
    if (*i == ps->get_primary()) continue;

    // peer_info 来自 GetInfo 阶段，描述该 shard 当前 PG log 与 backfill 进度。
    pg_info_t& pi = ps->peer_info[*i];

    // reset this so to make sure the pg_missing_t is initialized and
    // has the correct semantics even if we don't need to get a
    // missing set from a shard. This way later additions due to
    // lost+unfound delete work properly.
    ps->peer_missing[*i].may_include_deletes = !ps->perform_deletes_during_peering();

    if (pi.is_empty()) {
      // 空 PG 没有可与权威日志分歧的对象或日志记录。
      continue;
    }

    if (pi.last_update < ps->pg_log.get_tail()) {
      // peer 的最新版本早于权威日志仍保留的起点，无法用连续日志找出差异；
      // 后续会重新开始 backfill，因此此时可直接视为没有可用 missing 集合。
      psdout(10) << " osd." << *i << " is not contiguous, will restart backfill" << dendl;
      ps->peer_missing[*i].clear();
      continue;
    }
    if (pi.last_backfill == hobject_t()) {
      // 已选定对该 peer 做完整 backfill，遗漏对象将由全量扫描处理。
      psdout(10) << " osd." << *i << " will fully backfill; can infer empty missing set" << dendl;
      ps->peer_missing[*i].clear();
      continue;
    }

    if (pi.last_update == pi.last_complete &&  // peer has no missing
	pi.last_update == ps->info.last_update) {  // peer is up to date
      // replica has no missing and identical log as us.  no need to
      // pull anything.
      // FIXME: we can do better here.  if last_update==last_complete we
      //        can infer the rest!
      psdout(10) << " osd." << *i << " has no missing, identical log" << dendl;
      ps->peer_missing[*i].clear();
      continue;
    }

    // 从 peer 本轮 interval 的起点开始获取，确保有足够日志识别分歧更新。
    since.epoch = pi.last_epoch_started;

    // 能进入 acting_recovery_backfill 的 peer 至少覆盖本地 PG log tail。
    ceph_assert(pi.last_update >= ps->info.log_tail);  // or else choose_acting() did a bad thing
    if (pi.log_tail <= since) {
      // peer 仍保留 since 起的连续日志，只请求该范围的增量 LOG 与 missing。
      psdout(10) << " requesting log+missing since " << since << " from osd." << *i << dendl;
      context< PeeringMachine >().send_query(
	i->osd,
	pg_query_t(
	  pg_query_t::LOG,
	  i->shard, ps->pg_whoami.shard,
	  since, ps->info.history,
	  ps->get_osdmap_epoch()));
    } else {
      // peer 已裁剪掉所需起点之前的日志，必须请求 FULLLOG 才能正确重建差异。
      psdout(10) << " requesting fulllog+missing from osd." << *i
			 << " (want since " << since << " < log.tail "
			 << pi.log_tail << ")" << dendl;
      context< PeeringMachine >().send_query(
	i->osd, pg_query_t(
	  pg_query_t::FULLLOG,
	  i->shard, ps->pg_whoami.shard,
	  ps->info.history, ps->get_osdmap_epoch()));
    }

    // 记录尚未返回的 peer，并在 PG 状态中显示本阶段被这些 OSD 阻塞。
    peer_missing_requested.insert(*i);
    ps->blocked_by.insert(i->osd);
  }

  if (peer_missing_requested.empty()) {
    // need_up_thru 是一个标志：
    // 当前 primary 需要等待 Monitor 在新 OSDMap 中把本 OSD 的 up_thru
    // 推进到本次 PG interval 的起始 epoch，才能继续 activation。
    if (ps->need_up_thru) {
      // missing 已无需等待，但 primary 尚未满足 up_thru 要求，不能激活。
      psdout(10) << " still need up_thru update before going active"
		 << dendl;
      post_event(NeedUpThru());
      return;
    }

    // 所有 peer 的 missing 已知且 up_thru 满足，开始 activation。
    post_event(Activate(ps->get_osdmap_epoch()));
  } else {
    // 仍在等待 peer 回复，先发布 blocked_by 等最新 PG 状态。
    pl->publish_stats_to_osd();
  }
}

/**
 * 接收 GetMissing 阶段的 LOG/FULLLOG 回复：合并该副本的日志和 missing 集合；
 * 所有查询完成后，等待 up_thru 确认或投递 Activate 开始激活。
 */
boost::statechart::result PeeringState::GetMissing::react(const MLogRec& logevt)
{
  DECLARE_LOCALS;

  // 该副本已返回 missing 信息，不再阻塞本阶段；重复/非请求回复即使到达，
  // erase()也只是无副作用地返回，不会破坏等待集合。
  peer_missing_requested.erase(logevt.from);

  // 处理对端携带的 pg_info、日志条目和 missing 集合，更新 peer_info、peer_missing
  // 及本地 PGLog 对该副本的认知；missing 的所有权通过 move 转入处理流程。
  ps->proc_replica_log(logevt.msg->info,
		       logevt.msg->log,
		       std::move(logevt.msg->missing),
		       logevt.from);

  if (peer_missing_requested.empty()) {
    if (ps->need_up_thru) {
    // up_thru: Monitor 已确认该 OSD 至少持续处于 up 状态直到哪个 OSDMap epoch。
    // 当前 OSD 的 up_thru 尚小于本 PG interval 的起始 epoch；
    // 等待 Monitor 在后续 OSDMap 中推进 up_thru 后才能激活。
      psdout(10) << " still need up_thru update before going active"
		 << dendl;
      post_event(NeedUpThru());
    } else {
      // missing 信息已收齐且 up_thru 已满足，投递 Activate 进入 peering 的激活流程。
      psdout(10) << "Got last missing, don't need missing "
		 << "posting Activate" << dendl;
      post_event(Activate(ps->get_osdmap_epoch()));
    }
  }

  // 当前 MLogRec 已消费；上面投递的内部事件会在随后由状态机分发。
  return discard_event();
}

boost::statechart::result PeeringState::GetMissing::react(const QueryState& q)
{
  DECLARE_LOCALS;
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;

  q.f->open_array_section("peer_missing_requested");
  for (auto p = peer_missing_requested.begin();
       p != peer_missing_requested.end();
       ++p) {
    q.f->open_object_section("osd");
    q.f->dump_stream("osd") << *p;
    if (ps->peer_missing.count(*p)) {
      q.f->open_object_section("got_missing");
      ps->peer_missing[*p].dump(q.f);
      q.f->close_section();
    }
    q.f->close_section();
  }
  q.f->close_section();

  q.f->close_section();
  return forward_event();
}

boost::statechart::result PeeringState::GetMissing::react(const QueryUnfound& q)
{
  q.f->dump_string("state", "GetMising");
  q.f->dump_bool("available_might_have_unfound", false);
  return discard_event();
}

void PeeringState::GetMissing::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);

  DECLARE_LOCALS;
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_getmissing_latency, dur);
  ps->blocked_by.clear();
}

/*------WaitUpThru--------*/
PeeringState::WaitUpThru::WaitUpThru(my_context ctx)
  : my_base(ctx),
    NamedState(context< PeeringMachine >().state_history, "Started/Primary/Peering/WaitUpThru")
{
  context< PeeringMachine >().log_enter(state_name);
}

boost::statechart::result PeeringState::WaitUpThru::react(const ActMap& am)
{
  DECLARE_LOCALS;
  if (!ps->need_up_thru) {
    post_event(Activate(ps->get_osdmap_epoch()));
  }
  return forward_event();
}

boost::statechart::result PeeringState::WaitUpThru::react(const MLogRec& logevt)
{
  DECLARE_LOCALS;
  psdout(10) << "Noting missing from osd." << logevt.from << dendl;
  ps->peer_missing[logevt.from].claim(std::move(logevt.msg->missing));
  ps->peer_info[logevt.from] = logevt.msg->info;
  ps->update_peer_info(logevt.from, logevt.msg->info);
  return discard_event();
}

boost::statechart::result PeeringState::WaitUpThru::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->dump_string("comment", "waiting for osdmap to reflect a new up_thru for this osd");
  q.f->close_section();
  return forward_event();
}

boost::statechart::result PeeringState::WaitUpThru::react(const QueryUnfound& q)
{
  q.f->dump_string("state", "WaitUpThru");
  q.f->dump_bool("available_might_have_unfound", false);
  return discard_event();
}

void PeeringState::WaitUpThru::exit()
{
  context< PeeringMachine >().log_exit(state_name, enter_time);
  DECLARE_LOCALS;
  utime_t dur = ceph_clock_now() - enter_time;
  pl->get_peering_perf().tinc(rs_waitupthru_latency, dur);
}

/*----PeeringState::PeeringMachine Methods-----*/
#undef dout_prefix
#define dout_prefix dpp->gen_prefix(*_dout)

void PeeringState::PeeringMachine::log_enter(const char *state_name)
{
  DECLARE_LOCALS;
  psdout(5) << "enter " << state_name << dendl;
  pl->log_state_enter(state_name);
}

void PeeringState::PeeringMachine::log_exit(const char *state_name, utime_t enter_time)
{
  DECLARE_LOCALS;
  utime_t dur = ceph_clock_now() - enter_time;
  psdout(5) << "exit " << state_name << " " << dur << " " << event_count << " " << event_time << dendl;
  pl->log_state_exit(state_name, enter_time, event_count, event_time);
  event_count = 0;
  event_time = utime_t();
}

ostream &operator<<(ostream &out, const PeeringState &ps) {
  out << "pg[" << ps.info
      << " " << pg_vector_string(ps.up);
  if (ps.acting != ps.up)
    out << "/" << pg_vector_string(ps.acting);
  if (ps.is_ec_pg())
    out << "p" << ps.get_primary();
  if (!ps.async_recovery_targets.empty())
    out << " async=[" << ps.async_recovery_targets << "]";
  if (!ps.backfill_targets.empty())
    out << " backfill=[" << ps.backfill_targets << "]";
  out << " r=" << ps.get_role();
  out << " lpr=" << ps.get_last_peering_reset();

  if (ps.deleting)
    out << " DELETING";

  if (!ps.past_intervals.empty()) {
    out << " pi=[" << ps.past_intervals.get_bounds()
	<< ")/" << ps.past_intervals.size();
  }

  if (ps.is_peered()) {
    if (ps.pg_committed_to != ps.info.last_update)
      out << " pct=" << ps.pg_committed_to;
    if (ps.last_update_applied != ps.info.last_update)
      out << " lua=" << ps.last_update_applied;
  }

  if (ps.pg_log.get_tail() != ps.info.log_tail ||
      ps.pg_log.get_head() != ps.info.last_update)
    out << " (info mismatch, " << ps.pg_log.get_log() << ")";

  if (!ps.pg_log.get_log().empty()) {
    if ((ps.pg_log.get_log().log.begin()->version <= ps.pg_log.get_tail())) {
      out << " (log bound mismatch, actual=["
	  << ps.pg_log.get_log().log.begin()->version << ","
	  << ps.pg_log.get_log().log.rbegin()->version << "]";
      out << ")";
    }
  }

  out << " crt=" << ps.pg_log.get_can_rollback_to();

  if (ps.last_complete_ondisk != ps.info.last_complete)
    out << " lcod " << ps.last_complete_ondisk;

  if (ps.is_primary())
    out << " mlcod " << ps.min_last_complete_ondisk;

  out << " " << pg_state_string(ps.get_state());
  if (ps.should_send_notify())
    out << " NOTIFY";

  if (ps.prior_readable_until_ub != ceph::signedspan::zero()) {
    out << " pruub " << ps.prior_readable_until_ub
	<< "@" << ps.get_prior_readable_down_osds();
  }
  return out;
}

std::vector<pg_shard_t> PeeringState::get_replica_recovery_order() const
{
  std::vector<std::pair<unsigned int, pg_shard_t>> replicas_by_num_missing,
    async_by_num_missing;
  replicas_by_num_missing.reserve(get_acting_recovery_backfill().size() - 1);
  for (auto &p : get_acting_recovery_backfill()) {
    if (p == get_primary()) {
      continue;
    }
    auto pm = get_peer_missing().find(p);
    ceph_assert(pm != get_peer_missing().end());
    auto nm = pm->second.num_missing();
    if (nm != 0) {
      if (is_async_recovery_target(p)) {
	async_by_num_missing.push_back(make_pair(nm, p));
      } else {
	replicas_by_num_missing.push_back(make_pair(nm, p));
      }
    }
  }
  // sort by number of missing objects, in ascending order.
  auto func = [](const std::pair<unsigned int, pg_shard_t> &lhs,
		 const std::pair<unsigned int, pg_shard_t> &rhs) {
    return lhs.first < rhs.first;
  };
  // acting goes first
  std::sort(replicas_by_num_missing.begin(), replicas_by_num_missing.end(), func);
  // then async_recovery_targets
  std::sort(async_by_num_missing.begin(), async_by_num_missing.end(), func);
  replicas_by_num_missing.insert(replicas_by_num_missing.end(),
    async_by_num_missing.begin(), async_by_num_missing.end());

  std::vector<pg_shard_t> ret;
  ret.reserve(replicas_by_num_missing.size());
  for (auto p : replicas_by_num_missing) {
    ret.push_back(p.second);
  }
  return ret;
}
