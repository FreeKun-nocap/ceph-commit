// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include "ReplicatedBackend.h"

#include <sstream>

#include "common/debug.h"
#include "common/errno.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDPGPCT.h"
#include "messages/MOSDRepOp.h"
#include "messages/MOSDRepOpReply.h"
#include "messages/MOSDPGPush.h"
#include "messages/MOSDPGPull.h"
#include "messages/MOSDPGPushReply.h"
#include "common/EventTrace.h"
#include "include/random.h"
#include "include/util.h"
#include "OSD.h"
#include "osd_tracer.h"

#define dout_context cct
#define dout_subsys ceph_subsys_osd
#define DOUT_PREFIX_ARGS this
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)
static ostream& _prefix(std::ostream *_dout, ReplicatedBackend *pgb) {
  return pgb->get_parent()->gen_dbg_prefix(*_dout);
}

using std::less;
using std::list;
using std::make_pair;
using std::map;
using std::ostringstream;
using std::set;
using std::pair;
using std::string;
using std::unique_ptr;
using std::vector;

using ceph::bufferhash;
using ceph::bufferlist;
using ceph::decode;
using ceph::encode;

namespace {
class PG_SendMessageOnConn: public Context {
  PGBackend::Listener *pg;
  Message *reply;
  ConnectionRef conn;
  public:
  PG_SendMessageOnConn(
    PGBackend::Listener *pg,
    Message *reply,
    ConnectionRef conn) : pg(pg), reply(reply), conn(conn) {}
  void finish(int) override {
    pg->send_message_osd_cluster(MessageRef(reply, false), conn.get());
  }
};

class PG_RecoveryQueueAsync : public Context {
  PGBackend::Listener *pg;
  unique_ptr<GenContext<ThreadPool::TPHandle&>> c;
  uint64_t cost;
  public:
  PG_RecoveryQueueAsync(
    PGBackend::Listener *pg,
    GenContext<ThreadPool::TPHandle&> *c,
    uint64_t cost) : pg(pg), c(c), cost(cost) {}
  void finish(int) override {
    pg->schedule_recovery_work(c.release(), cost);
  }
};
}

struct ReplicatedBackend::C_OSD_RepModifyCommit : public Context {
  ReplicatedBackend *pg;
  RepModifyRef rm;
  C_OSD_RepModifyCommit(ReplicatedBackend *pg, RepModifyRef r)
    : pg(pg), rm(r) {}
  void finish(int r) override {
    pg->repop_commit(rm);
  }
};

static void log_subop_stats(
  PerfCounters *logger,
  OpRequestRef op, int subop)
{
  utime_t latency = ceph_clock_now();
  latency -= op->get_req()->get_recv_stamp();


  logger->inc(l_osd_sop);
  logger->tinc(l_osd_sop_lat, latency);
  logger->inc(subop);

  if (subop != l_osd_sop_pull) {
    uint64_t inb = op->get_req()->get_data().length();
    logger->inc(l_osd_sop_inb, inb);
    if (subop == l_osd_sop_w) {
      logger->inc(l_osd_sop_w_inb, inb);
      logger->tinc(l_osd_sop_w_lat, latency);
    } else if (subop == l_osd_sop_push) {
      logger->inc(l_osd_sop_push_inb, inb);
      logger->tinc(l_osd_sop_push_lat, latency);
    } else
      ceph_abort_msg("no support subop");
  } else {
    logger->tinc(l_osd_sop_pull_lat, latency);
  }
}

ReplicatedBackend::ReplicatedBackend(
  PGBackend::Listener *pg,
  const coll_t &coll,
  ObjectStore::CollectionHandle &c,
  ObjectStore *store,
  CephContext *cct) :
  PGBackend(cct, pg, store, coll, c),
  pct_callback(this)
{}

/**
 * 提交本轮累计的 replicated recovery 操作：
 * 分别发送 push、pull 和 delete 请求，完成后释放保存这些待执行操作的 RPGHandle。
 */
void ReplicatedBackend::run_recovery_op(
  PGBackend::RecoveryHandle *_h,
  int priority)
{
  // _h 由本 ReplicatedBackend::open_recovery_op() 创建，实际类型为 RPGHandle。
  RPGHandle *h = static_cast<RPGHandle *>(_h);
  // primary 向缺失副本推送对象数据。
  send_pushes(priority, h->pushes);
  // primary 从拥有对象的副本拉取对象数据。
  send_pulls(priority, h->pulls);
  // 将 primary 已确认删除的对象同步删除到副本。
  send_recovery_deletes(priority, h->deletes);
  // 三类操作已经交给 backend 发送/处理，释放本轮临时操作集合。
  delete h;
}

/**
 * 根据对象在本地是否缺失，准备一次对象 recovery：
 * 本地缺失时从 peer pull，本地已有对象时将其 push 到缺失或版本落后的副本。
 */
int ReplicatedBackend::recover_object(
  const hobject_t &hoid,
  eversion_t v,
  ObjectContextRef head,
  ObjectContextRef obc,
  RecoveryHandle *_h
  )
{
  dout(10) << __func__ << ": " << hoid << dendl;
  // RPGHandle 收集本轮待发送的 pull/push 操作，稍后由 run_recovery_op() 统一提交。
  RPGHandle *h = static_cast<RPGHandle *>(_h);
  if (get_parent()->get_local_missing().is_missing(hoid)) {
    ceph_assert(!obc);
    // primary 本地没有对象，不能以 obc 作为数据源，准备从拥有该版本的 peer 拉取。
    prepare_pull(
      v,
      hoid,
      head,
      h);
  } else {
    ceph_assert(obc);
    // primary 本地持有对象上下文，准备向指定副本推送对象数据。
    int started = start_pushes(
      hoid,
      obc,
      h);
    if (started < 0) {
      // push 准备失败时清除该对象的临时 push 状态，并把错误返回给上层。
      pushing[hoid].clear();
      return started;
    }
  }
  return 0;
}

void ReplicatedBackend::check_recovery_sources(const OSDMapRef& osdmap)
{
  for(map<pg_shard_t, set<hobject_t> >::iterator i = pull_from_peer.begin();
      i != pull_from_peer.end();
      ) {
    if (osdmap->is_down(i->first.osd)) {
      dout(10) << "check_recovery_sources resetting pulls from osd." << i->first
	       << ", osdmap has it marked down" << dendl;
      for (set<hobject_t>::iterator j = i->second.begin();
	   j != i->second.end();
	   ++j) {
	get_parent()->cancel_pull(*j);
	clear_pull(pulling.find(*j), false);
      }
      pull_from_peer.erase(i++);
    } else {
      ++i;
    }
  }
}

bool ReplicatedBackend::can_handle_while_inactive(OpRequestRef op)
{
  dout(10) << __func__ << ": " << *op->get_req() << dendl;
  switch (op->get_req()->get_type()) {
  case MSG_OSD_PG_PULL:
    return true;
  default:
    return false;
  }
}

/**
 * 将未被 PGBackend 基类处理的副本协议消息分派到具体处理函数。
 *
 * push/pull 及其回复负责对象恢复数据的传输，repop/repopreply 负责副本写入协议，
 * pct 用于处理副本一致性相关消息。消息被对应函数消费后返回 true；
 * 未识别的类型返回 false，由上层继续判断或报告未处理消息。
 */
bool ReplicatedBackend::_handle_message(
  OpRequestRef op
  )
{
  dout(10) << __func__ << ": " << *op->get_req() << dendl;
  switch (op->get_req()->get_type()) {
  case MSG_OSD_PG_PUSH:
    do_push(op);
    return true;

  case MSG_OSD_PG_PULL:
    do_pull(op);
    return true;

  case MSG_OSD_PG_PUSH_REPLY:
    do_push_reply(op);
    return true;

  case MSG_OSD_REPOP: {
    do_repop(op);
    return true;
  }

  case MSG_OSD_REPOPREPLY: {
    do_repop_reply(op);
    return true;
  }

  case MSG_OSD_PG_PCT:
    do_pct(op);
    return true;

  default:
    break;
  }
  return false;
}

void ReplicatedBackend::clear_recovery_state()
{
  // clear pushing/pulling maps
  for (auto &&i: pushing) {
    for (auto &&j: i.second) {
      get_parent()->release_locks(j.second.lock_manager);
    }
  }
  pushing.clear();

  for (auto &&i: pulling) {
    get_parent()->release_locks(i.second.lock_manager);
  }
  pulling.clear();
  pull_from_peer.clear();
}

void ReplicatedBackend::on_change()
{
  dout(10) << __func__ << dendl;
  for (auto& op : in_progress_ops) {
    delete op.second->on_commit;
    op.second->on_commit = nullptr;
  }
  in_progress_ops.clear();
  clear_recovery_state();
  cancel_pct_update();
}

int ReplicatedBackend::objects_read_sync(
  const hobject_t &hoid,
  uint64_t off,
  uint64_t len,
  uint32_t op_flags,
  bufferlist *bl)
{
  return store->read(ch, ghobject_t(hoid), off, len, *bl, op_flags);
}

int ReplicatedBackend::objects_readv_sync(
  const hobject_t &hoid,
  map<uint64_t, uint64_t>& m,
  uint32_t op_flags,
  bufferlist *bl)
{
  interval_set<uint64_t> im(std::move(m));
  auto r = store->readv(ch, ghobject_t(hoid), im, *bl, op_flags);
  if (r >= 0) {
    m = std::move(im).detach();
  }
  return r;
}

void ReplicatedBackend::objects_read_async(
  const hobject_t &hoid,
  uint64_t object_size,
  const list<pair<ec_align_t,
		  pair<bufferlist*, Context*> > > &to_read,
  Context *on_complete,
  bool fast_read)
{
  ceph_abort_msg("async read is not used by replica pool");
}

bool ReplicatedBackend::get_ec_supports_crc_encode_decode() const {
  ceph_abort_msg("crc encode decode is not used by replica pool");
  return false;
}

bool ReplicatedBackend::ec_can_decode(
    const shard_id_set &available_shards) const {
  ceph_abort_msg("can decode is not used by replica pool");
  return false;
}

shard_id_map<bufferlist> ReplicatedBackend::ec_encode_acting_set(
    const bufferlist &in_bl) const {
  ceph_abort_msg("encode is not used by replica pool");
  return {0};
}

shard_id_map<bufferlist> ReplicatedBackend::ec_decode_acting_set(
    const shard_id_map<bufferlist> &shard_map, int chunk_size) const {
  ceph_abort_msg("decode is not used by replica pool");
  return {0};
}

ECUtil::stripe_info_t ReplicatedBackend::ec_get_sinfo() const {
  ceph_abort_msg("get_ec_sinfo is not used by replica pool");
  return {0, 0, 0};
}

class C_OSD_OnOpCommit : public Context {
  ReplicatedBackend *pg;
  ceph::ref_t<ReplicatedBackend::InProgressOp> op;
public:
  C_OSD_OnOpCommit(ReplicatedBackend *pg, ceph::ref_t<ReplicatedBackend::InProgressOp> op)
    : pg(pg), op(std::move(op)) {}
  void finish(int) override {
    pg->op_commit(op);
  }
};

void generate_transaction(
  PGTransactionUPtr &pgt,
  const coll_t &coll,
  vector<pg_log_entry_t> &log_entries,
  ObjectStore::Transaction *t,
  set<hobject_t> *added,
  set<hobject_t> *removed,
  const ceph_release_t require_osd_release = ceph_release_t::unknown )
{
  ceph_assert(t);
  ceph_assert(added);
  ceph_assert(removed);

  for (auto &&le: log_entries) {
    le.mark_unrollbackable();
    auto oiter = pgt->op_map.find(le.soid);
    if (oiter != pgt->op_map.end() && oiter->second.updated_snaps) {
      bufferlist bl(oiter->second.updated_snaps->second.size() * 8 + 8);
      encode(oiter->second.updated_snaps->second, bl);
      le.snaps.swap(bl);
      le.snaps.reassign_to_mempool(mempool::mempool_osd_pglog);
    }
  }

  pgt->safe_create_traverse(
    [&](pair<const hobject_t, PGTransaction::ObjectOperation> &obj_op) {
      const hobject_t &oid = obj_op.first;
      const ghobject_t goid =
	ghobject_t(oid, ghobject_t::NO_GEN, shard_id_t::NO_SHARD);
      const PGTransaction::ObjectOperation &op = obj_op.second;

      if (oid.is_temp()) {
	if (op.is_fresh_object()) {
	  added->insert(oid);
	} else if (op.is_delete()) {
	  removed->insert(oid);
	}
      }

      if (op.delete_first) {
	t->remove(coll, goid);
      }

      match(
	op.init_type,
	[&](const PGTransaction::ObjectOperation::Init::None &) {
	},
	[&](const PGTransaction::ObjectOperation::Init::Create &op) {
	  if (require_osd_release >= ceph_release_t::octopus) {
	    t->create(coll, goid);
	  } else {
	    t->touch(coll, goid);
	  }
	},
	[&](const PGTransaction::ObjectOperation::Init::Clone &op) {
	  t->clone(
	    coll,
	    ghobject_t(
	      op.source, ghobject_t::NO_GEN, shard_id_t::NO_SHARD),
	    goid);
	},
	[&](const PGTransaction::ObjectOperation::Init::Rename &op) {
	  ceph_assert(op.source.is_temp());
	  t->collection_move_rename(
	    coll,
	    ghobject_t(
	      op.source, ghobject_t::NO_GEN, shard_id_t::NO_SHARD),
	    coll,
	    goid);
	});

      if (op.truncate) {
	t->truncate(coll, goid, op.truncate->first);
	if (op.truncate->first != op.truncate->second)
	  t->truncate(coll, goid, op.truncate->second);
      }

      if (!op.attr_updates.empty()) {
	map<string, bufferlist, less<>> attrs;
	for (auto &&p: op.attr_updates) {
	  if (p.second)
	    attrs[p.first] = *(p.second);
	  else
	    t->rmattr(coll, goid, p.first);
	}
	t->setattrs(coll, goid, attrs);
      }

      if (op.clear_omap)
	t->omap_clear(coll, goid);
      if (op.omap_header)
	t->omap_setheader(coll, goid, *(op.omap_header));

      for (auto &&up: op.omap_updates) {
	using UpdateType = PGTransaction::ObjectOperation::OmapUpdateType;
	switch (up.first) {
	case UpdateType::Remove:
	  t->omap_rmkeys(coll, goid, up.second);
	  break;
	case UpdateType::Insert:
	  t->omap_setkeys(coll, goid, up.second);
	  break;
	case UpdateType::RemoveRange:
	  t->omap_rmkeyrange(coll, goid, up.second);
	  break;
	}
      }

      // updated_snaps doesn't matter since we marked unrollbackable

      if (op.alloc_hint) {
	auto &hint = *(op.alloc_hint);
	t->set_alloc_hint(
	  coll,
	  goid,
	  hint.expected_object_size,
	  hint.expected_write_size,
	  hint.flags);
      }

      for (auto &&extent: op.buffer_updates) {
	using BufferUpdate = PGTransaction::ObjectOperation::BufferUpdate;
	match(
	  extent.get_val(),
	  [&](const BufferUpdate::Write &op) {
	    t->write(
	      coll,
	      goid,
	      extent.get_off(),
	      extent.get_len(),
	      op.buffer,
	      op.fadvise_flags);
	  },
	  [&](const BufferUpdate::Zero &op) {
	    t->zero(
	      coll,
	      goid,
	      extent.get_off(),
	      extent.get_len());
	  },
	  [&](const BufferUpdate::CloneRange &op) {
	    ceph_assert(op.len == extent.get_len());
	    t->clone_range(
	      coll,
	      ghobject_t(op.from, ghobject_t::NO_GEN, shard_id_t::NO_SHARD),
	      goid,
	      op.offset,
	      extent.get_len(),
	      extent.get_off());
	  });
      }
    });
}

void ReplicatedBackend::do_pct(OpRequestRef op)
{
  const MOSDPGPCT *m = static_cast<const MOSDPGPCT*>(op->get_req());
  dout(10) << __func__ << ": received pct update to "
	   << m->pg_committed_to << dendl;
  parent->update_pct(m->pg_committed_to);
}

void ReplicatedBackend::send_pct_update()
{
  dout(10) << __func__ << ": sending pct update" << dendl;
  ceph_assert(
    PG_HAVE_FEATURE(parent->get_pg_acting_features(), PCT));
  for (const auto &i: parent->get_acting_shards()) {
    if (i == parent->whoami_shard()) continue;

    auto *pct_update = new MOSDPGPCT(
      spg_t(parent->whoami_spg_t().pgid, i.shard),
      get_osdmap_epoch(), parent->get_interval_start_epoch(),
      parent->get_pg_committed_to()
    );

    dout(10) << __func__ << ": sending pct update to i " << i
	     << ", i.osd " << i.osd << dendl;
    parent->send_message_osd_cluster(
      i.osd, pct_update, get_osdmap_epoch());
  }
  dout(10) << __func__ << ": sending pct update complete" << dendl;
}

void ReplicatedBackend::maybe_kick_pct_update()
{
  if (!in_progress_ops.empty()) {
    dout(20) << __func__ << ": not scheduling pct update, "
	     << in_progress_ops.size() << " ops pending" << dendl;
    return;
  }

  if (!PG_HAVE_FEATURE(parent->get_pg_acting_features(), PCT)) {
    dout(20) << __func__ << ": not scheduling pct update, PCT feature not"
	     << " supported" << dendl;
    return;
  }

  if (pct_callback.is_scheduled()) {
    derr << __func__
	 << ": pct_callback is already scheduled, this should be impossible"
	 << dendl;
    return;
  }

  int64_t pct_delay;
  if (!parent->get_pool().opts.get(
	pool_opts_t::PCT_UPDATE_DELAY, &pct_delay)) {
    dout(20) << __func__ << ": not scheduling pct update, PCT_UPDATE_DELAY not"
	     << " set" << dendl;
    return;
  }

  dout(10) << __func__ << ": scheduling pct update after "
	   << pct_delay << " seconds" << dendl;
  parent->get_pg_timer().schedule_after(
    pct_callback, std::chrono::seconds(pct_delay));
}

void ReplicatedBackend::cancel_pct_update()
{
  if (pct_callback.is_scheduled()) {
    dout(10) << __func__ << ": canceling pct update" << dendl;
    parent->get_pg_timer().cancel(pct_callback);
  }
}

void ReplicatedBackend::submit_transaction(
  const hobject_t &soid,
  const object_stat_sum_t &delta_stats,
  const eversion_t &at_version,
  PGTransactionUPtr &&_t,
  const eversion_t &trim_to,
  const eversion_t &pg_committed_to,
  vector<pg_log_entry_t>&& _log_entries,
  std::optional<pg_hit_set_history_t> &hset_history,
  Context *on_all_commit,
  ceph_tid_t tid,
  osd_reqid_t reqid,
  OpRequestRef orig_op)
{
  // 有新写入到来时，取消空闲期间用于更新 PCT 的定时任务。
  cancel_pct_update();

  // 先把本次操作造成的统计增量投影到主 PG 的内存状态中。
  // 这些统计信息稍后会和对象修改、PGLog 一起进入本地事务。
  parent->apply_stats(
    soid,
    delta_stats);

  // PrimaryLogPG 构造的是与存储后端无关的 PGTransaction。
  vector<pg_log_entry_t> log_entries(_log_entries);
  ObjectStore::Transaction op_t;
  PGTransactionUPtr t(std::move(_t));
  set<hobject_t> added, removed;
  // generate_transaction() 将它翻译成可交给 ObjectStore 的事务 op_t，
  // 同时收集本次新建/删除的临时对象，供主副本同步维护临时对象集合。
  generate_transaction(
    t,
    coll,
    log_entries,
    &op_t,
    &added,
    &removed,
    get_osdmap()->require_osd_release);
  ceph_assert(added.size() <= 1);
  ceph_assert(removed.size() <= 1);

  // 为这次复制写建立跟踪对象。tid 唯一标识本次复制事务；
  // on_all_commit 最终会回到 PrimaryLogPG，使对应 RepGather 进入 committed。
  auto insert_res = in_progress_ops.insert(
    make_pair(
      tid,
      ceph::make_ref<InProgressOp>(
	tid, on_all_commit,
	orig_op, at_version)
      )
    );
  ceph_assert(insert_res.second);
  InProgressOp &op = *insert_res.first->second;

  // 初始时认为 acting、recovery/backfill 集合里的所有 shard 都尚未提交。
  // 本地主 OSD commit 和各副本的 commit reply 会分别从集合中删除自己；
  // 集合清空后，才触发 on_all_commit。
  op.waiting_for_commit.insert(
    parent->get_acting_recovery_backfill_shards().begin(),
    parent->get_acting_recovery_backfill_shards().end());

  // 把 op_t、PGLog 条目及版本信息封装成 MOSDRepOp，发送给除自己外的所有目标 shard。
  // 这里只发送副本请求；主 OSD 的本地事务在后面入队。
  issue_op(
    soid,
    at_version,
    tid,
    reqid,
    trim_to,
    pg_committed_to,
    added.size() ? *(added.begin()) : hobject_t(),
    removed.size() ? *(removed.begin()) : hobject_t(),
    log_entries,
    hset_history,
    &op,
    op_t);

  // 更新主 PG 内存中记录的临时对象集合。实际对象的创建/删除已经包含在 op_t 中；
  // 这里维护的是 PG 对临时对象生命周期的跟踪信息。
  add_temp_objs(added);
  clear_temp_objs(removed);

  // 将 PGLog、PG 元数据、trim 等修改追加到主 OSD 的 op_t。
  // issue_op() 已经把副本所需的事务和日志编码进消息，因此这里追加的是 primary 自己落盘所需的本地 PG 状态更新。
  parent->log_operation(
    std::move(log_entries),
    hset_history,
    trim_to,
    at_version,
    pg_committed_to,
    true,
    op_t);

  // ObjectStore 承诺本地 op_t 已持久化后执行该回调：
  // op_commit() 会把主 OSD 自己从 waiting_for_commit 中移除，并重新检查是否全部提交。
  op_t.register_on_commit(
    parent->bless_context(
      new C_OSD_OnOpCommit(this, &op)));

  // 将完整的本地事务交给 ObjectStore。
  // 对象数据/元数据和本地 PGLog 位于同一事务中，因此它们以 ObjectStore 的事务语义原子提交。
  vector<ObjectStore::Transaction> tls;
  tls.push_back(std::move(op_t));

  parent->queue_transactions(tls, op.op);
  // 事务已经成功排入本地 ObjectStore 后，推进 PG 的 applied 版本。
  // 这不代表已经持久化；持久化完成由上面的 on_commit 回调表示。
  if (at_version != eversion_t()) {
    parent->op_applied(at_version);
  }
}

/**
 * 主 OSD 本地事务持久化
 *   -> C_OSD_OnOpCommit::finish()
 *   -> op_commit()
 *   -> waiting_for_commit 删除 primary
 */
void ReplicatedBackend::op_commit(const ceph::ref_t<InProgressOp>& op)
{
  // PG 状态变化或取消流程可能已经撤销 on_commit。
  // 此时虽然旧的 ObjectStore 回调仍然到达，也不能再完成原 RepGather，直接忽略即可。
  if (op->on_commit == nullptr) {
    return;
  }

  // 以下只记录 primary 本地事务达到 commit 的时间点，便于性能跟踪。
  FUNCTRACE(cct);
  OID_EVENT_TRACE_WITH_MSG((op && op->op) ? op->op->get_req() : NULL, "OP_COMMIT_BEGIN", true);
  dout(10) << __func__ << ": " << op->tid << dendl;
  if (op->op) {
    op->op->mark_event("op_commit");
    op->op->pg_trace.event("op commit");
  }

  // waiting_for_commit 初始包含 primary 和所有要求参与的副本 shard。
  // 本函数由 primary 本地 ObjectStore 的 on_commit 触发，因此删除自己；
  // 各副本则由 do_repop_reply() 收到 ONDISK 回复后分别删除。
  op->waiting_for_commit.erase(get_parent()->whoami_shard());

  // 如果此时集合为空，说明 primary 本地事务和全部副本事务均已持久化。
  // on_commit 就是 submit_transaction() 收到的 on_all_commit，
  // 它连接回 PrimaryLogPG 的 RepGather committed 回调。
  if (op->waiting_for_commit.empty()) {
    op->on_commit->complete(0);
    // complete() 会消费/释放 Context；置空避免取消或迟到路径重复执行。
    op->on_commit = 0;
    // backend 已无需继续按复制 tid 跟踪这次写入。
    in_progress_ops.erase(op->tid);
  }
  // 若已经没有复制写在途，可按配置启动 PCT 更新定时任务。
  maybe_kick_pct_update();
}

/**
 * 副本 OSD
 * repop_commit()
 *   -> send_message_osd_cluster(MOSDRepOpReply)
 *      |
 *      v
 * primary OSD
 * PrimaryLogPG::do_request()
 *   -> PGBackend::handle_message()
 *   -> ReplicatedBackend::_handle_message()
 *   -> MSG_OSD_REPOPREPLY
 *   -> do_repop_reply()
 */
void ReplicatedBackend::do_repop_reply(OpRequestRef op)
{
  // 完成 MOSDRepOpReply 的延迟解码，并确认消息类型。
  // 该 reply 是副本在 repop_commit() 中达到持久化点后发回 primary 的确认。
  static_cast<MOSDRepOpReply*>(op->get_nonconst_req())->finish_decode();
  auto r = op->get_req<MOSDRepOpReply>();
  ceph_assert(r->get_header().type == MSG_OSD_REPOPREPLY);

  op->mark_started();

  // tid 标识 primary 当初发出的复制事务，用来查找对应 InProgressOp；
  // from 是发送确认的副本 shard，用来删除 waiting_for_commit 中的成员。
  ceph_tid_t rep_tid = r->get_tid();
  pg_shard_t from = r->from;

  // InProgressOp 可能已经因 PG 状态变化而取消，或者该 reply 重复/迟到；
  // 找不到时不再修改旧事务状态，直接忽略这条确认。
  auto iter = in_progress_ops.find(rep_tid);
  if (iter != in_progress_ops.end()) {
    InProgressOp &ip_op = *iter->second;
    // 部分内部操作没有原始客户端 MOSDOp；这里取 m 只为输出更完整的日志。
    const MOSDOp *m = nullptr;
    if (ip_op.op)
      m = ip_op.op->get_req<MOSDOp>();

    if (m)
      dout(7) << __func__ << ": tid " << ip_op.tid << " op " //<< *m
	      << " ack_type " << (int)r->ack_type
	      << " from " << from
	      << dendl;
    else
      dout(7) << __func__ << ": tid " << ip_op.tid << " (no op) "
	      << " ack_type " << (int)r->ack_type
	      << " from " << from
	      << dendl;

    // ONDISK 表示该副本已把对象事务和本地 PGLog/PG 元数据事务持久化。
    // 每个副本只能确认一次，因此先断言它仍在等待集合中，再将其删除。
    if (r->ack_type & CEPH_OSD_FLAG_ONDISK) {
      ceph_assert(ip_op.waiting_for_commit.count(from));
      ip_op.waiting_for_commit.erase(from);
      // 仅记录副本 commit reply 的接收时点，用于请求 trace 和性能诊断。
      if (ip_op.op) {
	ip_op.op->mark_event("sub_op_commit_rec");
	ip_op.op->pg_trace.event("sub_op_commit_rec");
      }
    } else {
      // 旧协议可能发送不带 ONDISK 的 ACK；当前路径只以持久化确认推进
      // waiting_for_commit，因此忽略这种普通 ACK。
    }

    // 同步 primary 维护的该 peer 持久化完成水位，供 peering、恢复和
    // PGLog 管理判断副本已经可靠保存到哪个 complete 位置。
    parent->update_peer_last_complete_ondisk(
      from,
      r->get_last_complete_ondisk());

    // 主 OSD 本地 commit 和所有副本 ONDISK reply 都会从同一个集合中删除对应 shard。
    // 集合清空表示全部要求参与的 OSD 均已持久化。
    if (ip_op.waiting_for_commit.empty() &&
        ip_op.on_commit) {
      // on_commit 实际连接到 PrimaryLogPG 的 RepGather：complete(0) 会
      // 标记这次 repop committed，并继续触发 eval_repop() 完成客户端请求。
      ip_op.on_commit->complete(0);
      ip_op.on_commit = 0;
      // 全部提交后不再需要按 tid 跟踪本次 backend 复制操作。
      in_progress_ops.erase(iter);
    }
  }
  // 如果当前已经没有复制写在途，可按配置启动 PCT 更新定时任务。
  maybe_kick_pct_update();
}

static uint32_t crc32_netstring(const uint32_t orig_crc, std::string_view data)
{
  // XXX: This function MUST be compliant with the bufferlist marshalling format!
  // Otherwise scrubs-during-upgrade will explode.
  __u32 len = data.length();
  auto crc = ceph_crc32c(orig_crc, (unsigned char*)&len, sizeof(len));
  crc = ceph_crc32c(crc, (unsigned char*)data.data(), data.length());

#ifndef NDEBUG
  // let's verify the compatibility but, due to performance penalty,
  // only in debug builds.
  ceph::bufferlist bl;
  bl.append(data);
  ceph::bufferlist bl_encoded;
  encode(bl, bl_encoded);
  ceph_assert(bl_encoded.crc32c(orig_crc) == crc);
  // also as string view -- for the sake of keys
  ceph::bufferlist sv_encoded;
  encode(data, sv_encoded);
  ceph_assert(sv_encoded.crc32c(orig_crc) == crc);
#endif
  return crc;
}


std::optional<int32_t> ReplicatedBackend::be_deep_scrub_read_data(
    const Scrub::ScrubCounterSet &io_counters,
    const hobject_t& poid,
    ScrubMapBuilder& pos,
    ScrubMap::object& smap_object)
{
  if (pos.data_pos == 0) {
    pos.data_hash = bufferhash(-1);
  }
  ceph_assert(pos.data_pos >= 0);  // also simplifies subtraction below
  const uint64_t stride = cct->_conf->osd_deep_scrub_stride;

  // note re the '1' (and not '0') in the next line: we should never
  // reach here with pos == size != 0, as that is caught by the check
  // after the 'read'. But if size==0, we do want to try to read
  // something (and get EOF).
  uint64_t to_read{1};
  if (std::cmp_greater(smap_object.size, pos.data_pos)) {
    to_read = std::min(stride, smap_object.size - pos.data_pos);
    // the implicit 'else' is to_read = 1.
  }

  auto& perf_logger = *(get_parent()->get_logger());
  perf_logger.inc(io_counters.read_cnt);
  bufferlist bl;
  const int r = store->read(
      ch,
      ghobject_t(poid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
      pos.data_pos, to_read, bl, scrub_fadvise_flags);
  if (r < 0) {
    dout(5) << fmt::format(
                   "{}: {} got {} on read, read_error", __func__, poid, r)
            << dendl;
    smap_object.read_error = true;
    return 0;
  }
  if (r > 0) {
    pos.data_hash << bl;
    perf_logger.inc(io_counters.read_bytes, r);
  }
  pos.data_pos += r;
  if (std::cmp_greater_equal(pos.data_pos, smap_object.size) ||
      std::cmp_less(r, to_read)) {
    // done with bytes
    smap_object.digest = pos.data_hash.digest();
    smap_object.digest_present = true;
    dout(10) << fmt::format(
                    "{}: {} read {} bytes total ({} now; expected:{}; "
                    "obj-size:{}), done with data. Digest {:#x}",
                    __func__, poid, pos.data_pos, r, to_read, smap_object.size,
                    smap_object.digest)
             << dendl;
    pos.data_pos = -1;
    // the caller is not required to return immediately, and may continue
    // analyzing the object.
    return std::nullopt;
  }
  dout(10) << fmt::format(
                  "{}: {} read {} bytes total ({} now; obj-size:{}), more data "
                  "to read. Digest so far: {:#x}",
                  __func__, poid, pos.data_pos, r, smap_object.size,
                  pos.data_hash.digest())
           << dendl;
  return -EINPROGRESS;
}


int ReplicatedBackend::be_deep_scrub(
  const Scrub::ScrubCounterSet& io_counters,
  const hobject_t &poid,
  ScrubMap &map,
  ScrubMapBuilder &pos,
  ScrubMap::object& smap_object)
{
  dout(10) << fmt::format("{} {} pos {}", __func__, poid, pos) << dendl;
  auto& perf_logger = *(get_parent()->get_logger());

  {
    // possible debug sleep
    utime_t sleeptime;
    sleeptime.set_from_double(cct->_conf->osd_debug_deep_scrub_sleep);
    if (sleeptime != utime_t()) {
      lgeneric_derr(cct) << __func__ << " sleeping for " << sleeptime << dendl;
      sleeptime.sleep();
    }
  }

  ceph_assert(poid == pos.ls[pos.pos]);
  if (!pos.data_done()) {
    if (auto maybe_must_rtrn =
            be_deep_scrub_read_data(io_counters, poid, pos, smap_object);
        maybe_must_rtrn) {
      // either -EINPROGRESS or a 0 (which means read error)
      return *maybe_must_rtrn;
    }
  }

  // omap header
  if (pos.omap_pos.empty()) {
    pos.omap_hash = -1;

    perf_logger.inc(io_counters.omapgetheader_cnt);
    bufferlist hdrbl;
    int r = store->omap_get_header(
      ch,
      ghobject_t(
	poid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
      &hdrbl, true);
    if (r == -EIO) {
      dout(20) << __func__ << "  " << poid << " got "
	       << r << " on omap header read, read_error" << dendl;
      smap_object.read_error = true;
      return 0;
    }
    if (r == 0 && hdrbl.length()) {
      bool encoded = false;
      dout(25) << "CRC header " << cleanbin(hdrbl, encoded, true) << dendl;
      pos.omap_hash = hdrbl.crc32c(pos.omap_hash);
      perf_logger.inc(io_counters.omapgetheader_bytes, hdrbl.length());
    }
  }

  // omap

  perf_logger.inc(io_counters.omapget_cnt);
  using omap_iter_seek_t = ObjectStore::omap_iter_seek_t;
  auto result = store->omap_iterate(
    ch,
    ghobject_t{
      poid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard},
    // try to seek as many keys-at-once as possible for the sake of performance.
    // note complexity should be logarithmic, so seek(n/2) + seek(n/2) is worse
    // than just seek(n).
    ObjectStore::omap_iter_seek_t{
      .seek_position = pos.omap_pos,
      .seek_type = omap_iter_seek_t::LOWER_BOUND
    },
    [&pos, first=true, max=cct->_conf->osd_deep_scrub_keys]
    (std::string_view key, std::string_view value) mutable {
      --max;
      if (first) {
        first = false; // preserve exact compat on `max` with old code
      } else if (max == 0) {
        pos.omap_pos = key;
        return ObjectStore::omap_iter_ret_t::STOP;
      }
      pos.omap_bytes += value.length();
      ++pos.omap_keys;
      pos.omap_hash = crc32_netstring(pos.omap_hash, key);
      pos.omap_hash = crc32_netstring(pos.omap_hash, value);
      return ObjectStore::omap_iter_ret_t::NEXT;
    });
  if (result < 0) {
    return -EIO;
  } else if (const auto more = static_cast<bool>(result); more) {
    return -EINPROGRESS;
  }

  // we have the full omap now. Finalize the perf counting
  perf_logger.inc(io_counters.omapget_bytes, pos.omap_bytes);

  if (pos.omap_keys > cct->_conf->
	osd_deep_scrub_large_omap_object_key_threshold ||
      pos.omap_bytes > cct->_conf->
	osd_deep_scrub_large_omap_object_value_sum_threshold) {
    dout(25) << __func__ << " " << poid
	     << " large omap object detected. Object has " << pos.omap_keys
	     << " keys and size " << pos.omap_bytes << " bytes" << dendl;
    smap_object.large_omap_object_found = true;
    smap_object.large_omap_object_key_count = pos.omap_keys;
    smap_object.large_omap_object_value_size = pos.omap_bytes;
    map.has_large_omap_object_errors = true;
  }

  smap_object.omap_digest = pos.omap_hash;
  smap_object.omap_digest_present = true;
  dout(20) << __func__ << " done with " << poid << " omap_digest "
	   << std::hex << smap_object.omap_digest << std::dec << dendl;

  // Sum up omap usage
  if (pos.omap_keys > 0 || pos.omap_bytes > 0) {
    dout(25) << __func__ << " adding " << pos.omap_keys << " keys and "
             << pos.omap_bytes << " bytes to pg_stats sums" << dendl;
    map.has_omap_keys = true;
    smap_object.object_omap_bytes = pos.omap_bytes;
    smap_object.object_omap_keys = pos.omap_keys;
  }

  // done!
  return 0;
}

/**
 * 在副本端处理 primary 发来的 MOSDPGPush 消息。
 *
 * 函数为本条消息创建一个 ObjectStore 事务，逐个调用 handle_push() 将各个
 * PushOp 的对象数据和元数据写入事务，并收集对应的 PushReplyOp。
 * 事务提交完成后通过 on-complete 回调发送 MOSDPGPushReply，
 * 通知 primary 本次 push 的处理结果；因此回复发送建立在本地写入完成之后。
 *
 * MOSDPGPush
 * PushOp(A) -> submit_push_data() -> 追加对象 A 的写操作
 * PushOp(B) -> submit_push_data() -> 追加对象 B 的写操作
 * PushOp(C) -> submit_push_data() -> 追加对象 C 的写操作
 *                                   ↓
 *                          一个 Transaction t
 *
 * 每收到一条 MOSDPGPush
 *  -> 创建并提交一个 ObjectStore Transaction
 *
 * 如果同一个对象被拆成三块，通常是三条消息、三个事务：
 *   第 1 个 PushOp -> Transaction 1：写临时对象
 *   第 2 个 PushOp -> Transaction 2：继续写临时对象
 *   第 3 个 PushOp -> Transaction 3：写最后数据并切换正式对象
 */
void ReplicatedBackend::_do_push(OpRequestRef op)
{
  // 将收到的请求解码为 MOSDPGPush，里面可能包含多个对象的 PushOp。
  auto m = op->get_req<MOSDPGPush>();
  ceph_assert(m->get_type() == MSG_OSD_PG_PUSH);
  // 记录发送 push 的 peer，后续 handle_push() 用它更新对应的 recovery 状态。
  pg_shard_t from = m->from;

  // 标记该内部请求已经开始在 PG 中处理。
  op->mark_started();

  // replies 与 m->pushes 中的 PushOp 一一对应，最后随回复消息发回 primary。
  vector<PushReplyOp> replies;
  // 同一条 push 消息中的对象写入同一个 ObjectStore 事务，统一提交。
  ObjectStore::Transaction t{get_parent()->min_peer_features()};
  if (get_parent()->check_failsafe_full()) {
    // failsafe full 表示 OSD 已不能继续安全写入，直接中止而不是接受更多恢复数据。
    dout(10) << __func__ << " Out of space (failsafe) processing push request." << dendl;
    ceph_abort();
  }
  // 逐个处理 primary 携带的对象；handle_push() 把写操作加入 t 并填充对应回复。
  for (vector<PushOp>::const_iterator i = m->pushes.begin();
       i != m->pushes.end();
       ++i) {
    replies.push_back(PushReplyOp());
    handle_push(from, *i, &(replies.back()), &t, m->is_repair);
  }

  // 构造本次 push 的统一回复，携带发送端、PG、epoch 和每个对象的处理结果。
  MOSDPGPushReply *reply = new MOSDPGPushReply;
  reply->from = get_parent()->whoami_shard();
  reply->set_priority(m->get_priority());
  reply->pgid = get_info().pgid;
  reply->map_epoch = m->map_epoch;
  reply->min_epoch = m->min_epoch;
  reply->replies.swap(replies);
  // 根据回复中包含的对象结果计算其调度成本。
  reply->compute_cost(cct);

  // 只有事务完成后才发送回复，确保 primary 收到成功结果时本地写入已经完成。
  t.register_on_complete(
    new PG_SendMessageOnConn(
      get_parent(), reply, m->get_connection()));

  // 将对象写入和完成回调交给 ObjectStore 异步执行。
  get_parent()->queue_transaction(std::move(t));
}

struct C_ReplicatedBackend_OnPullComplete : GenContext<ThreadPool::TPHandle&> {
  ReplicatedBackend *bc;
  list<ReplicatedBackend::pull_complete_info> to_continue;
  int priority;
  C_ReplicatedBackend_OnPullComplete(
    ReplicatedBackend *bc,
    int priority,
    list<ReplicatedBackend::pull_complete_info> &&to_continue)
    : bc(bc), to_continue(std::move(to_continue)), priority(priority) {}

  /**
   * 在 primary 的本地 pull 事务完成后，继续已拉完整对象的 recovery。
   *
   * _do_pull_response() 仅在 to_continue 非空时注册本回调，
   * 并经 PG_RecoveryQueueAsync 重新进入 OSD recovery 调度器。
   * 每个对象先清除 pulling 状态，再尝试向仍缺失它的副本准备 push；
   * 所有准备结果集中到 一个 RPGHandle，最后由 run_recovery_op() 统一发送。
   */
  void finish(ThreadPool::TPHandle &handle) override {
    // 同一回调中的多个已完成 pull 对象共用一个 recovery handle，合并后续消息发送。
    ReplicatedBackend::RPGHandle *h = bc->_open_recovery_op();
    for (auto &&i: to_continue) {
      // pull 信息保留到事务完成后，确保 OBC 和 pulling 状态在本地数据可用前不会被释放。
      auto j = bc->pulling.find(i.hoid);
      ceph_assert(j != bc->pulling.end());
      ObjectContextRef obc = j->second.obc;
      // handle_pull_response() 已清除 pull_from_peer 反向索引；这里只删除 pulling 条目。
      bc->clear_pull(j, false /* already did it */);
      ceph_assert(obc);
      // 为仍缺失该对象的 peer 准备 PushOp，并追加到 h。
      int started = bc->start_pushes(i.hoid, obc, h);
      if (started < 0) {
	// 准备 push 失败时撤销本对象的临时 push 状态，并按本地 pull 失败处理。
	bc->pushing[i.hoid].clear();
	bc->get_parent()->on_failed_pull(
	  { bc->get_parent()->whoami_shard() },
	  i.hoid, obc->obs.oi.version);
      } else if (!started) {
	// 没有 peer 需要继续接收该对象，primary 可直接登记该对象全局恢复完成。
	bc->get_parent()->on_global_recover(
	  i.hoid, i.stat, false);
      }
      // 防止处理多个对象时 worker 因长时间运行触发线程池超时。
      handle.reset_tp_timeout();
    }
    // 统一提交本批准备好的 push/pull/delete recovery 消息；空 handle 也由该函数收尾。
    bc->run_recovery_op(h, priority);
  }

  /// Estimate total data reads required to perform pushes
  uint64_t estimate_push_costs() const {
    uint64_t cost = 0;
    for (const auto &i: to_continue) {
      cost += i.stat.num_bytes_recovered;
    }
    return cost;
  }
};

/**
 * 在 primary 端处理数据源副本以 MOSDPGPush 形式返回的 Pull 响应。
 *
 * 函数逐个将 PushOp 中收到的数据和恢复状态加入本地 ObjectStore 事务。
 * 对象尚未完整时构造下一轮 PullOp；
 * 对象完整时则在事务完成后清理 pulling 状态，并继续向需要该对象的副本发起 push 恢复。
 * 下一轮 Pull 消息同样只在本事务完成后发送，因而不会要求对端继续传输而本地尚未完成前一块数据的写入。
 */
void ReplicatedBackend::_do_pull_response(OpRequestRef op)
{
  // 该 MOSDPGPush 是先前 PullOp 的响应；from 为本次数据的提供者。
  auto m = op->get_req<MOSDPGPush>();
  ceph_assert(m->get_type() == MSG_OSD_PG_PUSH);
  pg_shard_t from = m->from;

  op->mark_started();

  // 尾部占位元素供当前 PushOp 填充；只有需要下一块数据时才保留它。
  vector<PullOp> replies(1);
  if (get_parent()->check_failsafe_full()) {
    dout(10) << __func__ << " Out of space (failsafe) processing pull response (push)." << dendl;
    ceph_abort();
  }

  // 将本批收到的数据、属性及 PG 恢复状态作为一个本地事务提交。
  ObjectStore::Transaction t{get_parent()->min_peer_features()};
  // 已完成 pull 的对象需等待上述事务完成后，才能开始向其他副本 push。
  list<pull_complete_info> to_continue;
  for (vector<PushOp>::const_iterator i = m->pushes.begin();
       i != m->pushes.end();
       ++i) {
    // handle_pull_response() 更新 pulling 进度，并在未完成时写入下一轮 PullOp。
    bool more = handle_pull_response(from, *i, &(replies.back()), &to_continue, &t);
    if (more)
      replies.push_back(PullOp());
  }
  if (!to_continue.empty()) {
    // 回调在本地数据写入完成后清理 pull，并将已恢复对象继续推给目标副本。
    C_ReplicatedBackend_OnPullComplete *c =
      new C_ReplicatedBackend_OnPullComplete(
	this,
	m->get_priority(),
	std::move(to_continue));
    t.register_on_complete(
      new PG_RecoveryQueueAsync(
	get_parent(),
	get_parent()->bless_unlocked_gencontext(c),
        std::max<uint64_t>(1, c->estimate_push_costs())));
  }
  // 最后一个元素始终是下一个 PushOp 的预留槽；循环结束后将其移除。
  replies.erase(replies.end() - 1);

  if (replies.size()) {
    // 仍未完成的对象向同一数据源请求下一块；发送也延后到事务完成后。
    MOSDPGPull *reply = new MOSDPGPull;
    reply->from = parent->whoami_shard();
    reply->set_priority(m->get_priority());
    reply->pgid = get_info().pgid;
    reply->map_epoch = m->map_epoch;
    reply->min_epoch = m->min_epoch;
    reply->set_pulls(std::move(replies));
    reply->compute_cost(cct);

    t.register_on_complete(
      new PG_SendMessageOnConn(
	get_parent(), reply, m->get_connection()));
  }

  // ObjectStore 异步执行事务，并在完成时依次触发恢复续接和消息发送回调。
  get_parent()->queue_transaction(std::move(t));
}

/**
 * 在提供数据的副本端处理 primary 发来的 MOSDPGPull 请求。
 *
 * 函数取出消息中的多个 PullOp，逐个调用 handle_pull() 从本地对象构造 PushOp，
 * 并按请求来源 peer 组织回复，最后通过 send_pushes() 将对象数据返回给 primary。
 */
void ReplicatedBackend::do_pull(OpRequestRef op)
{
  // 取得可修改的 pull 消息，以便转移其中的 PullOp 列表。
  MOSDPGPull *m = static_cast<MOSDPGPull *>(op->get_nonconst_req());
  ceph_assert(m->get_type() == MSG_OSD_PG_PULL);
  // from 是发起 pull 的 primary，后续 PushOp 将发送回该 shard。
  pg_shard_t from = m->from;

  // 按目标 peer 分组保存要返回的 PushOp；本消息通常只对应一个来源 peer。
  map<pg_shard_t, vector<PushOp> > replies;
  // 一条 MOSDPGPull 可以批量请求多个对象，逐个生成对应的数据回复。
  for (auto& i : m->take_pulls()) {
    // 为当前 PullOp 预留一个 PushOp，handle_pull() 会填充对象数据和进度。
    replies[from].push_back(PushOp());
    handle_pull(from, i, &(replies[from].back()));
  }
  // 将构造出的 PushOp 批量封装为 MOSDPGPush，发送回发起请求的 primary。
  send_pushes(m->get_priority(), replies);
}

/**
 * 在 primary 端处理副本返回的 MOSDPGPushReply。
 *
 * 函数逐个处理回复中的 PushReplyOp，由 handle_push_reply() 更新对象的 recovery 进度并构造下一块 PushOp；
 * 仍有数据未发送完的对象会被重新组织，最后通过 send_pushes() 继续向同一个副本发送。
 * 所有对象都完成时则不会生成新的 PushOp。
 */
void ReplicatedBackend::do_push_reply(OpRequestRef op)
{
  // 将请求解析为 push 回复消息，并确认消息类型正确。
  auto m = op->get_req<MOSDPGPushReply>();
  ceph_assert(m->get_type() == MSG_OSD_PG_PUSH_REPLY);
  // 记录回复来源，用于后续继续向同一个 peer 发送 recovery 数据。
  pg_shard_t from = m->from;

  // 预留一个输出元素；每个仍未完成的对象会对应一个新的 PushOp。
  vector<PushOp> replies(1);
  // 一条回复消息可能包含多个对象的处理结果，逐个推进其 recovery 状态。
  for (vector<PushReplyOp>::const_iterator i = m->replies.begin();
       i != m->replies.end();
       ++i) {
    // 根据副本反馈的进度构造下一块数据；返回 true 表示该对象尚未完成。
    bool more = handle_push_reply(from, *i, &(replies.back()));
    if (more) {
      // 当前输出 PushOp 已被当前对象使用，下一对象需要新的输出槽位。
      replies.push_back(PushOp());
    }
  }
  // 循环结束时最后一个元素只是预留槽位，移除它避免发送空 PushOp。
  replies.erase(replies.end() - 1);

  // 将仍需继续恢复的 PushOp 按原 peer 分组，交给统一发送函数。
  map<pg_shard_t, vector<PushOp> > _replies;
  _replies[from].swap(replies);
  // 对象还有剩余分块时发送下一轮；没有剩余对象时发送空集合，不会产生消息。
  send_pushes(m->get_priority(), _replies);
}

Message * ReplicatedBackend::generate_subop(
  const hobject_t &soid,
  const eversion_t &at_version,
  ceph_tid_t tid,
  osd_reqid_t reqid,
  eversion_t pg_trim_to,
  eversion_t pg_committed_to,
  hobject_t new_temp_oid,
  hobject_t discard_temp_oid,
  const bufferlist &log_entries,
  std::optional<pg_hit_set_history_t> &hset_hist,
  ObjectStore::Transaction &op_t,
  pg_shard_t peer,
  const pg_info_t &pinfo)
{
  // 要求副本报告“已应用”和“已持久化”。
  // 当前复制提交路径最终以 ONDISK/commit reply 为准，从而推进主 OSD 的 waiting_for_commit。
  int acks_wanted = CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK;

  // 构造发往指定 peer shard 的副本写消息。
  // 消息头携带客户端请求 ID、primary shard、目标 PG shard、对象、复制事务 tid 和本次 PG 版本；
  // map epoch/min epoch 用于接收端确认消息仍属于有效的 peering interval。
  // MOSDRepOp 构造函数的初始化列表中设置 MSG_OSD_REPOP 消息类型
  MOSDRepOp *wr = new MOSDRepOp(
    reqid, parent->whoami_shard(),
    spg_t(get_info().pgid.pgid, peer.shard),
    soid, acks_wanted,
    get_osdmap_epoch(),
    parent->get_last_peering_reset_epoch(),
    tid, at_version);

  if (!parent->should_send_op(peer, soid)) {
    // backfill/recovery 期间，某些目标副本尚不应接收这个对象的实际修改。
    // 此时仍发送 MOSDRepOp，但装入空事务，使副本可以同步 PGLog、版本和 PG 状态，
    // 而该对象的数据随后由 recovery/backfill 流程补齐。
    ObjectStore::Transaction t;
    encode(t, wr->get_data());
  } else {
    // 将主 OSD 已生成的 ObjectStore 事务序列化进消息。
    // p 是事务操作及元数据描述，d 是单独拆出的数据负载；副本 do_repop() 会重新合并解码。
    bufferlist p, d;
    op_t.encode(p, d, get_parent()->min_peer_features());
    if (d.length() != 0) {
      // 新格式把事务描述放在 message middle，把大块数据放在 data，
      // 避免把两类内容重新拼接成一个连续 bufferlist。
      wr->set_txn_payload(p);
      wr->set_data(d);
    } else {
      // 没有独立数据段时沿用兼容格式：全部编码内容都放入 data。
      wr->set_data(p);
    }
  }

  // 对象事务之外，副本还必须写入相同的 PGLog 条目。
  wr->logbl = log_entries;

  // backfill 未完成的副本有自己的进度统计，不能直接覆盖为 primary 的完整统计；
  // 正常副本则随本次复制接收 primary 的最新 PG 统计。
  if (pinfo.is_incomplete())
    wr->pg_stats = pinfo.stats;  // reflects backfill progress
  else
    wr->pg_stats = get_info().stats;

  // 告诉副本本次可以裁剪到哪个 PGLog 版本。
  wr->pg_trim_to = pg_trim_to;

  // this feature is from 2019 (6f12bf27cb91), assume present
  ceph_assert(HAVE_FEATURE(parent->min_peer_features(), OSD_REPOP_MLCOD));
  // primary 当前确认的全 PG committed 边界；
  // 副本用它维护自身的持久化/可读状态，而不是把本次 at_version 当作已全局提交。
  wr->pg_committed_to = pg_committed_to;

  // 同步临时对象生命周期和 hit-set 历史等附属 PG 状态。
  wr->new_temp_oid = new_temp_oid;
  wr->discard_temp_oid = discard_temp_oid;
  wr->updated_hit_set_history = hset_hist;
  return wr;
}

void ReplicatedBackend::issue_op(
  const hobject_t &soid,
  const eversion_t &at_version,
  ceph_tid_t tid,
  osd_reqid_t reqid,
  eversion_t pg_trim_to,
  eversion_t pg_committed_to,
  hobject_t new_temp_oid,
  hobject_t discard_temp_oid,
  const vector<pg_log_entry_t> &log_entries,
  std::optional<pg_hit_set_history_t> &hset_hist,
  InProgressOp *op,
  ObjectStore::Transaction &op_t)
{
  // 集合只有主 OSD 自己时（例如 size=1 的副本池），没有副本消息需要发送。
  // 主 OSD 的本地事务仍会由 submit_transaction() 在 issue_op() 返回后提交。
  if (parent->get_acting_recovery_backfill_shards().size() > 1) {
    // 以下代码只记录跟踪信息，不参与复制正确性。
    // replicas 中排除主 shard，便于在请求 trace 中展示本次写正在等待哪些副本子操作。
    if (op->op) {
      op->op->pg_trace.event("issue replication ops");
      ostringstream ss;
      set<pg_shard_t> replicas = parent->get_acting_recovery_backfill_shards();
      replicas.erase(parent->whoami_shard());
      ss << "waiting for subops from " << replicas;
      op->op->mark_sub_op_sent(ss.str());
    }

    // 所有副本收到的 PGLog 条目相同，提前编码一次，避免 generate_subop() 在遍历每个副本时重复执行 encode。
    bufferlist logs;
    encode(log_entries, logs);

    // acting、recovery/backfill 集合是这次操作要求参与提交的全部 shard；
    // submit_transaction() 也用同一个集合初始化了 waiting_for_commit。
    for (const auto& shard : get_parent()->get_acting_recovery_backfill_shards()) {
      // 主 OSD 不需要给自己发送 MOSDRepOp；它走本地 queue_transactions()。
      if (shard == parent->whoami_shard()) continue;

      // 每个副本的 pg_info 可能不同，尤其是 backfill 尚未完成时；
      // generate_subop() 会据此选择随消息发送的 PG 统计信息。
      const pg_info_t &pinfo = parent->get_shard_info().find(shard)->second;

      // 为当前目标副本构造 MOSDRepOp。消息中包含对象事务 op_t、PGLog、
      // 版本/trim 信息以及临时对象变化；副本收到后由 do_repop() 处理。
      Message *wr;
      wr = generate_subop(
        soid,
        at_version,
        tid,
        reqid,
        pg_trim_to,
        pg_committed_to,
        new_temp_oid,
        discard_temp_oid,
        logs,
        hset_hist,
        op_t,
        shard,
        pinfo);

      // 将副本子操作接到原客户端请求的 trace 上，便于跨 OSD 跟踪延迟。
      if (op->op && op->op->pg_trace)
	wr->trace.init("replicated op", nullptr, &op->op->pg_trace);

      // 通过 OSD cluster 网络把消息发给目标 OSD。这里传入当前 OSDMap epoch，
      // 接收端会用消息携带的 epoch 校验它是否仍属于有效 interval。
      get_parent()->send_message_osd_cluster(
        shard.osd, wr, get_osdmap_epoch());
    }
  }
}

// sub op modify
void ReplicatedBackend::do_repop(OpRequestRef op)
{
  // MOSDRepOp 的事务数据可能分布在 middle 和 data 两段；先完成延迟解码，
  // 再取得带有完整字段的消息对象，并确认本入口只处理副本写消息。
  static_cast<MOSDRepOp*>(op->get_nonconst_req())->finish_decode();
  auto m = op->get_req<MOSDRepOp>();
  int msg_type = m->get_type();
  ceph_assert(MSG_OSD_REPOP == msg_type);

  const hobject_t& soid = m->poid;

  dout(10) << __func__ << " " << soid
           << " v " << m->version
	   << (m->logbl.length() ? " (transaction)" : " (parallel exec")
	   << " " << m->logbl.length()
	   << dendl;


  // 消息必须来自当前 peering interval 或之后；
  // 旧 interval 的复制事务不能应用到当前 PG 状态中。
  ceph_assert(m->map_epoch >= get_info().history.same_interval_since);

  dout(30) << __func__ << " missing before " << get_parent()->get_log().get_missing().get_items() << dendl;
  // 如果副本正在 scrub 同一个对象，新写入可能使当前检查结果失效，
  // 因此先通知 scrub 流程尝试抢占/中止该对象的检查。
  parent->maybe_preempt_replica_scrub(soid);

  // 消息来源就是稍后需要接收 commit reply 的 primary OSD。
  int ackerosd = m->get_source().num();

  op->mark_started();

  // RepModify 保存这次副本写从接收直到持久化回包所需的状态：
  // opt 是 primary 发来的对象事务，localt 是副本本地生成的 PGLog/PG 元数据事务。
  RepModifyRef rm(std::make_shared<RepModify>(get_parent()->min_peer_features()));
  rm->op = op;
  rm->ackerosd = ackerosd;
  rm->last_complete = get_info().last_complete;
  rm->epoch_started = get_osdmap_epoch();

  ceph_assert(m->logbl.length());
  // 解码 primary 随消息发送的 PGLog 条目。
  vector<pg_log_entry_t> log;

  // 新消息格式从 middle 读取事务描述、从 data 读取写入数据；
  // 兼容格式没有 middle，事务描述和数据都从 data 中解码。结果写入 rm->opt。
  auto p = const_cast<bufferlist&>(m->get_middle()).cbegin();
  auto d = const_cast<bufferlist&>(m->get_data()).cbegin();
  rm->opt.decode(m->get_middle().length() != 0 ?  p : d, d);

  // 同步 primary 对临时对象生命周期的跟踪。
  if (m->new_temp_oid != hobject_t()) {
    // new_temp_oid 表示：开始跟踪的临时对象是这次复制请求需要完成的一部分。
    dout(20) << __func__ << " start tracking temp " << m->new_temp_oid << dendl;
    add_temp_obj(m->new_temp_oid);
  }
  if (m->discard_temp_oid != hobject_t()) {
    // discard_temp_oid 表示：删除某个临时对象是这次复制请求需要完成的一部分。
    dout(20) << __func__ << " stop tracking temp " << m->discard_temp_oid << dendl;
    if (rm->opt.empty()) {
      // backfill/recovery 边界可能让 primary 只发送空对象事务。
      // 此时若要求丢弃临时对象，副本仍需在自己的 localt 中显式删除它。
      dout(10) << __func__ << ": removing object " << m->discard_temp_oid
	       << " since we won't get the transaction" << dendl;
      rm->localt.remove(coll, ghobject_t(m->discard_temp_oid));
    }
    clear_temp_obj(m->discard_temp_oid);
  }

  p = const_cast<bufferlist&>(m->logbl).begin();
  decode(log, p);
  // 副本写入的数据近期通常不会再次使用，提示 ObjectStore 避免长期缓存。
  rm->opt.set_fadvise_flag(CEPH_OSD_OP_FLAG_FADVISE_DONTNEED);

  bool update_snaps = false;
  if (!rm->opt.empty()) {
    // 非空 opt 说明按 primary 的判断，该对象位于已经 backfill 到的范围内，
    // 此次可以随正常复制立即更新 snap collection。
    // 若 opt 为空，则对象内容和相应 collection 将在后续 recovery/backfill push 时处理。
    update_snaps = true;
  }

  // async recovery 期间，本副本可能仍把 soid 标记为 missing。
  // 此时先把收到的日志事件加入 local next events，避免增量写与后续恢复结果失序。
  bool async = false;
  pg_missing_tracker_t pmissing = get_parent()->get_local_missing();
  if (pmissing.is_missing(soid)) {
    async = true;
    dout(30) << __func__ << " is_missing " << pmissing.is_missing(soid) << dendl;
    for (auto &&e: log) {
      dout(30) << " add_next_event entry " << e << dendl;
      get_parent()->add_local_next_event(e);
      dout(30) << " entry is_delete " << e.is_delete() << dendl;
    }
  }

  // 采用 primary 随消息发送的 PG 统计，然后把 PGLog、trim、committed
  // 边界及相关 PG 元数据修改写入副本自己的 localt。
  parent->update_stats(m->pg_stats);
  parent->log_operation(
    std::move(log),
    m->updated_hit_set_history,
    m->pg_trim_to,
    m->version, /* Replicated PGs don't have rollback info */
    m->pg_committed_to,
    update_snaps,
    rm->localt,
    async);

  // 当副本的 ObjectStore 事务达到持久化点后，C_OSD_RepModifyCommit 调用 repop_commit()，
  // 向 primary 发送带 ONDISK 标志的 MOSDRepOpReply。
  rm->opt.register_on_commit(
    parent->bless_context(
      new C_OSD_RepModifyCommit(this, rm)));

  // localt 负责副本 PGLog/PG 元数据，opt 负责对象数据和对象元数据。
  // 按此顺序作为同一批次交给副本 ObjectStore；commit 回调挂在最后的 opt 上。
  vector<ObjectStore::Transaction> tls;
  tls.reserve(2);
  tls.push_back(std::move(rm->localt));
  tls.push_back(std::move(rm->opt));
  parent->queue_transactions(tls, op);
  // 此处仅完成入队；OpRequest 的清理由 ObjectStore 的完成/提交回调负责。
  dout(30) << __func__ << " missing after" << get_parent()->get_log().get_missing().get_items() << dendl;
}

/**
 * 副本事务持久化完成后的调用路径：
 * do_repop()
 *  -> rm->opt.register_on_commit(C_OSD_RepModifyCommit)
 *  -> PrimaryLogPG::queue_transactions()
 *  -> BlueStore::queue_transactions() 收集 on_commit Context
 *  -> BlueStore::_txc_committed_kv() 将 Context 放入 OSD shard context_queue
 *  -> OSD::ShardedOpWQ::handle_oncommits() 调用 Context::complete(0)
 *  -> C_OSD_RepModifyCommit::finish()
 *  -> ReplicatedBackend::repop_commit()
 * 到达这里说明副本 ObjectStore 已达到持久化点；
 * 本函数随后向 primary 发送带 CEPH_OSD_FLAG_ONDISK 的 MOSDRepOpReply。
 */
void ReplicatedBackend::repop_commit(RepModifyRef rm)
{
  // 记录副本子操作已经走到 commit 回包阶段，仅用于请求跟踪和诊断。
  // rm->committed 是 RepModify 的内存状态，不是触发持久化的动作；进入本函数前 ObjectStore 已经完成 commit。
  rm->op->mark_commit_sent();
  rm->op->pg_trace.event("sup_op_commit");
  rm->committed = true;

  // 取回最初由 primary 发来的 MOSDRepOp。
  // 构造 reply 时需要复用其中的 tid、reqid、PG、primary shard 和 peering interval 等关联信息。
  auto m = rm->op->get_req<MOSDRepOp>();
  ceph_assert(m->get_type() == MSG_OSD_REPOP);
  dout(10) << __func__ << " on op " << *m
		   << ", sending commit to osd." << rm->ackerosd
		   << dendl;
  // ackerosd 在 do_repop() 中取自请求的网络来源，正常情况下就是 primary；
  // 回复前要求它在当前 OSDMap 中仍处于 up 状态。
  ceph_assert(get_osdmap()->is_up(rm->ackerosd));

  // rm->last_complete 是接收该副本写时保存的 PG complete 水位。
  // 现在相关事务已经持久化，可以推进本副本记录的 last_complete_ondisk。
  get_parent()->update_last_complete_ondisk(rm->last_complete);

  // 构造 MSG_OSD_REPOPREPLY：from 是当前副本 shard，result=0 表示成功，
  // ONDISK 表示副本 ObjectStore 已达到持久化点，而不只是接收或应用事务。
  MOSDRepOpReply *reply = new MOSDRepOpReply(
    m,
    get_parent()->whoami_shard(),
    0, get_osdmap_epoch(), m->get_min_epoch(), CEPH_OSD_FLAG_ONDISK);
  // 将副本的持久化完成水位带回 primary，供其维护 peer 的 ondisk 状态。
  reply->set_last_complete_ondisk(rm->last_complete);
  // commit ACK 会解除 primary 对整个客户端写请求的等待，因此使用高优先级，
  // 避免它被普通集群流量长时间阻塞。
  reply->set_priority(CEPH_MSG_PRIO_HIGH); // this better match ack priority!
  // 延续原副本写消息的分布式 trace，便于把发包、落盘和回包关联起来。
  reply->trace = rm->op->pg_trace;
  // 通过 OSD cluster 网络把持久化确认发回 primary；primary 收到后进入
  // do_repop_reply()，并从 waiting_for_commit 中删除当前副本 shard。
  get_parent()->send_message_osd_cluster(
    rm->ackerosd, reply, get_osdmap_epoch());

  // 更新副本写子操作的数量、字节数和延迟统计，不参与提交正确性。
  log_subop_stats(get_parent()->get_logger(), rm->op, l_osd_sop_w);
}


// ===========================================================

/**
 * 计算恢复 head 时需要从 primary 发送的数据范围，
 * 以及可由 peer 的旧 clone 复用的范围；结果分别写入 data_subset 和 clone_subsets。
 */
void ReplicatedBackend::calc_head_subsets(
  ObjectContextRef obc, SnapSet& snapset, const hobject_t& head,
  const pg_missing_t& missing,
  const hobject_t &last_backfill,
  interval_set<uint64_t>& data_subset,
  map<hobject_t, interval_set<uint64_t>>& clone_subsets,
  ObcLockManager &manager)
{
  // 初始假设需要发送整个 head，后续根据 dirty 区域和 clone overlap 缩小范围。
  dout(10) << "calc_head_subsets " << head
	   << " clone_overlap " << snapset.clone_overlap << dendl;

  uint64_t size = obc->obs.oi.size;
  // head 的完整数据范围为 [0, size)。
  if (size)
    data_subset.insert(0, size);

  ceph_assert(HAVE_FEATURE(parent->min_peer_features(), SERVER_OCTOPUS));
  const auto it = missing.get_items().find(head);
  ceph_assert(it != missing.get_items().end());
  // 只发送 peer 可能不一致的 dirty 区域，clean 区域无需重新传输。
  data_subset.intersection_of(it->second.clean_regions.get_dirty_regions());
  dout(10) << "calc_head_subsets " << head
	   << " data_subset " << data_subset << dendl;

  if (get_parent()->get_pool().allow_incomplete_clones()) {
    // 允许不完整 clone 时不使用 clone overlap 优化，保留 dirty 区域直接发送。
    dout(10) << __func__ << ": caching (was) enabled, skipping clone subsets" << dendl;
    return;
  }

  if (!cct->_conf->osd_recover_clone_overlap) {
    // 配置关闭 clone overlap 优化，直接发送 data_subset。
    dout(10) << "calc_head_subsets " << head << " -- osd_recover_clone_overlap disabled" << dendl;
    return;
  }


  interval_set<uint64_t> cloning;
  interval_set<uint64_t> prev;
  hobject_t c = head;
  if (size)
    prev.insert(0, size);

  // 从最新 clone 向较旧 clone 查找一个 peer 已有且可作为数据来源的 clone。
  for (int j=snapset.clones.size()-1; j>=0; j--) {
    c.snap = snapset.clones[j];
    // 沿 clone 链求交集，只保留从该 clone 到 head 一直未变化的范围。
    prev.intersection_of(snapset.clone_overlap[snapset.clones[j]]);
    if (!missing.is_missing(c) &&
	c < last_backfill &&
	get_parent()->try_lock_for_read(c, manager)) {
      dout(10) << "calc_head_subsets " << head << " has prev " << c
	       << " overlap " << prev << dendl;
      cloning = prev;
      break;
    }
    dout(10) << "calc_head_subsets " << head << " does not have prev " << c
	     << " overlap " << prev << dendl;
  }

  // 只保留同时属于 dirty 区域和 clone 共享区域的部分，避免复用 clean 数据。
  cloning.intersection_of(data_subset);
  if (cloning.empty()) {
    // 没有可复用且确实需要恢复的范围，继续按普通数据发送路径处理。
    dout(10) << "skipping clone, nothing needs to clone" << dendl;
    return;
  }

  if (cloning.num_intervals() > g_conf().get_val<uint64_t>("osd_recover_clone_overlap_limit")) {
    // 共享范围过于零散时优化收益不足，释放已持有的 clone 读锁并放弃复用。
    dout(10) << "skipping clone, too many holes" << dendl;
    get_parent()->release_locks(manager);
    clone_subsets.clear();
    cloning.clear();
    return;
  }

  // 保存 peer 可提供的 clone 范围，并从待发送范围中扣除它。
  clone_subsets[c] = cloning;
  data_subset.subtract(cloning);

  dout(10) << "calc_head_subsets " << head
	   << "  data_subset " << data_subset
	   << "  clone_subsets " << clone_subsets << dendl;
}

/**
 * 计算恢复 snap clone 时可由 peer 已有 clone 复用的数据范围；
 * 结果写入 clone_subsets，未找到可复用来源的范围保留在 data_subset 中。
 */
void ReplicatedBackend::calc_clone_subsets(
  SnapSet& snapset, const hobject_t& soid,
  const pg_missing_t& missing,
  const hobject_t &last_backfill,
  interval_set<uint64_t>& data_subset,
  map<hobject_t, interval_set<uint64_t>>& clone_subsets,
  ObcLockManager &manager)
{
  // 初始假设需要发送整个 clone，后续再用可复用范围扣除它。
  dout(10) << "calc_clone_subsets " << soid
	   << " clone_overlap " << snapset.clone_overlap << dendl;

  uint64_t size = snapset.clone_size[soid.snap];
  // clone 的完整数据范围为 [0, size)。
  if (size)
    data_subset.insert(0, size);

  if (get_parent()->get_pool().allow_incomplete_clones()) {
    // 允许不完整 clone 时不依赖 clone overlap，直接发送完整数据。
    dout(10) << __func__ << ": caching (was) enabled, skipping clone subsets" << dendl;
    return;
  }

  if (!cct->_conf->osd_recover_clone_overlap) {
    // 配置关闭 clone overlap 优化，保留完整 data_subset。
    dout(10) << "calc_clone_subsets " << soid << " -- osd_recover_clone_overlap disabled" << dendl;
    return;
  }

  unsigned i;
  for (i=0; i < snapset.clones.size(); i++)
    if (snapset.clones[i] == soid.snap)
      break;

  // 从当前 clone 向更旧的 clone 查找一个可复用的数据来源。
  interval_set<uint64_t> cloning;
  interval_set<uint64_t> prev;
  if (size)
    prev.insert(0, size);
  for (int j=i-1; j>=0; j--) {
    hobject_t c = soid;
    c.snap = snapset.clones[j];
    // 逐步计算当前 clone 与该旧 clone 之间仍可共享的范围。
    prev.intersection_of(snapset.clone_overlap[snapset.clones[j]]);
    if (!missing.is_missing(c) &&
	    c < last_backfill &&
	    get_parent()->try_lock_for_read(c, manager)) {
      dout(10) << "calc_clone_subsets " << soid << " has prev " << c
	       << " overlap " << prev << dendl;
      clone_subsets[c] = prev;
      // 记录该旧 clone 可提供的范围，并停止继续向更旧版本搜索。
      cloning.union_of(prev);
      break;
    }
    dout(10) << "calc_clone_subsets " << soid << " does not have prev " << c
	     << " overlap " << prev << dendl;
  }

  // 再从当前 clone 向更新的 clone 查找一个可复用的数据来源。
  interval_set<uint64_t> next;
  if (size)
    next.insert(0, size);
  for (unsigned j=i+1; j<snapset.clones.size(); j++) {
    hobject_t c = soid;
    c.snap = snapset.clones[j];
    // 逐步计算当前 clone 与该新 clone 之间仍可共享的范围。
    next.intersection_of(snapset.clone_overlap[snapset.clones[j-1]]);
    if (!missing.is_missing(c) &&
	c < last_backfill &&
	get_parent()->try_lock_for_read(c, manager)) {
      dout(10) << "calc_clone_subsets " << soid << " has next " << c
	       << " overlap " << next << dendl;
      clone_subsets[c] = next;
      // 合并该新 clone 可提供的范围，并停止继续向更新版本搜索。
      cloning.union_of(next);
      break;
    }
    dout(10) << "calc_clone_subsets " << soid << " does not have next " << c
	     << " overlap " << next << dendl;
  }

  if (cloning.num_intervals() > g_conf().get_val<uint64_t>("osd_recover_clone_overlap_limit")) {
    // 可复用范围过于零散时，优化收益不足；释放锁并退回完整发送。
    dout(10) << "skipping clone, too many holes" << dendl;
    get_parent()->release_locks(manager);
    clone_subsets.clear();
    cloning.clear();
  }


  // 从完整数据范围中扣除 peer 可复用的部分，留下实际需要发送的数据。
  data_subset.subtract(cloning);

  dout(10) << "calc_clone_subsets " << soid
	   << "  data_subset " << data_subset
	   << "  clone_subsets " << clone_subsets << dendl;
}

void ReplicatedBackend::prepare_pull(
  eversion_t v,
  const hobject_t& soid,
  ObjectContextRef headctx,
  RPGHandle *h)
{
  const auto missing_iter = get_parent()->get_local_missing().get_items().find(soid);
  ceph_assert(missing_iter != get_parent()->get_local_missing().get_items().end());
  eversion_t _v = missing_iter->second.need;
  ceph_assert(_v == v);
  const map<hobject_t, set<pg_shard_t>> &missing_loc(
    get_parent()->get_missing_loc_shards());
  const map<pg_shard_t, pg_missing_t > &peer_missing(
    get_parent()->get_shard_missing());
  map<hobject_t, set<pg_shard_t>>::const_iterator q = missing_loc.find(soid);
  ceph_assert(q != missing_loc.end());
  ceph_assert(!q->second.empty());

  // pick a pullee
  auto p = q->second.end();
  if (cct->_conf->osd_debug_feed_pullee >= 0) {
    for (auto it = q->second.begin(); it != q->second.end(); it++) {
      if (it->osd == cct->_conf->osd_debug_feed_pullee) {
        p = it;
        break;
      }
    }
  }
  if (p == q->second.end()) {
    // probably because user feed a wrong pullee
    p = q->second.begin();
    std::advance(p,
                 ceph::util::generate_random_number<int>(0,
							 q->second.size() - 1));
  }
  ceph_assert(get_osdmap()->is_up(p->osd));
  pg_shard_t fromshard = *p;

  dout(7) << "pull " << soid
	  << " v " << v
	  << " on osds " << q->second
	  << " from osd." << fromshard
	  << dendl;

  ceph_assert(peer_missing.count(fromshard));
  const pg_missing_t &pmissing = peer_missing.find(fromshard)->second;
  if (pmissing.is_missing(soid, v)) {
    ceph_assert(pmissing.get_items().find(soid)->second.have != v);
    dout(10) << "pulling soid " << soid << " from osd " << fromshard
	     << " at version " << pmissing.get_items().find(soid)->second.have
	     << " rather than at version " << v << dendl;
    v = pmissing.get_items().find(soid)->second.have;
    ceph_assert(get_parent()->get_log().get_log().objects.count(soid) &&
	   (get_parent()->get_log().get_log().objects.find(soid)->second->op ==
	    pg_log_entry_t::LOST_REVERT) &&
	   (get_parent()->get_log().get_log().objects.find(
	     soid)->second->reverting_to ==
	    v));
  }

  ObjectRecoveryInfo recovery_info;
  ObcLockManager lock_manager;

  if (soid.is_snap()) {
    ceph_assert(!get_parent()->get_local_missing().is_missing(soid.get_head()));
    ceph_assert(headctx);
    // check snapset
    SnapSetContext *ssc = headctx->ssc;
    ceph_assert(ssc);
    dout(10) << " snapset " << ssc->snapset << dendl;
    recovery_info.ss = ssc->snapset;
    calc_clone_subsets(
      ssc->snapset, soid, get_parent()->get_local_missing(),
      get_info().last_backfill,
      recovery_info.copy_subset,
      recovery_info.clone_subset,
      lock_manager);
    // FIXME: this may overestimate if we are pulling multiple clones in parallel...
    dout(10) << " pulling " << recovery_info << dendl;

    ceph_assert(ssc->snapset.clone_size.count(soid.snap));
    recovery_info.size = ssc->snapset.clone_size[soid.snap];
    recovery_info.object_exist = missing_iter->second.clean_regions.object_is_exist();
  } else {
    // pulling head or unversioned object.
    // always pull the whole thing.
    recovery_info.copy_subset.insert(0, (uint64_t)-1);
    ceph_assert(HAVE_FEATURE(parent->min_peer_features(), SERVER_OCTOPUS));
    recovery_info.copy_subset.intersection_of(missing_iter->second.clean_regions.get_dirty_regions());
    recovery_info.size = ((uint64_t)-1);
    recovery_info.object_exist = missing_iter->second.clean_regions.object_is_exist();
  }

  h->pulls[fromshard].push_back(PullOp());
  PullOp &op = h->pulls[fromshard].back();
  op.soid = soid;

  op.recovery_info = recovery_info;
  op.recovery_info.soid = soid;
  op.recovery_info.version = v;
  op.recovery_progress.data_complete = false;
  op.recovery_progress.omap_complete = !missing_iter->second.clean_regions.omap_is_dirty();
  op.recovery_progress.data_recovered_to = 0;
  op.recovery_progress.first = true;

  ceph_assert(!pulling.count(soid));
  pull_from_peer[fromshard].insert(soid);
  pull_info_t &pull_info = pulling[soid];
  pull_info.from = fromshard;
  pull_info.soid = soid;
  pull_info.head_ctx = headctx;
  pull_info.recovery_info = op.recovery_info;
  pull_info.recovery_progress = op.recovery_progress;
  pull_info.cache_dont_need = h->cache_dont_need;
  pull_info.lock_manager = std::move(lock_manager);
}

/**
 * intelligently push an object to a replica.  make use of existing
 * clones/heads and dup data ranges where possible.
 *
 * 为一个 peer 准备 PushOp：根据对象是 snap clone 还是 head，
 * 计算 peer 可复用的 clone 数据和 primary 必须发送的数据范围，尽量避免传输重复数据。
 */
int ReplicatedBackend::prep_push_to_replica(
  ObjectContextRef obc, const hobject_t& soid, pg_shard_t peer,
  PushOp *pop, bool cache_dont_need)
{
  // oi.version 是本次 recovery 要同步的对象版本；size 用于日志和数据范围计算。
  const object_info_t& oi = obc->obs.oi;
  uint64_t size = obc->obs.oi.size;

  dout(10) << __func__ << ": " << soid << " v" << oi.version
	   << " size " << size << " to osd." << peer << dendl;

  // clone_subsets 是 peer 可从其已有 clone 复用的范围；data_subset 是仍需从 primary 发送的范围。
  map<hobject_t, interval_set<uint64_t>> clone_subsets;
  interval_set<uint64_t> data_subset;

  ObcLockManager lock_manager;
  // 历史 snap clone：可尝试基于 peer 已有的相邻 clone 复用数据。
  if (soid.snap && soid.snap < CEPH_NOSNAP) {
    hobject_t head = soid;
    head.snap = CEPH_NOSNAP;

    // 复用 clone 数据需要本地 head 及其当前 SnapSet；head 缺失时只能完整发送 clone。
    if (get_parent()->get_local_missing().is_missing(head)) {
      dout(15) << "push_to_replica missing head " << head << ", pushing raw clone" << dendl;
      return prep_push(obc, soid, peer, pop, cache_dont_need);
    }

    SnapSetContext *ssc = obc->ssc;
    ceph_assert(ssc);
    dout(15) << "push_to_replica snapset is " << ssc->snapset << dendl;
    // 把 SnapSet 一并放入 recovery 信息，让 target 能按相同的快照关系重建对象。
    pop->recovery_info.ss = ssc->snapset;
    map<pg_shard_t, pg_missing_t>::const_iterator pm =
      get_parent()->get_shard_missing().find(peer);
    ceph_assert(pm != get_parent()->get_shard_missing().end());
    map<pg_shard_t, pg_info_t>::const_iterator pi =
      get_parent()->get_shard_info().find(peer);
    ceph_assert(pi != get_parent()->get_shard_info().end());
    // 结合 peer 的 missing 表和已完成 backfill 边界，计算需发送范围与可复用 clone 范围。
    calc_clone_subsets(
      ssc->snapset, soid,
      pm->second,
      pi->second.last_backfill,
      data_subset, clone_subsets,
      lock_manager);
  } else if (soid.snap == CEPH_NOSNAP) {
    // 当前 head（或无版本对象）也可能复用 peer 现有 clone 中的数据。
    SnapSetContext *ssc = obc->ssc;
    ceph_assert(ssc);
    dout(15) << "push_to_replica snapset is " << ssc->snapset << dendl;
    // 计算 head 的最小发送范围及可作为来源的 peer clone 范围。
    calc_head_subsets(
      obc,
      ssc->snapset, soid, get_parent()->get_shard_missing().find(peer)->second,
      get_parent()->get_shard_info().find(peer)->second.last_backfill,
      data_subset, clone_subsets,
      lock_manager);
  }

  // 将版本、数据范围、可复用 clone 和必要锁状态写入具体 PushOp。
  return prep_push(
    obc,
    soid,
    peer,
    oi.version,
    data_subset,
    clone_subsets,
    pop,
    cache_dont_need,
    std::move(lock_manager));
}

int ReplicatedBackend::prep_push(ObjectContextRef obc,
			     const hobject_t& soid, pg_shard_t peer,
			     PushOp *pop, bool cache_dont_need)
{
  interval_set<uint64_t> data_subset;
  if (obc->obs.oi.size)
    data_subset.insert(0, obc->obs.oi.size);
  map<hobject_t, interval_set<uint64_t>> clone_subsets;

  return prep_push(obc, soid, peer,
	    obc->obs.oi.version, data_subset, clone_subsets,
	    pop, cache_dont_need, ObcLockManager());
}

/**
 * 保存一次向 peer 推送对象所需的 recovery 状态，并根据当前进度构造 PushOp；
 * data_subset/clone_subsets 已由上层计算，具体对象内容由 build_push_op() 填充。
 */
int ReplicatedBackend::prep_push(
  ObjectContextRef obc,
  const hobject_t& soid, pg_shard_t peer,
  eversion_t version,
  interval_set<uint64_t> &data_subset,
  map<hobject_t, interval_set<uint64_t>>& clone_subsets,
  PushOp *pop,
  bool cache_dont_need,
  ObcLockManager &&lock_manager)
{
  // 标记该 peer 正在恢复此对象，并确认 peer 的 missing 表仍包含该对象。
  get_parent()->begin_peer_recover(peer, soid);
  const auto pmissing_iter = get_parent()->get_shard_missing().find(peer);
  const auto missing_iter = pmissing_iter->second.get_items().find(soid);
  ceph_assert(missing_iter != pmissing_iter->second.get_items().end());
  // 在 pushing 中建立该对象到 peer 的 backend 内部状态，后续分块 push 会继续更新它。
  push_info_t &push_info = pushing[soid][peer];
  push_info.obc = obc;
  // 保存对象大小、版本、属性、SnapSet 及已计算出的数据/clone 范围。
  push_info.recovery_info.size = obc->obs.oi.size;
  push_info.recovery_info.copy_subset = data_subset;
  push_info.recovery_info.clone_subset = clone_subsets;
  push_info.recovery_info.soid = soid;
  push_info.recovery_info.oi = obc->obs.oi;
  push_info.recovery_info.ss = pop->recovery_info.ss;
  push_info.recovery_info.version = version;
  push_info.recovery_info.object_exist = missing_iter->second.clean_regions.object_is_exist();
  // omap 脏标记决定 build_push_op 是否还需要读取并发送 omap 内容。
  push_info.recovery_progress.omap_complete = !missing_iter->second.clean_regions.omap_is_dirty();
  // 保留 clone 复用过程中获取的读锁，直到该对象 recovery 完成。
  push_info.lock_manager = std::move(lock_manager);

  // 按当前 recovery 进度读取对象并填充 PushOp，同时计算下一次进度。
  // push_info.recovery_progress 副本收到 PushOp 后，返回 PushReply，
  // primary 收到 PushReply -> handle_push_reply() 再次根据 push_info.recovery_progress 调用 build_push_op()
  ObjectRecoveryProgress new_progress;
  int r = build_push_op(push_info.recovery_info,
			push_info.recovery_progress,
			&new_progress,
			pop,
			&(push_info.stat), cache_dont_need);
  if (r < 0) {
    // 读取或构造 push 内容失败，交由上层执行 recovery 失败清理。
    return r;
  }
  // 保存增量 push 的新进度，后续继续请求时从这里接着读取。
  push_info.recovery_progress = new_progress;
  return 0;
}

/**
 * 将一次 PushOp 携带的数据提交到副本端的 ObjectStore 事务。
 *
 * 首块负责选择并初始化正式对象或临时恢复对象，后续各块向该对象写入数据、零区间、omap 和属性。
 * 收到最后一块后，函数将临时对象原子切换为正式对象，
 * 并调用 submit_push_complete() 处理 clone 数据和恢复收尾。
 */
void ReplicatedBackend::submit_push_data(
  const ObjectRecoveryInfo &recovery_info,
  bool first,
  bool complete,
  bool clear_omap,
  bool cache_dont_need,
  interval_set<uint64_t> &data_zeros,
  const interval_set<uint64_t> &intervals_included,
  bufferlist data_included,
  bufferlist omap_header,
  const map<string, bufferlist, less<>> &attrs,
  const map<string, bufferlist> &omap_entries,
  ObjectStore::Transaction *t)
{
  // 未完成的分块恢复先写入临时对象，避免客户端看到不完整的数据。
  hobject_t target_oid;
  if (first && complete) {
    // 单块即可完成时直接写入正式对象。
    target_oid = recovery_info.soid;
  } else {
    // 多块恢复或非首块都使用与对象版本绑定的临时对象。
    target_oid = get_parent()->get_temp_recovery_object(recovery_info.soid,
								recovery_info.version);
    if (first) {
      // 首次使用临时对象时，将其登记到临时 collection，便于恢复中断时清理。
      dout(10) << __func__ << ": Adding oid "
		       << target_oid << " in the temp collection" << dendl;
      add_temp_obj(target_oid);
    }
  }

  if (first) {
    if (!complete) {
      // 首块开始前重置临时对象，并设置预期大小和写入大小等分配提示。
      t->remove(coll, ghobject_t(target_oid));
      t->touch(coll, ghobject_t(target_oid));
      object_info_t oi(attrs.at(OI_ATTR));
      t->set_alloc_hint(coll, ghobject_t(target_oid),
		        oi.expected_object_size,
		        oi.expected_write_size,
		        oi.alloc_hint_flags);
      } else {
        // 单块完成或覆盖正式对象时，仅在原对象不存在时先创建对象。
        if (!recovery_info.object_exist) {
		  t->remove(coll, ghobject_t(target_oid));
          t->touch(coll, ghobject_t(target_oid));
          object_info_t oi(attrs.at(OI_ATTR));
          t->set_alloc_hint(coll, ghobject_t(target_oid),
                            oi.expected_object_size,
                            oi.expected_write_size,
	                            oi.alloc_hint_flags);
        }
        // 覆盖已有对象时清除旧属性，随后由本次 push 的 attrs 重新设置。
        t->rmattrs(coll, ghobject_t(target_oid));
        // 本次需要更新 omap 时先清空旧键，避免残留不属于当前版本的键。
        if (clear_omap)
          t->omap_clear(coll, ghobject_t(target_oid));
      }

    // 首块设置对象最终大小，并写入 omap header（如果消息携带）。
    t->truncate(coll, ghobject_t(target_oid), recovery_info.size);
    if (omap_header.length())
      t->omap_setheader(coll, ghobject_t(target_oid), omap_header);

    // 读取本地正式对象大小，用于 backfill 字节统计和 clone overlap 复制。
    struct stat st;
    int r = store->stat(ch, ghobject_t(recovery_info.soid), &st);
    if (get_parent()->pg_is_remote_backfilling()) {
      // 远端 backfill 需要把目标对象大小变化反映到 PG 的字节统计中。
      uint64_t size = 0;
      if (r == 0)
        size = st.st_size;
      // Don't need to do anything if object is still the same size
      if (size != recovery_info.oi.size) {
        get_parent()->pg_add_local_num_bytes((int64_t)recovery_info.oi.size - (int64_t)size);
        get_parent()->pg_add_num_bytes((int64_t)recovery_info.oi.size - (int64_t)size);
        dout(10) << __func__ << " " << recovery_info.soid
               << " backfill size " << recovery_info.oi.size
               << " previous size " << size
               << " net size " << recovery_info.oi.size - size
               << dendl;
      }
    }
    if (!complete) {
      // 某对象恢复的第一块，并且对象还没有恢复完成 -> first == true + complete == false
      // 此时副本不能直接把不完整数据写入正式对象，而是先创建临时对象。
      // 临时对象需要包含完整内容，因此要把“本地已有、primary 不会发送”的部分也复制过去。
      if (recovery_info.object_exist) {
        ceph_assert(r == 0);
        uint64_t local_size = std::min(recovery_info.size, (uint64_t)st.st_size);
        // local_intervals_included 表示副本本地正式对象已有的全部范围
        // local_intervals_excluded 计算本地范围和 primary 发送范围的交集。这些区间之后会由 primary 提供，因此本地副本不需要复制。
        interval_set<uint64_t> local_intervals_included, local_intervals_excluded;
        if (local_size) {
          // 以本地对象实际大小为上限，排除本次需要从 primary 恢复的范围。
          local_intervals_included.insert(0, local_size);
          // recovery_info.copy_subset 表示 primary 会通过 recovery push 发送给副本的范围
          local_intervals_excluded.intersection_of(local_intervals_included, recovery_info.copy_subset);
          // 从本地已有范围中扣除 primary 将发送的范围，剩下的就是可以直接复用的本地数据。
          local_intervals_included.subtract(local_intervals_excluded);
        }
       for (interval_set<uint64_t>::const_iterator q = local_intervals_included.begin();
          q != local_intervals_included.end();
         ++q) {
         // clone_range 复用本地已有内容，减少从 primary 传输的数据量。
         dout(15) << " clone_range " << recovery_info.soid << " "
                  << q.get_start() << "~" << q.get_len() << dendl;
        // 把这些本地区间复制到临时对象。
         t->clone_range(coll, ghobject_t(recovery_info.soid), ghobject_t(target_oid),
             q.get_start(), q.get_len(), q.get_start());
        }
      }
    }
  }
  uint64_t off = 0;
  // data_included 中的数据按区间紧密排列，off 表示当前区间在 bufferlist 中的偏移。
  uint32_t fadvise_flags = CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL;
  if (cache_dont_need)
    fadvise_flags |= CEPH_OSD_OP_FLAG_FADVISE_DONTNEED;
  // 对 fiemap 中未实际分配但被标记为 dirty 的范围显式写零，保持对象内容正确。
  if (data_zeros.size() > 0) {
    // 只处理对象恢复范围内的零区间，并扣除本次已经携带真实数据的区间。
    data_zeros.intersection_of(recovery_info.copy_subset);
    ceph_assert(intervals_included.subset_of(data_zeros));
    data_zeros.subtract(intervals_included);

    dout(20) << __func__ <<" recovering object " << recovery_info.soid
             << " copy_subset: " << recovery_info.copy_subset
             << " intervals_included: " << intervals_included
             << " data_zeros: " << data_zeros << dendl;

    // 对剩余空洞生成 zero 操作，后续与普通写操作一起提交。
    for (auto p = data_zeros.begin(); p != data_zeros.end(); ++p)
      t->zero(coll, ghobject_t(target_oid), p.get_start(), p.get_len());
  }
  // 将 data_included 中的紧密数据逐段写入目标对象的实际偏移。
  for (interval_set<uint64_t>::const_iterator p = intervals_included.begin();
       p != intervals_included.end();
       ++p) {
    bufferlist bit;
    bit.substr_of(data_included, off, p.get_len());
    t->write(coll, ghobject_t(target_oid),
	     p.get_start(), p.get_len(), bit, fadvise_flags);
    off += p.get_len();
  }

  // 写入本次 push 携带的 omap 键和值，以及对象属性。
  if (!omap_entries.empty())
    t->omap_setkeys(coll, ghobject_t(target_oid), omap_entries);
  if (!attrs.empty())
    t->setattrs(coll, ghobject_t(target_oid), attrs);

  if (complete) {
    if (!first) {
      // 后续块完成时，删除正式对象并把完整临时对象重命名为正式对象。
      dout(10) << __func__ << ": Removing oid "
               << target_oid << " from the temp collection" << dendl;
      clear_temp_obj(target_oid);
      t->remove(coll, ghobject_t(recovery_info.soid));
      t->collection_move_rename(coll, ghobject_t(target_oid),
                                coll, ghobject_t(recovery_info.soid));
    }

    // 登记对象恢复完成后的 clone 复制和 PG 状态更新操作。
    submit_push_complete(recovery_info, t);

  }
}

/**
 * 将对象恢复完成阶段可复用的 clone 数据追加到 ObjectStore 事务。
 *
 * clone_subset 按源对象和数据区间记录无需从 primary 传输、可以在本地复制的内容；
 * 本函数只生成对应的 clone_range() 操作，不负责提交事务。
 * 事务由调用方随后通过 queue_transaction() 统一提交。
 */
void ReplicatedBackend::submit_push_complete(
  const ObjectRecoveryInfo &recovery_info,
  ObjectStore::Transaction *t)
{
  // 外层遍历每个可提供数据的源 clone 对象。
  for (map<hobject_t, interval_set<uint64_t>>::const_iterator p =
	 recovery_info.clone_subset.begin();
       p != recovery_info.clone_subset.end();
       ++p) {
    // 同一个源对象可能对应多个不连续的可复用区间。
    for (interval_set<uint64_t>::const_iterator q = p->second.begin();
	 q != p->second.end();
	 ++q) {
      // 将源 clone 的指定区间复制到已恢复的正式对象相同偏移处。
      dout(15) << " clone_range " << p->first << " "
	       << q.get_start() << "~" << q.get_len() << dendl;
      t->clone_range(coll, ghobject_t(p->first), ghobject_t(recovery_info.soid),
		     q.get_start(), q.get_len(), q.get_start());
    }
  }
}

ObjectRecoveryInfo ReplicatedBackend::recalc_subsets(
  const ObjectRecoveryInfo& recovery_info,
  SnapSetContext *ssc,
  ObcLockManager &manager)
{
  if (!recovery_info.soid.snap || recovery_info.soid.snap >= CEPH_NOSNAP)
    return recovery_info;
  ObjectRecoveryInfo new_info = recovery_info;
  new_info.copy_subset.clear();
  new_info.clone_subset.clear();
  ceph_assert(ssc);
  get_parent()->release_locks(manager); // might already have locks
  calc_clone_subsets(
    ssc->snapset, new_info.soid, get_parent()->get_local_missing(),
    get_info().last_backfill,
    new_info.copy_subset, new_info.clone_subset,
    manager);
  return new_info;
}

/**
 * 在 primary 端处理副本返回的一块 PushOp（即先前 Pull 请求的响应）。
 *
 * 函数根据 pulling 中保存的恢复状态裁剪并接收本块数据，
 * 调用 submit_push_data() 将数据和对象元数据加入事务。
 * 对象尚未完整时填充下一轮 PullOp 并返回 true；
 * 对象完整时清理 pull 来源索引、登记本地恢复完成并返回 false。
 */
bool ReplicatedBackend::handle_pull_response(
  pg_shard_t from, const PushOp &pop, PullOp *response,
  list<pull_complete_info> *to_continue,
  ObjectStore::Transaction *t)
{
  // 先复制消息中本块数据及其区间；后续会裁掉本次恢复不需要的范围。
  interval_set<uint64_t> data_included = pop.data_included;
  bufferlist data;
  data = pop.data;
  dout(10) << "handle_pull_response "
	   << pop.recovery_info
	   << pop.after_progress
	   << " data.size() is " << data.length()
	   << " data_included: " << data_included
	   << dendl;
  // 如果 pop.version 是空版本，说明副本端没有该对象，它可能被回收或丢失了；primary 需要重新选择可用来源。
  if (pop.version == eversion_t()) {
    // replica doesn't have it!
    // 数据源副本没有该对象，结束本次 pull 并通知 PG 重新处理对象的可用来源。
    _failed_pull(from, pop.soid);
    return false;
  }

  const hobject_t &hoid = pop.soid;
  // 带数据的 PushOp 必须同时说明这些数据对应的对象区间，反之亦然。
  ceph_assert((data_included.empty() && data.length() == 0) ||
         (!data_included.empty() && data.length() > 0));

  auto piter = pulling.find(hoid);
  if (piter == pulling.end()) {
    // 本地已因 map 变化或失败清除了 pull 状态；迟到回复无需再写入。
    return false;
  }

  pull_info_t &pull_info = piter->second;
  if (pull_info.recovery_info.size == (uint64_t(-1))) {
    // primary 首次 pull 时可能未知对象大小；以数据源确认的范围收窄请求范围。
    pull_info.recovery_info.size = pop.recovery_info.size;
    pull_info.recovery_info.copy_subset.intersection_of(
      pop.recovery_info.copy_subset);
  }
  // If primary doesn't have object info and didn't know version
  // primary 本地缺失对象时可能连版本也未知，首个有效响应提供该版本。
  if (pull_info.recovery_info.version == eversion_t()) {
    pull_info.recovery_info.version = pop.version;
  }

  bool first = pull_info.recovery_progress.first;
  if (first) {
    // 首块建立对象上下文，并据此重新计算 clone 可复用范围和实际待接收区间。
    // attrs only reference the origin bufferlist (decode from
    // MOSDPGPush message) whose size is much greater than attrs in
    // recovery. If obc cache it (get_obc maybe cache the attr), this
    // causes the whole origin bufferlist would not be free until obc
    // is evicted from obc cache. So rebuild the bufferlists before
    // cache it.
    auto attrset = pop.attrset;
    for (auto& a : attrset) {
      a.second.rebuild();
    }
    pull_info.obc = get_parent()->get_obc(pull_info.recovery_info.soid, attrset);
    if (attrset.find(SS_ATTR) != attrset.end()) {
      bufferlist ssbv = attrset.at(SS_ATTR);
      SnapSet ss(ssbv);
      ceph_assert(!pull_info.obc->ssc->exists || ss.seq  == pull_info.obc->ssc->snapset.seq);
    }
    pull_info.recovery_info.oi = pull_info.obc->obs.oi;
    pull_info.recovery_info = recalc_subsets(
      pull_info.recovery_info,
      pull_info.obc->ssc,
      pull_info.lock_manager);
  }

  // if `first` is true, obc was just set above. Otherwise, we should be
  // able to reuse it.
  // 首块刚建立 obc，后续块复用同一个对象上下文和锁。
  ceph_assert(pull_info.obc);
  interval_set<uint64_t> usable_intervals;
  bufferlist usable_data;
  trim_pushed_data(pull_info.recovery_info.copy_subset,
		   data_included,
		   data,
		   &usable_intervals,
		   &usable_data);
  data_included = usable_intervals;
  data = std::move(usable_data);

  // 接受本块后推进内存中的恢复进度；该进度将被带入下一轮 PullOp。
  pull_info.recovery_progress = pop.after_progress;

  dout(10) << "new recovery_info " << pull_info.recovery_info
           << ", new progress " << pull_info.recovery_progress
           << dendl;
  // data_recovered_to 前进的范围可能含稀疏空洞，需在事务中显式写零。
  interval_set<uint64_t> data_zeros;
  uint64_t z_offset = pop.before_progress.data_recovered_to;
  uint64_t z_length = pop.after_progress.data_recovered_to - pop.before_progress.data_recovered_to;
  if (z_length)
    data_zeros.insert(z_offset, z_length);
  // 完整性同时取决于数据、omap 等恢复进度，而不只是本块数据长度。
  bool complete = pull_info.is_complete();
  // 首个携带 omap 的块到来前，目标对象需要先清除旧 omap 键。
  bool clear_omap = !pop.before_progress.omap_complete;

  // 仅把本块写操作追加到事务草稿；真正落盘由调用者稍后的 queue_transaction() 发起。
  submit_push_data(pull_info.recovery_info,
                  first,
                  complete,
                  clear_omap,
                  pull_info.cache_dont_need,
                  data_zeros,
                  data_included,
                  data,
                  pop.omap_header,
                  pop.attrset,
                  pop.omap_entries,
                  t);

  // 统计采用裁剪后的有效数据，而非消息中可能超出 copy_subset 的原始数据。
  pull_info.stat.num_keys_recovered += pop.omap_entries.size();
  pull_info.stat.num_bytes_recovered += data.length();
  get_parent()->get_logger()->inc(l_osd_rbytes, pop.omap_entries.size() + data.length());

  if (complete) {
    // 此处对象数据已加入事务，但 pull_info 保留到事务完成回调再释放锁和删除。
    pull_info.stat.num_objects_recovered++;
    // XXX: This could overcount if regular recovery is needed right after a repair
    if (get_parent()->pg_is_repair()) {
      pull_info.stat.num_objects_repaired++;
      get_parent()->inc_osd_stat_repaired();
    }
    // 立即撤销“正从 from 拉取”的反向索引，避免同一对象再被调度到该来源。
    clear_pull_from(piter);
    to_continue->push_back({hoid, pull_info.stat});
    // 通过同一事务记录 primary 的本地恢复状态；其 applied 回调会在事务执行后继续收尾。
    get_parent()->on_local_recover(
      hoid, pull_info.recovery_info, pull_info.obc, false, t);
    return false;
  } else {
    // 返回当前恢复信息和进度，由调用者封装为事务完成后发送的下一轮 PullOp。
    response->soid = pop.soid;
    response->recovery_info = pull_info.recovery_info;
    response->recovery_progress = pull_info.recovery_progress;
    return true;
  }
}

/**
 * 在副本端处理一个来自 primary 的 PushOp。
 *
 * 函数根据 PushOp 的前后恢复进度判断本次数据是否为首块、对象是否已经完整恢复，
 * 以及是否需要清理 omap，然后调用 submit_push_data() 将数据、属性和 omap 内容加入 ObjectStore 事务。
 * 对象的最后一块处理完成后，通过 on_local_recover() 更新 PG 的本地恢复状态。
 */
void ReplicatedBackend::handle_push(
  pg_shard_t from, const PushOp &pop, PushReplyOp *response,
  ObjectStore::Transaction *t, bool is_repair)
{
  // 输出本次 PushOp 携带的恢复信息和处理前后的进度，便于诊断分块恢复。
  dout(10) << "handle_push "
	   << pop.recovery_info
	   << pop.after_progress
	   << dendl;
  // 复制数据使用独立的 bufferlist，后续交由 submit_push_data() 写入事务。
  bufferlist data;
  data = pop.data;
  // before_progress.first 表示这是该对象恢复的第一块数据。
  bool first = pop.before_progress.first;
  // 数据和 omap 都完成时，整个对象的 push 才算完成。
  bool complete = pop.after_progress.data_complete &&
    pop.after_progress.omap_complete;
  // 如果此前 omap 尚未完成，本次写入需要按恢复数据处理并更新 omap。
  bool clear_omap = !pop.before_progress.omap_complete;
  interval_set<uint64_t> data_zeros;
  // 根据前后进度计算本次 push 覆盖的数据区间；区间中的空洞需要按零处理。
  uint64_t z_offset = pop.before_progress.data_recovered_to;
  uint64_t z_length = pop.after_progress.data_recovered_to - pop.before_progress.data_recovered_to;
  if (z_length)
    data_zeros.insert(z_offset, z_length);
  // 回复必须带回本次处理的对象标识，供 primary 匹配其 recovery 状态。
  response->soid = pop.recovery_info.soid;

  // 将本块的数据、属性、omap 和进度信息加入副本端事务。
  submit_push_data(pop.recovery_info,
		   first,
		   complete,
		   clear_omap,
		   true, // must be replicate
		   data_zeros,
		   pop.data_included,
		   data,
		   pop.omap_header,
		   pop.attrset,
		   pop.omap_entries,
		   t);

  if (complete) {
    // repair push 完成时额外累计 repaired 统计。
    if (is_repair) {
      get_parent()->inc_osd_stat_repaired();
      dout(20) << __func__ << " repair complete" << dendl;
    }
    // 对象已在副本端恢复完成，登记本地恢复完成回调并关联当前事务。
    get_parent()->on_local_recover(
      pop.recovery_info.soid,
      pop.recovery_info,
      ObjectContextRef(), // ok, is replica
      false,
      t);
  }
}

/**
 * 将待恢复对象按目标副本组织成 MOSDPGPush 消息并发送；
 * 单条消息会受对象数量和估算成本上限约束，避免一次 push 过大。
 */
void ReplicatedBackend::send_pushes(int prio, map<pg_shard_t, vector<PushOp> > &pushes)
{
  // pushes 按目标 PG shard 分组；每组对应一个远端副本。
  for (map<pg_shard_t, vector<PushOp> >::iterator i = pushes.begin();
       i != pushes.end();
       ++i) {
    // 获取到目标 OSD 的集群连接；连接暂不可用时，本轮不发送该组 push。
    ConnectionRef con = get_parent()->get_con_osd_cluster(
      i->first.osd,
      get_osdmap_epoch());
    if (!con)
      continue;
    // 同一目标副本的对象可能需要拆成多条消息。
    vector<PushOp>::iterator j = i->second.begin();
    while (j != i->second.end()) {
      uint64_t cost = 0;
      uint64_t pushes = 0;
      // 当前消息从本 PG shard 发出，携带 PG 标识、地图/peering epoch 和优先级。
      MOSDPGPush *msg = new MOSDPGPush();
      msg->from = get_parent()->whoami_shard();
      msg->pgid = get_parent()->primary_spg_t();
      msg->map_epoch = get_osdmap_epoch();
      msg->min_epoch = get_parent()->get_last_peering_reset_epoch();
      msg->set_priority(prio);
      msg->is_repair = get_parent()->pg_is_repair();
      // 累积对象，直到达到单消息的成本或对象数量上限。
      for (;
           (j != i->second.end() &&
	    cost < cct->_conf->osd_max_push_cost &&
	    pushes < cct->_conf->osd_max_push_objects) ;
	   ++j) {
	dout(20) << __func__ << ": sending push " << *j
		 << " to osd." << i->first << dendl;
	cost += j->cost(cct);
	pushes += 1;
	msg->pushes.push_back(*j);
      }
      msg->set_cost(cost);
      // 将这一批 push 交给消息层异步发送给目标副本。
      get_parent()->send_message_osd_cluster(msg, con);
    }
  }
}

/**
 * 将 primary 缺失对象的 pull 请求按目标副本组织成 MOSDPGPull 消息并发送；
 * 每个目标副本对应一条批量消息，消息成本由其中的请求对象计算。
 */
void ReplicatedBackend::send_pulls(int prio, map<pg_shard_t, vector<PullOp> > &pulls)
{
  // pulls 按提供对象的远端 PG shard 分组。
  for (map<pg_shard_t, vector<PullOp> >::iterator i = pulls.begin();
       i != pulls.end();
       ++i) {
    // 获取到目标副本 OSD 的集群连接；连接不可用时跳过本组请求。
    ConnectionRef con = get_parent()->get_con_osd_cluster(
      i->first.osd,
      get_osdmap_epoch());
    if (!con)
      continue;
    // 当前分组中的 PullOp 都发往同一个远端副本。
    dout(20) << __func__ << ": sending pulls " << i->second
	     << " to osd." << i->first << dendl;
    // 构造 pull 消息，标明来源 shard、优先级、PG 和消息适用的 epoch。
    MOSDPGPull *msg = new MOSDPGPull();
    msg->from = parent->whoami_shard();
    msg->set_priority(prio);
    msg->pgid = get_parent()->primary_spg_t();
    msg->map_epoch = get_osdmap_epoch();
    msg->min_epoch = get_parent()->get_last_peering_reset_epoch();
    // 将该目标副本的请求列表转移给消息对象，避免再次复制 vector 内容。
    msg->set_pulls(std::move(i->second));
    // 根据消息中的 pull 请求计算调度成本，供 OSD 队列节流使用。
    msg->compute_cost(cct);
    // 通过消息层异步发送给提供对象的远端副本。
    get_parent()->send_message_osd_cluster(msg, con);
  }
}

static bufferlist to_bufferlist(std::string_view in) {
  bufferlist bl;
  bl.append(in);
  return bl;
}

/**
 * 按当前 recovery 进度构造一个 PushOp：
 * 读取对象元数据、omap 和数据范围，将本次分块内容写入 out_op，并返回下一次继续读取所需的进度。
 * 一个对象可能需要多个 PushOp 才能完成，progress 用于保证分块读取可续接。
 */
int ReplicatedBackend::build_push_op(const ObjectRecoveryInfo &recovery_info,
				     const ObjectRecoveryProgress &progress,
				     ObjectRecoveryProgress *out_progress,
				     PushOp *out_op,
				     object_stat_sum_t *stat,
                                     bool cache_dont_need)
{
  // 调用者可以传入输出进度；未提供时使用局部对象，仍保证本次计算有独立结果。
  ObjectRecoveryProgress _new_progress;
  if (!out_progress)
    out_progress = &_new_progress;
  ObjectRecoveryProgress &new_progress = *out_progress;
  // 先继承上次进度，下面只推进本次实际读取到的位置。
  new_progress = progress;

  dout(7) << __func__ << " " << recovery_info.soid
	  << " v " << recovery_info.version
	  << " size " << recovery_info.size
	  << " recovery_info: " << recovery_info
          << dendl;

  eversion_t v  = recovery_info.version;
  object_info_t oi;
  if (progress.first) {
    // 第一个分块读取 omap header 和对象属性，并据此校验本地对象版本。
    int r = store->omap_get_header(ch, ghobject_t(recovery_info.soid), &out_op->omap_header);
    if (r < 0) {
      dout(1) << __func__ << " get omap header failed: " << cpp_strerror(-r) << dendl;
      return r;
    }
    r = store->getattrs(ch, ghobject_t(recovery_info.soid), out_op->attrset);
    if (r < 0) {
      dout(1) << __func__ << " getattrs failed: " << cpp_strerror(-r) << dendl;
      return r;
    }

    // 解码 OI_ATTR，既用于版本校验，也用于后面的完整对象 CRC 校验。
    try {
     oi.decode(out_op->attrset[OI_ATTR]);
    } catch (...) {
      dout(0) << __func__ << ": bad object_info_t: " << recovery_info.soid << dendl;
      return -EINVAL;
    }

    // 请求方未提供版本时采用本地版本；若版本不一致，说明恢复源已发生变化。
    if (v == eversion_t()) {
      v = oi.version;
    } else if (oi.version != v) {
      get_parent()->clog_error() << get_info().pgid << " push "
				 << recovery_info.soid << " v "
				 << recovery_info.version
				 << " failed because local copy is "
				 << oi.version;
      return -EINVAL;
    }

    new_progress.first = false;
  }
  // Once we provide the version subsequent requests will have it, so
  // at this point it must be known.
  ceph_assert(v != eversion_t());

  uint64_t available = cct->_conf->osd_recovery_max_chunk;
  if (!progress.omap_complete) {
    // omap 尚未读完时，先在本次 chunk 预算内继续读取 omap 条目。
    using omap_iter_seek_t = ObjectStore::omap_iter_seek_t;
    auto result = store->omap_iterate(
      ch,
      ghobject_t{recovery_info.soid},
      // try to seek as many keys-at-once as possible for the sake of performance.
      // note complexity should be logarithmic, so seek(n/2) + seek(n/2) is worse
      // than just seek(n).
      ObjectStore::omap_iter_seek_t{
        .seek_position = progress.omap_recovered_to,
        .seek_type = omap_iter_seek_t::LOWER_BOUND
      },
      [&available, &new_progress, &omap_entries=out_op->omap_entries,
       max_entries=cct->_conf->osd_recovery_max_omap_entries_per_chunk]
      (std::string_view key, std::string_view value) mutable {
        const auto num_new_bytes = key.size() + value.size();
        if (auto cur_num_entries = omap_entries.size(); cur_num_entries > 0) {
          if (max_entries > 0 && cur_num_entries >= max_entries) {
            // 达到单个 push 允许携带的 omap 条目数，保存当前 key 供下一块继续。
            new_progress.omap_recovered_to = key;
            return ObjectStore::omap_iter_ret_t::STOP; // want more!
	  }
          if (num_new_bytes >= available) {
            // 当前条目放不进剩余预算，保留其 key，下一块从这里继续。
            new_progress.omap_recovered_to = key;
            return ObjectStore::omap_iter_ret_t::STOP;
	  }
        }
        omap_entries.insert(make_pair(key, to_bufferlist(value)));
	        // omap 条目占用与对象数据共享同一个 recovery chunk 预算。
	        available -= std::min(available, num_new_bytes);
        return ObjectStore::omap_iter_ret_t::NEXT;
      });
    if (result < 0) {
      return -EIO;
    } else if (const auto more = static_cast<bool>(result); !more) {
      // 迭代器已到末尾，本次之后 omap 部分完成。
      new_progress.omap_complete = true;
    }
  }

  if (available > 0) {
    if (!recovery_info.copy_subset.empty()) {
      // 只从 copy_subset 中选择本次要发送的范围，并过滤掉本地未实际分配的区间。
      interval_set<uint64_t> copy_subset = recovery_info.copy_subset;
      map<uint64_t, uint64_t> m;
      int r = store->fiemap(ch, ghobject_t(recovery_info.soid), 0,
                            copy_subset.range_end(), m);
      if (r >= 0)  {
        interval_set<uint64_t> fiemap_included(std::move(m));
        copy_subset.intersection_of(fiemap_included);
      } else {
        // intersection of copy_subset and empty interval_set would be empty anyway
        copy_subset.clear();
      }

      // 从上次进度位置开始，最多选取 available 字节的连续/稀疏范围。
      out_op->data_included.span_of(copy_subset, progress.data_recovered_to,
                                    available);
      // 若没有可读范围或已到 copy_subset 末尾，直接把数据进度推进到末尾。
      if (out_op->data_included.empty() ||
          out_op->data_included.range_end() == copy_subset.range_end())
        new_progress.data_recovered_to = recovery_info.copy_subset.range_end();
      else
        new_progress.data_recovered_to = out_op->data_included.range_end();
    }
  } else {
    // omap 已耗尽本次 chunk 预算，不再读取对象数据。
    out_op->data_included.clear();
  }

  auto origin_size = out_op->data_included.size();
  bufferlist bit;
  // 按 data_included 指定的范围读取对象数据
  int r = store->readv(ch, ghobject_t(recovery_info.soid),
		       out_op->data_included, bit,
                       cache_dont_need ? CEPH_OSD_OP_FLAG_FADVISE_DONTNEED: 0);
  if (cct->_conf->osd_debug_random_push_read_error &&
        (rand() % (int)(cct->_conf->osd_debug_random_push_read_error * 100.0)) == 0) {
    dout(0) << __func__ << ": inject EIO " << recovery_info.soid << dendl;
    r = -EIO;
  }
  if (r < 0) {
    // 本地读取失败时，当前 PushOp 无法发送，交由上层 recovery 失败处理。
    return r;
  }
  if (out_op->data_included.size() != origin_size) {
    dout(10) << __func__ << " some extents get pruned "
             << out_op->data_included.size() << "/" << origin_size
             << dendl;
    new_progress.data_complete = true;
  }
  out_op->data.claim_append(bit);
  if (progress.first && !out_op->data_included.empty() &&
      out_op->data_included.begin().get_start() == 0 &&
      out_op->data.length() == oi.size && oi.is_data_digest()) {
    // 第一个分块恰好包含完整对象时，用 OI_ATTR 中的 digest 校验读取内容。
    uint32_t crc = out_op->data.crc32c(-1);
    if (oi.data_digest != crc) {
      dout(0) << __func__ << " " << coll << std::hex
                         << " full-object read crc 0x" << crc
                         << " != expected 0x" << oi.data_digest
                         << std::dec << " on " << recovery_info.soid << dendl;
      return -EIO;
    }
  }

  if (new_progress.is_complete(recovery_info)) {
    // 数据和 omap 都已完成，记录对象恢复/修复统计。
    new_progress.data_complete = true;
    if (stat) {
      stat->num_objects_recovered++;
      if (get_parent()->pg_is_repair())
        stat->num_objects_repaired++;
    }
  } else if (progress.first && progress.omap_complete) {
    // 数据仍未完成时，强制后续分块继续处理 omap 状态，避免遗漏元数据。
    // If omap is not changed, we need recovery omap when recovery cannot be completed once
    new_progress.omap_complete = false;
  }

  if (stat) {
    // 统计本次 push 实际读取的 omap 条目和数据字节数。
    stat->num_keys_recovered += out_op->omap_entries.size();
    stat->num_bytes_recovered += out_op->data.length();
    get_parent()->get_logger()->inc(l_osd_rbytes, out_op->omap_entries.size() + out_op->data.length());
  }

  get_parent()->get_logger()->inc(l_osd_push);
  get_parent()->get_logger()->inc(l_osd_push_outb, out_op->data.length());

  // 把版本、对象标识、前后进度和 recovery 信息写入待发送的 PushOp。
  out_op->version = v;
  out_op->soid = recovery_info.soid;
  out_op->recovery_info = recovery_info;
  out_op->after_progress = new_progress;
  out_op->before_progress = progress;
  return 0;
}

void ReplicatedBackend::prep_push_op_blank(const hobject_t& soid, PushOp *op)
{
  op->recovery_info.version = eversion_t();
  op->version = eversion_t();
  op->soid = soid;
}

/**
 * 处理一个副本对 PushOp 的回复，并推进该对象向该副本的恢复状态。
 *
 * primary 从 pushing[soid][peer] 取得此前保存的发送进度；
 * 若对象还有未发送的数据，则构造下一块 PushOp 并返回 true。否则释放该 peer 的恢复状态；
 * 当同一对象的所有目标 peer 都回复完成时，通知 PG 全局恢复成功或失败并返回 false。
 */
bool ReplicatedBackend::handle_push_reply(
  pg_shard_t peer, const PushReplyOp &op, PushOp *reply)
{
  // 回复只携带对象标识；primary 用它定位此前为该对象建立的 pushing 状态。
  const hobject_t &soid = op.soid;
  if (pushing.count(soid) == 0) {
    // 本地已无该对象的 push 状态，说明回复过期或重复，不能再继续发送。
    dout(10) << "huh, i wasn't pushing " << soid << " to osd." << peer
	     << ", or anybody else"
	     << dendl;
    return false;
  } else if (pushing[soid].count(peer) == 0) {
    // 该对象仍在恢复，但当前 peer 并不是等待回复的目标，同样忽略此回复。
    dout(10) << "huh, i wasn't pushing " << soid << " to osd." << peer
	     << dendl;
    return false;
  } else {
    // 取得这个对象到当前 peer 的恢复进度、统计信息和 clone 读锁。
    push_info_t *push_info = &pushing[soid][peer];
    // 同一对象发往多个 peer 时，任一 peer 的读取失败都会传播为对象级错误。
    bool error = pushing[soid].begin()->second.recovery_progress.error;

    if (!push_info->recovery_progress.data_complete && !error) {
      // 副本已确认上一块，primary 从保存的位置继续读取并构造下一块 PushOp。
      dout(10) << " pushing more from, "
	       << push_info->recovery_progress.data_recovered_to
	       << " of " << push_info->recovery_info.copy_subset << dendl;
      ObjectRecoveryProgress new_progress;
      int r = build_push_op(
	push_info->recovery_info,
	push_info->recovery_progress, &new_progress, reply,
	&(push_info->stat));
      // 上一块已成功发送后本地读取下一块仍可能失败；此时转入统一失败清理。
      if (r < 0) {
        dout(5) << __func__ << ": oid " << soid << " error " << r << dendl;

	error = true;
	goto done;
      }
      // 保存本次已构造的末尾进度，等待下一条 PushReply 再继续推进。
      push_info->recovery_progress = new_progress;
      // true 让 do_push_reply() 将 reply 加入下一轮 MOSDPGPush。
      return true;
    } else {
      // 该 peer 的数据已全部确认，或此前其他 peer 已使该对象进入错误状态。
done:
      // 成功时先通知 PG：当前 peer 已恢复该对象。
      if (!error)
	get_parent()->on_peer_recover( peer, soid, push_info->recovery_info);

      // 此 peer 不再需要 clone 范围读锁和恢复统计，清除对应 pushing 条目。
      get_parent()->release_locks(push_info->lock_manager);
      object_stat_sum_t stat = push_info->stat;
      eversion_t v = push_info->recovery_info.version;
      pushing[soid].erase(peer);
      push_info = nullptr;

      if (pushing[soid].empty()) {
	// 最后一个 peer 完成时，才将对象标记为全局恢复完成或全局恢复失败。
	if (!error)
	  get_parent()->on_global_recover(soid, stat, false);
	else
	  // 后续 pull/recovery 会重新选择来源或按 PG 的失败路径处理该对象。
	  get_parent()->on_failed_pull(
	    std::set<pg_shard_t>{ get_parent()->whoami_shard() },
	    soid,
	    v);
	pushing.erase(soid);
      } else {
	// 当前 peer 已完成，但仍在等待其他 peer 的回复。
	if (error)
	  // 将错误保存到剩余状态，避免其他 peer 完成后错误地报告全局成功。
	  pushing[soid].begin()->second.recovery_progress.error = true;
	dout(10) << "pushed " << soid << ", still waiting for push ack from "
		 << pushing[soid].size() << " others" << dendl;
      }
      return false;
    }
  }
}

/**
 * 根据 primary 发来的一个 PullOp，从本地对象构造对应的 PushOp 回复。
 *
 * 函数先确认对象仍存在并取得其大小；首次请求且大小尚未知时，用本地 stat 结果修正 recovery_info 的数据范围。
 * 随后调用 build_push_op() 按当前进度读取一个恢复分块；
 * 对象不存在或读取失败时生成空 PushOp，由请求方按失败路径处理。
 */
void ReplicatedBackend::handle_pull(pg_shard_t peer, PullOp &op, PushOp *reply)
{
  // PullOp 指定了请求方希望读取的对象。
  const hobject_t &soid = op.soid;
  // 先检查本地对象是否存在，并取得其实际大小。
  struct stat st;
  int r = store->stat(ch, ghobject_t(soid), &st);
  if (r != 0) {
    // 对象不可读时记录错误，并生成带空版本信息的回复，避免发送无效数据。
    get_parent()->clog_error() << get_info().pgid << " "
			       << peer << " tried to pull " << soid
			       << " but got " << cpp_strerror(-r);
    prep_push_op_blank(soid, reply);
  } else {
    // 使用 PullOp 中保存的恢复信息和进度继续构造本对象的下一块数据。
    ObjectRecoveryInfo &recovery_info = op.recovery_info;
    ObjectRecoveryProgress &progress = op.recovery_progress;
    if (progress.first && recovery_info.size == ((uint64_t)-1)) {
      // 首次请求可能不知道对象大小，此时以本地 stat 结果补全 recovery 范围。
      recovery_info.size = st.st_size;
      if (st.st_size) {
        // copy_subset 不能超出本地对象实际范围。
        interval_set<uint64_t> object_range;
        object_range.insert(0, st.st_size);
        recovery_info.copy_subset.intersection_of(object_range);
      } else {
        // 空对象没有可发送的数据范围。
        recovery_info.copy_subset.clear();
      }
      // pull 路径只处理 primary 请求的对象，不应携带 clone 复用范围。
      ceph_assert(recovery_info.clone_subset.empty());
    }

    // 按当前 progress 读取并填充一个 PushOp；此处不需要额外输出新 progress。
    r = build_push_op(recovery_info, progress, 0, reply);
    if (r < 0) {
      // 本地读取或构造失败时发送空回复，由 primary 进入失败处理。
      prep_push_op_blank(soid, reply);
    }
  }
}

/**
 * trim received data to remove what we don't want
 *
 * @param copy_subset intervals we want
 * @param data_included intervals we got
 * @param data_recieved data we got
 * @param intervals_usable intervals we want to keep
 * @param data_usable matching data we want to keep
 */
void ReplicatedBackend::trim_pushed_data(
  const interval_set<uint64_t> &copy_subset,
  const interval_set<uint64_t> &intervals_received,
  bufferlist data_received,
  interval_set<uint64_t> *intervals_usable,
  bufferlist *data_usable)
{
  if (intervals_received.subset_of(copy_subset)) {
    *intervals_usable = intervals_received;
    *data_usable = data_received;
    return;
  }

  intervals_usable->intersection_of(copy_subset,
				    intervals_received);

  uint64_t off = 0;
  for (interval_set<uint64_t>::const_iterator p = intervals_received.begin();
       p != intervals_received.end();
       ++p) {
    interval_set<uint64_t> x;
    x.insert(p.get_start(), p.get_len());
    x.intersection_of(copy_subset);
    for (interval_set<uint64_t>::const_iterator q = x.begin();
	 q != x.end();
	 ++q) {
      bufferlist sub;
      uint64_t data_off = off + (q.get_start() - p.get_start());
      sub.substr_of(data_received, data_off, q.get_len());
      data_usable->claim_append(sub);
    }
    off += p.get_len();
  }
}

void ReplicatedBackend::_failed_pull(pg_shard_t from, const hobject_t &soid)
{
  dout(20) << __func__ << ": " << soid << " from " << from << dendl;
  auto it = pulling.find(soid);
  ceph_assert(it != pulling.end());
  get_parent()->on_failed_pull(
    { from },
    soid,
    it->second.recovery_info.version);

  clear_pull(it);
}

void ReplicatedBackend::clear_pull_from(
  map<hobject_t, pull_info_t>::iterator piter)
{
  auto from = piter->second.from;
  pull_from_peer[from].erase(piter->second.soid);
  if (pull_from_peer[from].empty())
    pull_from_peer.erase(from);
}

void ReplicatedBackend::clear_pull(
  map<hobject_t, pull_info_t>::iterator piter,
  bool clear_pull_from_peer)
{
  if (clear_pull_from_peer) {
    clear_pull_from(piter);
  }
  get_parent()->release_locks(piter->second.lock_manager);
  pulling.erase(piter);
}

/**
 * 找出 acting 集合中缺少指定对象的 peer，并为每个 peer 准备一个 PushOp；
 * 返回本次准备的 push 数量，任一对象读取准备失败时回滚本轮已加入的 PushOp。
 */
int ReplicatedBackend::start_pushes(
  const hobject_t &soid,
  ObjectContextRef obc,
  RPGHandle *h)
{
  // 保存需要该对象的 peer；这里只保存 missing 表迭代器，具体 PushOp 稍后写入 h。
  list< map<pg_shard_t, pg_missing_t>::const_iterator > shards;

  dout(20) << __func__ << " soid " << soid << dendl;
  // 从 acting recovery/backfill 集合中筛选除本地 primary 外仍缺少 soid 的副本。
  ceph_assert(get_parent()->get_acting_recovery_backfill_shards().size() > 0);
  for (set<pg_shard_t>::iterator i =
	 get_parent()->get_acting_recovery_backfill_shards().begin();
       i != get_parent()->get_acting_recovery_backfill_shards().end();
       ++i) {
    if (*i == get_parent()->whoami_shard()) continue;
    pg_shard_t peer = *i;
    map<pg_shard_t, pg_missing_t>::const_iterator j =
      get_parent()->get_shard_missing().find(peer);
    ceph_assert(j != get_parent()->get_shard_missing().end());
    if (j->second.is_missing(soid)) {
      // 该 peer 的 missing 表包含对象，后续需要向它发送 push。
      shards.push_back(j);
    }
  }

  // 只有一个 peer 需要读取时才遵守“不缓存”提示；多 peer 读取时统一保留缓存结果。
  bool cache = shards.size() == 1 ? h->cache_dont_need : false;

  for (auto j : shards) {
    pg_shard_t peer = j->first;
    // 先在按 peer 分组的列表中占位，再由 prep_push_to_replica 填充 PushOp 内容。
    h->pushes[peer].push_back(PushOp());
    int r = prep_push_to_replica(obc, soid, peer,
	    &(h->pushes[peer].back()), cache);
    if (r < 0) {
      // 任一 peer 准备失败，撤销本轮已经为当前及之前 peer 加入的 PushOp。
      for (auto k : shards) {
	pg_shard_t p = k->first;
	dout(10) << __func__ << " clean up peer " << p << dendl;
	h->pushes[p].pop_back();
	if (p == peer) break;
      }
      return r;
    }
  }
  return shards.size();
}
