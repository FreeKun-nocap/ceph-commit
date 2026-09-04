// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include <errno.h>

#include <charconv>
#include <sstream>
#include <utility>

#include <boost/intrusive_ptr.hpp>
#include <boost/tuple/tuple.hpp>

#include "PrimaryLogPG.h"

#include "cls/cas/cls_cas_ops.h"
#include "common/CDC.h"
#include "common/debug.h"
#include "common/EventTrace.h"
#include "common/ceph_crypto.h"
#include "common/config.h"
#include "common/errno.h"
#include "common/perf_counters.h"
#include "common/scrub_types.h"
#include "include/compat.h"
#include "json_spirit/json_spirit_reader.h"
#include "json_spirit/json_spirit_value.h"
#include "messages/MCommandReply.h"
#include "messages/MOSDBackoff.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDPGBackfill.h"
#include "messages/MOSDPGBackfillRemove.h"
#include "messages/MOSDPGLog.h"
#include "messages/MOSDPGScan.h"
#include "messages/MOSDPGTrim.h"
#include "messages/MOSDPGUpdateLogMissing.h"
#include "messages/MOSDPGUpdateLogMissingReply.h"
#include "messages/MOSDRepScrub.h"
#include "messages/MOSDScrubReserve.h"
#include "mon/MonClient.h"
#include "objclass/objclass.h"
#include "osd/ClassHandler.h"
#include "osdc/Objecter.h"
#include "osd/scrubber/PrimaryLogScrub.h"
#include "osd/scrubber/ScrubStore.h"
#include "osd/scrubber/pg_scrubber.h"
#include "ECInject.h"

#include "OSD.h"
#include "OpRequest.h"
#include "PG.h"
#include "Session.h"

// required includes order:
#include "json_spirit/json_spirit_value.h"
#include "json_spirit/json_spirit_reader.h"
#include "include/ceph_assert.h"  // json_spirit clobbers it
#include "include/rados/rados_types.hpp"

#ifdef WITH_LTTNG
#include "tracing/osd.h"
#else
#define tracepoint(...)
#endif

#define dout_context cct
#define dout_subsys ceph_subsys_osd
#define DOUT_PREFIX_ARGS this, osd->whoami, get_osdmap()
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)

#include "osd_tracer.h"

MEMPOOL_DEFINE_OBJECT_FACTORY(PrimaryLogPG, replicatedpg, osd);

using std::less;
using std::list;
using std::ostream;
using std::pair;
using std::make_pair;
using std::make_unique;
using std::map;
using std::ostringstream;
using std::set;
using std::string;
using std::string_view;
using std::stringstream;
using std::unique_ptr;
using std::vector;

using ceph::bufferlist;
using ceph::bufferptr;
using ceph::Formatter;
using ceph::decode;
using ceph::decode_noclear;
using ceph::encode;
using ceph::encode_destructively;

using namespace ceph::osd::scheduler;
using TOPNSPC::common::cmd_getval;
using TOPNSPC::common::cmd_getval_or;

template <typename T>
static ostream& _prefix(std::ostream *_dout, T *pg) {
  return pg->gen_prefix(*_dout);
}

/**
 * The CopyCallback class defines an interface for completions to the
 * copy_start code. Users of the copy infrastructure must implement
 * one and give an instance of the class to start_copy.
 *
 * The implementer is responsible for making sure that the CopyCallback
 * can associate itself with the correct copy operation.
 */
class PrimaryLogPG::CopyCallback : public GenContext<CopyCallbackResults> {
protected:
  CopyCallback() {}
  /**
   * results.get<0>() is the return code: 0 for success; -ECANCELED if
   * the operation was cancelled by the local OSD; -errno for other issues.
   * results.get<1>() is a pointer to a CopyResults object, which you are
   * responsible for deleting.
   */
  void finish(CopyCallbackResults results_) override = 0;

public:
  /// Provide the final size of the copied object to the CopyCallback
  ~CopyCallback() override {}
};

template <typename T>
class PrimaryLogPG::BlessedGenContext : public GenContext<T> {
  PrimaryLogPGRef pg;
  unique_ptr<GenContext<T>> c;
  epoch_t e;
public:
  BlessedGenContext(PrimaryLogPG *pg, GenContext<T> *c, epoch_t e)
    : pg(pg), c(c), e(e) {}
  void finish(T t) override {
    std::scoped_lock locker{*pg};
    if (pg->pg_has_reset_since(e))
      c.reset();
    else
      c.release()->complete(t);
  }
  bool sync_finish(T t) {
    // we assume here all blessed/wrapped Contexts can complete synchronously.
    c.release()->complete(t);
    return true;
  }
};

GenContext<ThreadPool::TPHandle&> *PrimaryLogPG::bless_gencontext(
  GenContext<ThreadPool::TPHandle&> *c) {
  return new BlessedGenContext<ThreadPool::TPHandle&>(
    this, c, get_osdmap_epoch());
}

template <typename T>
class PrimaryLogPG::UnlockedBlessedGenContext : public GenContext<T> {
  PrimaryLogPGRef pg;
  unique_ptr<GenContext<T>> c;
  epoch_t e;
public:
  UnlockedBlessedGenContext(PrimaryLogPG *pg, GenContext<T> *c, epoch_t e)
    : pg(pg), c(c), e(e) {}
  void finish(T t) override {
    if (pg->pg_has_reset_since(e))
      c.reset();
    else
      c.release()->complete(t);
  }
  bool sync_finish(T t) {
    // we assume here all blessed/wrapped Contexts can complete synchronously.
    c.release()->complete(t);
    return true;
  }
};

GenContext<ThreadPool::TPHandle&> *PrimaryLogPG::bless_unlocked_gencontext(
  GenContext<ThreadPool::TPHandle&> *c) {
  return new UnlockedBlessedGenContext<ThreadPool::TPHandle&>(
    this, c, get_osdmap_epoch());
}

class PrimaryLogPG::BlessedContext : public Context {
  PrimaryLogPGRef pg;
  unique_ptr<Context> c;
  epoch_t e;
public:
  BlessedContext(PrimaryLogPG *pg, Context *c, epoch_t e)
    : pg(pg), c(c), e(e) {}
  void finish(int r) override {
    std::scoped_lock locker{*pg};
    if (pg->pg_has_reset_since(e))
      c.reset();
    else
      c.release()->complete(r);
  }
  bool sync_finish(int r) override {
    // we assume here all blessed/wrapped Contexts can complete synchronously.
    c.release()->complete(r);
    return true;
  }
};

Context *PrimaryLogPG::bless_context(Context *c) {
  return new BlessedContext(this, c, get_osdmap_epoch());
}

class PrimaryLogPG::C_PG_ObjectContext : public Context {
  PrimaryLogPGRef pg;
  ObjectContext *obc;
  public:
  C_PG_ObjectContext(PrimaryLogPG *p, ObjectContext *o) :
    pg(p), obc(o) {}
  void finish(int r) override {
    pg->object_context_destructor_callback(obc);
  }
};

struct OnReadComplete : public Context {
  PrimaryLogPG *pg;
  PrimaryLogPG::OpContext *opcontext;
  OnReadComplete(
    PrimaryLogPG *pg,
    PrimaryLogPG::OpContext *ctx) : pg(pg), opcontext(ctx) {}
  void finish(int r) override {
    opcontext->finish_read(pg);
  }
  ~OnReadComplete() override {}
};

class PrimaryLogPG::C_OSD_AppliedRecoveredObject : public Context {
  PrimaryLogPGRef pg;
  ObjectContextRef obc;
  public:
  C_OSD_AppliedRecoveredObject(PrimaryLogPG *p, ObjectContextRef o) :
    pg(p), obc(o) {}
  bool sync_finish(int r) override {
    pg->_applied_recovered_object(obc);
    return true;
  }
  void finish(int r) override {
    std::scoped_lock locker{*pg};
    pg->_applied_recovered_object(obc);
  }
};

class PrimaryLogPG::C_OSD_CommittedPushedObject : public Context {
  PrimaryLogPGRef pg;
  epoch_t epoch;
  eversion_t last_complete;
  public:
  C_OSD_CommittedPushedObject(
    PrimaryLogPG *p, epoch_t epoch, eversion_t lc) :
    pg(p), epoch(epoch), last_complete(lc) {
  }
  void finish(int r) override {
    pg->_committed_pushed_object(epoch, last_complete);
  }
};

class PrimaryLogPG::C_OSD_AppliedRecoveredObjectReplica : public Context {
  PrimaryLogPGRef pg;
  public:
  explicit C_OSD_AppliedRecoveredObjectReplica(PrimaryLogPG *p) :
    pg(p) {}
  bool sync_finish(int r) override {
    pg->_applied_recovered_object_replica();
    return true;
  }
  void finish(int r) override {
    std::scoped_lock locker{*pg};
    pg->_applied_recovered_object_replica();
  }
};

// OpContext
void PrimaryLogPG::OpContext::start_async_reads(PrimaryLogPG *pg)
{
  inflightreads = 1;
  list<pair<boost::tuple<uint64_t, uint64_t, unsigned>,
	    pair<bufferlist*, Context*> > > in;
  in.swap(pending_async_reads);
  // TODO: drop the converter
  list<pair<ec_align_t,
	    pair<bufferlist*, Context*> > > in_native;
  for (auto [align_tuple, ctx_pair] : in) {
    in_native.emplace_back(
      ec_align_t{
        align_tuple.get<0>(), align_tuple.get<1>(), align_tuple.get<2>()
      },
      std::move(ctx_pair));
  }
  pg->pgbackend->objects_read_async(
    obc->obs.oi.soid,
    obc->obs.oi.size,
    in_native,
    new OnReadComplete(pg, this), pg->get_pool().fast_read);
}
void PrimaryLogPG::OpContext::finish_read(PrimaryLogPG *pg)
{
  ceph_assert(inflightreads > 0);
  --inflightreads;
  if (async_reads_complete()) {
    ceph_assert(pg->in_progress_async_reads.size());
    ceph_assert(pg->in_progress_async_reads.front().second == this);
    pg->in_progress_async_reads.pop_front();

    // Restart the op context now that all reads have been
    // completed. Read failures will be handled by the op finisher
    pg->execute_ctx(this);
  }
}

class CopyFromCallback : public PrimaryLogPG::CopyCallback {
public:
  PrimaryLogPG::CopyResults *results = nullptr;
  PrimaryLogPG::OpContext *ctx;
  OSDOp &osd_op;
  uint32_t truncate_seq;
  uint64_t truncate_size;
  bool have_truncate = false;

  CopyFromCallback(PrimaryLogPG::OpContext *ctx, OSDOp &osd_op)
    : ctx(ctx), osd_op(osd_op) {
  }
  ~CopyFromCallback() override {}

  void finish(PrimaryLogPG::CopyCallbackResults results_) override {
    results = results_.get<1>();
    int r = results_.get<0>();

    // Only use truncate_{seq,size} from the original object if the client
    // did not sent us these parameters
    if (!have_truncate) {
      truncate_seq = results->truncate_seq;
      truncate_size = results->truncate_size;
    }

    // for finish_copyfrom
    ctx->user_at_version = results->user_version;

    if (r >= 0) {
      ctx->pg->execute_ctx(ctx);
    } else {
      if (r != -ECANCELED) { // on cancel just toss it out; client resends
	if (ctx->op)
	  ctx->pg->osd->reply_op_error(ctx->op, r);
      } else if (results->should_requeue) {
	if (ctx->op)
	  ctx->pg->requeue_op(ctx->op);
      }
      ctx->pg->close_op_ctx(ctx);
    }
  }

  bool is_temp_obj_used() {
    return results->started_temp_obj;
  }
  uint64_t get_data_size() {
    return results->object_size;
  }
  void set_truncate(uint32_t seq, uint64_t size) {
    truncate_seq = seq;
    truncate_size = size;
    have_truncate = true;
  }
};

struct CopyFromFinisher : public PrimaryLogPG::OpFinisher {
  CopyFromCallback *copy_from_callback;

  explicit CopyFromFinisher(CopyFromCallback *copy_from_callback)
    : copy_from_callback(copy_from_callback) {
  }

  int execute() override {
    // instance will be destructed after this method completes
    copy_from_callback->ctx->pg->finish_copyfrom(copy_from_callback);
    return 0;
  }
};

// ======================
// PGBackend::Listener

void PrimaryLogPG::on_local_recover(
  const hobject_t &hoid,
  const ObjectRecoveryInfo &_recovery_info,
  ObjectContextRef obc,
  bool is_delete,
  ObjectStore::Transaction *t
  )
{
  dout(10) << __func__ << ": " << hoid << dendl;

  ObjectRecoveryInfo recovery_info(_recovery_info);
  clear_object_snap_mapping(t, hoid);
  if (!is_delete && recovery_info.soid.is_snap()) {
    OSDriver::OSTransaction _t(osdriver.get_transaction(t));
    set<snapid_t> snaps;
    dout(20) << " snapset " << recovery_info.ss << dendl;
    auto p = recovery_info.ss.clone_snaps.find(hoid.snap);
    if (p != recovery_info.ss.clone_snaps.end()) {
      snaps.insert(p->second.begin(), p->second.end());
      dout(20) << " snaps " << snaps << dendl;
      snap_mapper.add_oid(
	recovery_info.soid,
	snaps,
	&_t);
    } else {
      derr << __func__ << " " << hoid << " had no clone_snaps" << dendl;
    }
  }
  if (!is_delete && recovery_state.get_pg_log().get_missing().is_missing(recovery_info.soid) &&
      recovery_state.get_pg_log().get_missing().get_items().find(recovery_info.soid)->second.need > recovery_info.version) {
    ceph_assert(is_primary());
    const pg_log_entry_t *latest = recovery_state.get_pg_log().get_log().objects.find(recovery_info.soid)->second;
    if (latest->op == pg_log_entry_t::LOST_REVERT &&
	latest->reverting_to == recovery_info.version) {
      dout(10) << " got old revert version " << recovery_info.version
	       << " for " << *latest << dendl;
      recovery_info.version = latest->version;
      // update the attr to the revert event version
      recovery_info.oi.prior_version = recovery_info.oi.version;
      recovery_info.oi.version = latest->version;
      bufferlist bl;
      encode(recovery_info.oi, bl,
	       get_osdmap()->get_features(CEPH_ENTITY_TYPE_OSD, nullptr));
      ceph_assert(!pool.info.is_erasure());
      t->setattr(coll, ghobject_t(recovery_info.soid), OI_ATTR, bl);
      if (obc)
	obc->attr_cache[OI_ATTR] = bl;
    }
  }

  // keep track of active pushes for scrub
  ++active_pushes;

  recovery_state.recover_got(
    recovery_info.soid,
    recovery_info.version,
    is_delete,
    *t);

  if (is_primary()) {
    if (!is_delete) {
      obc->obs.exists = true;

      bool got = obc->get_recovery_read();
      ceph_assert(got);

      ceph_assert(recovering.count(obc->obs.oi.soid));
      recovering[obc->obs.oi.soid] = obc;
      obc->obs.oi = recovery_info.oi;  // may have been updated above
    }

    t->register_on_applied(new C_OSD_AppliedRecoveredObject(this, obc));

    publish_stats_to_osd();
    release_backoffs(hoid);
    if (!is_unreadable_object(hoid)) {
      auto unreadable_object_entry = waiting_for_unreadable_object.find(hoid);
      if (unreadable_object_entry != waiting_for_unreadable_object.end()) {
	dout(20) << " kicking unreadable waiters on " << hoid << dendl;
	requeue_ops(unreadable_object_entry->second);
	finish_unreadable_object(unreadable_object_entry->first);
	waiting_for_unreadable_object.erase(unreadable_object_entry);
      }
    }
  } else {
    t->register_on_applied(
      new C_OSD_AppliedRecoveredObjectReplica(this));

  }

  t->register_on_commit(
    new C_OSD_CommittedPushedObject(
      this,
      get_osdmap_epoch(),
      info.last_complete));
}

/**
 * 处理一个对象已经完成全局 recovery 的通知。
 *
 * 该通知表示对象已经在 primary 以及所有需要恢复的 peer 上完成同步。
 * 函数更新 recovery 状态和统计，清理对象级 tracking，
 * 并通过 finish_recovery_op() 触发下一轮 recovery 调度；
 * 同时唤醒因对象 degraded 或 unreadable 而等待的请求。
 */
void PrimaryLogPG::on_global_recover(
  const hobject_t &soid,
  const object_stat_sum_t &stat_diff,
  bool is_delete)
{
  // 从 recovery 状态中移除该对象，并应用本次恢复产生的统计变化。
  recovery_state.object_recovered(soid, stat_diff);
  publish_stats_to_osd();
  dout(10) << "pushed " << soid << " to all replicas" << dendl;
  // recovering 中应存在该对象的全局恢复记录。
  auto i = recovering.find(soid);
  ceph_assert(i != recovering.end());

  if (i->second && i->second->rwstate.recovery_read_marker) {
    // 释放 recovery 期间持有的读标记，并重新排队此前被该标记阻塞的请求。
    ceph_assert(i->second);
    list<OpRequestRef> requeue_list;
    i->second->drop_recovery_read(&requeue_list);
    requeue_ops(requeue_list);
  }

  // 该对象不再占用 backfill 并发槽位。
  backfills_in_flight.erase(soid);

  // 清除对象级恢复记录，并减少当前 PG 的活动 recovery 数量。
  recovering.erase(i);
  // finish_recovery_op() 内部会重新 queue_recovery()，让 OSD 调度下一轮工作。
  finish_recovery_op(soid);
  // 恢复完成后解除针对该对象的 backoff 限制。
  release_backoffs(soid);
  // 恢复对象后，唤醒等待 PG 变为非 degraded 的请求。
  auto degraded_object_entry = waiting_for_degraded_object.find(soid);
  if (degraded_object_entry != waiting_for_degraded_object.end()) {
    dout(20) << " kicking degraded waiters on " << soid << dendl;
    requeue_ops(degraded_object_entry->second);
    waiting_for_degraded_object.erase(degraded_object_entry);
  }
  // 对象恢复后，唤醒此前因对象不可读而等待的请求。
  auto unreadable_object_entry = waiting_for_unreadable_object.find(soid);
  if (unreadable_object_entry != waiting_for_unreadable_object.end()) {
    dout(20) << " kicking unreadable waiters on " << soid << dendl;
    requeue_ops(unreadable_object_entry->second);
    waiting_for_unreadable_object.erase(unreadable_object_entry);
  }
  // 清理对象级 degraded/unreadable 等待状态。
  finish_degraded_object(soid);
  finish_unreadable_object(soid);
}

void PrimaryLogPG::schedule_recovery_work(
  GenContext<ThreadPool::TPHandle&> *c,
  uint64_t cost)
{
  osd->queue_recovery_context(
    this, c, cost,
    recovery_state.get_recovery_op_priority());
}

common::intrusive_timer &PrimaryLogPG::get_pg_timer()
{
  return osd->pg_timer;
}

void PrimaryLogPG::clear_repop_obc(
  const vector<pg_log_entry_t> &logv,
  ObjectStore::Transaction &t)
{
  for (auto &&e: logv) {
    /* Have to blast all clones, they share a snapset */
    object_contexts.clear_range(
      e.soid.get_object_boundary(), e.soid.get_head());
    ceph_assert(
      snapset_contexts.find(e.soid.get_head()) ==
      snapset_contexts.end());
  }
}

bool PrimaryLogPG::should_send_op(
  pg_shard_t peer,
  const hobject_t &hoid) {
  if (peer == get_primary())
    return true;
  bool should_send =
      hoid.pool != (int64_t)info.pgid.pool() ||
      hoid <= last_backfill_started ||
      hoid <= recovery_state.get_peer_info(peer).last_backfill;
  if (!should_send) {
    ceph_assert(is_backfill_target(peer));
    dout(10) << __func__ << " issue_repop shipping empty opt to osd." << peer
             << ", object " << hoid
             << " beyond std::max(last_backfill_started "
             << ", peer_info[peer].last_backfill "
             << recovery_state.get_peer_info(peer).last_backfill
	     << ")" << dendl;
    return should_send;
  }
  if (is_async_recovery_target(peer) &&
      recovery_state.get_peer_missing(peer).is_missing(hoid)) {
    should_send = false;
    dout(10) << __func__ << " issue_repop shipping empty opt to osd." << peer
             << ", object " << hoid
             << " which is pending recovery in async_recovery_targets" << dendl;
  }
  return should_send;
}


ConnectionRef PrimaryLogPG::get_con_osd_cluster(
  int peer, epoch_t from_epoch)
{
  return osd->get_con_osd_cluster(peer, from_epoch);
}

PerfCounters *PrimaryLogPG::get_logger()
{
  return osd->logger;
}


// ====================
// missing objects

bool PrimaryLogPG::is_missing_object(const hobject_t& soid) const
{
  return recovery_state.get_pg_log().get_missing().get_items().count(soid);
}

void PrimaryLogPG::maybe_kick_recovery(
  const hobject_t &soid)
{
  eversion_t v;
  bool work_started = false;
  if (!recovery_state.get_missing_loc().needs_recovery(soid, &v))
    return;

  map<hobject_t, ObjectContextRef>::const_iterator p = recovering.find(soid);
  if (p != recovering.end()) {
    dout(7) << "object " << soid << " v " << v << ", already recovering." << dendl;
  } else if (recovery_state.get_missing_loc().is_unfound(soid)) {
    dout(7) << "object " << soid << " v " << v << ", is unfound." << dendl;
  } else {
    dout(7) << "object " << soid << " v " << v << ", recovering." << dendl;
    PGBackend::RecoveryHandle *h = pgbackend->open_recovery_op();
    if (is_missing_object(soid)) {
      recover_missing(soid, v, CEPH_MSG_PRIO_HIGH, h);
    } else if (recovery_state.get_missing_loc().is_deleted(soid)) {
      prep_object_replica_deletes(soid, v, h, &work_started);
    } else {
      prep_object_replica_pushes(soid, v, h, &work_started);
    }
    pgbackend->run_recovery_op(h, CEPH_MSG_PRIO_HIGH);
  }
}

void PrimaryLogPG::wait_for_unreadable_object(
  const hobject_t& soid, OpRequestRef op)
{
  ceph_assert(is_unreadable_object(soid));
  maybe_kick_recovery(soid);
  waiting_for_unreadable_object[soid].push_back(op);
  op->mark_delayed("waiting for missing object");
  osd->logger->inc(l_osd_op_delayed_unreadable);
}

bool PrimaryLogPG::is_degraded_or_backfilling_object(const hobject_t& soid)
{
  /* The conditions below may clear (on_local_recover, before we queue
   * the transaction) before we actually requeue the degraded waiters
   * in on_global_recover after the transaction completes.
   */
  if (waiting_for_degraded_object.count(soid))
    return true;
  if (recovery_state.get_pg_log().get_missing().get_items().count(soid))
    return true;
  ceph_assert(!get_acting_recovery_backfill().empty());
  for (set<pg_shard_t>::iterator i = get_acting_recovery_backfill().begin();
       i != get_acting_recovery_backfill().end();
       ++i) {
    if (*i == get_primary()) continue;
    pg_shard_t peer = *i;
    auto peer_missing_entry = recovery_state.get_peer_missing().find(peer);
    // If an object is missing on an async_recovery_target, return false.
    // This will not block the op and the object is async recovered later.
    if (peer_missing_entry != recovery_state.get_peer_missing().end() &&
	peer_missing_entry->second.get_items().count(soid)) {
      if (is_async_recovery_target(peer))
	continue;
      else
	return true;
    }
    // Object is degraded if after last_backfill AND
    // we are backfilling it
    if (is_backfill_target(peer) &&
        recovery_state.get_peer_info(peer).last_backfill <= soid &&
	last_backfill_started >= soid &&
	backfills_in_flight.count(soid))
      return true;
  }
  return false;
}

bool PrimaryLogPG::is_degraded_on_async_recovery_target(const hobject_t& soid)
{
  for (auto &i: get_async_recovery_targets()) {
    auto peer_missing_entry = recovery_state.get_peer_missing().find(i);
    if (peer_missing_entry != recovery_state.get_peer_missing().end() &&
        peer_missing_entry->second.get_items().count(soid)) {
      dout(30) << __func__ << " " << soid << dendl;
      return true;
    }
  }
  return false;
}

void PrimaryLogPG::wait_for_degraded_object(const hobject_t& soid, OpRequestRef op)
{
  ceph_assert(is_degraded_or_backfilling_object(soid) || is_degraded_on_async_recovery_target(soid));

  maybe_kick_recovery(soid);
  waiting_for_degraded_object[soid].push_back(op);
  op->mark_delayed("waiting for degraded object");
  osd->logger->inc(l_osd_op_delayed_degraded);
}

void PrimaryLogPG::block_write_on_full_cache(
  const hobject_t& _oid, OpRequestRef op)
{
  const hobject_t oid = _oid.get_head();
  dout(20) << __func__ << ": blocking object " << oid
	   << " on full cache" << dendl;
  objects_blocked_on_cache_full.insert(oid);
  waiting_for_cache_not_full.push_back(op);
  op->mark_delayed("waiting for cache not full");
}

void PrimaryLogPG::block_for_clean(
  const hobject_t& oid, OpRequestRef op)
{
  dout(20) << __func__ << ": blocking object " << oid
	   << " on primary repair" << dendl;
  waiting_for_clean_to_primary_repair.push_back(op);
  op->mark_delayed("waiting for clean to repair");
}

void PrimaryLogPG::block_write_on_snap_rollback(
  const hobject_t& oid, ObjectContextRef obc, OpRequestRef op)
{
  dout(20) << __func__ << ": blocking object " << oid.get_head()
	   << " on snap promotion " << obc->obs.oi.soid << dendl;
  // otherwise, we'd have blocked in do_op
  ceph_assert(oid.is_head());
  ceph_assert(objects_blocked_on_snap_promotion.count(oid) == 0);
  /*
   * We block the head object here.
   *
   * Let's assume that there is racing read When the head object is being rollbacked.
   * Since the two different ops can trigger promote_object() with the same source,
   * infinite loop happens by canceling ops each other.
   * To avoid this, we block the head object during rollback.
   * So, the racing read will be blocked until the rollback is completed.
   * see also: https://tracker.ceph.com/issues/49726
   */
  ObjectContextRef head_obc = get_object_context(oid, false);
  head_obc->start_block();
  objects_blocked_on_snap_promotion[oid] = obc;
  wait_for_blocked_object(obc->obs.oi.soid, op);
}

void PrimaryLogPG::block_write_on_degraded_snap(
  const hobject_t& snap, OpRequestRef op)
{
  dout(20) << __func__ << ": blocking object " << snap.get_head()
	   << " on degraded snap " << snap << dendl;
  // otherwise, we'd have blocked in do_op
  ceph_assert(objects_blocked_on_degraded_snap.count(snap.get_head()) == 0);
  objects_blocked_on_degraded_snap[snap.get_head()] = snap.snap;
  wait_for_degraded_object(snap, op);
}

void PrimaryLogPG::block_write_on_unreadable_snap(
  const hobject_t& snap, OpRequestRef op)
{
  dout(20) << __func__ << ": blocking object " << snap.get_head()
	   << " on unreadable snap " << snap << dendl;
  // otherwise, we'd have blocked in do_op
  ceph_assert(objects_blocked_on_unreadable_snap.count(snap.get_head()) == 0);
  objects_blocked_on_unreadable_snap[snap.get_head()] = snap.snap;
  // the op must be queued before calling block_write_on_unreadable_snap
  ceph_assert(waiting_for_unreadable_object.count(snap) == 1);
}

bool PrimaryLogPG::maybe_await_blocked_head(
  const hobject_t &hoid,
  OpRequestRef op)
{
  ObjectContextRef obc;
  obc = object_contexts.lookup(hoid.get_head());
  if (obc) {
    if (obc->is_blocked()) {
      wait_for_blocked_object(obc->obs.oi.soid, op);
      return true;
    } else {
      return false;
    }
  }
  return false;
}

void PrimaryLogPG::wait_for_blocked_object(const hobject_t& soid, OpRequestRef op)
{
  dout(10) << __func__ << " " << soid << " " << *op->get_req() << dendl;
  waiting_for_blocked_object[soid].push_back(op);
  op->mark_delayed("waiting for blocked object");
}

void PrimaryLogPG::maybe_force_recovery()
{
  // no force if not in degraded/recovery/backfill states
  if (!is_degraded() &&
      !state_test(PG_STATE_RECOVERING |
                  PG_STATE_RECOVERY_WAIT |
		  PG_STATE_BACKFILLING |
		  PG_STATE_BACKFILL_WAIT |
		  PG_STATE_BACKFILL_TOOFULL))
    return;

  if (recovery_state.get_pg_log().get_log().approx_size() <
      cct->_conf->osd_max_pg_log_entries *
        cct->_conf->osd_force_recovery_pg_log_entries_factor)
    return;

  // find the oldest missing object
  eversion_t min_version = recovery_state.get_pg_log().get_log().head;
  hobject_t soid;
  if (!recovery_state.get_pg_log().get_missing().get_rmissing().empty()) {
    min_version = recovery_state.get_pg_log().get_missing().get_rmissing().begin()->first;
    soid = recovery_state.get_pg_log().get_missing().get_rmissing().begin()->second;
  }
  ceph_assert(!get_acting_recovery_backfill().empty());
  for (set<pg_shard_t>::iterator it = get_acting_recovery_backfill().begin();
       it != get_acting_recovery_backfill().end();
       ++it) {
    if (*it == get_primary()) continue;
    pg_shard_t peer = *it;
    auto it_missing = recovery_state.get_peer_missing().find(peer);
    if (it_missing != recovery_state.get_peer_missing().end() &&
	!it_missing->second.get_rmissing().empty()) {
      const auto& min_obj = recovery_state.get_peer_missing(peer).get_rmissing().begin();
      dout(20) << __func__ << " peer " << peer << " min_version " << min_obj->first
               << " oid " << min_obj->second << dendl;
      if (min_version > min_obj->first) {
        min_version = min_obj->first;
        soid = min_obj->second;
      }
    }
  }

  // recover it
  if (soid != hobject_t())
    maybe_kick_recovery(soid);
}

bool PrimaryLogPG::check_laggy(OpRequestRef& op)
{
  ceph_assert(HAVE_FEATURE(recovery_state.get_min_upacting_features(),
		      SERVER_OCTOPUS));
  if (state_test(PG_STATE_WAIT)) {
    dout(10) << __func__ << " PG is WAIT state" << dendl;
  } else if (!state_test(PG_STATE_LAGGY)) {
    auto mnow = osd->get_mnow();
    auto ru = recovery_state.get_readable_until();
    if (mnow <= ru) {
      // not laggy
      return true;
    }
    dout(10) << __func__
	     << " mnow " << mnow
	     << " > readable_until " << ru << dendl;

    if (!is_primary()) {
      osd->reply_op_error(op, -EAGAIN);
      return false;
    }

    // go to laggy state
    state_set(PG_STATE_LAGGY);
    publish_stats_to_osd();
  }
  dout(10) << __func__ << " not readable" << dendl;
  waiting_for_readable.push_back(op);
  op->mark_delayed("waiting for readable");
  return false;
}

bool PrimaryLogPG::check_laggy_requeue(OpRequestRef& op)
{
  ceph_assert(HAVE_FEATURE(recovery_state.get_min_upacting_features(),
		      SERVER_OCTOPUS));
  if (!state_test(PG_STATE_WAIT) && !state_test(PG_STATE_LAGGY)) {
    return true; // not laggy
  }
  dout(10) << __func__ << " not readable" << dendl;
  waiting_for_readable.push_front(op);
  op->mark_delayed("waiting for readable");
  return false;
}

void PrimaryLogPG::recheck_readable()
{
  if (!is_wait() && !is_laggy()) {
    dout(20) << __func__ << " wasn't wait or laggy" << dendl;
    return;
  }
  auto mnow = osd->get_mnow();
  bool pub = false;
  if (is_wait()) {
    auto prior_readable_until_ub = recovery_state.get_prior_readable_until_ub();
    if (mnow < prior_readable_until_ub) {
      dout(10) << __func__ << " still wait (mnow " << mnow
	       << " < prior_readable_until_ub " << prior_readable_until_ub
	       << ")" << dendl;
    } else {
      dout(10) << __func__ << " no longer wait (mnow " << mnow
	       << " >= prior_readable_until_ub " << prior_readable_until_ub
	       << ")" << dendl;
      state_clear(PG_STATE_WAIT);
      recovery_state.clear_prior_readable_until_ub();
      pub = true;
    }
  }
  if (is_laggy()) {
    auto ru = recovery_state.get_readable_until();
    if (ru == ceph::signedspan::zero()) {
      dout(10) << __func__ << " still laggy (mnow " << mnow
	       << ", readable_until zero)" << dendl;
    } else if (mnow >= ru) {
      dout(10) << __func__ << " still laggy (mnow " << mnow
	       << " >= readable_until " << ru << ")" << dendl;
    } else {
      dout(10) << __func__ << " no longer laggy (mnow " << mnow
	       << " < readable_until " << ru << ")" << dendl;
      state_clear(PG_STATE_LAGGY);
      pub = true;
    }
  }
  if (pub) {
    publish_stats_to_osd();
  }
  if (!is_laggy() && !is_wait()) {
    requeue_ops(waiting_for_readable);
  }
}

bool PrimaryLogPG::pgls_filter(const PGLSFilter& filter, const hobject_t& sobj)
{
  bufferlist bl;

  // If filter has expressed an interest in an xattr, load it.
  if (!filter.get_xattr().empty()) {
    int ret = pgbackend->objects_get_attr(
      sobj,
      filter.get_xattr(),
      &bl);
    dout(0) << "getattr (sobj=" << sobj << ", attr=" << filter.get_xattr() << ") returned " << ret << dendl;
    if (ret < 0) {
      if (ret != -ENODATA || filter.reject_empty_xattr()) {
        return false;
      }
    }
  }

  return filter.filter(sobj, bl);
}

std::pair<int, std::unique_ptr<const PGLSFilter>>
PrimaryLogPG::get_pgls_filter(bufferlist::const_iterator& iter)
{
  string type;
  // storing non-const PGLSFilter for the sake of ::init()
  std::unique_ptr<PGLSFilter> filter;

  try {
    decode(type, iter);
  }
  catch (ceph::buffer::error& e) {
    return { -EINVAL, nullptr };
  }

  if (type.compare("plain") == 0) {
    filter = std::make_unique<PGLSPlainFilter>();
  } else {
    std::size_t dot = type.find('.');
    if (dot == std::string::npos || dot == 0 || dot == type.size() - 1) {
      return { -EINVAL, nullptr };
    }

    const std::string class_name = type.substr(0, dot);
    const std::string filter_name = type.substr(dot + 1);
    ClassHandler::ClassData *cls = NULL;
    int r = ClassHandler::get_instance().open_class(class_name, &cls);
    if (r != 0) {
      derr << "Error opening class '" << class_name << "': "
           << cpp_strerror(r) << dendl;
      if (r != -EPERM) // propagate permission error
        r = -EINVAL;
      return { r, nullptr };
    } else {
      ceph_assert(cls);
    }

    ClassHandler::ClassFilter *class_filter = cls->get_filter(filter_name);
    if (class_filter == NULL) {
      derr << "Error finding filter '" << filter_name << "' in class "
           << class_name << dendl;
      return { -EINVAL, nullptr };
    }
    filter.reset(class_filter->fn());
    if (!filter) {
      // Object classes are obliged to return us something, but let's
      // give an error rather than asserting out.
      derr << "Buggy class " << class_name << " failed to construct "
              "filter " << filter_name << dendl;
      return { -EINVAL, nullptr };
    }
  }

  ceph_assert(filter);
  int r = filter->init(iter);
  if (r < 0) {
    derr << "Error initializing filter " << type << ": "
         << cpp_strerror(r) << dendl;
    return { -EINVAL, nullptr };
  } else {
    // Successfully constructed and initialized, return it.
    return std::make_pair(0, std::move(filter));
  }
}


// ==========================================================

void PrimaryLogPG::do_command(
  string_view orig_prefix,
  const cmdmap_t& cmdmap,
  const bufferlist& idata,
  asok_finisher on_finish)
{
  string format;
  cmd_getval(cmdmap, "format", format);
  auto f(Formatter::create_unique(format, "json-pretty", "json-pretty"));
  int ret = 0;
  stringstream ss;   // stderr error message stream
  bufferlist outbl;  // if empty at end, we'll dump formatter as output

  // get final prefix:
  // - ceph pg <pgid> foo -> prefix=pg, cmd=foo
  // - ceph tell <pgid> foo -> prefix=foo
  string prefix(orig_prefix);
  string command;
  cmd_getval(cmdmap, "cmd", command);
  if (command.size()) {
    prefix = command;
  }

  if (prefix == "query") {
    f->open_object_section("pg");
    f->dump_stream("snap_trimq") << snap_trimq;
    f->dump_unsigned("snap_trimq_len", snap_trimq.size());
    recovery_state.dump_peering_state(f.get());

    f->open_array_section("recovery_state");
    handle_query_state(f.get());
    f->close_section();

    if (is_primary() && is_active() && m_scrubber) {
      m_scrubber->dump_scrubber(f.get());
    }

    f->open_object_section("agent_state");
    if (agent_state)
      agent_state->dump(f.get());
    f->close_section();

    f->close_section();
  }
  else if (prefix == "log") {

    f->open_object_section("op_log");
    f->open_object_section("pg_log_t");
    recovery_state.get_pg_log().get_log().dump(f.get());
    f->close_section();
    f->close_section();
  }
  else if (prefix == "mark_unfound_lost") {
    string mulcmd;
    cmd_getval(cmdmap, "mulcmd", mulcmd);
    int mode = -1;
    if (mulcmd == "revert") {
      if (pool.info.is_erasure()) {
	ss << "mode must be 'delete' for ec pool";
	ret = -EINVAL;
	goto out;
      }
      mode = pg_log_entry_t::LOST_REVERT;
    } else if (mulcmd == "delete") {
      mode = pg_log_entry_t::LOST_DELETE;
    } else {
      ss << "mode must be 'revert' or 'delete'; mark not yet implemented";
      ret = -EINVAL;
      goto out;
    }
    ceph_assert(mode == pg_log_entry_t::LOST_REVERT ||
		mode == pg_log_entry_t::LOST_DELETE);

    if (!is_primary()) {
      ss << "not primary";
      ret = -EROFS;
      goto out;
    }

    uint64_t unfound = recovery_state.get_missing_loc().num_unfound();
    if (!unfound) {
      ss << "pg has no unfound objects";
      goto out;  // make command idempotent
    }

    if (!recovery_state.all_unfound_are_queried_or_lost(get_osdmap())) {
      ss << "pg has " << unfound
	 << " unfound objects but we haven't probed all sources, not marking lost";
      ret = -EINVAL;
      goto out;
    }

    mark_all_unfound_lost(mode, on_finish);
    return;
  }

  else if (prefix == "list_unfound") {
    hobject_t offset;
    string offset_json;
    bool show_offset = false;
    if (cmd_getval(cmdmap, "offset", offset_json)) {
      json_spirit::Value v;
      try {
	if (!json_spirit::read(offset_json, v))
	  throw std::runtime_error("bad json");
	offset.decode(v);
      } catch (std::runtime_error& e) {
	ss << "error parsing offset: " << e.what();
	ret = -EINVAL;
	goto out;
      }
      show_offset = true;
    }
    f->open_object_section("missing");
    if (show_offset) {
      f->open_object_section("offset");
      offset.dump(f.get());
      f->close_section();
    }
    auto &needs_recovery_map = recovery_state.get_missing_loc()
      .get_needs_recovery();
    f->dump_int("num_missing", needs_recovery_map.size());
    f->dump_int("num_unfound", get_num_unfound());
    map<hobject_t, pg_missing_item>::const_iterator p =
      needs_recovery_map.upper_bound(offset);
    {
      f->open_array_section("objects");
      int32_t num = 0;
      for (; p != needs_recovery_map.end() &&
	     num < cct->_conf->osd_command_max_records;
	   ++p) {
        if (recovery_state.get_missing_loc().is_unfound(p->first)) {
	  f->open_object_section("object");
	  {
	    f->open_object_section("oid");
	    p->first.dump(f.get());
	    f->close_section();
	  }
          p->second.dump(f.get()); // have, need keys
	  {
	    f->open_array_section("locations");
            for (auto &&r : recovery_state.get_missing_loc().get_locations(
		   p->first)) {
              f->dump_stream("shard") << r;
	    }
	    f->close_section();
	  }
	  f->close_section();
	  num++;
        }
      }
      f->close_section();
    }
    // Get possible locations of missing objects from pg information
    PeeringState::QueryUnfound q(f.get());
    recovery_state.handle_event(q, 0);
    f->dump_bool("more", p != needs_recovery_map.end());
    f->close_section();
  }

  else if (prefix == "scrub" || prefix == "deep-scrub") {
    if (is_primary()) {
      scrub_level_t deep = (prefix == "deep-scrub") ? scrub_level_t::deep
						    : scrub_level_t::shallow;
      m_scrubber->on_operator_forced_scrub(f.get(), deep);
    } else {
      ss << "Not primary";
      ret = -EPERM;
    }
    outbl.append(ss.str());
  }

  else if (prefix == "scrub-abort") {
    if (is_primary()) {
      m_scrubber->on_operator_abort_scrub(f.get());
    } else {
      ss << "Not primary";
      ret = -EPERM;
      outbl.append(ss.str());
    }
  }

  // the test/debug commands that schedule a scrub by modifying timestamps
  else if (prefix == "schedule-scrub" || prefix == "schedule-deep-scrub") {
    if (is_primary()) {
      scrub_level_t deep = (prefix == "schedule-deep-scrub")
			       ? scrub_level_t::deep
			       : scrub_level_t::shallow;
      const int64_t offst = cmd_getval_or<int64_t>(cmdmap, "time", 0);
      m_scrubber->on_operator_periodic_cmd(f.get(), deep, offst);
    } else {
      ss << "Not primary";
      ret = -EPERM;
    }
    outbl.append(ss.str());
  }

  else if (prefix == "block" || prefix == "unblock" || prefix == "set" ||
           prefix == "unset") {
    string value;
    cmd_getval(cmdmap, "value", value);

    if (is_primary()) {
      ret = m_scrubber->asok_debug(prefix, value, f.get(), ss);
      f->open_object_section("result");
      f->dump_bool("success", true);
      f->close_section();
    } else {
      ss << "Not primary";
      ret = -EPERM;
    }
    outbl.append(ss.str());
  }

  else {
    ret = -ENOSYS;
    ss << "prefix '" << prefix << "' not implemented";
  }

 out:
  if (ret >= 0 && outbl.length() == 0) {
    f->flush(outbl);
  }
  on_finish(ret, ss.str(), outbl);
}


// ==========================================================

void PrimaryLogPG::do_pg_op(OpRequestRef op)
{
  const MOSDOp *m = static_cast<const MOSDOp *>(op->get_req());
  ceph_assert(m->get_type() == CEPH_MSG_OSD_OP);
  dout(10) << "do_pg_op " << *m << dendl;

  op->mark_started();

  int result = 0;
  string cname, mname;

  snapid_t snapid = m->get_snapid();

  vector<OSDOp> ops = m->ops;

  for (vector<OSDOp>::iterator p = ops.begin(); p != ops.end(); ++p) {
    std::unique_ptr<const PGLSFilter> filter;
    OSDOp& osd_op = *p;
    auto bp = p->indata.cbegin();
    switch (p->op.op) {
    case CEPH_OSD_OP_PGNLS_FILTER:
      try {
	decode(cname, bp);
	decode(mname, bp);
      }
      catch (const ceph::buffer::error& e) {
	dout(0) << "unable to decode PGLS_FILTER description in " << *m << dendl;
	result = -EINVAL;
	break;
      }
      std::tie(result, filter) = get_pgls_filter(bp);
      if (result < 0)
        break;

      ceph_assert(filter);

      // fall through

    case CEPH_OSD_OP_PGNLS:
      if (snapid != CEPH_NOSNAP) {
	result = -EINVAL;
	break;
      }
      if (get_osdmap()->raw_pg_to_pg(m->get_pg()) != info.pgid.pgid) {
        dout(10) << " pgnls pg=" << m->get_pg()
		 << " " << get_osdmap()->raw_pg_to_pg(m->get_pg())
		 << " != " << info.pgid << dendl;
	result = 0; // hmm?
      } else {
	unsigned list_size = std::min<uint64_t>(cct->_conf->osd_max_pgls,
						p->op.pgls.count);

        dout(10) << " pgnls pg=" << m->get_pg() << " count " << list_size
		 << dendl;
	// read into a buffer
        vector<hobject_t> sentries;
        pg_nls_response_t response;
	try {
	  decode(response.handle, bp);
	}
	catch (const ceph::buffer::error& e) {
	  dout(0) << "unable to decode PGNLS handle in " << *m << dendl;
	  result = -EINVAL;
	  break;
	}

	hobject_t next;
	hobject_t lower_bound = response.handle;
	hobject_t pg_start = info.pgid.pgid.get_hobj_start();
	hobject_t pg_end = info.pgid.pgid.get_hobj_end(pool.info.get_pg_num());
        dout(10) << " pgnls lower_bound " << lower_bound
		 << " pg_end " << pg_end << dendl;
	if (((!lower_bound.is_max() && lower_bound >= pg_end) ||
	     (lower_bound != hobject_t() && lower_bound < pg_start))) {
	  // this should only happen with a buggy client.
	  dout(10) << "outside of PG bounds " << pg_start << " .. "
		   << pg_end << dendl;
	  result = -EINVAL;
	  break;
	}

	hobject_t current = lower_bound;
	int r = pgbackend->objects_list_partial(
	  current,
	  list_size,
	  list_size,
	  &sentries,
	  &next);
	if (r != 0) {
	  result = -EINVAL;
	  break;
	}

	map<hobject_t, pg_missing_item>::const_iterator missing_iter =
	  recovery_state.get_pg_log().get_missing().get_items().lower_bound(current);
	vector<hobject_t>::iterator ls_iter = sentries.begin();
	hobject_t _max = hobject_t::get_max();
	while (1) {
	  const hobject_t &mcand =
	    missing_iter == recovery_state.get_pg_log().get_missing().get_items().end() ?
	    _max :
	    missing_iter->first;
	  const hobject_t &lcand =
	    ls_iter == sentries.end() ?
	    _max :
	    *ls_iter;

	  hobject_t candidate;
	  if (mcand == lcand) {
	    candidate = mcand;
	    if (!mcand.is_max()) {
	      ++ls_iter;
	      ++missing_iter;
	    }
	  } else if (mcand < lcand) {
	    candidate = mcand;
	    ceph_assert(!mcand.is_max());
	    ++missing_iter;
	  } else {
	    candidate = lcand;
	    ceph_assert(!lcand.is_max());
	    ++ls_iter;
	  }

          dout(10) << " pgnls candidate 0x" << std::hex << candidate.get_hash()
		   << " vs lower bound 0x" << lower_bound.get_hash()
		   << std::dec << dendl;

	  if (candidate >= next) {
	    break;
	  }

	  if (response.entries.size() == list_size) {
	    next = candidate;
	    break;
	  }

	  if (candidate.snap != CEPH_NOSNAP)
	    continue;

	  // skip internal namespace
	  if (candidate.get_namespace() == cct->_conf->osd_hit_set_namespace)
	    continue;

	  if (recovery_state.get_missing_loc().is_deleted(candidate))
	    continue;

	  // skip wrong namespace
	  if (m->get_hobj().nspace != librados::all_nspaces &&
               candidate.get_namespace() != m->get_hobj().nspace)
	    continue;

	  if (filter && !pgls_filter(*filter, candidate))
	    continue;

          dout(20) << "pgnls item 0x" << std::hex
            << candidate.get_hash()
            << ", rev 0x" << hobject_t::_reverse_bits(candidate.get_hash())
            << std::dec << " "
            << candidate.oid.name << dendl;

	  librados::ListObjectImpl item;
	  item.nspace = candidate.get_namespace();
	  item.oid = candidate.oid.name;
	  item.locator = candidate.get_key();
	  response.entries.push_back(item);
	}

	if (next.is_max() &&
	    missing_iter == recovery_state.get_pg_log().get_missing().get_items().end() &&
	    ls_iter == sentries.end()) {
	  result = 1;

	  // Set response.handle to the start of the next PG according
	  // to the object sort order.
	  response.handle = info.pgid.pgid.get_hobj_end(pool.info.get_pg_num());
	} else {
          response.handle = next;
        }
        dout(10) << "pgnls handle=" << response.handle << dendl;
	encode(response, osd_op.outdata);
	dout(10) << " pgnls result=" << result << " outdata.length()="
		 << osd_op.outdata.length() << dendl;
      }
      break;

    case CEPH_OSD_OP_PGLS_FILTER:
      try {
	decode(cname, bp);
	decode(mname, bp);
      }
      catch (const ceph::buffer::error& e) {
	dout(0) << "unable to decode PGLS_FILTER description in " << *m << dendl;
	result = -EINVAL;
	break;
      }
      std::tie(result, filter) = get_pgls_filter(bp);
      if (result < 0)
        break;

      ceph_assert(filter);

      // fall through

    case CEPH_OSD_OP_PGLS:
      if (snapid != CEPH_NOSNAP) {
	result = -EINVAL;
	break;
      }
      if (get_osdmap()->raw_pg_to_pg(m->get_pg()) != info.pgid.pgid) {
        dout(10) << " pgls pg=" << m->get_pg()
		 << " " << get_osdmap()->raw_pg_to_pg(m->get_pg())
		 << " != " << info.pgid << dendl;
	result = 0; // hmm?
      } else {
	unsigned list_size = std::min<uint64_t>(cct->_conf->osd_max_pgls,
						p->op.pgls.count);

        dout(10) << " pgls pg=" << m->get_pg() << " count " << list_size << dendl;
	// read into a buffer
        vector<hobject_t> sentries;
        pg_ls_response_t response;
	try {
	  decode(response.handle, bp);
	}
	catch (const ceph::buffer::error& e) {
	  dout(0) << "unable to decode PGLS handle in " << *m << dendl;
	  result = -EINVAL;
	  break;
	}

	hobject_t next;
	hobject_t current = response.handle;
	int r = pgbackend->objects_list_partial(
	  current,
	  list_size,
	  list_size,
	  &sentries,
	  &next);
	if (r != 0) {
	  result = -EINVAL;
	  break;
	}

	ceph_assert(snapid == CEPH_NOSNAP || recovery_state.get_pg_log().get_missing().get_items().empty());

	map<hobject_t, pg_missing_item>::const_iterator missing_iter =
	  recovery_state.get_pg_log().get_missing().get_items().lower_bound(current);
	vector<hobject_t>::iterator ls_iter = sentries.begin();
	hobject_t _max = hobject_t::get_max();
	while (1) {
	  const hobject_t &mcand =
	    missing_iter == recovery_state.get_pg_log().get_missing().get_items().end() ?
	    _max :
	    missing_iter->first;
	  const hobject_t &lcand =
	    ls_iter == sentries.end() ?
	    _max :
	    *ls_iter;

	  hobject_t candidate;
	  if (mcand == lcand) {
	    candidate = mcand;
	    if (!mcand.is_max()) {
	      ++ls_iter;
	      ++missing_iter;
	    }
	  } else if (mcand < lcand) {
	    candidate = mcand;
	    ceph_assert(!mcand.is_max());
	    ++missing_iter;
	  } else {
	    candidate = lcand;
	    ceph_assert(!lcand.is_max());
	    ++ls_iter;
	  }

	  if (candidate >= next) {
	    break;
	  }

	  if (response.entries.size() == list_size) {
	    next = candidate;
	    break;
	  }

	  if (candidate.snap != CEPH_NOSNAP)
	    continue;

	  // skip wrong namespace
	  if (candidate.get_namespace() != m->get_hobj().nspace)
	    continue;

	  if (recovery_state.get_missing_loc().is_deleted(candidate))
	    continue;

	  if (filter && !pgls_filter(*filter, candidate))
	    continue;

	  response.entries.push_back(make_pair(candidate.oid,
					       candidate.get_key()));
	}
	if (next.is_max() &&
	    missing_iter == recovery_state.get_pg_log().get_missing().get_items().end() &&
	    ls_iter == sentries.end()) {
	  result = 1;
	}
	response.handle = next;
	encode(response, osd_op.outdata);
	dout(10) << " pgls result=" << result << " outdata.length()="
		 << osd_op.outdata.length() << dendl;
      }
      break;

    case CEPH_OSD_OP_PG_HITSET_LS:
      {
	list< pair<utime_t,utime_t> > ls;
	for (list<pg_hit_set_info_t>::const_iterator p = info.hit_set.history.begin();
	     p != info.hit_set.history.end();
	     ++p)
	  ls.push_back(make_pair(p->begin, p->end));
	if (hit_set)
	  ls.push_back(make_pair(hit_set_start_stamp, utime_t()));
	encode(ls, osd_op.outdata);
      }
      break;

    case CEPH_OSD_OP_PG_HITSET_GET:
      {
	utime_t stamp(osd_op.op.hit_set_get.stamp);
	if (hit_set_start_stamp && stamp >= hit_set_start_stamp) {
	  // read the current in-memory HitSet, not the version we've
	  // checkpointed.
	  if (!hit_set) {
	    result= -ENOENT;
	    break;
	  }
	  encode(*hit_set, osd_op.outdata);
	  result = osd_op.outdata.length();
	} else {
	  // read an archived HitSet.
	  hobject_t oid;
	  for (list<pg_hit_set_info_t>::const_iterator p = info.hit_set.history.begin();
	       p != info.hit_set.history.end();
	       ++p) {
	    if (stamp >= p->begin && stamp <= p->end) {
	      oid = get_hit_set_archive_object(p->begin, p->end, p->using_gmt);
	      break;
	    }
	  }
	  if (oid == hobject_t()) {
	    result = -ENOENT;
	    break;
	  }
	  if (!pool.info.is_replicated()) {
	    // FIXME: EC not supported yet
	    result = -EOPNOTSUPP;
	    break;
	  }
	  if (is_unreadable_object(oid)) {
	    wait_for_unreadable_object(oid, op);
	    return;
	  }
	  result = osd->store->read(ch, ghobject_t(oid), 0, 0, osd_op.outdata);
	}
      }
      break;

   case CEPH_OSD_OP_SCRUBLS:
      result = do_scrub_ls(m, &osd_op);
      break;

    default:
      result = -EINVAL;
      break;
    }

    if (result < 0)
      break;
  }

  // reply
  MOSDOpReply *reply = new MOSDOpReply(m, 0, get_osdmap_epoch(),
				       CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK,
				       false);
  reply->claim_op_out_data(ops);
  reply->set_result(result);
  reply->set_reply_versions(info.last_update, info.last_user_version);
  osd->send_message_osd_client(reply, m->get_connection());
}

int PrimaryLogPG::do_scrub_ls(const MOSDOp *m, OSDOp *osd_op)
{
  if (m->get_pg() != info.pgid.pgid) {
    dout(10) << " scrubls pg=" << m->get_pg() << " != " << info.pgid << dendl;
    return -EINVAL; // hmm?
  }
  auto bp = osd_op->indata.cbegin();
  scrub_ls_arg_t arg;
  try {
    arg.decode(bp);
  } catch (ceph::buffer::error&) {
    dout(10) << " corrupted scrub_ls_arg_t" << dendl;
    return -EINVAL;
  }

  int r = 0;
  scrub_ls_result_t result = {.interval = info.history.same_interval_since};

  if (arg.interval != 0 && arg.interval != info.history.same_interval_since) {
    r = -EAGAIN;
  } else {
    bool store_queried = m_scrubber && m_scrubber->get_store_errors(arg, result);
    if (store_queried) {
      encode(result, osd_op->outdata); 
    } else {
      // the scrubber's store is not initialized
      r = -ENOENT;
    }
  }

  return r;
}

/**
 * Grabs locks for OpContext, should be cleaned up in close_op_ctx
 *
 * @param ctx [in,out] ctx to get locks for
 * @return true on success, false if we are queued
 */
bool PrimaryLogPG::get_rw_locks(bool write_ordered, OpContext *ctx)
{
  /* If head_obc, !obc->obs->exists and we will always take the
   * snapdir lock *before* the head lock.  Since all callers will do
   * this (read or write) if we get the first we will be guaranteed
   * to get the second.
   */
  if (write_ordered && ctx->op->may_read()) {
    if (ctx->op->may_read_data()) {
      ctx->lock_type = RWState::RWEXCL;
    } else {
      ctx->lock_type = RWState::RWWRITE;
    }
  } else if (write_ordered) {
    ctx->lock_type = RWState::RWWRITE;
  } else {
    ceph_assert(ctx->op->may_read());
    ctx->lock_type = RWState::RWREAD;
  }

  if (ctx->head_obc) {
    ceph_assert(!ctx->obc->obs.exists);
    if (!ctx->lock_manager.get_lock_type(
          ctx->lock_type,
          ctx->head_obc->obs.oi.soid,
          ctx->head_obc,
          ctx->op)) {
      ctx->lock_type = RWState::RWNONE;
      return false;
    }
  }
  if (ctx->lock_manager.get_lock_type(
        ctx->lock_type,
        ctx->obc->obs.oi.soid,
        ctx->obc,
        ctx->op)) {
    return true;
  } else {
    ceph_assert(!ctx->head_obc);
    ctx->lock_type = RWState::RWNONE;
    return false;
  }
}

/**
 * Releases locks
 *
 * @param manager [in] manager with locks to release
 */
void PrimaryLogPG::release_object_locks(
  ObcLockManager &lock_manager) {
  std::list<std::pair<ObjectContextRef, std::list<OpRequestRef> > > to_req;
  bool requeue_recovery = false;
  bool requeue_snaptrim = false;
  lock_manager.put_locks(
    &to_req,
    &requeue_recovery,
    &requeue_snaptrim);
  if (requeue_recovery)
    queue_recovery();
  if (requeue_snaptrim)
    snap_trimmer_machine.process_event(TrimWriteUnblocked());

  if (!to_req.empty()) {
    // requeue at front of scrub blocking queue if we are blocked by scrub
    for (auto &&p: to_req) {
      if (m_scrubber->write_blocked_by_scrub(p.first->obs.oi.soid.get_head())) {
        for (auto& op : p.second) {
          op->mark_delayed("waiting for scrub");
        }

	waiting_for_scrub.splice(
	  waiting_for_scrub.begin(),
	  p.second,
	  p.second.begin(),
	  p.second.end());
      } else if (is_laggy()) {
        for (auto& op : p.second) {
          op->mark_delayed("waiting for readable");
        }
	waiting_for_readable.splice(
	  waiting_for_readable.begin(),
	  p.second,
	  p.second.begin(),
	  p.second.end());
      } else {
	requeue_ops(p.second);
      }
    }
  }
}

PrimaryLogPG::PrimaryLogPG(OSDService *o, OSDMapRef curmap,
			   const PGPool &_pool,
			   const map<string,string>& ec_profile, spg_t p,
			   ECExtentCache::LRU &ec_extent_cache_lru) :
  PG(o, curmap, _pool, p),
  pgbackend(
    PGBackend::build_pg_backend(
      _pool.info, ec_profile, this, coll_t(p), ch, o->store, cct, ec_extent_cache_lru)),
  object_contexts(o->cct, o->cct->_conf->osd_pg_object_context_cache_count),
  new_backfill(false),
  temp_seq(0),
  snap_trimmer_machine(this)
{
  recovery_state.set_backend_predicates(
    pgbackend->get_is_readable_predicate(),
    pgbackend->get_is_recoverable_predicate());
  snap_trimmer_machine.initiate();

  m_scrubber = make_unique<PrimaryLogScrub>(this);
}

PrimaryLogPG::~PrimaryLogPG()
{
  m_scrubber.reset();
}

void PrimaryLogPG::get_src_oloc(const object_t& oid, const object_locator_t& oloc, object_locator_t& src_oloc)
{
  src_oloc = oloc;
  if (oloc.key.empty())
    src_oloc.key = oid.name;
}

void PrimaryLogPG::handle_backoff(OpRequestRef& op)
{
  auto m = op->get_req<MOSDBackoff>();
  auto session = ceph::ref_cast<Session>(m->get_connection()->get_priv());
  if (!session)
    return;  // drop it.
  hobject_t begin = info.pgid.pgid.get_hobj_start();
  hobject_t end = info.pgid.pgid.get_hobj_end(pool.info.get_pg_num());
  if (begin < m->begin) {
    begin = m->begin;
  }
  if (end > m->end) {
    end = m->end;
  }
  dout(10) << __func__ << " backoff ack id " << m->id
	   << " [" << begin << "," << end << ")" << dendl;
  session->ack_backoff(cct, m->pgid, m->id, begin, end);
}

void PrimaryLogPG::do_request(
  OpRequestRef& op,
  ThreadPool::TPHandle &handle)
{
  // PG 层的统一请求入口。调用者已持有 PG 锁；本函数先检查 map 和 PG 状态，
  // 再将请求分派到客户端 IO、recovery、backfill 或 scrub 路径。
  if (op->osd_trace) {
    // 在已有 OSD trace 下创建 PG 子 trace，记录请求进入 PG 的时刻。
    op->pg_trace.init("pg op", &trace_endpoint, &op->osd_trace);
    op->pg_trace.event("do request");
  }
  // 阶段 1：确保 PG 已获得处理请求所需的 OSDMap，并维持同一来源的顺序。
  auto p = waiting_for_map.find(op->get_source());
  if (p != waiting_for_map.end()) {
    // 同一来源已有更早请求在等 map，即使当前请求的 map 已满足，也必须排在它后面，避免后到请求越过先到请求。
    dout(20) << __func__ << " waiting_for_map "
	     << p->first << " not empty, queueing" << dendl;
    p->second.push_back(op);
    op->mark_delayed("waiting_for_map not empty");
    return;
  }
  if (!have_same_or_newer_map(op->min_epoch)) {
    // PG 当前 map 早于请求要求的 min_epoch：按来源暂存请求，并通知 OSD 获取至少达到 min_epoch 的新 map。
    dout(20) << __func__ << " min " << op->min_epoch
	     << ", queue on waiting_for_map " << op->get_source() << dendl;
    waiting_for_map[op->get_source()].push_back(op);
    op->mark_delayed("op must wait for map");
    osd->request_osdmap_update(op->min_epoch);
    return;
  }

  // 阶段 2：依据请求 epoch、PG 历史和发送方状态丢弃已经失效的请求。
  if (can_discard_request(op)) {
    return;
  }

  // 阶段 3：处理支持 RADOS_BACKOFF 的客户端。backoff 让客户端暂时停止向
  // 某个 PG/对象范围发送请求，避免 PG 不可服务时持续堆积操作。
  const Message *m = op->get_req();
  int msg_type = m->get_type();
  if (m->get_connection()->has_feature(CEPH_FEATURE_RADOS_BACKOFF)) {
    // Session 保存该连接已经下发的 backoff 区间及确认状态。
    auto session = ceph::ref_cast<Session>(m->get_connection()->get_priv());
    if (!session)
      return;  // drop it.
    if (msg_type == CEPH_MSG_OSD_OP) {
      // 请求命中此前已发送的 backoff 范围时，由 Session 处理并停止本次分派。
      if (session->check_backoff(cct, info.pgid,
				 info.pgid.pgid.get_hobj_start(), m)) {
	return;
      }

      // PG down/incomplete，或已经 peered 但尚未 active 时无法处理客户端 IO。
      bool backoff =
	is_down() ||
	is_incomplete() ||
	(!is_active() && is_peered());
      if (g_conf()->osd_backoff_on_peering && !backoff) {
        // 配置允许时，PG 正在 peering 也立即向客户端施加 backoff。
	if (is_peering()) {
	  backoff = true;
	}
      }
      if (backoff) {
        // 为整个 PG 添加 backoff；客户端收到后应等待解除，而不是持续重试。
        add_pg_backoff(session);
	return;
      }
    }
    // 非空范围的 PG 级 backoff ACK 在此确认；对象级 ACK 稍后在 active 的 OSD_OP/BACKOFF 请求上下文中处理。
    if (msg_type == CEPH_MSG_OSD_BACKOFF) {
      const MOSDBackoff *ba = static_cast<const MOSDBackoff*>(m);
      if (ba->begin != ba->end) {
	handle_backoff(op);
	return;
      }
    }
  }

  // 阶段 4：PG 尚未完成 peering。只有 backend 明确声明可在 inactive
  // 状态处理的内部消息可以继续，其余请求等待 PG 达到 peered。
  if (!is_peered()) {
    // Delay unless PGBackend says it's ok
    if (pgbackend->can_handle_while_inactive(op)) {
      // 例如某些副本/恢复协议消息不依赖 PG 已 active，由具体 backend 处理。
      bool handled = pgbackend->handle_message(op);
      ceph_assert(handled);
      return;
    } else {
      // 保存普通请求；状态机进入 peered 后会重新唤醒这些请求。
      waiting_for_peered.push_back(op);
      op->mark_delayed("waiting for peered");
      return;
    }
  }

  // peering 已完成，但上一个 interval 遗留的事务还需要 flush；
  // 在 flush 完成前不能让新 interval 的操作越过旧事务。
  if (recovery_state.needs_flush()) {
    dout(20) << "waiting for flush on " << *op->get_req() << dendl;
    waiting_for_flush.push_back(op);
    op->mark_delayed("waiting for flush");
    return;
  }

  ceph_assert(is_peered() && !recovery_state.needs_flush());
  // 先交给 PGBackend 识别副本写、EC sub-op 等内部协议消息；
  // 返回 true 表示消息已完全处理，无需进入下面的通用消息分派。
  if (pgbackend->handle_message(op))
    return;

  switch (msg_type) {
  case CEPH_MSG_OSD_OP:
  case CEPH_MSG_OSD_BACKOFF:
    // 客户端 IO 和对象级 backoff ACK 都要求 PG 已 active；
    // 仅 peered 尚不足以对外提供读写服务，因此先进入 waiting_for_active。
    if (!is_active()) {
      dout(20) << " peered, not active, waiting for active on "
               << *op->get_req() << dendl;
      waiting_for_active.push_back(op);
      op->mark_delayed("waiting for active");
      return;
    }
    switch (msg_type) {
    case CEPH_MSG_OSD_OP:
      // cache tier 请求要求客户端理解 cache-pool 协议，否则明确返回不支持。
      if ((pool.info.has_tiers() || pool.info.is_tier()) &&
	  !op->has_feature(CEPH_FEATURE_OSD_CACHEPOOL)) {
	osd->reply_op_error(op, -EOPNOTSUPP);
	return;
      }
      // 普通客户端对象操作的主入口，继续解析读写命令、权限和对象状态。
      do_op(op);
      break;
    case CEPH_MSG_OSD_BACKOFF:
      // active PG 在对象操作上下文中处理对象范围的 backoff ACK。
      handle_backoff(op);
      break;
    }
    break;

  case MSG_OSD_PG_SCAN:
    // backfill 前扫描对象集合/区间；可能耗时，因此向下传递 heartbeat handle。
    do_scan(op, handle);
    break;

  case MSG_OSD_PG_BACKFILL:
    // 执行 backfill 数据同步请求。
    do_backfill(op);
    break;

  case MSG_OSD_PG_BACKFILL_REMOVE:
    // 删除 backfill 过程中目标端不再需要的对象。
    do_backfill_remove(op);
    break;

  case MSG_OSD_SCRUB_RESERVE:
    // 处理 scrub 资源预留；scrubber 尚未就绪时让发送方稍后重试。
    if (!m_scrubber) {
      osd->reply_op_error(op, -EAGAIN);
      return;
    }
    m_scrubber->handle_scrub_reserve_msgs(op);
    break;

  case MSG_OSD_REP_SCRUB:
    // 在副本端执行 primary 发来的 scrub 请求。
    replica_scrub(op, handle);
    break;

  case MSG_OSD_REP_SCRUBMAP:
    // primary 接收并处理副本生成的 scrub map。
    do_replica_scrub_map(op);
    break;

  case MSG_OSD_PG_UPDATE_LOG_MISSING:
    // 更新副本端 PG log/missing 集合，用于恢复期间同步缺失对象信息。
    do_update_log_missing(op);
    break;

  case MSG_OSD_PG_UPDATE_LOG_MISSING_REPLY:
    // 处理上述 log/missing 更新请求的回复。
    do_update_log_missing_reply(op);
    break;

  default:
    // 到达 do_request() 的消息类型必须属于上述集合，其他类型表示分派错误。
    ceph_abort_msg("bad message type in do_request");
  }
}

/** do_op - do an op
 * pg lock will be held (if multithreaded)
 * osd_lock NOT held.
 */
void PrimaryLogPG::do_op(OpRequestRef& op)
{
  // 客户端对象操作的核心入口。调用者已经持有 PG 锁；
  // 本函数完成请求解码、路由/权限/状态校验、对象上下文与锁准备，最后交给 execute_ctx() 执行。
  FUNCTRACE(cct);
  // NOTE: take a non-const pointer here; we must be careful not to
  // change anything that will break other reads on m (operator<<).
  // 后续需要清理 payload 并访问 ops，因此取可写 MOSDOp；
  // 仍须遵守上方英文注释，不能修改会影响其他并发只读访问的消息字段。
  MOSDOp *m = static_cast<MOSDOp*>(op->get_nonconst_req());
  ceph_assert(m->get_type() == CEPH_MSG_OSD_OP);
  // 延迟解码尚未完成时在此完成，并释放原始 payload，降低请求驻留内存。
  if (m->finish_decode()) {
    op->reset_desc();   // for TrackedOp
    m->clear_payload();
  }

  dout(20) << __func__ << ": op " << *m << dendl;

  // snap clone 请求也以 head 对象作为 PG 归属和多数状态检查的基准。
  const hobject_t head = m->get_hobj().get_head();

  // 校验客户端计算出的对象 hash 确实属于当前 PG；
  // 失败通常意味着客户端使用了错误/过旧映射，或上游分派存在错误，不能在错误 PG 上执行。
  if (!info.pgid.pgid.contains(
	info.pgid.pgid.get_split_bits(pool.info.get_pg_num()), head)) {
    derr << __func__ << " " << info.pgid.pgid << " does not contain "
	 << head << " pg_num " << pool.info.get_pg_num() << " hash "
	 << std::hex << head.get_hash() << std::dec << dendl;
    osd->clog->warn() << info.pgid.pgid << " does not contain " << head
		      << " op " << *m;
    ceph_assert(!cct->_conf->osd_debug_misdirected_ops);
    return;
  }

  // 支持 RADOS_BACKOFF 的连接可由 OSD 对单对象施加临时反压；
  // Session 保存当前连接上的 backoff 状态。
  bool can_backoff =
    m->get_connection()->has_feature(CEPH_FEATURE_RADOS_BACKOFF);
  ceph::ref_t<Session> session;
  if (can_backoff) {
    session = static_cast<Session*>(m->get_connection()->get_priv().get());
    if (!session.get()) {
      dout(10) << __func__ << " no session" << dendl;
      return;
    }

    // 如果该对象范围仍处于 backoff，当前请求不能继续执行。
    if (session->check_backoff(cct, info.pgid, head, m)) {
      return;
    }
  }

  // 客户端请求并行执行语义尚未实现，明确拒绝，避免按普通顺序误执行。
  if (m->has_flag(CEPH_OSD_FLAG_PARALLELEXEC)) {
    // not implemented.
    dout(20) << __func__ << ": PARALLELEXEC not implemented " << *m << dendl;
    osd->reply_op_error(op, -EINVAL);
    return;
  }

  {
    // 解析 m->ops，汇总请求是否读、写、缓存、需要有序执行等属性，
    // 供下面的路由和状态检查复用；非法操作组合会直接返回错误码。
    int r = op->maybe_init_op_info(*get_osdmap());
    if (r) {
      osd->reply_op_error(op, r);
      return;
    }
  }

  // check for op with rwordered and rebalance or localize reads
  // write-ordered 请求不能同时要求本地化/均衡读，否则副本执行会破坏顺序。
  // CEPH_OSD_FLAGS_DIRECT_READ 表示请求允许做“直接读”，即为了均衡负载或就近访问，可以把纯读请求发到非 primary 副本。
  // op->rwordered() 表示该请求虽然可能包含读，但其读写必须遵守顺序，不能被调度到任意副本独立执行。通常意味着读结果依赖于前序写入的可见性。
  if (m->has_flag(CEPH_OSD_FLAGS_DIRECT_READ) && op->rwordered()) {
    dout(4) << __func__ << ": rebelance or localized reads with rwordered not allowed "
       << *m << dendl;
    osd->reply_op_error(op, -EINVAL);
    return;
  }

  // 阶段 2：依据请求 flag 和读写属性确定允许在哪个 PG 副本执行。
  if (m->get_flags() & CEPH_OSD_FLAG_EC_DIRECT_READ) {
    // EC direct read 可由 primary 或持有目标 shard 的 non-primary 提供。
    if (is_primary() || is_nonprimary()) {
      op->set_ec_direct_read();
    } else {
      // handle_misdirected_op() 不会把请求转发到正确 OSD，也通常不会立即回复客户端。
      // 它的主要处理方式是：丢弃当前请求，等待客户端超时后根据新 OSDMap 重发。
      osd->handle_misdirected_op(this, op);
      return;
    }
  } else if ((m->get_flags() & (CEPH_OSD_FLAG_BALANCE_READS |
                                CEPH_OSD_FLAG_LOCALIZE_READS)) &&
      op->may_read() &&
      !(op->may_write() || op->may_cache())) {
    // balanced reads; any replica will do
    // 纯读且请求允许 balance/localize 时，任意合法副本都可服务。
    if (!(is_primary() || is_nonprimary())) {
      // is_primary()      当前 OSD 是 acting primary
      // is_nonprimary()   当前 OSD 是 acting replica
      // 两者都不是         当前 OSD 不在该 PG 的 acting set 中
      osd->handle_misdirected_op(this, op);
      return;
    }
  } else {
    // normal case; must be primary
    // 普通请求（尤其写和缓存操作）必须由当前 acting primary 处理。
    if (!is_primary()) {
      osd->handle_misdirected_op(this, op);
      return;
    }
  }

  // 统计实际由 replica/non-primary 提供的直接读。
  if (!is_primary()) {
    osd->logger->inc(l_osd_replica_read);
  }

  // laggy 检查可能延迟或拒绝请求，避免状态不稳定的 PG 对外服务。
  if (!check_laggy(op)) {
    return;
  }

  // 根据连接 Session、pool、namespace、对象和操作类型验证客户端权限。
  if (!op_has_sufficient_caps(op)) {
    osd->reply_op_error(op, -EPERM);
    return;
  }

  // PG 级操作（而非具体对象数据操作）走独立分派，不进入对象上下文流程。
  if (op->includes_pg_op()) {
    return do_pg_op(op);
  }

  // object name too long?
  // 对象名、locator key 和 namespace 必须满足 OSD 配置及后端限制。
  if (m->get_oid().name.size() > cct->_conf->osd_max_object_name_len) {
    dout(4) << "do_op name is longer than "
	    << cct->_conf->osd_max_object_name_len
	    << " bytes" << dendl;
    osd->reply_op_error(op, -ENAMETOOLONG);
    return;
  }
  if (m->get_hobj().get_key().size() > cct->_conf->osd_max_object_name_len) {
    dout(4) << "do_op locator is longer than "
	    << cct->_conf->osd_max_object_name_len
	    << " bytes" << dendl;
    osd->reply_op_error(op, -ENAMETOOLONG);
    return;
  }
  if (m->get_hobj().nspace.size() > cct->_conf->osd_max_object_namespace_len) {
    dout(4) << "do_op namespace is longer than "
	    << cct->_conf->osd_max_object_namespace_len
	    << " bytes" << dendl;
    osd->reply_op_error(op, -ENAMETOOLONG);
    return;
  }
  if (m->get_hobj().oid.name.empty()) {
    dout(4) << "do_op empty oid name is not allowed" << dendl;
    osd->reply_op_error(op, -EINVAL);
    return;
  }

  // 让具体 ObjectStore（如 BlueStore）验证编码后的 hobject key 是否可存储。
  if (int r = osd->store->validate_hobject_key(head)) {
    dout(4) << "do_op object " << head << " invalid for backing store: "
	    << r << dendl;
    osd->reply_op_error(op, r);
    return;
  }

  // blocklisted?
  // 已被集群 blocklist 的客户端地址禁止继续访问，通常用于旧客户端实例隔离。
  if (get_osdmap()->is_blocklisted(m->get_source_addr())) {
    dout(10) << "do_op " << m->get_source_addr() << " is blocklisted" << dendl;
    osd->reply_op_error(op, -EBLOCKLISTED);
    return;
  }

  // order this op as a write?
  // rwordered 表示即使操作包含读，也必须进入写有序路径，不能越过先前写入。
  bool write_ordered = op->rwordered();

  // discard due to cluster full transition?  (we discard any op that
  // originates before the cluster or pool is marked full; the client
  // will resend after the full flag is removed or if they expect the
  // op to succeed despite being full).  The except is FULL_FORCE and
  // FULL_TRY ops, which there is no reason to discard because they
  // bypass all full checks anyway.  If this op isn't write or
  // read-ordered, we skip.
  // FIXME: we exclude mds writes for now.
  // 请求基于集群标记 full 之前的旧 map 发出时直接丢弃，
  // 让客户端基于最新 map 重发；FULL_TRY/FULL_FORCE 明确要求绕过该检查。
  if (write_ordered && !(m->get_source().is_mds() ||
			 m->has_flag(CEPH_OSD_FLAG_FULL_TRY) ||
			 m->has_flag(CEPH_OSD_FLAG_FULL_FORCE)) &&
      info.history.last_epoch_marked_full > m->get_map_epoch()) {
    dout(10) << __func__ << " discarding op sent before full " << m << " "
	     << *m << dendl;
    return;
  }
  // mds should have stopped writing before this point.
  // We can't allow OSD to become non-startable even if mds
  // could be writing as part of file removals.
  // 本地存储达到 fail-safe full 是最后保护线，避免继续写到 OSD 无法启动。
  if (write_ordered && osd->check_failsafe_full(get_dpp()) &&
      !m->has_flag(CEPH_OSD_FLAG_FULL_TRY)) {
    dout(10) << __func__ << " fail-safe full check failed, dropping request." << dendl;
    return;
  }
  int64_t poolid = get_pgid().pool();
  // 再次从当前 OSDMap 获取 pool；pool 已删除时请求已失去处理目标。
  const pg_pool_t *pi = get_osdmap()->get_pg_pool(poolid);
  if (!pi) {
    return;
  }
  if (pi->has_flag(pg_pool_t::FLAG_EIO)) {
    // drop op on the floor; the client will handle returning EIO
    // pool 被标记 EIO 后停止正常 IO。支持 pool EIO 协议的客户端自行完成错误处理；
    // 旧客户端需要 OSD 显式回复 -EIO。
    if (m->has_flag(CEPH_OSD_FLAG_SUPPORTSPOOLEIO)) {
      dout(10) << __func__ << " discarding op due to pool EIO flag" << dendl;
    } else {
      dout(10) << __func__ << " replying EIO due to pool EIO flag" << dendl;
      osd->reply_op_error(op, -EIO);
    }
    return;
  }
  if (op->may_write()) {
    // 写请求还需满足：只能写 head，且单次数据量不能超过配置上限。

    // invalid?
    if (m->get_snapid() != CEPH_NOSNAP) {
      dout(20) << __func__ << ": write to clone not valid " << *m << dendl;
      osd->reply_op_error(op, -EINVAL);
      return;
    }

    // too big?
    if (cct->_conf->osd_max_write_size &&
        m->get_data_len() > cct->_conf->osd_max_write_size << 20) {
      // journal can't hold commit!
      derr << "do_op msg data len " << m->get_data_len()
           << " > osd_max_write_size " << (cct->_conf->osd_max_write_size << 20)
           << " on " << *m << dendl;
      osd->reply_op_error(op, -OSD_WRITETOOBIG);
      return;
    }
  }

  dout(10) << "do_op " << *m
	   << (op->may_write() ? " may_write" : "")
	   << (op->may_read() ? " may_read" : "")
	   << (op->may_cache() ? " may_cache" : "")
	   << " -> " << (write_ordered ? "write-ordered" : "read-ordered")
	   << " flags " << ceph_osd_flag_string(m->get_flags())
	   << dendl;


  // missing object?
  // 阶段 3：确保目标对象当前可读。对象缺失、unfound 或恢复中时，
  // 选择向客户端施加单对象 backoff，或把请求挂到等待队列直到 recovery 完成。
  if (is_unreadable_object(head)) {
    // replica 可能缺少请求所需的 clone，无法可靠判断时让客户端转回 primary。
    if (!is_primary() && is_missing_any_head_or_clone_of(head)) {
      dout(10) << __func__ <<  "possibly missing clone object " << head
               << " on this replica, bouncing to primary" << dendl;
      osd->logger->inc(l_osd_replica_read_redirect_missing);
      osd->reply_op_error(op, -EAGAIN);
      return;
    }
    if (can_backoff &&
	(g_conf()->osd_backoff_on_degraded ||
	 (g_conf()->osd_backoff_on_unfound &&
	  recovery_state.get_missing_loc().is_unfound(head)))) {
      add_backoff(session, head, head);
      // 主动推动该对象恢复，争取尽快解除 backoff。
      maybe_kick_recovery(head);
    } else {
      // 不使用 backoff 时由 OSD 保存请求，待对象恢复可读后重新入队。
      wait_for_unreadable_object(head, op);
    }
    return;
  }

  if (write_ordered) {
    // degraded object?
    // 有序写不能覆盖正在 degraded/backfill 的对象，否则可能破坏副本恢复顺序。
    if (is_degraded_or_backfilling_object(head)) {
      if (can_backoff && g_conf()->osd_backoff_on_degraded) {
        add_backoff(session, head, head);
        maybe_kick_recovery(head);
      } else {
        wait_for_degraded_object(head, op);
      }
      return;
    }

    // scrub 正在检查该对象时暂停写入，避免校验过程中数据发生变化。
    if (m_scrubber->is_scrub_active() && m_scrubber->write_blocked_by_scrub(head)) {
      dout(20) << __func__ << ": waiting for scrub" << dendl;
      waiting_for_scrub.push_back(op);
      op->mark_delayed("waiting for scrub");
      return;
    }
    // 再次检查 laggy 状态；写路径可能被重新排队，等待副本状态稳定。
    if (!check_laggy_requeue(op)) {
      return;
    }

    // 准备写 head 时，发现它依赖的某个快照 clone 当前不可读而阻塞，因此先暂停写请求。
    if (auto blocked_iter = objects_blocked_on_unreadable_snap.find(head);
	blocked_iter != std::end(objects_blocked_on_unreadable_snap)) {
      hobject_t to_wait_on(head);
      to_wait_on.snap = blocked_iter->second;
      wait_for_unreadable_object(to_wait_on, op);
      return;
    }

    // blocked on snap?
    // 历史 snap clone 处于因为 degraded/backfill 阻塞时，同样阻塞 head 写入。
    if (auto blocked_iter = objects_blocked_on_degraded_snap.find(head);
	blocked_iter != std::end(objects_blocked_on_degraded_snap)) {
      hobject_t to_wait_on(head);
      to_wait_on.snap = blocked_iter->second;
      wait_for_degraded_object(to_wait_on, op);
      return;
    }
    // cache tier 正在把回滚所需的 snap clone 从底层池 promote 到缓存池
    if (auto blocked_snap_promote_iter = objects_blocked_on_snap_promotion.find(head);
	blocked_snap_promote_iter != std::end(objects_blocked_on_snap_promotion)) {
      wait_for_blocked_object(blocked_snap_promote_iter->second->obs.oi.soid, op);
      return;
    }
    // cache tier 已满且该对象写被阻塞时，挂起直到 agent 释放空间。
    if (objects_blocked_on_cache_full.count(head)) {
      block_write_on_full_cache(head, op);
      return;
    }
  }

  // dup/resent?
  // 阶段 4：对写/缓存请求按 reqid 去重，保证客户端重发不会重复修改对象。
  if (op->may_write() || op->may_cache()) {
    // warning: we will get back *a* request for this reqid, but not
    // necessarily the most recent.  this happens with flush and
    // promote ops, but we can't possible have both in our log where
    // the original request is still not stable on disk, so for our
    // purposes here it doesn't matter which one we get.
    eversion_t version;
    version_t user_version;
    int return_code = 0;
    vector<pg_log_op_return_item_t> op_returns;
    bool got = check_in_progress_op(
      m->get_reqid(), &version, &user_version, &return_code, &op_returns);
    if (got) {
      dout(3) << __func__ << " dup " << m->get_reqid()
	      << " version " << version << dendl;
      if (already_complete(version)) {
	// 原请求已经提交，说明是因为网络等问题，导致客户端没有收到写入成功回答。
  // 直接把之前记录的返回码、版本和各子操作结果返回给客户端。
	osd->reply_op_error(op, return_code, version, user_version, op_returns);
      } else {
	dout(10) << " waiting for " << version << " to commit" << dendl;
        // always queue ondisk waiters, so that we can requeue if needed
	// 原请求仍在提交中；重复请求等待同一版本落盘，不能再次执行。
	waiting_for_ondisk[version].emplace_back(op, user_version, return_code,
						 op_returns);
	op->mark_delayed("waiting for ondisk");
      }
      return;
    }
  }

  if (cct->_conf->bluestore_debug_inject_read_err &&
      op->may_write() &&
      pool.info.is_erasure() &&
      ECInject::test_write_error0(m->get_hobj(), m->get_reqid())) {
    // Fail retried write with error
    dout(0) << __func__ << " Error inject - Fail retried write with EINVAL" << dendl;
    osd->reply_op_error(op, -EINVAL);
    return;
  }

  // 阶段 5：定位或按需创建对象上下文 OBC。OBC 缓存对象元数据、snapset、锁状态等，是后续执行 OSDOp 的内存载体。
  ObjectContextRef obc;
  // 只有写请求允许 find_object_context() 为不存在的对象建立新上下文。
  bool can_create = op->may_write();
  hobject_t missing_oid;

  // kludge around the fact that LIST_SNAPS sets CEPH_SNAPDIR for LIST_SNAPS
  // LIST_SNAPS 使用特殊 snapdir 标识，但对象上下文仍从 head 对象取得。
  const hobject_t& oid =
    m->get_snapid() == CEPH_SNAPDIR ? head : m->get_hobj();

  // make sure LIST_SNAPS is on CEPH_SNAPDIR and nothing else
  // 严格约束 snapdir：只有 LIST_SNAPS 可使用 CEPH_SNAPDIR，其他操作禁止。
  for (vector<OSDOp>::iterator p = m->ops.begin(); p != m->ops.end(); ++p) {
    OSDOp& osd_op = *p;

    if (osd_op.op.op == CEPH_OSD_OP_LIST_SNAPS) {
      if (m->get_snapid() != CEPH_SNAPDIR) {
	dout(10) << "LIST_SNAPS with incorrect context" << dendl;
	osd->reply_op_error(op, -EINVAL);
	return;
      }
    } else {
      if (m->get_snapid() == CEPH_SNAPDIR) {
	dout(10) << "non-LIST_SNAPS on snapdir" << dendl;
	osd->reply_op_error(op, -EINVAL);
	return;
      }
    }
  }

  // io blocked on obc?
  // 对象 head 正被 promote/flush/copy 等异步操作占用时，普通 IO 先等待；
  // FLUSH 自身必须继续，避免等待自己造成死锁。
  if (!m->has_flag(CEPH_OSD_FLAG_FLUSH) &&
      maybe_await_blocked_head(oid, op)) {
    return;
  }

  if (!is_primary()) {
    // replica direct-read 还必须确认该对象没有与未稳定写入冲突；
    // 否则让客户端返回 primary 重试，避免读到旧数据。
    if (!recovery_state.can_serve_read(oid)) {
      std::string_view storage_object = "replica";
      if (pool.info.is_erasure()) {
        storage_object = "shard";
      }
      dout(20) << __func__
               << ": unstable write on " << storage_object
               << ", bouncing to primary "
	       << *m << dendl;
      osd->logger->inc(l_osd_replica_read_redirect_conflict);
      osd->reply_op_error(op, -EAGAIN);
      return;
    }
    dout(20) << __func__ << ": serving read on oid " << oid
             << dendl;
    osd->logger->inc(l_osd_replica_read_served);
  }

  // 查找具体 head/clone 的对象上下文；
  // 返回值区分成功、不存在、需要等待恢复及其他错误，missing_oid 给出真正缺失的对象。
  int r = find_object_context(
    oid, &obc, can_create,
    m->has_flag(CEPH_OSD_FLAG_MAP_SNAP_CLONE),
    &missing_oid);

  // LIST_SNAPS needs the ssc too
  // LIST_SNAPS 需要完整 SnapSetContext 才能枚举对象的 clone 信息。
  if (obc &&
      m->get_snapid() == CEPH_SNAPDIR &&
      !obc->ssc) {
    obc->ssc = get_snapset_context(oid, true);
  }

  if (r == -EAGAIN) {
    // If we're not the primary of this OSD, we just return -EAGAIN. Otherwise,
    // we have to wait for the object.
    if (is_primary()) {
      // missing the specific snap we need; requeue and wait.
      // primary 保存请求等待缺失 clone 恢复；replica 保留 -EAGAIN，稍后统一回复。
      ceph_assert(!op->may_write()); // only happens on a read/cache
      wait_for_unreadable_object(missing_oid, op);
      return;
    }
  } else if (r == 0) {
    // 找到了 clone OBC 后还要单独检查 clone 自身的可读和 degraded 状态；
    // 前面的检查只覆盖 head。
    if (is_unreadable_object(obc->obs.oi.soid)) {
      dout(10) << __func__ << ": clone " << obc->obs.oi.soid
	       << " is unreadable, waiting" << dendl;
      wait_for_unreadable_object(obc->obs.oi.soid, op);
      return;
    }

    // degraded object?  (the check above was for head; this could be a clone)
    // obc                   ObjectContext，对象内存上下文
    // obc->obs              ObjectState，对象当前状态
    // obc->obs.oi           object_info_t，对象元数据
    // obc->obs.oi.soid      hobject_t，实际存储对象的标识
    // obc->obs.oi.soid.snap != CEPH_NOSNAP 代表这不是 head，是某个历史 snap clone
    // 当前请求依赖历史 clone 且要求写级别的顺序保护时，
    // 如果该 clone 正处于 degraded/backfill，则等待其恢复。
    if (write_ordered &&
	obc->obs.oi.soid.snap != CEPH_NOSNAP &&
	is_degraded_or_backfilling_object(obc->obs.oi.soid)) {
      dout(10) << __func__ << ": clone " << obc->obs.oi.soid
	       << " is degraded, waiting" << dendl;
      wait_for_degraded_object(obc->obs.oi.soid, op);
      return;
    }
  }

  // cache tier 的 hit set 记录近期访问，用于 agent 判断对象是否值得 promote。
  // hit_set 是 PG 级别 的，每个 primary PG 的 PrimaryLogPG 对象维护一个当前 hit_set。
  // 因此一个 HitSet 记录的是，在当前统计周期内，这个 PG 中访问过哪些对象。
  // pool：定义 HitSet 策略和参数
  // PG：创建、维护并持久化自己的 HitSet
  // 对象：作为访问记录被插入 PG 的 HitSet
  bool in_hit_set = false;
  if (hit_set) {
    if (obc.get()) {
      if (obc->obs.oi.soid != hobject_t() && hit_set->contains(obc->obs.oi.soid))
	in_hit_set = true;
    } else {
      if (missing_oid != hobject_t() && hit_set->contains(missing_oid))
        in_hit_set = true;
    }
    if (!op->hitset_inserted) {
      hit_set->insert(oid);
      op->hitset_inserted = true;
      if (hit_set->is_full() ||
          hit_set_start_stamp + pool.info.hit_set_period <= m->get_recv_stamp()) {
        // 当前 hit set 已满或超过周期，持久化到底层存储，供 agent 后续分析,并创建新的 HitSet。
        hit_set_persist();
      }
    }
  }

  if (agent_state) {
    // cache tier agent 可能因空间压力切换模式并接管/延迟当前请求。
    if (agent_choose_mode(false, op))
      return;
  }

  if (obc.get() && obc->obs.exists) {
    // 如果相邻 snap clone 之间存在依赖关系，且当前请求需要访问的 clone 还未恢复，则先恢复相邻 clone。
    if (recover_adjacent_clones(obc, op)) {
      return;
    }
    // manifest 对象需要走 redirect/chunked 专用路径。返回 true 表示请求
    // 已被代理、异步处理或加入等待队列，不能再按普通对象继续执行。
    if (maybe_handle_manifest(op,
			       write_ordered,
			       obc))
    return;
  }

  // cache tier 根据命中、模式和 promote 条件决定本地处理、代理到底层池，或先提升对象；
  // 返回 true 表示请求已被接管，当前路径结束。
  if (maybe_handle_cache(op,
			 write_ordered,
			 obc,
			 r,
			 missing_oid,
			 false,
			 in_hit_set))
    return;

  // 对象上下文查找失败且无法继续时，按写/读语义记录可去重的写错误或直接回复客户端；
  // COPY_GET 的 ENOENT 需要填充专用返回结构。
  if (r && (r != -ENOENT || !obc)) {
    // copy the reqids for copy get on ENOENT
    if (r == -ENOENT &&
	(m->ops[0].op.op == CEPH_OSD_OP_COPY_GET)) {
      fill_in_copy_get_noent(op, oid, m->ops[0]);
      return;
    }
    dout(20) << __func__ << ": find_object_context got error " << r << dendl;
    if (op->may_write() &&
	get_osdmap()->require_osd_release >= ceph_release_t::kraken) {
      record_write_error(op, oid, nullptr, r);
    } else {
      osd->reply_op_error(op, r);
    }
    return;
  }

  // make sure locator is consistent
  /**
   * 客户端 locator 是随对象请求发送的“对象放置信息”，用于在多副本/多池环境中定位对象；
   * pool        对象属于哪个存储池
   * key         用于 CRUSH/PG 映射的 locator key
   * namespace   对象所在的 namespace
   * */ 
  // 客户端 locator 与对象实际 locator 不一致时记录告警；
  // 对象已经通过 hash 路由到本 PG，因此这里只告警，不立即终止请求。
  object_locator_t oloc(obc->obs.oi.soid);
  if (m->get_object_locator() != oloc) {
    dout(10) << " provided locator " << m->get_object_locator()
	     << " != object's " << obc->obs.oi.soid << dendl;
    osd->clog->warn() << "bad locator " << m->get_object_locator()
		     << " on object " << oloc
		      << " op " << *m;
  }

  // io blocked on obc?
  // 获取 OBC 后再次检查对象级阻塞状态，防止查找期间异步操作占用对象。
  if (obc->is_blocked() &&
      !m->has_flag(CEPH_OSD_FLAG_FLUSH)) {
    wait_for_blocked_object(obc->obs.oi.soid, op);
    return;
  }

  dout(25) << __func__ << " oi " << obc->obs.oi << dendl;

  // 阶段 6：创建本次操作上下文，集中保存 OSDOp 列表、OBC、事务、返回值以及稍后复制/提交所需的状态。
  OpContext *ctx = new OpContext(op, m->get_reqid(), &m->ops, obc, this);

  if (m->has_flag(CEPH_OSD_FLAG_SKIPRWLOCKS)) {
    // 内部受控请求可显式跳过对象读写锁；普通客户端不会使用此路径。
    dout(20) << __func__ << ": skipping rw locks" << dendl;
  } else if (m->get_flags() & CEPH_OSD_FLAG_FLUSH) {
    // flush 子操作忽略普通写锁，但必须确认该对象确实存在对应 flush 状态。
    dout(20) << __func__ << ": part of flush, will ignore write lock" << dendl;

    // verify there is in fact a flush in progress
    // FIXME: we could make this a stronger test.
    map<hobject_t,FlushOpRef>::iterator p = flush_ops.find(obc->obs.oi.soid);
    if (p == flush_ops.end()) {
      dout(10) << __func__ << " no flush in progress, aborting" << dendl;
      reply_ctx(ctx, -EINVAL);
      return;
    }
  } else if (!get_rw_locks(write_ordered, ctx)) {
    // 锁暂不可用时，get_rw_locks() 已将上下文挂入对象锁等待队列；
    // 关闭当前 ctx 的活动部分，待锁可用后请求会重新执行。
    dout(20) << __func__ << " waiting for rw locks " << dendl;
    op->mark_delayed("waiting for rw locks");
    close_op_ctx(ctx);
    return;
  }
  dout(20) << __func__ << " obc " << *obc << dendl;

  // find_object_context() 的延迟错误可能在 cache/锁处理后仍需返回；
  // 写错误进入 PG log，保证同一 reqid 重试时得到一致结果。
  if (r) {
    dout(20) << __func__ << " returned an error: " << r << dendl;
    if (op->may_write() &&
	get_osdmap()->require_osd_release >= ceph_release_t::kraken) {
      record_write_error(op, oid, nullptr, r,
			 ctx->op->allows_returnvec() ? ctx : nullptr);
    } else {
      osd->reply_op_error(op, r);
    }
    close_op_ctx(ctx);
    return;
  }

  // 客户端要求绕过 cache tier 时，把该意图保存到执行上下文。
  if (m->has_flag(CEPH_OSD_FLAG_IGNORE_CACHE)) {
    ctx->ignore_cache = true;
  }

  if ((op->may_read()) && (obc->obs.oi.is_lost())) {
    // This object is lost. Reading from it returns an error.
    // lost 表示集群已确认无法恢复，读请求不能再等待 recovery。
    dout(20) << __func__ << ": object " << obc->obs.oi.soid
	     << " is lost" << dendl;
    reply_ctx(ctx, -ENFILE);
    return;
  }
  if (!op->may_write() &&
      !op->may_cache() &&
      (!obc->obs.exists ||
       ((m->get_snapid() != CEPH_SNAPDIR) &&
	obc->obs.oi.is_whiteout()))) {
    // 纯读/非缓存操作遇到不存在或 whiteout 对象，按 ENOENT 返回。
    // copy the reqids for copy get on ENOENT
    if (m->ops[0].op.op == CEPH_OSD_OP_COPY_GET) {
      fill_in_copy_get_noent(op, oid, m->ops[0]);
      close_op_ctx(ctx);
      return;
    }
    reply_ctx(ctx, -ENOENT);
    return;
  }

  // 请求已通过所有前置门槛，更新慢请求跟踪状态为 started。
  op->mark_started();

  // 真正执行 m->ops：读取对象或构造写事务、PG log 和副本操作。
  // 写请求的持久化通常在 execute_ctx() 启动的后续异步流程中完成。
  execute_ctx(ctx);
  // 统计请求从工作队列出队到完成执行准备的耗时，并按读/写类型分类。
  utime_t prepare_latency = ceph_clock_now();
  prepare_latency -= op->get_dequeued_time();
  osd->logger->tinc(l_osd_op_prepare_lat, prepare_latency);
  if (op->may_read() && op->may_write()) {
    osd->logger->tinc(l_osd_op_rw_prepare_lat, prepare_latency);
  } else if (op->may_read()) {
    osd->logger->tinc(l_osd_op_r_prepare_lat, prepare_latency);
  } else if (op->may_write() || op->may_cache()) {
    osd->logger->tinc(l_osd_op_w_prepare_lat, prepare_latency);
  }

  // force recovery of the oldest missing object if too many logs
  // PG log 过多时主动恢复最老缺失对象，帮助推进日志裁剪。
  maybe_force_recovery();
}

PrimaryLogPG::cache_result_t PrimaryLogPG::maybe_handle_manifest_detail(
  OpRequestRef op,
  bool write_ordered,
  ObjectContextRef obc)
{
  if (!obc) {
    dout(20) << __func__ << ": no obc " << dendl;
    return cache_result_t::NOOP;
  }

  if (!obc->obs.oi.has_manifest()) {
    dout(20) << __func__ << ": " << obc->obs.oi.soid 
	     << " is not manifest object " << dendl;
    return cache_result_t::NOOP;
  }
  if (op->get_req<MOSDOp>()->get_flags() & CEPH_OSD_FLAG_IGNORE_REDIRECT) {
    dout(20) << __func__ << ": ignoring redirect due to flag" << dendl;
    return cache_result_t::NOOP;
  }

  // if it is write-ordered and blocked, stop now
  if (obc->is_blocked() && write_ordered) {
    // we're already doing something with this object
    dout(20) << __func__ << " blocked on " << obc->obs.oi.soid << dendl;
    return cache_result_t::NOOP;
  }

  vector<OSDOp> ops = op->get_req<MOSDOp>()->ops;
  for (vector<OSDOp>::iterator p = ops.begin(); p != ops.end(); ++p) {
    OSDOp& osd_op = *p;
    ceph_osd_op& op = osd_op.op;
    if (op.op == CEPH_OSD_OP_SET_REDIRECT ||
	op.op == CEPH_OSD_OP_SET_CHUNK ||
	op.op == CEPH_OSD_OP_UNSET_MANIFEST ||
	op.op == CEPH_OSD_OP_TIER_PROMOTE ||
	op.op == CEPH_OSD_OP_TIER_FLUSH ||
	op.op == CEPH_OSD_OP_TIER_EVICT ||
	op.op == CEPH_OSD_OP_ISDIRTY) {
      return cache_result_t::NOOP;
    }
  }

  switch (obc->obs.oi.manifest.type) {
  case object_manifest_t::TYPE_REDIRECT:
    if (op->may_write() || write_ordered) {
      do_proxy_write(op, obc);
    } else {
      // promoted object
      if (obc->obs.oi.size != 0) {
	return cache_result_t::NOOP;
      }
      do_proxy_read(op, obc);
    }
    return cache_result_t::HANDLED_PROXY;
  case object_manifest_t::TYPE_CHUNKED:
    {
      // in case of metadata handling ops don't need to promote chunk objects
      if (op->may_read() && !op->may_read_data() && !op->may_write()) {
        return cache_result_t::NOOP;
      }

      if (can_proxy_chunked_read(op, obc)) {
	map<hobject_t,FlushOpRef>::iterator p = flush_ops.find(obc->obs.oi.soid);
        if (p != flush_ops.end()) {
          do_proxy_chunked_op(op, obc->obs.oi.soid, obc, true);
          return cache_result_t::HANDLED_PROXY;
        }
	do_proxy_chunked_op(op, obc->obs.oi.soid, obc, write_ordered);
	return cache_result_t::HANDLED_PROXY;
      }

      MOSDOp *m = static_cast<MOSDOp*>(op->get_nonconst_req());
      ceph_assert(m->get_type() == CEPH_MSG_OSD_OP);
      hobject_t head = m->get_hobj();

      if (is_degraded_or_backfilling_object(head)) {
	dout(20) << __func__ << ": " << head << " is degraded, waiting" << dendl;
	wait_for_degraded_object(head, op);
	return cache_result_t::BLOCKED_RECOVERY;
      }

      if (m_scrubber->write_blocked_by_scrub(head)) {
	dout(20) << __func__ << ": waiting for scrub" << dendl;
	waiting_for_scrub.push_back(op);
	op->mark_delayed("waiting for scrub");
	return cache_result_t::BLOCKED_RECOVERY;
      }
      if (!check_laggy_requeue(op)) {
	return cache_result_t::BLOCKED_RECOVERY;
      }

      for (auto& p : obc->obs.oi.manifest.chunk_map) {
	if (p.second.is_missing()) {
	  auto m = op->get_req<MOSDOp>();
	  const object_locator_t oloc = m->get_object_locator();
	  promote_object(obc, obc->obs.oi.soid, oloc, op, NULL);
	  return cache_result_t::BLOCKED_PROMOTE;
	}
      }
      return cache_result_t::NOOP;
    }
  default:
    ceph_abort_msg("unrecognized manifest type");
  }

  return cache_result_t::NOOP;
}

void PrimaryLogPG::record_write_error(OpRequestRef op, const hobject_t &soid,
				      MOSDOpReply *orig_reply, int r,
				      OpContext *ctx_for_op_returns)
{
  dout(20) << __func__ << " r=" << r << dendl;
  ceph_assert(op->may_write());
  const osd_reqid_t &reqid = op->get_req<MOSDOp>()->get_reqid();
  mempool::osd_pglog::list<pg_log_entry_t> entries;
  entries.push_back(pg_log_entry_t(pg_log_entry_t::ERROR, soid,
				   get_next_version(), eversion_t(), 0,
				   reqid, utime_t(), r));
  if (ctx_for_op_returns) {
    entries.back().set_op_returns(*ctx_for_op_returns->ops);
    dout(20) << __func__ << " op_returns=" << entries.back().op_returns << dendl;
  }

  struct OnComplete {
    PrimaryLogPG *pg;
    OpRequestRef op;
    boost::intrusive_ptr<MOSDOpReply> orig_reply;
    int r;
    OnComplete(
      PrimaryLogPG *pg,
      OpRequestRef op,
      MOSDOpReply *orig_reply,
      int r)
      : pg(pg), op(op),
	orig_reply(orig_reply, false /* take over ref */), r(r)
      {}
    void operator()() {
      ldpp_dout(pg, 20) << "finished " << __func__ << " r=" << r << dendl;
      auto m = op->get_req<MOSDOp>();
      MOSDOpReply *reply = orig_reply.detach();
      ldpp_dout(pg, 10) << " sending commit on " << *m << " " << reply << dendl;
      pg->osd->send_message_osd_client(reply, m->get_connection());
    }
  };

  ObcLockManager lock_manager;
  submit_log_entries(
    entries,
    std::move(lock_manager),
    std::optional<std::function<void(void)> >(
      OnComplete(this, op, orig_reply, r)),
    op,
    r);
}

PrimaryLogPG::cache_result_t PrimaryLogPG::maybe_handle_cache_detail(
  OpRequestRef op,
  bool write_ordered,
  ObjectContextRef obc,
  int r, hobject_t missing_oid,
  bool must_promote,
  bool in_hit_set,
  ObjectContextRef *promote_obc)
{
  // return quickly if caching is not enabled
  if (pool.info.cache_mode == pg_pool_t::CACHEMODE_NONE)
    return cache_result_t::NOOP;

  if (op &&
      op->get_req() &&
      op->get_req()->get_type() == CEPH_MSG_OSD_OP &&
      (op->get_req<MOSDOp>()->get_flags() &
       CEPH_OSD_FLAG_IGNORE_CACHE)) {
    dout(20) << __func__ << ": ignoring cache due to flag" << dendl;
    return cache_result_t::NOOP;
  }

  must_promote = must_promote || op->need_promote();

  if (obc)
    dout(25) << __func__ << " " << obc->obs.oi << " "
	     << (obc->obs.exists ? "exists" : "DNE")
	     << " missing_oid " << missing_oid
	     << " must_promote " << (int)must_promote
	     << " in_hit_set " << (int)in_hit_set
	     << dendl;
  else
    dout(25) << __func__ << " (no obc)"
	     << " missing_oid " << missing_oid
	     << " must_promote " << (int)must_promote
	     << " in_hit_set " << (int)in_hit_set
	     << dendl;

  // if it is write-ordered and blocked, stop now
  if (obc.get() && obc->is_blocked() && write_ordered) {
    // we're already doing something with this object
    dout(20) << __func__ << " blocked on " << obc->obs.oi.soid << dendl;
    return cache_result_t::NOOP;
  }

  if (r == -ENOENT && missing_oid == hobject_t()) {
    // we know this object is logically absent (e.g., an undefined clone)
    return cache_result_t::NOOP;
  }

  if (obc.get() && obc->obs.exists) {
    osd->logger->inc(l_osd_op_cache_hit);
    return cache_result_t::NOOP;
  }
  if (!is_primary()) {
    dout(20) << __func__ << " cache miss; ask the primary" << dendl;
    osd->reply_op_error(op, -EAGAIN);
    return cache_result_t::REPLIED_WITH_EAGAIN;
  }

  if (missing_oid == hobject_t() && obc.get()) {
    missing_oid = obc->obs.oi.soid;
  }

  auto m = op->get_req<MOSDOp>();
  const object_locator_t oloc = m->get_object_locator();

  if (op->need_skip_handle_cache()) {
    return cache_result_t::NOOP;
  }

  OpRequestRef promote_op;

  switch (pool.info.cache_mode) {
  case pg_pool_t::CACHEMODE_WRITEBACK:
    if (agent_state &&
	agent_state->evict_mode == TierAgentState::EVICT_MODE_FULL) {
      if (!op->may_write() && !op->may_cache() &&
	  !write_ordered && !must_promote) {
	dout(20) << __func__ << " cache pool full, proxying read" << dendl;
	do_proxy_read(op);
	return cache_result_t::HANDLED_PROXY;
      }
      dout(20) << __func__ << " cache pool full, waiting" << dendl;
      block_write_on_full_cache(missing_oid, op);
      return cache_result_t::BLOCKED_FULL;
    }

    if (must_promote || (!hit_set && !op->need_skip_promote())) {
      promote_object(obc, missing_oid, oloc, op, promote_obc);
      return cache_result_t::BLOCKED_PROMOTE;
    }

    if (op->may_write() || op->may_cache()) {
      do_proxy_write(op);

      // Promote too?
      if (!op->need_skip_promote() &&
          maybe_promote(obc, missing_oid, oloc, in_hit_set,
	              pool.info.min_write_recency_for_promote,
		      OpRequestRef(),
		      promote_obc)) {
	return cache_result_t::BLOCKED_PROMOTE;
      }
      return cache_result_t::HANDLED_PROXY;
    } else {
      do_proxy_read(op);

      // Avoid duplicate promotion
      if (obc.get() && obc->is_blocked()) {
	if (promote_obc)
	  *promote_obc = obc;
        return cache_result_t::BLOCKED_PROMOTE;
      }

      // Promote too?
      if (!op->need_skip_promote()) {
        (void)maybe_promote(obc, missing_oid, oloc, in_hit_set,
                            pool.info.min_read_recency_for_promote,
                            promote_op, promote_obc);
      }

      return cache_result_t::HANDLED_PROXY;
    }
    ceph_abort_msg("unreachable");
    return cache_result_t::NOOP;

  case pg_pool_t::CACHEMODE_READONLY:
    // TODO: clean this case up
    if (!obc.get() && r == -ENOENT) {
      // we don't have the object and op's a read
      promote_object(obc, missing_oid, oloc, op, promote_obc);
      return cache_result_t::BLOCKED_PROMOTE;
    }
    if (!r) { // it must be a write
      do_cache_redirect(op);
      return cache_result_t::HANDLED_REDIRECT;
    }
    // crap, there was a failure of some kind
    return cache_result_t::NOOP;

  case pg_pool_t::CACHEMODE_FORWARD:
    // this mode is deprecated; proxy instead
  case pg_pool_t::CACHEMODE_PROXY:
    if (!must_promote) {
      if (op->may_write() || op->may_cache() || write_ordered) {
	do_proxy_write(op);
	return cache_result_t::HANDLED_PROXY;
      } else {
	do_proxy_read(op);
	return cache_result_t::HANDLED_PROXY;
      }
    }
    // ugh, we're forced to promote.
    if (agent_state &&
	agent_state->evict_mode == TierAgentState::EVICT_MODE_FULL) {
      dout(20) << __func__ << " cache pool full, waiting" << dendl;
      block_write_on_full_cache(missing_oid, op);
      return cache_result_t::BLOCKED_FULL;
    }
    promote_object(obc, missing_oid, oloc, op, promote_obc);
    return cache_result_t::BLOCKED_PROMOTE;

  case pg_pool_t::CACHEMODE_READFORWARD:
    // this mode is deprecated; proxy instead
  case pg_pool_t::CACHEMODE_READPROXY:
    // Do writeback to the cache tier for writes
    if (op->may_write() || write_ordered || must_promote) {
      if (agent_state &&
	  agent_state->evict_mode == TierAgentState::EVICT_MODE_FULL) {
	dout(20) << __func__ << " cache pool full, waiting" << dendl;
	block_write_on_full_cache(missing_oid, op);
	return cache_result_t::BLOCKED_FULL;
      }
      promote_object(obc, missing_oid, oloc, op, promote_obc);
      return cache_result_t::BLOCKED_PROMOTE;
    }

    // If it is a read, we can read, we need to proxy it
    do_proxy_read(op);
    return cache_result_t::HANDLED_PROXY;

  default:
    ceph_abort_msg("unrecognized cache_mode");
  }
  return cache_result_t::NOOP;
}

bool PrimaryLogPG::maybe_promote(ObjectContextRef obc,
				 const hobject_t& missing_oid,
				 const object_locator_t& oloc,
				 bool in_hit_set,
				 uint32_t recency,
				 OpRequestRef promote_op,
				 ObjectContextRef *promote_obc)
{
  dout(20) << __func__ << " missing_oid " << missing_oid
	   << "  in_hit_set " << in_hit_set << dendl;

  switch (recency) {
  case 0:
    break;
  case 1:
    // Check if in the current hit set
    if (in_hit_set) {
      break;
    } else {
      // not promoting
      return false;
    }
    break;
  default:
    {
      unsigned count = (int)in_hit_set;
      if (count) {
	// Check if in other hit sets
	const hobject_t& oid = obc.get() ? obc->obs.oi.soid : missing_oid;
	for (map<time_t,HitSetRef>::reverse_iterator itor =
	       agent_state->hit_set_map.rbegin();
	     itor != agent_state->hit_set_map.rend();
	     ++itor) {
	  if (!itor->second->contains(oid)) {
	    break;
	  }
	  ++count;
	  if (count >= recency) {
	    break;
	  }
	}
      }
      if (count >= recency) {
	break;
      }
      return false;	// not promoting
    }
    break;
  }

  if (osd->promote_throttle()) {
    dout(10) << __func__ << " promote throttled" << dendl;
    return false;
  }
  promote_object(obc, missing_oid, oloc, promote_op, promote_obc);
  return true;
}

void PrimaryLogPG::do_cache_redirect(OpRequestRef op)
{
  auto m = op->get_req<MOSDOp>();
  int flags = m->get_flags() & (CEPH_OSD_FLAG_ACK|CEPH_OSD_FLAG_ONDISK);
  MOSDOpReply *reply = new MOSDOpReply(m, -ENOENT, get_osdmap_epoch(),
                                       flags, false);
  request_redirect_t redir(m->get_object_locator(), pool.info.tier_of);
  reply->set_redirect(redir);
  dout(10) << "sending redirect to pool " << pool.info.tier_of << " for op "
	   << *op->get_req() << dendl;
  m->get_connection()->send_message(reply);
  return;
}

struct C_ProxyRead : public Context {
  PrimaryLogPGRef pg;
  hobject_t oid;
  epoch_t last_peering_reset;
  ceph_tid_t tid;
  PrimaryLogPG::ProxyReadOpRef prdop;
  utime_t start;
  C_ProxyRead(PrimaryLogPG *p, hobject_t o, epoch_t lpr,
	     const PrimaryLogPG::ProxyReadOpRef& prd)
    : pg(p), oid(o), last_peering_reset(lpr),
      tid(0), prdop(prd), start(ceph_clock_now())
  {}
  void finish(int r) override {
    if (prdop->canceled)
      return;
    std::scoped_lock locker{*pg};
    if (prdop->canceled) {
      return;
    }
    if (last_peering_reset == pg->get_last_peering_reset()) {
      pg->finish_proxy_read(oid, tid, r);
      pg->osd->logger->tinc(l_osd_tier_r_lat, ceph_clock_now() - start);
    }
  }
};

struct C_ProxyChunkRead : public Context {
  PrimaryLogPGRef pg;
  hobject_t oid;
  epoch_t last_peering_reset;
  ceph_tid_t tid;
  PrimaryLogPG::ProxyReadOpRef prdop;
  utime_t start;
  ObjectOperation *obj_op;
  int op_index = 0;
  uint64_t req_offset = 0;
  ObjectContextRef obc;
  uint64_t req_total_len = 0;
  C_ProxyChunkRead(PrimaryLogPG *p, hobject_t o, epoch_t lpr,
		   const PrimaryLogPG::ProxyReadOpRef& prd)
    : pg(p), oid(o), last_peering_reset(lpr),
      tid(0), prdop(prd), start(ceph_clock_now()), obj_op(NULL)
  {}
  void finish(int r) override {
    if (prdop->canceled)
      return;
    std::scoped_lock locker{*pg};
    if (prdop->canceled) {
      return;
    }
    if (last_peering_reset == pg->get_last_peering_reset()) {
      if (r >= 0) {
	if (!prdop->ops[op_index].outdata.length()) {
	  ceph_assert(req_total_len);
	  bufferlist list;
	  bufferptr bptr(req_total_len);
	  list.push_back(std::move(bptr));
	  prdop->ops[op_index].outdata.append(list);
	}
	ceph_assert(obj_op);
	uint64_t copy_offset;
	if (req_offset >= prdop->ops[op_index].op.extent.offset) {
	  copy_offset = req_offset - prdop->ops[op_index].op.extent.offset;
	} else {
	  copy_offset = 0;
	}
	prdop->ops[op_index].outdata.begin(copy_offset).copy_in(
          obj_op->ops[0].outdata.length(),
          obj_op->ops[0].outdata.c_str());
      }

      pg->finish_proxy_read(oid, tid, r);
      pg->osd->logger->tinc(l_osd_tier_r_lat, ceph_clock_now() - start);
      if (obj_op) {
	delete obj_op;
      }
    }
  }
};

void PrimaryLogPG::do_proxy_read(OpRequestRef op, ObjectContextRef obc)
{
  // NOTE: non-const here because the ProxyReadOp needs mutable refs to
  // stash the result in the request's OSDOp vector
  MOSDOp *m = static_cast<MOSDOp*>(op->get_nonconst_req());
  object_locator_t oloc;
  hobject_t soid;
  /* extensible tier */
  if (obc && obc->obs.exists && obc->obs.oi.has_manifest()) {
    switch (obc->obs.oi.manifest.type) {
      case object_manifest_t::TYPE_REDIRECT:
	  oloc = object_locator_t(obc->obs.oi.manifest.redirect_target);
	  soid = obc->obs.oi.manifest.redirect_target;
	  break;
      default:
	ceph_abort_msg("unrecognized manifest type");
    }
  } else {
  /* proxy */
    soid = m->get_hobj();
    oloc = object_locator_t(m->get_object_locator());
    oloc.pool = pool.info.tier_of;
  }
  unsigned flags = CEPH_OSD_FLAG_IGNORE_CACHE | CEPH_OSD_FLAG_IGNORE_OVERLAY;

  // pass through some original flags that make sense.
  //  - leave out redirection and balancing flags since we are
  //    already proxying through the primary
  //  - leave off read/write/exec flags that are derived from the op
  flags |= m->get_flags() & (CEPH_OSD_FLAG_RWORDERED |
			     CEPH_OSD_FLAG_ORDERSNAP |
			     CEPH_OSD_FLAG_ENFORCE_SNAPC |
			     CEPH_OSD_FLAG_MAP_SNAP_CLONE);

  dout(10) << __func__ << " Start proxy read for " << *m << dendl;

  ProxyReadOpRef prdop(std::make_shared<ProxyReadOp>(op, soid, m->ops));

  ObjectOperation obj_op;
  obj_op.dup(prdop->ops);

  if (pool.info.cache_mode == pg_pool_t::CACHEMODE_WRITEBACK &&
      (agent_state && agent_state->evict_mode != TierAgentState::EVICT_MODE_FULL)) {
    for (unsigned i = 0; i < obj_op.ops.size(); i++) {
      ceph_osd_op op = obj_op.ops[i].op;
      switch (op.op) {
	case CEPH_OSD_OP_READ:
	case CEPH_OSD_OP_SYNC_READ:
	case CEPH_OSD_OP_SPARSE_READ:
	case CEPH_OSD_OP_CHECKSUM:
	case CEPH_OSD_OP_CMPEXT:
	  op.flags = (op.flags | CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL) &
		       ~(CEPH_OSD_OP_FLAG_FADVISE_DONTNEED | CEPH_OSD_OP_FLAG_FADVISE_NOCACHE);
      }
    }
  }

  C_ProxyRead *fin = new C_ProxyRead(this, soid, get_last_peering_reset(),
				     prdop);
  ceph_tid_t tid = osd->objecter->read(
    soid.oid, oloc, obj_op,
    m->get_snapid(), NULL,
    flags, new C_OnFinisher(fin, osd->get_objecter_finisher(get_pg_shard())),
    &prdop->user_version,
    &prdop->data_offset,
    m->get_features());
  fin->tid = tid;
  prdop->objecter_tid = tid;
  proxyread_ops[tid] = prdop;
  in_progress_proxy_ops[soid].push_back(op);
}

void PrimaryLogPG::finish_proxy_read(hobject_t oid, ceph_tid_t tid, int r)
{
  dout(10) << __func__ << " " << oid << " tid " << tid
	   << " " << cpp_strerror(r) << dendl;

  map<ceph_tid_t, ProxyReadOpRef>::iterator p = proxyread_ops.find(tid);
  if (p == proxyread_ops.end()) {
    dout(10) << __func__ << " no proxyread_op found" << dendl;
    return;
  }
  ProxyReadOpRef prdop = p->second;
  if (tid != prdop->objecter_tid) {
    dout(10) << __func__ << " tid " << tid << " != prdop " << prdop
	     << " tid " << prdop->objecter_tid << dendl;
    return;
  }
  if (oid != prdop->soid) {
    dout(10) << __func__ << " oid " << oid << " != prdop " << prdop
	     << " soid " << prdop->soid << dendl;
    return;
  }
  proxyread_ops.erase(tid);

  map<hobject_t, list<OpRequestRef>>::iterator q = in_progress_proxy_ops.find(oid);
  if (q == in_progress_proxy_ops.end()) {
    dout(10) << __func__ << " no in_progress_proxy_ops found" << dendl;
    return;
  }
  ceph_assert(q->second.size());
  list<OpRequestRef>::iterator it = std::find(q->second.begin(),
                                              q->second.end(),
					      prdop->op);
  ceph_assert(it != q->second.end());
  OpRequestRef op = *it;
  q->second.erase(it);
  if (q->second.size() == 0) {
    in_progress_proxy_ops.erase(oid);
  } else if (std::find(q->second.begin(),
                       q->second.end(),
                       prdop->op) != q->second.end()) {
    /* multiple read case */
    dout(20) << __func__ << " " << oid << " is not completed  " << dendl;
    return;
  }

  osd->logger->inc(l_osd_tier_proxy_read);

  auto m = op->get_req<MOSDOp>();
  OpContext *ctx = new OpContext(op, m->get_reqid(), &prdop->ops, this);
  ctx->reply = new MOSDOpReply(m, 0, get_osdmap_epoch(), 0, false);
  ctx->user_at_version = prdop->user_version;
  ctx->data_off = prdop->data_offset;
  ctx->ignore_log_op_stats = true;
  complete_read_ctx(r, ctx);
}

void PrimaryLogPG::kick_proxy_ops_blocked(hobject_t& soid)
{
  map<hobject_t, list<OpRequestRef>>::iterator p = in_progress_proxy_ops.find(soid);
  if (p == in_progress_proxy_ops.end())
    return;

  list<OpRequestRef>& ls = p->second;
  dout(10) << __func__ << " " << soid << " requeuing " << ls.size() << " requests" << dendl;
  requeue_ops(ls);
  in_progress_proxy_ops.erase(p);
}

void PrimaryLogPG::cancel_proxy_read(ProxyReadOpRef prdop,
				     vector<ceph_tid_t> *tids)
{
  dout(10) << __func__ << " " << prdop->soid << dendl;
  prdop->canceled = true;

  // cancel objecter op, if we can
  if (prdop->objecter_tid) {
    tids->push_back(prdop->objecter_tid);
    for (uint32_t i = 0; i < prdop->ops.size(); i++) {
      prdop->ops[i].outdata.clear();
    }
    proxyread_ops.erase(prdop->objecter_tid);
    prdop->objecter_tid = 0;
  }
}

void PrimaryLogPG::cancel_proxy_ops(bool requeue, vector<ceph_tid_t> *tids)
{
  dout(10) << __func__ << dendl;

  // cancel proxy reads
  map<ceph_tid_t, ProxyReadOpRef>::iterator p = proxyread_ops.begin();
  while (p != proxyread_ops.end()) {
    cancel_proxy_read((p++)->second, tids);
  }

  // cancel proxy writes
  map<ceph_tid_t, ProxyWriteOpRef>::iterator q = proxywrite_ops.begin();
  while (q != proxywrite_ops.end()) {
    cancel_proxy_write((q++)->second, tids);
  }

  if (requeue) {
    map<hobject_t, list<OpRequestRef>>::iterator p =
      in_progress_proxy_ops.begin();
    while (p != in_progress_proxy_ops.end()) {
      list<OpRequestRef>& ls = p->second;
      dout(10) << __func__ << " " << p->first << " requeuing " << ls.size()
	       << " requests" << dendl;
      requeue_ops(ls);
      in_progress_proxy_ops.erase(p++);
    }
  } else {
    in_progress_proxy_ops.clear();
  }
}

struct C_ProxyWrite_Commit : public Context {
  PrimaryLogPGRef pg;
  hobject_t oid;
  epoch_t last_peering_reset;
  ceph_tid_t tid;
  PrimaryLogPG::ProxyWriteOpRef pwop;
  C_ProxyWrite_Commit(PrimaryLogPG *p, hobject_t o, epoch_t lpr,
	              const PrimaryLogPG::ProxyWriteOpRef& pw)
    : pg(p), oid(o), last_peering_reset(lpr),
      tid(0), pwop(pw)
  {}
  void finish(int r) override {
    if (pwop->canceled)
      return;
    std::scoped_lock locker{*pg};
    if (pwop->canceled) {
      return;
    }
    if (last_peering_reset == pg->get_last_peering_reset()) {
      pg->finish_proxy_write(oid, tid, r);
    }
  }
};

void PrimaryLogPG::do_proxy_write(OpRequestRef op, ObjectContextRef obc)
{
  // NOTE: non-const because ProxyWriteOp takes a mutable ref
  MOSDOp *m = static_cast<MOSDOp*>(op->get_nonconst_req());
  object_locator_t oloc;
  SnapContext snapc(m->get_snap_seq(), m->get_snaps());
  hobject_t soid;
  /* extensible tier */
  if (obc && obc->obs.exists && obc->obs.oi.has_manifest()) {
    switch (obc->obs.oi.manifest.type) {
      case object_manifest_t::TYPE_REDIRECT:
	  oloc = object_locator_t(obc->obs.oi.manifest.redirect_target);
	  soid = obc->obs.oi.manifest.redirect_target;
	  break;
      default:
	ceph_abort_msg("unrecognized manifest type");
    }
  } else {
  /* proxy */
    soid = m->get_hobj();
    oloc = object_locator_t(m->get_object_locator());
    oloc.pool = pool.info.tier_of;
  }

  unsigned flags = CEPH_OSD_FLAG_IGNORE_CACHE | CEPH_OSD_FLAG_IGNORE_OVERLAY;
  if (!(op->may_write() || op->may_cache())) {
    flags |= CEPH_OSD_FLAG_RWORDERED;
  }
  if (op->allows_returnvec()) {
    flags |= CEPH_OSD_FLAG_RETURNVEC;
  }

  dout(10) << __func__ << " Start proxy write for " << *m << dendl;

  ProxyWriteOpRef pwop(std::make_shared<ProxyWriteOp>(op, soid, m->ops, m->get_reqid()));
  pwop->ctx = new OpContext(op, m->get_reqid(), &pwop->ops, this);
  pwop->mtime = m->get_mtime();

  ObjectOperation obj_op;
  obj_op.dup(pwop->ops);

  C_ProxyWrite_Commit *fin = new C_ProxyWrite_Commit(
      this, soid, get_last_peering_reset(), pwop);
  ceph_tid_t tid = osd->objecter->mutate(
    soid.oid, oloc, obj_op, snapc,
    ceph::real_clock::from_ceph_timespec(pwop->mtime),
    flags, new C_OnFinisher(fin, osd->get_objecter_finisher(get_pg_shard())),
    &pwop->user_version, pwop->reqid);
  fin->tid = tid;
  pwop->objecter_tid = tid;
  proxywrite_ops[tid] = pwop;
  in_progress_proxy_ops[soid].push_back(op);
}

void PrimaryLogPG::do_proxy_chunked_op(OpRequestRef op, const hobject_t& missing_oid,
				       ObjectContextRef obc, bool write_ordered)
{
  MOSDOp *m = static_cast<MOSDOp*>(op->get_nonconst_req());
  OSDOp *osd_op = NULL;
  for (unsigned int i = 0; i < m->ops.size(); i++) {
    osd_op = &m->ops[i];
    uint64_t cursor = osd_op->op.extent.offset;
    uint64_t op_length = osd_op->op.extent.offset + osd_op->op.extent.length;
    uint64_t chunk_length = 0, chunk_index = 0, req_len = 0;
    object_manifest_t *manifest = &obc->obs.oi.manifest;
    map <uint64_t, map<uint64_t, uint64_t>> chunk_read;

    while (cursor < op_length) {
      chunk_index = 0;
      chunk_length = 0;
      /* find the right chunk position for cursor */
      for (auto &p : manifest->chunk_map) {
	if (p.first <= cursor && p.first + p.second.length > cursor) {
	  chunk_length = p.second.length;
	  chunk_index = p.first;
	  break;
	}
      }
      /* no index */
      if (!chunk_index && !chunk_length) {
	if (cursor == osd_op->op.extent.offset) {
	  OpContext *ctx = new OpContext(op, m->get_reqid(), &m->ops, this);
	  ctx->reply = new MOSDOpReply(m, 0, get_osdmap_epoch(), 0, false);
	  ctx->data_off = osd_op->op.extent.offset;
	  ctx->ignore_log_op_stats = true;
	  complete_read_ctx(0, ctx);
	}
	break;
      }
      uint64_t next_length = chunk_length;
      /* the size to read -> | op length | */
      /*		     | 	 a chunk   | */
      if (cursor + next_length > op_length) {
	next_length = op_length - cursor;
      }
      /* the size to read -> |   op length   | */
      /*		     | 	 a chunk | */
      if (cursor + next_length > chunk_index + chunk_length) {
	next_length = chunk_index + chunk_length - cursor;
      }

      chunk_read[cursor] = {{chunk_index, next_length}};
      cursor += next_length;
    }

    req_len = cursor - osd_op->op.extent.offset;
    for (auto &p : chunk_read) {
      auto chunks = p.second.begin();
      dout(20) << __func__ << " chunk_index: " << chunks->first
	      << " next_length: " << chunks->second << " cursor: "
	      << p.first << dendl;
      do_proxy_chunked_read(op, obc, i, chunks->first, p.first, chunks->second, req_len, write_ordered);
    }
  }
}

struct RefCountCallback : public Context {
public:
  PrimaryLogPG::OpContext *ctx;
  OSDOp& osd_op;
  bool requeue = false;

  RefCountCallback(PrimaryLogPG::OpContext *ctx, OSDOp &osd_op)
    : ctx(ctx), osd_op(osd_op) {}
  void finish(int r) override {
    // NB: caller must already have pg->lock held
    ctx->obc->stop_block();
    ctx->pg->kick_object_context_blocked(ctx->obc);
    if (r >= 0) {
      osd_op.rval = 0;
      ctx->pg->execute_ctx(ctx);
    } else {
       // on cancel simply toss op out,
       // or requeue as requested
      if (r != -ECANCELED) {
        if (ctx->op)
          ctx->pg->osd->reply_op_error(ctx->op, r);
      } else if (requeue) {
        if (ctx->op)
          ctx->pg->requeue_op(ctx->op);
      }
      ctx->pg->close_op_ctx(ctx);
    }
  }
  void set_requeue(bool rq) {
    requeue = rq;
  }
};

struct SetManifestFinisher : public PrimaryLogPG::OpFinisher {
  OSDOp& osd_op;

  explicit SetManifestFinisher(OSDOp& osd_op) : osd_op(osd_op) {
  }

  int execute() override {
    return osd_op.rval;
  }
};

struct C_SetManifestRefCountDone : public Context {
  PrimaryLogPGRef pg;
  hobject_t soid;
  uint64_t offset;
  ceph_tid_t tid = 0;
  C_SetManifestRefCountDone(PrimaryLogPG *p,
    hobject_t soid, uint64_t offset) :
          pg(p), soid(soid), offset(offset) {}
  void finish(int r) override {
    if (r == -ECANCELED)
      return;
    std::scoped_lock locker{*pg};
    pg->finish_set_manifest_refcount(soid, r, tid, offset);
  }
};

struct C_SetDedupChunks : public Context {
  PrimaryLogPGRef pg;
  hobject_t oid;
  epoch_t last_peering_reset;
  ceph_tid_t tid;
  uint64_t offset;

  C_SetDedupChunks(PrimaryLogPG *p, hobject_t o, epoch_t lpr, uint64_t offset)
    : pg(p), oid(o), last_peering_reset(lpr),
      tid(0), offset(offset)
  {}
  void finish(int r) override {
    if (r == -ECANCELED)
      return;
    std::scoped_lock locker{*pg};
    if (last_peering_reset != pg->get_last_peering_reset()) {
      return;
    }
    pg->finish_set_dedup(oid, r, tid, offset);
  }
};

void PrimaryLogPG::cancel_manifest_ops(bool requeue, vector<ceph_tid_t> *tids)
{
  dout(10) << __func__ << dendl;
  auto p = manifest_ops.begin();
  while (p != manifest_ops.end()) {
    auto mop = p->second;
    // cancel objecter op, if we can
    if (mop->objecter_tid) {
      tids->push_back(mop->objecter_tid);
      mop->objecter_tid = 0;
    } else if (!mop->tids.empty()) {
      for (auto &p : mop->tids) {
	tids->push_back(p.second);
      }
    }
    if (mop->cb) {
      mop->cb->set_requeue(requeue);
      mop->cb->complete(-ECANCELED);
    }
    manifest_ops.erase(p++);
  }
}

int PrimaryLogPG::get_manifest_ref_count(ObjectContextRef obc, std::string& fp_oid, OpRequestRef op) 
{
  int cnt = 0;
  // head
  for (auto &p : obc->obs.oi.manifest.chunk_map) {
    if (p.second.oid.oid.name == fp_oid) {
      cnt++;
    }
  }
  // snap
  SnapSet& ss = obc->ssc->snapset;
  const OSDMapRef& osdmap = get_osdmap();
  for (vector<snapid_t>::const_reverse_iterator p = ss.clones.rbegin();
      p != ss.clones.rend();
      ++p) {
    object_ref_delta_t refs;
    ObjectContextRef obc_l = nullptr;
    ObjectContextRef obc_g = nullptr;
    hobject_t clone_oid = obc->obs.oi.soid;
    clone_oid.snap = *p;
    if (osdmap->in_removed_snaps_queue(info.pgid.pgid.pool(), *p)) {
      return -EBUSY;
    }
    if (is_unreadable_object(clone_oid)) {
      dout(10) << __func__ << ": " << clone_oid
	       << " is unreadable. Need to wait for recovery" << dendl;
      wait_for_unreadable_object(clone_oid, op);
      return -EAGAIN;
    }
    ObjectContextRef clone_obc = get_object_context(clone_oid, false);
    if (!clone_obc) {
      break;
    }
    if (recover_adjacent_clones(clone_obc, op)) {
      return -EAGAIN;
    }
    get_adjacent_clones(clone_obc, obc_l, obc_g);
    clone_obc->obs.oi.manifest.calc_refs_to_inc_on_set(
      obc_g ? &(obc_g->obs.oi.manifest) : nullptr ,
      nullptr,
      refs);
    for (auto p = refs.begin(); p != refs.end(); ++p) {
      if (p->first.oid.name == fp_oid && p->second > 0) {
	cnt += p->second;
      }
    }
  }

  return cnt;
}

snapid_t PrimaryLogPG::do_recover_adjacent_clones(ObjectContextRef obc, OpRequestRef op) 
{
  ceph_assert(op);
  const SnapSet& snapset = obc->ssc->snapset;
  auto s = std::find(snapset.clones.begin(), snapset.clones.end(), obc->obs.oi.soid.snap);
  auto is_unreadable_snap = [this, obc, &snapset, op](auto iter) -> snapid_t {
    hobject_t cid = obc->obs.oi.soid;
    cid.snap = (iter == snapset.clones.end()) ? snapid_t(CEPH_NOSNAP) : *iter;
    if (is_unreadable_object(cid)) {
      dout(10) << __func__ << ": clone " << cid
	       << " is unreadable, waiting" << dendl;
      wait_for_unreadable_object(cid, op);
      return cid.snap;
    }
    return snapid_t();
  };
  if (s != snapset.clones.begin()) {
    snapid_t snap = is_unreadable_snap(s - 1);
    if (snap != snapid_t()) {
      return snap;
    }
  }
  if (s != snapset.clones.end()) {
    snapid_t snap = is_unreadable_snap(s + 1);
    if (snap != snapid_t()) {
      return snap;
    }
  }
  return snapid_t();
}

bool PrimaryLogPG::recover_adjacent_clones(ObjectContextRef obc, OpRequestRef op)
{
  if (!obc->ssc || !obc->ssc->snapset.clones.size()) {
    return false;
  }
  MOSDOp *m = static_cast<MOSDOp*>(op->get_nonconst_req());
  bool has_manifest_op = false;
  for (auto& osd_op : m->ops) {
    if (osd_op.op.op == CEPH_OSD_OP_ROLLBACK) {
      return false;
    } else if (osd_op.op.op == CEPH_OSD_OP_SET_CHUNK) {
      has_manifest_op = true;	
      break;
    }
  }
  if (!obc->obs.oi.manifest.is_chunked() && !has_manifest_op) {
    return false;
  }
  return do_recover_adjacent_clones(obc, op) != snapid_t();
}

ObjectContextRef PrimaryLogPG::get_prev_clone_obc(ObjectContextRef obc)
{
  auto s = std::find(obc->ssc->snapset.clones.begin(), obc->ssc->snapset.clones.end(),
		    obc->obs.oi.soid.snap);
  if (s != obc->ssc->snapset.clones.begin()) {
    auto s_iter = s - 1;
    hobject_t cid = obc->obs.oi.soid;
    object_ref_delta_t refs;
    cid.snap = *s_iter;
    ObjectContextRef cobc = get_object_context(cid, false, NULL);
    ceph_assert(cobc);
    return cobc;
  }
  return nullptr;
}

void PrimaryLogPG::dec_refcount(const hobject_t& soid, const object_ref_delta_t& refs)
{
  for (auto p = refs.begin(); p != refs.end(); ++p) {
    int dec_ref_count = p->second;
    ceph_assert(dec_ref_count < 0);
    while (dec_ref_count < 0) {
      dout(10) << __func__ << ": decrement reference on offset oid: " << p->first << dendl;
      refcount_manifest(soid, p->first, 
			refcount_t::DECREMENT_REF, NULL, std::nullopt);
      dec_ref_count++;
    }
  }
}


void PrimaryLogPG::get_adjacent_clones(ObjectContextRef src_obc, 
				       ObjectContextRef& _l, ObjectContextRef& _g) 
{
  const SnapSet& snapset = src_obc->ssc->snapset;
  const object_info_t& oi = src_obc->obs.oi;

  auto get_context = [this, &oi, &snapset](auto iter)
    -> ObjectContextRef {
    hobject_t cid = oi.soid;
    cid.snap = (iter == snapset.clones.end()) ? snapid_t(CEPH_NOSNAP) : *iter;
    ObjectContextRef obc = get_object_context(cid, false, NULL);
    ceph_assert(obc);
    return obc;
  };

  // check adjacent clones
  auto s = std::find(snapset.clones.begin(), snapset.clones.end(), oi.soid.snap);

  // We *must* find the clone iff it's not head,
  // let s == snapset.clones.end() mean head
  ceph_assert((s == snapset.clones.end()) == oi.soid.is_head());

  if (s != snapset.clones.begin()) {
    _l = get_context(s - 1);
  }

  if (s != snapset.clones.end()) {
    _g = get_context(s + 1);
  }
}

bool PrimaryLogPG::inc_refcount_by_set(OpContext* ctx, object_manifest_t& set_chunk,
				       OSDOp& osd_op)
{
  object_ref_delta_t refs;
  ObjectContextRef obc_l, obc_g;
  get_adjacent_clones(ctx->obc, obc_l, obc_g);
  set_chunk.calc_refs_to_inc_on_set(
    obc_l ? &(obc_l->obs.oi.manifest) : nullptr,
    obc_g ? &(obc_g->obs.oi.manifest) : nullptr,
    refs);
  bool need_inc_ref = false;
  if (!refs.is_empty()) {
    ManifestOpRef mop(std::make_shared<ManifestOp>(ctx->obc, nullptr));
    for (auto c : set_chunk.chunk_map) {
      auto p = refs.find(c.second.oid);
      if (p == refs.end()) {
	continue;
      }

      int inc_ref_count = p->second;
      if (inc_ref_count > 0) {
	/*
	 * In set-chunk case, the first thing we should do is to increment
	 * the reference the targe object has prior to update object_manifest in object_info_t.
	 * So, call directly refcount_manifest.
	 */
	auto target_oid = p->first;
	auto offset = c.first;
	auto length = c.second.length;	
	auto* fin = new C_SetManifestRefCountDone(this, ctx->obs->oi.soid, offset);
	ceph_tid_t tid = refcount_manifest(ctx->obs->oi.soid, target_oid,
					    refcount_t::INCREMENT_REF, fin, std::nullopt);
	fin->tid = tid;
	mop->chunks[target_oid] = make_pair(offset, length);
	mop->num_chunks++;
	mop->tids[offset] = tid;

	if (!ctx->obc->is_blocked()) {
          dout(15) << fmt::format("{}: blocking object on rc: tid:{}", __func__, tid) << dendl;
	  ctx->obc->start_block();
	}
	need_inc_ref = true;
      } else if (inc_ref_count < 0) {
	hobject_t src = ctx->obs->oi.soid;
	hobject_t tgt = p->first;
	ctx->register_on_commit(
	    [src, tgt, this](){
	      refcount_manifest(src, tgt, refcount_t::DECREMENT_REF, NULL, std::nullopt);
	    });
      }
    }
    if (mop->tids.size()) {
      mop->cb = new RefCountCallback(ctx, osd_op);
      manifest_ops[ctx->obs->oi.soid] = mop;
      manifest_ops[ctx->obs->oi.soid]->op = ctx->op;
    }
  }

  return need_inc_ref;
}

void PrimaryLogPG::update_chunk_map_by_dirty(OpContext* ctx) {
  /* 
   * We should consider two cases here: 
   *  1) just modification: This created dirty regions, but didn't update chunk_map.
   *  2) rollback: In rollback, head will be converted to the clone the rollback targets.
   *  		Also, rollback already updated chunk_map.  
   * So, we should do here is to check whether chunk_map is updated and the clean_region has dirty regions.
   * In case of the rollback, chunk_map doesn't need to be clear
   */
  for (auto &p : ctx->obs->oi.manifest.chunk_map) {
    if (!ctx->clean_regions.is_clean_region(p.first, p.second.length)) {
      ctx->new_obs.oi.manifest.chunk_map.erase(p.first);
      if (ctx->new_obs.oi.manifest.chunk_map.empty()) {
	ctx->new_obs.oi.manifest.type = object_manifest_t::TYPE_NONE;
	ctx->new_obs.oi.clear_flag(object_info_t::FLAG_MANIFEST);
	ctx->delta_stats.num_objects_manifest--;
      }
    }
  }
}

void PrimaryLogPG::dec_refcount_by_dirty(OpContext* ctx)
{
  object_ref_delta_t refs;
  ObjectContextRef cobc = nullptr;
  ObjectContextRef obc = ctx->obc;
  // Look over previous snapshot, then figure out whether updated chunk needs to be deleted
  cobc = get_prev_clone_obc(obc);
  obc->obs.oi.manifest.calc_refs_to_drop_on_modify(
    cobc ? &cobc->obs.oi.manifest : nullptr,
    ctx->clean_regions,
    refs);
  if (!refs.is_empty()) {
    hobject_t soid = obc->obs.oi.soid;
    ctx->register_on_commit(
      [soid, this, refs](){
	dec_refcount(soid, refs);
      });
  }
}

void PrimaryLogPG::dec_all_refcount_manifest(const object_info_t& oi, OpContext* ctx)
{
  ceph_assert(oi.has_manifest());
  ceph_assert(ctx->obc->ssc);

  if (oi.manifest.is_chunked()) {
    object_ref_delta_t refs;
    ObjectContextRef obc_l, obc_g, obc;
    /* in trim_object, oi and ctx can have different oid */
    obc = get_object_context(oi.soid, false, NULL);
    ceph_assert(obc);
    get_adjacent_clones(obc, obc_l, obc_g);
    oi.manifest.calc_refs_to_drop_on_removal(
      obc_l ? &(obc_l->obs.oi.manifest) : nullptr,
      obc_g ? &(obc_g->obs.oi.manifest) : nullptr,
      refs);

    if (!refs.is_empty()) {
      /* dec_refcount will use head object anyway */
      hobject_t soid = ctx->obc->obs.oi.soid;
      ctx->register_on_commit(
	[soid, this, refs](){
	  dec_refcount(soid, refs);
	});
    }
  } else if (oi.manifest.is_redirect() &&
	     oi.test_flag(object_info_t::FLAG_REDIRECT_HAS_REFERENCE)) {
    ctx->register_on_commit(
      [oi, this](){
	refcount_manifest(oi.soid, oi.manifest.redirect_target, 
			  refcount_t::DECREMENT_REF, NULL, std::nullopt);
      });
  }
}

ceph_tid_t PrimaryLogPG::refcount_manifest(hobject_t src_soid, hobject_t tgt_soid, refcount_t type,
                                     Context *cb, std::optional<bufferlist> chunk)
{
  unsigned flags = CEPH_OSD_FLAG_IGNORE_CACHE | CEPH_OSD_FLAG_IGNORE_OVERLAY |
                   CEPH_OSD_FLAG_RWORDERED;

  dout(10) << __func__ << " Start refcount from " << src_soid
           << " to " << tgt_soid << dendl;

  ObjectOperation obj_op;
  bufferlist in;
  if (type == refcount_t::INCREMENT_REF) {
    cls_cas_chunk_get_ref_op call;
    call.source = src_soid.get_head();
    ::encode(call, in);
    obj_op.call("cas", "chunk_get_ref", in);
  } else if (type == refcount_t::DECREMENT_REF) {
    cls_cas_chunk_put_ref_op call;
    call.source = src_soid.get_head();
    ::encode(call, in);
    obj_op.call("cas", "chunk_put_ref", in);
  } else if (type == refcount_t::CREATE_OR_GET_REF) {
    cls_cas_chunk_create_or_get_ref_op get_call;
    get_call.source = src_soid.get_head();
    ceph_assert(chunk);
    get_call.data = std::move(*chunk);
    ::encode(get_call, in);
    obj_op.call("cas", "chunk_create_or_get_ref", in);
  } else {
    ceph_assert(0 == "unrecognized type");
  }

  Context *c = nullptr;
  if (cb) {
    c = new C_OnFinisher(cb, osd->get_objecter_finisher(get_pg_shard()));
  }

  object_locator_t oloc(tgt_soid);
  ObjectContextRef src_obc = get_object_context(src_soid, false, NULL);
  ceph_assert(src_obc);
  auto tid = osd->objecter->mutate(
    tgt_soid.oid, oloc, obj_op, SnapContext(),
    ceph::real_clock::from_ceph_timespec(src_obc->obs.oi.mtime),
    flags, c);
  return tid;
}

void PrimaryLogPG::do_proxy_chunked_read(OpRequestRef op, ObjectContextRef obc, int op_index,
					 uint64_t chunk_index, uint64_t req_offset, uint64_t req_length,
					 uint64_t req_total_len, bool write_ordered)
{
  MOSDOp *m = static_cast<MOSDOp*>(op->get_nonconst_req());
  object_manifest_t *manifest = &obc->obs.oi.manifest;
  if (!manifest->chunk_map.count(chunk_index)) {
    return;
  }
  uint64_t chunk_length = manifest->chunk_map[chunk_index].length;
  hobject_t soid = manifest->chunk_map[chunk_index].oid;
  hobject_t ori_soid = m->get_hobj();
  object_locator_t oloc(soid);
  unsigned flags = CEPH_OSD_FLAG_IGNORE_CACHE | CEPH_OSD_FLAG_IGNORE_OVERLAY;
  if (write_ordered) {
    flags |= CEPH_OSD_FLAG_RWORDERED;
  }

  if (!chunk_length || soid == hobject_t()) {
    return;
  }

  /* same as do_proxy_read() */
  flags |= m->get_flags() & (CEPH_OSD_FLAG_RWORDERED |
			     CEPH_OSD_FLAG_ORDERSNAP |
			     CEPH_OSD_FLAG_ENFORCE_SNAPC |
			     CEPH_OSD_FLAG_MAP_SNAP_CLONE);

  dout(10) << __func__ << " Start do chunk proxy read for " << *m
	   << " index: " << op_index << " oid: " << soid.oid.name << " req_offset: " << req_offset
	   << " req_length: " << req_length << dendl;

  ProxyReadOpRef prdop(std::make_shared<ProxyReadOp>(op, ori_soid, m->ops));

  ObjectOperation *pobj_op = new ObjectOperation;
  OSDOp &osd_op = pobj_op->add_op(m->ops[op_index].op.op);

  if (chunk_index <= req_offset) {
    osd_op.op.extent.offset = manifest->chunk_map[chunk_index].offset + req_offset - chunk_index;
  } else {
    ceph_abort_msg("chunk_index > req_offset");
  }
  osd_op.op.extent.length = req_length;

  ObjectOperation obj_op;
  obj_op.dup(pobj_op->ops);

  C_ProxyChunkRead *fin = new C_ProxyChunkRead(this, ori_soid, get_last_peering_reset(),
					       prdop);
  fin->obj_op = pobj_op;
  fin->op_index = op_index;
  fin->req_offset = req_offset;
  fin->obc = obc;
  fin->req_total_len = req_total_len;

  ceph_tid_t tid = osd->objecter->read(
    soid.oid, oloc, obj_op,
    m->get_snapid(), NULL,
    flags, new C_OnFinisher(fin, osd->get_objecter_finisher(get_pg_shard())),
    &prdop->user_version,
    &prdop->data_offset,
    m->get_features());
  fin->tid = tid;
  prdop->objecter_tid = tid;
  proxyread_ops[tid] = prdop;
  in_progress_proxy_ops[ori_soid].push_back(op);
}

bool PrimaryLogPG::can_proxy_chunked_read(OpRequestRef op, ObjectContextRef obc)
{
  MOSDOp *m = static_cast<MOSDOp*>(op->get_nonconst_req());
  OSDOp *osd_op = NULL;
  bool ret = true;
  for (unsigned int i = 0; i < m->ops.size(); i++) {
    osd_op = &m->ops[i];
    ceph_osd_op op = osd_op->op;
    switch (op.op) {
      case CEPH_OSD_OP_READ:
      case CEPH_OSD_OP_SYNC_READ: {
	uint64_t cursor = osd_op->op.extent.offset;
	uint64_t remain = osd_op->op.extent.length;

	/* requested chunks exist in chunk_map ? */
	for (auto &p : obc->obs.oi.manifest.chunk_map) {
	  if (p.first <= cursor && p.first + p.second.length > cursor) {
	    if (!p.second.is_missing()) {
	      return false;
	    }
	    if (p.second.length >= remain) {
	      remain = 0;
	      break;
	    } else {
	      remain = remain - p.second.length;
	    }
	    cursor += p.second.length;
	  }
	}

	if (remain) {
	  dout(20) << __func__ << " requested chunks don't exist in chunk_map " << dendl;
	  return false;
	}
	continue;
      }
      default:
	return false;
    }
  }
  return ret;
}

void PrimaryLogPG::finish_proxy_write(hobject_t oid, ceph_tid_t tid, int r)
{
  dout(10) << __func__ << " " << oid << " tid " << tid
	   << " " << cpp_strerror(r) << dendl;

  map<ceph_tid_t, ProxyWriteOpRef>::iterator p = proxywrite_ops.find(tid);
  if (p == proxywrite_ops.end()) {
    dout(10) << __func__ << " no proxywrite_op found" << dendl;
    return;
  }
  ProxyWriteOpRef pwop = p->second;
  ceph_assert(tid == pwop->objecter_tid);
  ceph_assert(oid == pwop->soid);

  proxywrite_ops.erase(tid);

  map<hobject_t, list<OpRequestRef> >::iterator q = in_progress_proxy_ops.find(oid);
  if (q == in_progress_proxy_ops.end()) {
    dout(10) << __func__ << " no in_progress_proxy_ops found" << dendl;
    delete pwop->ctx;
    pwop->ctx = NULL;
    return;
  }
  list<OpRequestRef>& in_progress_op = q->second;
  ceph_assert(in_progress_op.size());
  list<OpRequestRef>::iterator it = std::find(in_progress_op.begin(),
                                              in_progress_op.end(),
					      pwop->op);
  ceph_assert(it != in_progress_op.end());
  in_progress_op.erase(it);
  if (in_progress_op.size() == 0) {
    in_progress_proxy_ops.erase(oid);
  } else if (std::find(in_progress_op.begin(),
                        in_progress_op.end(),
                        pwop->op) != in_progress_op.end()) {
    if (pwop->ctx)
      delete pwop->ctx;
    pwop->ctx = NULL;
    dout(20) << __func__ << " " << oid << " tid " << tid
            << " in_progress_op size: "
            << in_progress_op.size() << dendl;
    return;
  }

  osd->logger->inc(l_osd_tier_proxy_write);

  auto m = pwop->op->get_req<MOSDOp>();
  ceph_assert(m != NULL);

  if (!pwop->sent_reply) {
    // send commit.
    ceph_assert(pwop->ctx->reply == nullptr);
    MOSDOpReply *reply = new MOSDOpReply(m, r, get_osdmap_epoch(), 0,
					 true /* we claim it below */);
    reply->set_reply_versions(eversion_t(), pwop->user_version);
    reply->add_flags(CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK);
    reply->claim_op_out_data(pwop->ops);
    dout(10) << " sending commit on " << pwop << " " << reply << dendl;
    osd->send_message_osd_client(reply, m->get_connection());
    pwop->sent_reply = true;
    pwop->ctx->op->mark_commit_sent();
  }

  delete pwop->ctx;
  pwop->ctx = NULL;
}

void PrimaryLogPG::cancel_proxy_write(ProxyWriteOpRef pwop,
				      vector<ceph_tid_t> *tids)
{
  dout(10) << __func__ << " " << pwop->soid << dendl;
  pwop->canceled = true;

  // cancel objecter op, if we can
  if (pwop->objecter_tid) {
    tids->push_back(pwop->objecter_tid);
    delete pwop->ctx;
    pwop->ctx = NULL;
    proxywrite_ops.erase(pwop->objecter_tid);
    pwop->objecter_tid = 0;
  }
}

class PromoteCallback: public PrimaryLogPG::CopyCallback {
  ObjectContextRef obc;
  PrimaryLogPG *pg;
  utime_t start;
public:
  PromoteCallback(ObjectContextRef obc_, PrimaryLogPG *pg_)
    : obc(obc_),
      pg(pg_),
      start(ceph_clock_now()) {}

  void finish(PrimaryLogPG::CopyCallbackResults results) override {
    PrimaryLogPG::CopyResults *results_data = results.get<1>();
    int r = results.get<0>();
    if (obc->obs.oi.has_manifest() && obc->obs.oi.manifest.is_chunked()) {
      pg->finish_promote_manifest(r, results_data, obc);
    } else {
      pg->finish_promote(r, results_data, obc);
    }
    pg->osd->logger->tinc(l_osd_tier_promote_lat, ceph_clock_now() - start);
  }
};

class PromoteManifestCallback: public PrimaryLogPG::CopyCallback {
  ObjectContextRef obc;
  PrimaryLogPG *pg;
  utime_t start;
  PrimaryLogPG::OpContext *ctx;
  PrimaryLogPG::CopyCallbackResults promote_results;
public:
  PromoteManifestCallback(ObjectContextRef obc_, PrimaryLogPG *pg_, PrimaryLogPG::OpContext *ctx)
    : obc(obc_),
      pg(pg_),
      start(ceph_clock_now()), ctx(ctx) {}

  void finish(PrimaryLogPG::CopyCallbackResults results) override {
    PrimaryLogPG::CopyResults *results_data = results.get<1>();
    int r = results.get<0>();
    promote_results = results;
    if (obc->obs.oi.has_manifest() && obc->obs.oi.manifest.is_redirect()) {
      ctx->user_at_version = results_data->user_version;
    }
    if (r >= 0) {
      ctx->pg->execute_ctx(ctx);
    } else {
      if (r != -ECANCELED) { 
	if (ctx->op)
	  ctx->pg->osd->reply_op_error(ctx->op, r);
      } else if (results_data->should_requeue) {
	if (ctx->op)
	  ctx->pg->requeue_op(ctx->op);
      }
      ctx->pg->close_op_ctx(ctx);
    }
    pg->osd->logger->tinc(l_osd_tier_promote_lat, ceph_clock_now() - start);
  }
  friend struct PromoteFinisher;
};

struct PromoteFinisher : public PrimaryLogPG::OpFinisher {
  PromoteManifestCallback *promote_callback;

  explicit PromoteFinisher(PromoteManifestCallback *promote_callback)
    : promote_callback(promote_callback) {
  }

  int execute() override {
    if (promote_callback->ctx->obc->obs.oi.manifest.is_redirect()) {
      promote_callback->ctx->pg->finish_promote(promote_callback->promote_results.get<0>(),
						promote_callback->promote_results.get<1>(),
						promote_callback->obc);
    } else if (promote_callback->ctx->obc->obs.oi.manifest.is_chunked()) {
      promote_callback->ctx->pg->finish_promote_manifest(promote_callback->promote_results.get<0>(),
						promote_callback->promote_results.get<1>(),
						promote_callback->obc);
    } else {
      ceph_abort_msg("unrecognized manifest type");
    }
    return 0;
  }
};

void PrimaryLogPG::promote_object(ObjectContextRef obc,
				  const hobject_t& missing_oid,
				  const object_locator_t& oloc,
				  OpRequestRef op,
				  ObjectContextRef *promote_obc)
{
  hobject_t hoid = obc ? obc->obs.oi.soid : missing_oid;
  ceph_assert(hoid != hobject_t());
  if (m_scrubber->write_blocked_by_scrub(hoid)) {
    dout(10) << __func__ << " " << hoid
	     << " blocked by scrub" << dendl;
    if (op) {
      waiting_for_scrub.push_back(op);
      op->mark_delayed("waiting for scrub");
      dout(10) << __func__ << " " << hoid
	       << " placing op in waiting_for_scrub" << dendl;
    } else {
      dout(10) << __func__ << " " << hoid
	       << " no op, dropping on the floor" << dendl;
    }
    return;
  }
  if (op && !check_laggy_requeue(op)) {
    return;
  }
  if (!obc) { // we need to create an ObjectContext
    ceph_assert(missing_oid != hobject_t());
    obc = get_object_context(missing_oid, true);
  }
  if (promote_obc)
    *promote_obc = obc;

  /*
   * Before promote complete, if there are  proxy-reads for the object,
   * for this case we don't use DONTNEED.
   */
  unsigned src_fadvise_flags = LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL;
  map<hobject_t, list<OpRequestRef>>::iterator q = in_progress_proxy_ops.find(obc->obs.oi.soid);
  if (q == in_progress_proxy_ops.end()) {
    src_fadvise_flags |= LIBRADOS_OP_FLAG_FADVISE_DONTNEED;
  }

  CopyCallback *cb;
  object_locator_t my_oloc;
  hobject_t src_hoid;
  if (!obc->obs.oi.has_manifest()) {
    my_oloc = oloc;
    my_oloc.pool = pool.info.tier_of;
    src_hoid = obc->obs.oi.soid;
    cb = new PromoteCallback(obc, this);
  } else {
    if (obc->obs.oi.manifest.is_chunked()) {
      src_hoid = obc->obs.oi.soid;
      cb = new PromoteCallback(obc, this);
    } else if (obc->obs.oi.manifest.is_redirect()) {
      object_locator_t src_oloc(obc->obs.oi.manifest.redirect_target);
      my_oloc = src_oloc;
      src_hoid = obc->obs.oi.manifest.redirect_target;
      cb = new PromoteCallback(obc, this);
    } else {
      ceph_abort_msg("unrecognized manifest type");
    }
  }

  unsigned flags = CEPH_OSD_COPY_FROM_FLAG_IGNORE_OVERLAY |
                   CEPH_OSD_COPY_FROM_FLAG_IGNORE_CACHE |
                   CEPH_OSD_COPY_FROM_FLAG_MAP_SNAP_CLONE |
                   CEPH_OSD_COPY_FROM_FLAG_RWORDERED;
  start_copy(cb, obc, src_hoid, my_oloc, 0, flags,
	     obc->obs.oi.soid.snap == CEPH_NOSNAP,
	     src_fadvise_flags, 0);

  ceph_assert(obc->is_blocked());

  if (op)
    wait_for_blocked_object(obc->obs.oi.soid, op);

  recovery_state.update_stats(
    [](auto &history, auto &stats) {
      stats.stats.sum.num_promote++;
      return false;
    });
}

void PrimaryLogPG::execute_ctx(OpContext *ctx)
{
  // 执行已经完成对象定位和加锁的 OSDOp。读操作在本函数内完成；
  // 写操作先构造 PGTransaction，再交给 RepGather 复制到副本并等待提交。
  FUNCTRACE(cct);
  dout(10) << __func__ << " " << ctx << dendl;
  // COPY_FROM 等异步操作完成后会使用同一个 ctx 重新执行本函数。
  // 丢弃上一次未提交的对象状态草稿，从 OBC 当前状态重新构造本次事务。
  ctx->reset_obs(ctx->obc);
  ctx->update_log_only = false; // finish_copyfrom() 重入时重新计算该标志
  OpRequestRef op = ctx->op;
  auto m = op->get_req<MOSDOp>();
  ObjectContextRef obc = ctx->obc;
  const hobject_t& soid = obc->obs.oi.soid;

  // 本函数在事务真正 apply 前可能执行多次，因此必须保持幂等；
  // 每次重建事务，丢弃前一次未提交的操作，不能在旧 PGTransaction 上重复追加。
  ctx->op_t.reset(new PGTransaction);

  if (op->may_write() || op->may_cache()) {
    // snapc 是 Snapshot Context（快照上下文），描述一次写操作发生时，哪些快照仍然存在。
    // 阶段 1：确定写操作使用的快照上下文。pool snap 模式默认采用服务端 snapc；
    // 当客户端修改对象时，OSD 根据 snapc 判断是否需要先保留旧数据
    if (!(m->has_flag(CEPH_OSD_FLAG_ENFORCE_SNAPC)) &&
	pool.info.is_pool_snaps_mode()) {
      // ENFORCE_SNAPC：即使处于 pool snapshot 模式，也强制使用客户端提交的 snapc。
      // use pool's snapc
      ctx->snapc = pool.snapc;
    } else {
      // Self-managed snapshot 模式：使用客户端请求携带的快照序列和快照列表。
      // client specified snapc
      ctx->snapc.seq = m->get_snap_seq();
      ctx->snapc.snaps = m->get_snaps();
      // 去掉当前 PG/pool 已不再有效的快照，避免为无效 snap 创建 clone。
      filter_snapc(ctx->snapc.snaps);
    }
    // 表示客户端的 snapc 比对象当前的 SnapSet 还旧。
    // 继续写可能错误地创建 clone，因此返回 -EOLDSNAPC，要求客户端使用更新的快照上下文重试。
    if ((m->has_flag(CEPH_OSD_FLAG_ORDERSNAP)) &&
	ctx->snapc.seq < obc->ssc->snapset.seq) {
      dout(10) << " ORDERSNAP flag set and snapc seq " << ctx->snapc.seq
	       << " < snapset seq " << obc->ssc->snapset.seq
	       << " on " << obc->obs.oi.soid << dendl;
      reply_ctx(ctx, -EOLDSNAPC);
      return;
    }

    // 为本次修改预分配 PG log 版本，并保留客户端指定的 mtime。
    ctx->at_version = get_next_version();
    ctx->mtime = m->get_mtime();

    dout(10) << __func__ << " " << soid << " " << *ctx->ops
	     << " ov " << obc->obs.oi.version << " av " << ctx->at_version
	     << " snapc " << ctx->snapc
	     << " snapset " << obc->ssc->snapset
	     << dendl;
  } else {
    dout(10) << __func__ << " " << soid << " " << *ctx->ops
	     << " ov " << obc->obs.oi.version
	     << dendl;
  }

  // copy-from 重入时可能已经设置 user_at_version，因此只在首次执行时继承对象版本。
  if (!ctx->user_at_version)
    ctx->user_at_version = obc->obs.oi.user_version;
  dout(30) << __func__ << " user_at_version " << ctx->user_at_version << dendl;

  {
#ifdef WITH_LTTNG
    osd_reqid_t reqid = ctx->op->get_reqid();
#endif
    tracepoint(osd, prepare_tx_enter, reqid.name._type,
        reqid.name._num, reqid.tid, reqid.inc);
  }
  // 阶段 2：逐个执行 OSDOp。读操作填充返回数据；
  // 写操作只在 op_t 中构造事务和 PG log 变更，此时尚未写入 ObjectStore。
  int result = prepare_transaction(ctx);

  {
#ifdef WITH_LTTNG
    osd_reqid_t reqid = ctx->op->get_reqid();
#endif
    tracepoint(osd, prepare_tx_exit, reqid.name._type,
        reqid.name._num, reqid.tid, reqid.inc);
  }

  bool pending_async_reads = !ctx->pending_async_reads.empty();
  if (result == -EINPROGRESS || pending_async_reads) {
    // copy-from 等子操作尚未完成，或 EC overwrite 需要先异步读取旧 shard。
    // ctx 必须保留，完成回调会重新进入后续执行流程。
    if (pending_async_reads) {
      ceph_assert(pool.info.is_erasure());
      // 队列同时持有请求和 ctx，确保异步读完成前二者都不会被释放。
      in_progress_async_reads.push_back(make_pair(op, ctx));
      ctx->start_async_reads(this);
    }
    return;
  }

  if (result == -EAGAIN) {
    // prepare_transaction() 已经安排请求稍后重试；
    // 本次上下文不再使用，释放对象锁及临时事务，但不在这里回复客户端。
    close_op_ctx(ctx);
    return;
  }

  bool ignore_out_data = false;
  if (!ctx->op_t->empty() &&
      op->may_write() &&
      result >= 0) {
    // 写事务已成功构造。支持 returnvec 的新客户端可接收每个子操作的返回值，
    // 但仍需限制单个返回缓冲区，防止写回复占用无界内存。
    if (ctx->op->allows_returnvec()) {
      // enforce reasonable bound on the return buffer sizes
      for (auto& i : *ctx->ops) {
	if (i.outdata.length() > cct->_conf->osd_max_write_op_reply_len) {
	  dout(10) << __func__ << " op " << i << " outdata overflow" << dendl;
	  result = -EOVERFLOW;  // overall result is overflow
	  i.rval = -EOVERFLOW;
	  i.outdata.clear();
	}
      }
    } else {
      // 旧客户端不理解 write returnvec：保持历史行为，只返回整体成功并忽略数据。
      ignore_out_data = true;
      result = 0;
    }
  }

  // 阶段 3：先创建统一回复。读路径会立即发送；
  // 写路径将其保存在 ctx 中，等事务提交后再补充 ACK/ONDISK 标志并发送。
  ctx->reply = new MOSDOpReply(m, result, get_osdmap_epoch(), 0,
			       ignore_out_data);
  dout(20) << __func__ << " alloc reply " << ctx->reply
	   << " result " << result << dendl;

  // 没有待提交事务的是纯读/无修改操作；执行失败也不能提交已构造的事务。
  // update_log_only 是例外：它虽无对象修改，仍要把错误写入 PG log 用于去重。
  if ((ctx->op_t->empty() || result < 0) && !ctx->update_log_only) {
    // 成功读的 watch/notify 等副作用在回复前完成；失败操作不执行副作用。
    if (result >= 0)
      do_osd_op_effects(ctx, m->get_connection());

    complete_read_ctx(result, ctx);
    return;
  }

  ctx->reply->set_reply_versions(ctx->at_version, ctx->user_at_version);

  ceph_assert(op->may_write() || op->may_cache());

  // 在提交这次写事务前，重新计算 PG Log 最多可以安全裁剪到哪个版本，然后把这个裁剪位置随本次副本写一起发送给各副本。
  recovery_state.update_trim_to();

  // 可选调试检查：同一客户端对同一对象的 tid 不得倒退。
  // 仅用于发现顺序错误，tier 场景可能合法地改变请求路径，因此不参与该断言。
  if (cct->_conf->osd_debug_op_order && m->get_source().is_client() &&
      !pool.info.is_tier() && !pool.info.has_tiers()) {
    map<client_t,ceph_tid_t>& cm = debug_op_order[obc->obs.oi.soid];
    ceph_tid_t t = m->get_tid();
    client_t n = m->get_source().num();
    map<client_t,ceph_tid_t>::iterator p = cm.find(n);
    if (p == cm.end()) {
      dout(20) << " op order client." << n << " tid " << t << " (first)" << dendl;
      cm[n] = t;
    } else {
      dout(20) << " op order client." << n << " tid " << t << " last was " << p->second << dendl;
      if (p->second > t) {
	derr << "bad op order, already applied " << p->second << " > this " << t << dendl;
	ceph_abort_msg("out of order op");
      }
      p->second = t;
    }
  }

  if (ctx->update_log_only) {
    // 某些确定性的写错误不修改对象，只追加 PG log。
    // 这样客户端以相同 reqid 重试时可以返回与首次执行完全一致的错误及 returnvec。
    if (result >= 0)
      do_osd_op_effects(ctx, m->get_connection());

    dout(20) << __func__ << " update_log_only -- result=" << result << dendl;
    // reply 的所有权转交给去重记录；置空指针，避免 close_op_ctx() 重复释放。
    MOSDOpReply *reply = ctx->reply;
    ctx->reply = nullptr;
    // data_off：执行读操作时，它通常取自请求的读取起点
    // 它主要是给 Messenger/客户端提供数据缓冲区的对齐信息。
    // 提前准备具有相同页内偏移的接收缓冲区，从而减少数据复制。
    // 这里的作用是：使客户端第一次收到的回复，与通过 PG Log 去重后重新生成的回复，在消息头信息上保持一致。
    reply->get_header().data_off = (ctx->data_off ? *ctx->data_off : 0);

    if (result == -ENOENT) {
      // -ENOENT 表示目标对象不存在。
      // 虽然没有对象可以提供自身版本，但客户端仍然需要一个版本基准，特别是为了处理请求重放和后续写入。
      reply->set_enoent_reply_versions(info.last_update,
				       info.last_user_version);
    }
    reply->add_flags(CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK);
    // 将错误追加到 PG log 用于重复请求检测。
    // 错误路径无条件保存 returnvec，以便重试回复与首次处理保持一致（参见上面的 ignore_out_data）。
    record_write_error(
      op, soid, reply, result,
      (ctx->op->allows_returnvec() || result < 0) ? ctx : nullptr);
    close_op_ctx(ctx);
    return;
  }

  // 阶段 4：注册复制事务的生命周期回调。此后 ctx 由 RepGather 持有；

  // on_commit：主 OSD和所需副本都满足提交条件后，统计 IO 并回复客户端。
  ctx->register_on_commit(
    [m, ctx, this](){
      if (ctx->op)
	log_op_stats(*ctx->op, ctx->bytes_written, ctx->bytes_read);

      if (m && !ctx->sent_reply) {
	MOSDOpReply *reply = ctx->reply;
	ctx->reply = nullptr;
	reply->add_flags(CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK);
	dout(10) << " sending reply on " << *m << " " << reply << dendl;
	osd->send_message_osd_client(reply, m->get_connection());
	ctx->sent_reply = true;
	ctx->op->mark_commit_sent();
      }
    });
  // on_success：按 repop_queue 顺序完成成功处理，执行 watch/notify 等副作用。
  // OSDOp 除了修改 ObjectStore 中的数据和元数据之外，还需要改变内存状态、连接状态或发送通知的动作。
  ctx->register_on_success(
    [ctx, this]() {
      do_osd_op_effects(
	ctx,
	ctx->op ? ctx->op->get_req()->get_connection() :
	ConnectionRef());
    });
  // on_finish：整个 RepGather 生命周期结束后释放 ctx。
  ctx->register_on_finish(
    [ctx]() {
      // RepGather 的所有完成/取消路径最终都通过该回调释放 ctx。
      delete ctx;
    });

  // 为本次副本操作分配唯一 tid，创建 RepGather 汇总本地及各副本的完成状态。
  ceph_tid_t rep_tid = osd->get_tid();

  RepGather *repop = new_repop(ctx, rep_tid);

  // 向 acting 副本发出写入并把主 OSD 的 PG Log 合入本地事务提交给 ObjectStore；
  issue_repop(repop, ctx);
  eval_repop(repop);
  // 释放调用者持有的初始引用，后续由未完成的本地/副本回调继续持有 repop。
  repop->put();
}

void PrimaryLogPG::close_op_ctx(OpContext *ctx) {
  release_object_locks(ctx->lock_manager);

  ctx->op_t.reset();

  for (auto p = ctx->on_finish.begin(); p != ctx->on_finish.end();
       ctx->on_finish.erase(p++)) {
    (*p)();
  }
  delete ctx;
}

void PrimaryLogPG::reply_ctx(OpContext *ctx, int r)
{
  if (ctx->op)
    osd->reply_op_error(ctx->op, r);
  close_op_ctx(ctx);
}

void PrimaryLogPG::log_op_stats(const OpRequest& op,
				const uint64_t inb,
				const uint64_t outb)
{
  auto m = op.get_req<MOSDOp>();
  const utime_t now = ceph_clock_now();

  const utime_t latency = now - m->get_recv_stamp();
  const utime_t process_latency = now - op.get_dequeued_time();

  osd->logger->inc(l_osd_op);

  osd->logger->inc(l_osd_op_outb, outb);
  osd->logger->inc(l_osd_op_inb, inb);
  osd->logger->tinc(l_osd_op_lat, latency);
  osd->logger->tinc(l_osd_op_process_lat, process_latency);

  if (op.may_read() && op.may_write()) {
    osd->logger->inc(l_osd_op_rw);
    osd->logger->inc(l_osd_op_rw_inb, inb);
    osd->logger->inc(l_osd_op_rw_outb, outb);
    osd->logger->tinc(l_osd_op_rw_lat, latency);
    osd->logger->hinc(l_osd_op_rw_lat_inb_hist, latency.to_nsec(), inb);
    osd->logger->hinc(l_osd_op_rw_lat_outb_hist, latency.to_nsec(), outb);
    osd->logger->tinc(l_osd_op_rw_process_lat, process_latency);
  } else if (op.may_read()) {
    osd->logger->inc(l_osd_op_r);
    osd->logger->inc(l_osd_op_r_outb, outb);
    osd->logger->tinc(l_osd_op_r_lat, latency);
    osd->logger->hinc(l_osd_op_r_lat_outb_hist, latency.to_nsec(), outb);
    osd->logger->tinc(l_osd_op_r_process_lat, process_latency);
  } else if (op.may_write() || op.may_cache()) {
    osd->logger->inc(l_osd_op_w);
    osd->logger->inc(l_osd_op_w_inb, inb);
    osd->logger->tinc(l_osd_op_w_lat, latency);
    osd->logger->hinc(l_osd_op_w_lat_inb_hist, latency.to_nsec(), inb);
    osd->logger->tinc(l_osd_op_w_process_lat, process_latency);
  } else {
    ceph_abort();
  }

  dout(15) << "log_op_stats " << *m
	   << " inb " << inb
	   << " outb " << outb
	   << " lat " << latency << dendl;

  if (m_dynamic_perf_stats.is_enabled()) {
    m_dynamic_perf_stats.add(osd->get_nodeid(), info, op, inb, outb, latency);
  }
}

void PrimaryLogPG::set_dynamic_perf_stats_queries(
    const std::list<OSDPerfMetricQuery> &queries)
{
  m_dynamic_perf_stats.set_queries(queries);
}

void PrimaryLogPG::get_dynamic_perf_stats(DynamicPerfStats *stats)
{
  std::swap(m_dynamic_perf_stats, *stats);
}

void PrimaryLogPG::do_scan(
  OpRequestRef op,
  ThreadPool::TPHandle &handle)
{
  auto m = op->get_req<MOSDPGScan>();
  ceph_assert(m->get_type() == MSG_OSD_PG_SCAN);
  dout(10) << "do_scan " << *m << dendl;

  op->mark_started();

  switch (m->op) {
  case MOSDPGScan::OP_SCAN_GET_DIGEST:
    {
      auto dpp = get_dpp();
      if (osd->check_backfill_full(dpp)) {
	dout(1) << __func__ << ": Suspending backfill: Full." << dendl;
	queue_peering_event(
	  PGPeeringEventRef(
	    std::make_shared<PGPeeringEvent>(
	      get_osdmap_epoch(),
	      get_osdmap_epoch(),
	      PeeringState::BackfillTooFull())));
	return;
      }

      ReplicaBackfillInterval bi;
      bi.begin = m->begin;
      // No need to flush, there won't be any in progress writes occuring
      // past m->begin
      scan_range_replica(
	cct->_conf->osd_backfill_scan_min,
	cct->_conf->osd_backfill_scan_max,
	&bi,
	handle);
      MOSDPGScan *reply = new MOSDPGScan(
	MOSDPGScan::OP_SCAN_DIGEST,
	pg_whoami,
	get_osdmap_epoch(), m->query_epoch,
	spg_t(info.pgid.pgid, get_primary().shard), bi.begin, bi.end);
      encode(bi.objects, reply->get_data());
      osd->send_message_osd_cluster(reply, m->get_connection());
    }
    break;

  case MOSDPGScan::OP_SCAN_DIGEST:
    {
      pg_shard_t from = m->from;

      // Check that from is in backfill_targets vector
      ceph_assert(is_backfill_target(from));

      ReplicaBackfillInterval& bi = peer_backfill_info[from];
      bi.begin = m->begin;
      bi.end = m->end;
      auto p = m->get_data().cbegin();

      // take care to preserve ordering!
      bi.clear_objects();
      decode_noclear(bi.objects, p);
      dout(10) << __func__ << " bi.begin=" << bi.begin << " bi.end=" << bi.end
               << " bi.objects.size()=" << bi.objects.size() << dendl;

      if (waiting_on_backfill.erase(from)) {
	if (waiting_on_backfill.empty()) {
	  ceph_assert(
	    peer_backfill_info.size() ==
	    get_backfill_targets().size());
	  finish_recovery_op(hobject_t::get_max());
	}
      } else {
	// we canceled backfill for a while due to a too full, and this
	// is an extra response from a non-too-full peer
	dout(20) << __func__ << " suspended backfill (too full?)" << dendl;
      }
    }
    break;
  }
}

void PrimaryLogPG::do_backfill(OpRequestRef op)
{
  auto m = op->get_req<MOSDPGBackfill>();
  ceph_assert(m->get_type() == MSG_OSD_PG_BACKFILL);
  dout(10) << "do_backfill " << *m << dendl;

  op->mark_started();

  switch (m->op) {
  case MOSDPGBackfill::OP_BACKFILL_FINISH:
    {
      ceph_assert(cct->_conf->osd_kill_backfill_at != 1);

      MOSDPGBackfill *reply = new MOSDPGBackfill(
	MOSDPGBackfill::OP_BACKFILL_FINISH_ACK,
	get_osdmap_epoch(),
	m->query_epoch,
	spg_t(info.pgid.pgid, get_primary().shard));
      reply->set_priority(recovery_state.get_recovery_op_priority());
      osd->send_message_osd_cluster(reply, m->get_connection());
      queue_peering_event(
	PGPeeringEventRef(
	  std::make_shared<PGPeeringEvent>(
	    get_osdmap_epoch(),
	    get_osdmap_epoch(),
	    RecoveryDone())));
    }
    // fall-thru

  case MOSDPGBackfill::OP_BACKFILL_PROGRESS:
    {
      ceph_assert(cct->_conf->osd_kill_backfill_at != 2);

      ObjectStore::Transaction t;
      recovery_state.update_backfill_progress(
	m->last_backfill,
	m->stats,
	m->op == MOSDPGBackfill::OP_BACKFILL_PROGRESS,
	t);

      int tr = osd->store->queue_transaction(ch, std::move(t), NULL);
      ceph_assert(tr == 0);
    }
    break;

  case MOSDPGBackfill::OP_BACKFILL_FINISH_ACK:
    {
      ceph_assert(is_primary());
      ceph_assert(cct->_conf->osd_kill_backfill_at != 3);
      finish_recovery_op(hobject_t::get_max());
    }
    break;
  }
}

void PrimaryLogPG::do_backfill_remove(OpRequestRef op)
{
  const MOSDPGBackfillRemove *m = static_cast<const MOSDPGBackfillRemove*>(
    op->get_req());
  ceph_assert(m->get_type() == MSG_OSD_PG_BACKFILL_REMOVE);
  dout(7) << __func__ << " " << m->ls << dendl;

  op->mark_started();

  ObjectStore::Transaction t;
  for (auto& p : m->ls) {
    if (is_remote_backfilling()) {
      struct stat st;
      int r = osd->store->stat(ch, ghobject_t(p.first, ghobject_t::NO_GEN,
                               pg_whoami.shard) , &st);
      if (r == 0) {
        sub_local_num_bytes(st.st_size);
        int64_t usersize;
        if (pool.info.is_erasure()) {
          bufferlist bv;
	  int r = osd->store->getattr(
	      ch,
              ghobject_t(p.first, ghobject_t::NO_GEN, pg_whoami.shard),
	      OI_ATTR,
	      bv);
	  if (r >= 0) {
	    object_info_t oi(bv);
            usersize = oi.size * pgbackend->get_ec_data_chunk_count();
          } else {
            dout(0) << __func__ << " " << ghobject_t(p.first, ghobject_t::NO_GEN, pg_whoami.shard)
                    << " can't get object info" << dendl;
            usersize = 0;
          }
        } else {
          usersize = st.st_size;
        }
        sub_num_bytes(usersize);
        dout(10) << __func__ << " " << ghobject_t(p.first, ghobject_t::NO_GEN, pg_whoami.shard)
                 << " sub actual data by " << st.st_size
                 << " sub num_bytes by " << usersize
                 << dendl;
      }
    }
    remove_snap_mapped_object(t, p.first);
  }
  int r = osd->store->queue_transaction(ch, std::move(t), NULL);
  ceph_assert(r == 0);
}

int PrimaryLogPG::trim_object(
  bool first, const hobject_t &coid, snapid_t snap_to_trim,
  PrimaryLogPG::OpContextUPtr *ctxp)
{
  *ctxp = NULL;

  // load clone info
  bufferlist bl;
  ObjectContextRef obc = get_object_context(coid, false, NULL);
  if (!obc || !obc->ssc || !obc->ssc->exists) {
    osd->clog->error() << __func__ << ": Can not trim " << coid
      << " repair needed " << (obc ? "(no obc->ssc or !exists)" : "(no obc)");
    return -ENOENT;
  }

  hobject_t head_oid = coid.get_head();
  ObjectContextRef head_obc = get_object_context(head_oid, false);
  if (!head_obc) {
    osd->clog->error() << __func__ << ": Can not trim " << coid
      << " repair needed, no snapset obc for " << head_oid;
    return -ENOENT;
  }

  SnapSet& snapset = obc->ssc->snapset;

  object_info_t &coi = obc->obs.oi;
  auto citer = snapset.clone_snaps.find(coid.snap);
  if (citer == snapset.clone_snaps.end()) {
    osd->clog->error() << "No clone_snaps in snapset " << snapset
		       << " for object " << coid << "\n";
    return -ENOENT;
  }
  set<snapid_t> old_snaps(citer->second.begin(), citer->second.end());
  if (old_snaps.empty()) {
    osd->clog->error() << "No object info snaps for object " << coid;
    return -ENOENT;
  }

  dout(10) << coid << " old_snaps " << old_snaps
	   << " old snapset " << snapset << dendl;
  if (snapset.seq == 0) {
    osd->clog->error() << "No snapset.seq for object " << coid;
    return -ENOENT;
  }

  set<snapid_t> new_snaps;
  const OSDMapRef& osdmap = get_osdmap();
  for (set<snapid_t>::iterator i = old_snaps.begin();
       i != old_snaps.end();
       ++i) {
    if (!osdmap->in_removed_snaps_queue(info.pgid.pgid.pool(), *i) &&
	*i != snap_to_trim) {
      new_snaps.insert(*i);
    }
  }

  vector<snapid_t>::iterator p = snapset.clones.end();

  if (new_snaps.empty()) {
    p = std::find(snapset.clones.begin(), snapset.clones.end(), coid.snap);
    if (p == snapset.clones.end()) {
      osd->clog->error() << "Snap " << coid.snap << " not in clones";
      return -ENOENT;
    }
  }

  OpContextUPtr ctx = simple_opc_create(obc);
  ctx->head_obc = head_obc;

  if (!ctx->lock_manager.get_snaptrimmer_write(
	coid,
	obc,
	first)) {
    close_op_ctx(ctx.release());
    dout(10) << __func__ << ": Unable to get a wlock on " << coid << dendl;
    return -ENOLCK;
  }

  if (!ctx->lock_manager.get_snaptrimmer_write(
	head_oid,
	head_obc,
	first)) {
    close_op_ctx(ctx.release());
    dout(10) << __func__ << ": Unable to get a wlock on " << head_oid << dendl;
    return -ENOLCK;
  }

  ctx->at_version = get_next_version();

  PGTransaction *t = ctx->op_t.get();

  int64_t num_objects_before_trim = ctx->delta_stats.num_objects;

  if (new_snaps.empty()) {
    // remove clone
    dout(10) << coid << " snaps " << old_snaps << " -> "
	     << new_snaps << " ... deleting" << dendl;

    // ...from snapset
    ceph_assert(p != snapset.clones.end());

    snapid_t last = coid.snap;
    ctx->delta_stats.num_bytes -= snapset.get_clone_bytes(last);

    if (p != snapset.clones.begin()) {
      // not the oldest... merge overlap into next older clone
      vector<snapid_t>::iterator n = p - 1;
      hobject_t prev_coid = coid;
      prev_coid.snap = *n;
      bool adjust_prev_bytes = is_present_clone(prev_coid);

      if (adjust_prev_bytes)
	ctx->delta_stats.num_bytes -= snapset.get_clone_bytes(*n);

      snapset.clone_overlap[*n].intersection_of(
	snapset.clone_overlap[*p]);

      if (adjust_prev_bytes)
	ctx->delta_stats.num_bytes += snapset.get_clone_bytes(*n);
    }
    ctx->delta_stats.num_objects--;
    if (coi.is_dirty())
      ctx->delta_stats.num_objects_dirty--;
    if (coi.is_omap())
      ctx->delta_stats.num_objects_omap--;
    if (coi.is_whiteout()) {
      dout(20) << __func__ << " trimming whiteout on " << coid << dendl;
      ctx->delta_stats.num_whiteouts--;
    }
    ctx->delta_stats.num_object_clones--;
    if (coi.is_cache_pinned())
      ctx->delta_stats.num_objects_pinned--;
    if (coi.has_manifest()) {
      dec_all_refcount_manifest(coi, ctx.get());
      ctx->delta_stats.num_objects_manifest--;
    }
    obc->obs.exists = false;

    snapset.clones.erase(p);
    snapset.clone_overlap.erase(last);
    snapset.clone_size.erase(last);
    snapset.clone_snaps.erase(last);

    ctx->log.push_back(
      pg_log_entry_t(
	pg_log_entry_t::DELETE,
	coid,
	ctx->at_version,
	ctx->obs->oi.version,
	0,
	osd_reqid_t(),
	ctx->mtime,
	0)
      );
    t->remove(coid);
    t->update_snaps(
      coid,
      old_snaps,
      new_snaps);

    coi = object_info_t(coid);

    ctx->at_version.version++;
  } else {
    // save adjusted snaps for this object
    dout(10) << coid << " snaps " << old_snaps << " -> " << new_snaps << dendl;
    snapset.clone_snaps[coid.snap] =
      vector<snapid_t>(new_snaps.rbegin(), new_snaps.rend());
    // we still do a 'modify' event on this object just to trigger a
    // snapmapper.update ... :(

    coi.prior_version = coi.version;
    coi.version = ctx->at_version;
    bl.clear();
    encode(coi, bl, get_osdmap()->get_features(CEPH_ENTITY_TYPE_OSD, nullptr));
    t->setattr(coid, OI_ATTR, bl);

    ctx->log.push_back(
      pg_log_entry_t(
	pg_log_entry_t::MODIFY,
	coid,
	coi.version,
	coi.prior_version,
	0,
	osd_reqid_t(),
	ctx->mtime,
	0)
      );
    ctx->at_version.version++;

    t->update_snaps(
      coid,
      old_snaps,
      new_snaps);
  }

  // save head snapset
  dout(10) << coid << " new snapset " << snapset << " on "
	   << head_obc->obs.oi << dendl;
  if (snapset.clones.empty() &&
      (head_obc->obs.oi.is_whiteout() &&
       !(head_obc->obs.oi.is_dirty() && pool.info.is_tier()) &&
       !head_obc->obs.oi.is_cache_pinned())) {
    // NOTE: this arguably constitutes minor interference with the
    // tiering agent if this is a cache tier since a snap trim event
    // is effectively evicting a whiteout we might otherwise want to
    // keep around.
    dout(10) << coid << " removing " << head_oid << dendl;
    ctx->log.push_back(
      pg_log_entry_t(
	pg_log_entry_t::DELETE,
	head_oid,
	ctx->at_version,
	head_obc->obs.oi.version,
	0,
	osd_reqid_t(),
	ctx->mtime,
	0)
      );
    dout(10) << "removing snap head" << dendl;
    object_info_t& oi = head_obc->obs.oi;
    ctx->delta_stats.num_objects--;
    if (oi.is_dirty()) {
      ctx->delta_stats.num_objects_dirty--;
    }
    if (oi.is_omap())
      ctx->delta_stats.num_objects_omap--;
    if (oi.is_whiteout()) {
      dout(20) << __func__ << " trimming whiteout on " << oi.soid << dendl;
      ctx->delta_stats.num_whiteouts--;
    }
    if (oi.is_cache_pinned()) {
      ctx->delta_stats.num_objects_pinned--;
    }
    if (oi.has_manifest()) {
      ctx->delta_stats.num_objects_manifest--;
      dec_all_refcount_manifest(oi, ctx.get());
    }
    head_obc->obs.exists = false;
    head_obc->obs.oi = object_info_t(head_oid);
    t->remove(head_oid);
  } else {
    dout(10) << coid << " writing updated snapset on " << head_oid
	     << ", snapset is " << snapset << dendl;
    ctx->log.push_back(
      pg_log_entry_t(
	pg_log_entry_t::MODIFY,
	head_oid,
	ctx->at_version,
	head_obc->obs.oi.version,
	0,
	osd_reqid_t(),
	ctx->mtime,
	0)
      );

    head_obc->obs.oi.prior_version = head_obc->obs.oi.version;
    head_obc->obs.oi.version = ctx->at_version;

    map <string, bufferlist, less<>> attrs;
    bl.clear();
    encode(snapset, bl);
    attrs[SS_ATTR] = std::move(bl);

    bl.clear(); //NOLINT(bugprone-use-after-move)
    encode(head_obc->obs.oi, bl,
	     get_osdmap()->get_features(CEPH_ENTITY_TYPE_OSD, nullptr));
    attrs[OI_ATTR] = std::move(bl);
    t->setattrs(head_oid, attrs);
  }

  // Stats reporting - Set number of objects trimmed
  if (num_objects_before_trim > ctx->delta_stats.num_objects) {
    int64_t num_objects_trimmed =
      num_objects_before_trim - ctx->delta_stats.num_objects;
    add_objects_trimmed_count(num_objects_trimmed);
  }

  *ctxp = std::move(ctx);
  return 0;
}

void PrimaryLogPG::kick_snap_trim()
{
  ceph_assert(is_active());
  ceph_assert(is_primary());
  if (is_clean() &&
      !state_test(PG_STATE_PREMERGE) &&
      !snap_trimq.empty()) {
    if (get_osdmap()->test_flag(CEPH_OSDMAP_NOSNAPTRIM)) {
      dout(10) << __func__ << ": nosnaptrim set, not kicking" << dendl;
    } else {
      dout(10) << __func__ << ": clean and snaps to trim, kicking" << dendl;
      reset_objects_trimmed();
      set_snaptrim_begin_stamp();
      snap_trimmer_machine.process_event(KickTrim());
    }
  }
}

void PrimaryLogPG::snap_trimmer_scrub_complete()
{
  if (is_primary() && is_active() && is_clean() && !snap_trimq.empty()) {
    dout(10) << "scrub finished - requeuing snap_trimmer" << dendl;
    snap_trimmer_machine.process_event(ScrubComplete());
  }
}

void PrimaryLogPG::snap_trimmer(epoch_t queued)
{
  if (recovery_state.is_deleting() || pg_has_reset_since(queued)) {
    return;
  }

  ceph_assert(is_primary());

  dout(10) << "snap_trimmer posting" << dendl;
  snap_trimmer_machine.process_event(DoSnapWork());
  dout(10) << "snap_trimmer complete" << dendl;
  return;
}

namespace {

template<typename U, typename V>
int do_cmp_xattr(int op, const U& lhs, const V& rhs)
{
  switch (op) {
  case CEPH_OSD_CMPXATTR_OP_EQ:
    return lhs == rhs;
  case CEPH_OSD_CMPXATTR_OP_NE:
    return lhs != rhs;
  case CEPH_OSD_CMPXATTR_OP_GT:
    return lhs > rhs;
  case CEPH_OSD_CMPXATTR_OP_GTE:
    return lhs >= rhs;
  case CEPH_OSD_CMPXATTR_OP_LT:
    return lhs < rhs;
  case CEPH_OSD_CMPXATTR_OP_LTE:
    return lhs <= rhs;
  default:
    return -EINVAL;
  }
}

} // anonymous namespace

int PrimaryLogPG::do_xattr_cmp_u64(int op, uint64_t v1, bufferlist& xattr)
{
  uint64_t v2;

  if (xattr.length()) {
    const char* first = xattr.c_str();
    if (auto [p, ec] = std::from_chars(first, first + xattr.length(), v2);
	ec != std::errc()) {
      return -EINVAL;
    }
  } else {
    v2 = 0;
  }
  dout(20) << "do_xattr_cmp_u64 '" << v1 << "' vs '" << v2 << "' op " << op << dendl;
  return do_cmp_xattr(op, v1, v2);
}

int PrimaryLogPG::do_xattr_cmp_str(int op, string& v1s, bufferlist& xattr)
{
  string_view v2s(xattr.c_str(), xattr.length());
  dout(20) << "do_xattr_cmp_str '" << v1s << "' vs '" << v2s << "' op " << op << dendl;
  return do_cmp_xattr(op, v1s, v2s);
}

int PrimaryLogPG::do_writesame(OpContext *ctx, OSDOp& osd_op)
{
  ceph_osd_op& op = osd_op.op;
  vector<OSDOp> write_ops(1);
  OSDOp& write_op = write_ops[0];
  uint64_t write_length = op.writesame.length;
  int result = 0;

  if (!write_length)
    return 0;

  if (!op.writesame.data_length || write_length % op.writesame.data_length)
    return -EINVAL;

  if (op.writesame.data_length != osd_op.indata.length()) {
    derr << "invalid length ws data length " << op.writesame.data_length << " actual len " << osd_op.indata.length() << dendl;
    return -EINVAL;
  }

  while (write_length) {
    write_op.indata.append(osd_op.indata);
    write_length -= op.writesame.data_length;
  }

  write_op.op.op = CEPH_OSD_OP_WRITE;
  write_op.op.extent.offset = op.writesame.offset;
  write_op.op.extent.length = op.writesame.length;
  result = do_osd_ops(ctx, write_ops);
  if (result < 0)
    derr << "do_writesame do_osd_ops failed " << result << dendl;

  return result;
}

// ========================================================================
// low level osd ops

int PrimaryLogPG::do_tmap2omap(OpContext *ctx, unsigned flags)
{
  dout(20) << " convert tmap to omap for " << ctx->new_obs.oi.soid << dendl;
  bufferlist header, vals;
  int r = _get_tmap(ctx, &header, &vals);
  if (r < 0) {
    if (r == -ENODATA && (flags & CEPH_OSD_TMAP2OMAP_NULLOK))
      r = 0;
    return r;
  }

  vector<OSDOp> ops(3);

  ops[0].op.op = CEPH_OSD_OP_TRUNCATE;
  ops[0].op.extent.offset = 0;
  ops[0].op.extent.length = 0;

  ops[1].op.op = CEPH_OSD_OP_OMAPSETHEADER;
  ops[1].indata = std::move(header);

  ops[2].op.op = CEPH_OSD_OP_OMAPSETVALS;
  ops[2].indata = std::move(vals);

  return do_osd_ops(ctx, ops);
}

int PrimaryLogPG::do_tmapup_slow(OpContext *ctx, bufferlist::const_iterator& bp,
				 OSDOp& osd_op, bufferlist& bl)
{
  // decode
  bufferlist header;
  map<string, bufferlist> m;
  if (bl.length()) {
    auto p = bl.cbegin();
    decode(header, p);
    decode(m, p);
    ceph_assert(p.end());
  }

  // do the update(s)
  while (!bp.end()) {
    __u8 op;
    string key;
    decode(op, bp);

    switch (op) {
    case CEPH_OSD_TMAP_SET: // insert key
      {
	decode(key, bp);
	bufferlist data;
	decode(data, bp);
	m[key] = data;
      }
      break;
    case CEPH_OSD_TMAP_RM: // remove key
      decode(key, bp);
      if (!m.count(key)) {
	return -ENOENT;
      }
      m.erase(key);
      break;
    case CEPH_OSD_TMAP_RMSLOPPY: // remove key
      decode(key, bp);
      m.erase(key);
      break;
    case CEPH_OSD_TMAP_HDR: // update header
      {
	decode(header, bp);
      }
      break;
    default:
      return -EINVAL;
    }
  }

  // reencode
  bufferlist obl;
  encode(header, obl);
  encode(m, obl);

  // write it out
  vector<OSDOp> nops(1);
  OSDOp& newop = nops[0];
  newop.op.op = CEPH_OSD_OP_WRITEFULL;
  newop.op.extent.offset = 0;
  newop.op.extent.length = obl.length();
  newop.indata = obl;
  do_osd_ops(ctx, nops);
  return 0;
}

int PrimaryLogPG::do_tmapup(OpContext *ctx, bufferlist::const_iterator& bp, OSDOp& osd_op)
{
  bufferlist::const_iterator orig_bp = bp;
  int result = 0;
  if (bp.end()) {
    dout(10) << "tmapup is a no-op" << dendl;
  } else {
    // read the whole object
    vector<OSDOp> nops(1);
    OSDOp& newop = nops[0];
    newop.op.op = CEPH_OSD_OP_READ;
    newop.op.extent.offset = 0;
    newop.op.extent.length = 0;
    result = do_osd_ops(ctx, nops);

    dout(10) << "tmapup read " << newop.outdata.length() << dendl;

    dout(30) << " starting is \n";
    newop.outdata.hexdump(*_dout);
    *_dout << dendl;

    auto ip = newop.outdata.cbegin();
    bufferlist obl;

    dout(30) << "the update command is: \n";
    osd_op.indata.hexdump(*_dout);
    *_dout << dendl;

    // header
    bufferlist header;
    __u32 nkeys = 0;
    if (newop.outdata.length()) {
      decode(header, ip);
      decode(nkeys, ip);
    }
    dout(10) << "tmapup header " << header.length() << dendl;

    if (!bp.end() && *bp == CEPH_OSD_TMAP_HDR) {
      ++bp;
      decode(header, bp);
      dout(10) << "tmapup new header " << header.length() << dendl;
    }

    encode(header, obl);

    dout(20) << "tmapup initial nkeys " << nkeys << dendl;

    // update keys
    bufferlist newkeydata;
    string nextkey, last_in_key;
    bufferlist nextval;
    bool have_next = false;
    if (!ip.end()) {
      have_next = true;
      decode(nextkey, ip);
      decode(nextval, ip);
    }
    while (!bp.end() && !result) {
      __u8 op;
      string key;
      try {
	decode(op, bp);
	decode(key, bp);
      }
      catch (ceph::buffer::error& e) {
	return -EINVAL;
      }
      if (key < last_in_key) {
	dout(5) << "tmapup warning: key '" << key << "' < previous key '" << last_in_key
		<< "', falling back to an inefficient (unsorted) update" << dendl;
	bp = orig_bp;
	return do_tmapup_slow(ctx, bp, osd_op, newop.outdata);
      }
      last_in_key = key;

      dout(10) << "tmapup op " << (int)op << " key " << key << dendl;

      // skip existing intervening keys
      bool key_exists = false;
      while (have_next && !key_exists) {
	dout(20) << "  (have_next=" << have_next << " nextkey=" << nextkey << ")" << dendl;
	if (nextkey > key)
	  break;
	if (nextkey < key) {
	  // copy untouched.
	  encode(nextkey, newkeydata);
	  encode(nextval, newkeydata);
	  dout(20) << "  keep " << nextkey << " " << nextval.length() << dendl;
	} else {
	  // don't copy; discard old value.  and stop.
	  dout(20) << "  drop " << nextkey << " " << nextval.length() << dendl;
	  key_exists = true;
	  nkeys--;
	}
	if (!ip.end()) {
	  decode(nextkey, ip);
	  decode(nextval, ip);
	} else {
	  have_next = false;
	}
      }

      if (op == CEPH_OSD_TMAP_SET) {
	bufferlist val;
	try {
	  decode(val, bp);
	}
	catch (ceph::buffer::error& e) {
	  return -EINVAL;
	}
	encode(key, newkeydata);
	encode(val, newkeydata);
	dout(20) << "   set " << key << " " << val.length() << dendl;
	nkeys++;
      } else if (op == CEPH_OSD_TMAP_CREATE) {
	if (key_exists) {
	  return -EEXIST;
	}
	bufferlist val;
	try {
	  decode(val, bp);
	}
	catch (ceph::buffer::error& e) {
	  return -EINVAL;
	}
	encode(key, newkeydata);
	encode(val, newkeydata);
	dout(20) << "   create " << key << " " << val.length() << dendl;
	nkeys++;
      } else if (op == CEPH_OSD_TMAP_RM) {
	// do nothing.
	if (!key_exists) {
	  return -ENOENT;
	}
      } else if (op == CEPH_OSD_TMAP_RMSLOPPY) {
	// do nothing
      } else {
	dout(10) << "  invalid tmap op " << (int)op << dendl;
	return -EINVAL;
      }
    }

    // copy remaining
    if (have_next) {
      encode(nextkey, newkeydata);
      encode(nextval, newkeydata);
      dout(20) << "  keep " << nextkey << " " << nextval.length() << dendl;
    }
    if (!ip.end()) {
      bufferlist rest;
      rest.substr_of(newop.outdata, ip.get_off(), newop.outdata.length() - ip.get_off());
      dout(20) << "  keep trailing " << rest.length()
	       << " at " << newkeydata.length() << dendl;
      newkeydata.claim_append(rest);
    }

    // encode final key count + key data
    dout(20) << "tmapup final nkeys " << nkeys << dendl;
    encode(nkeys, obl);
    obl.claim_append(newkeydata);

    if (0) {
      dout(30) << " final is \n";
      obl.hexdump(*_dout);
      *_dout << dendl;

      // sanity check
      auto tp = obl.cbegin();
      bufferlist h;
      decode(h, tp);
      map<string,bufferlist> d;
      decode(d, tp);
      ceph_assert(tp.end());
      dout(0) << " **** debug sanity check, looks ok ****" << dendl;
    }

    // write it out
    if (!result) {
      dout(20) << "tmapput write " << obl.length() << dendl;
      newop.op.op = CEPH_OSD_OP_WRITEFULL;
      newop.op.extent.offset = 0;
      newop.op.extent.length = obl.length();
      newop.indata = obl;
      do_osd_ops(ctx, nops);
    }
  }
  return result;
}

static int check_offset_and_length(uint64_t offset, uint64_t length,
  uint64_t max, DoutPrefixProvider *dpp)
{
  if (offset >= max ||
      length > max ||
      offset + length > max) {
    ldpp_dout(dpp, 10) << __func__ << " "
      << "osd_max_object_size: " << max
      << "; Hard limit of object size is 4GB." << dendl;
    return -EFBIG;
  }

  return 0;
}

struct FillInVerifyExtent : public Context {
  ceph_le64 *r;
  int32_t *rval;
  bufferlist *outdatap;
  std::optional<uint32_t> maybe_crc;
  uint64_t size;
  OSDService *osd;
  hobject_t soid;
  uint32_t flags;
  FillInVerifyExtent(ceph_le64 *r, int32_t *rv, bufferlist *blp,
		     std::optional<uint32_t> mc, uint64_t size,
		     OSDService *osd, hobject_t soid, uint32_t flags) :
    r(r), rval(rv), outdatap(blp), maybe_crc(mc),
    size(size), osd(osd), soid(soid), flags(flags) {}
  void finish(int len) override {
    if (len < 0) {
      *rval = len;
      return;
    }
    *r = len;
    *rval = 0;

    // whole object?  can we verify the checksum?
    if (maybe_crc && *r == size) {
      uint32_t crc = outdatap->crc32c(-1);
      if (maybe_crc != crc) {
        osd->clog->error() << std::hex << " full-object read crc 0x" << crc
			   << " != expected 0x" << *maybe_crc
			   << std::dec << " on " << soid;
        if (!(flags & CEPH_OSD_OP_FLAG_FAILOK)) {
	  *rval = -EIO;
	  *r = 0;
	}
      }
    }
  }
};

struct ToSparseReadResult : public Context {
  int* result;
  bufferlist* data_bl;
  uint64_t data_offset;
  ceph_le64* len;
  ToSparseReadResult(int* result, bufferlist* bl, uint64_t offset,
		     ceph_le64* len)
    : result(result), data_bl(bl), data_offset(offset),len(len) {}
  void finish(int r) override {
    if (r < 0) {
      *result = r;
      return;
    }
    *result = 0;
    *len = r;
    bufferlist outdata;
    map<uint64_t, uint64_t> extents = {{data_offset, r}};
    encode(extents, outdata);
    encode_destructively(*data_bl, outdata);
    data_bl->swap(outdata);
  }
};

template<typename V>
static string list_keys(const map<string, V>& m) {
  string s;
  for (typename map<string, V>::const_iterator itr = m.begin(); itr != m.end(); ++itr) {
    if (!s.empty()) {
      s.push_back(',');
    }
    s.append(itr->first);
  }
  return s;
}

template<typename T>
static string list_entries(const T& m) {
  string s;
  for (typename T::const_iterator itr = m.begin(); itr != m.end(); ++itr) {
    if (!s.empty()) {
      s.push_back(',');
    }
    s.append(*itr);
  }
  return s;
}

void PrimaryLogPG::maybe_create_new_object(
  OpContext *ctx,
  bool ignore_transaction)
{
  ObjectState& obs = ctx->new_obs;
  if (!obs.exists) {
    ctx->delta_stats.num_objects++;
    obs.exists = true;
    ceph_assert(!obs.oi.is_whiteout());
    obs.oi.new_object();
    if (!ignore_transaction)
      ctx->op_t->create(obs.oi.soid);
  } else if (obs.oi.is_whiteout()) {
    dout(10) << __func__ << " clearing whiteout on " << obs.oi.soid << dendl;
    ctx->new_obs.oi.clear_flag(object_info_t::FLAG_WHITEOUT);
    --ctx->delta_stats.num_whiteouts;
  }
}

struct ReadFinisher : public PrimaryLogPG::OpFinisher {
  OSDOp& osd_op;

  explicit ReadFinisher(OSDOp& osd_op) : osd_op(osd_op) {
  }

  int execute() override {
    return osd_op.rval;
  }
};

struct C_ChecksumRead : public Context {
  PrimaryLogPG *primary_log_pg;
  OSDOp &osd_op;
  Checksummer::CSumType csum_type;
  bufferlist init_value_bl;
  ceph_le64 read_length;
  bufferlist read_bl;
  Context *fill_extent_ctx;

  C_ChecksumRead(PrimaryLogPG *primary_log_pg, OSDOp &osd_op,
		 Checksummer::CSumType csum_type, bufferlist &&init_value_bl,
		 std::optional<uint32_t> maybe_crc, uint64_t size,
		 OSDService *osd, hobject_t soid, uint32_t flags)
    : primary_log_pg(primary_log_pg), osd_op(osd_op),
      csum_type(csum_type), init_value_bl(std::move(init_value_bl)),
      fill_extent_ctx(new FillInVerifyExtent(&read_length, &osd_op.rval,
					     &read_bl, maybe_crc, size,
					     osd, soid, flags)) {
  }
  ~C_ChecksumRead() override {
    delete fill_extent_ctx;
  }

  void finish(int r) override {
    fill_extent_ctx->complete(r);
    fill_extent_ctx = nullptr;

    if (osd_op.rval >= 0) {
      bufferlist::const_iterator init_value_bl_it = init_value_bl.begin();
      osd_op.rval = primary_log_pg->finish_checksum(osd_op, csum_type,
						    &init_value_bl_it, read_bl);
    }
  }
};

int PrimaryLogPG::do_checksum(OpContext *ctx, OSDOp& osd_op,
			      bufferlist::const_iterator *bl_it)
{
  dout(20) << __func__ << dendl;

  auto& op = osd_op.op;
  if (op.checksum.chunk_size > 0) {
    if (op.checksum.length == 0) {
      dout(10) << __func__ << ": length required when chunk size provided"
	       << dendl;
      return -EINVAL;
    }
    if (op.checksum.length % op.checksum.chunk_size != 0) {
      dout(10) << __func__ << ": length not aligned to chunk size" << dendl;
      return -EINVAL;
    }
  }

  auto& oi = ctx->new_obs.oi;
  if (op.checksum.offset == 0 && op.checksum.length == 0) {
    // zeroed offset+length implies checksum whole object
    op.checksum.length = oi.size;
  } else if (op.checksum.offset >= oi.size) {
    // read size was trimmed to zero, do nothing
    // see PrimaryLogPG::do_read
    return 0;
  } else if (op.extent.offset + op.extent.length > oi.size) {
    op.extent.length = oi.size - op.extent.offset;
    if (op.checksum.chunk_size > 0 &&
        op.checksum.length % op.checksum.chunk_size != 0) {
      dout(10) << __func__ << ": length (trimmed to 0x"
               << std::hex << op.checksum.length
               << ") not aligned to chunk size 0x"
               << op.checksum.chunk_size << std::dec
               << dendl;
      return -EINVAL;
    }
  }

  Checksummer::CSumType csum_type;
  switch (op.checksum.type) {
  case CEPH_OSD_CHECKSUM_OP_TYPE_XXHASH32:
    csum_type = Checksummer::CSUM_XXHASH32;
    break;
  case CEPH_OSD_CHECKSUM_OP_TYPE_XXHASH64:
    csum_type = Checksummer::CSUM_XXHASH64;
    break;
  case CEPH_OSD_CHECKSUM_OP_TYPE_CRC32C:
    csum_type = Checksummer::CSUM_CRC32C;
    break;
  default:
    dout(10) << __func__ << ": unknown crc type ("
	     << static_cast<uint32_t>(op.checksum.type) << ")" << dendl;
    return -EINVAL;
  }

  size_t csum_init_value_size = Checksummer::get_csum_init_value_size(csum_type);
  if (bl_it->get_remaining() < csum_init_value_size) {
    dout(10) << __func__ << ": init value not provided" << dendl;
    return -EINVAL;
  }

  bufferlist init_value_bl;
  init_value_bl.substr_of(bl_it->get_bl(), bl_it->get_off(),
			  csum_init_value_size);
  *bl_it += csum_init_value_size;

  if (pool.info.is_erasure() && op.checksum.length > 0) {
    // If there is a data digest and it is possible we are reading
    // entire object, pass the digest.
    std::optional<uint32_t> maybe_crc;
    if (oi.is_data_digest() && op.checksum.offset == 0 &&
        op.checksum.length >= oi.size) {
      maybe_crc = oi.data_digest;
    }

    // async read
    auto& soid = oi.soid;
    auto checksum_ctx = new C_ChecksumRead(this, osd_op, csum_type,
					   std::move(init_value_bl), maybe_crc,
					   oi.size, osd, soid, op.flags);

    ctx->pending_async_reads.push_back({
      {op.checksum.offset, op.checksum.length, op.flags},
      {&checksum_ctx->read_bl, checksum_ctx}});

    dout(10) << __func__ << ": async_read noted for " << soid << dendl;
    ctx->op_finishers[ctx->current_osd_subop_num].reset(
      new ReadFinisher(osd_op));
    return -EINPROGRESS;
  }

  // sync read
  std::vector<OSDOp> read_ops(1);
  auto& read_op = read_ops[0];
  if (op.checksum.length > 0) {
    read_op.op.op = CEPH_OSD_OP_READ;
    read_op.op.flags = op.flags;
    read_op.op.extent.offset = op.checksum.offset;
    read_op.op.extent.length = op.checksum.length;
    read_op.op.extent.truncate_size = 0;
    read_op.op.extent.truncate_seq = 0;

    int r = do_osd_ops(ctx, read_ops);
    if (r < 0) {
      derr << __func__ << ": do_osd_ops failed: " << cpp_strerror(r) << dendl;
      return r;
    }
  }

  bufferlist::const_iterator init_value_bl_it = init_value_bl.begin();
  return finish_checksum(osd_op, csum_type, &init_value_bl_it,
			 read_op.outdata);
}

int PrimaryLogPG::finish_checksum(OSDOp& osd_op,
				  Checksummer::CSumType csum_type,
				  bufferlist::const_iterator *init_value_bl_it,
				  const bufferlist &read_bl) {
  dout(20) << __func__ << dendl;

  auto& op = osd_op.op;

  if (op.checksum.length > 0 && read_bl.length() != op.checksum.length) {
    derr << __func__ << ": bytes read " << read_bl.length() << " != "
	 << op.checksum.length << dendl;
    return -EINVAL;
  }

  size_t csum_chunk_size = (op.checksum.chunk_size != 0 ?
			      op.checksum.chunk_size : read_bl.length());
  uint32_t csum_count = (csum_chunk_size > 0 ?
			   read_bl.length() / csum_chunk_size : 0);

  bufferlist csum;
  bufferptr csum_data;
  if (csum_count > 0) {
    size_t csum_value_size = Checksummer::get_csum_value_size(csum_type);
    csum_data = ceph::buffer::create(csum_value_size * csum_count);
    csum_data.zero();
    csum.append(csum_data);

    switch (csum_type) {
    case Checksummer::CSUM_XXHASH32:
      {
        Checksummer::xxhash32::init_value_t init_value;
        decode(init_value, *init_value_bl_it);
        Checksummer::calculate<Checksummer::xxhash32>(
	  init_value, csum_chunk_size, 0, read_bl.length(), read_bl,
	  &csum_data);
      }
      break;
    case Checksummer::CSUM_XXHASH64:
      {
        Checksummer::xxhash64::init_value_t init_value;
        decode(init_value, *init_value_bl_it);
        Checksummer::calculate<Checksummer::xxhash64>(
	  init_value, csum_chunk_size, 0, read_bl.length(), read_bl,
	  &csum_data);
      }
      break;
    case Checksummer::CSUM_CRC32C:
      {
        Checksummer::crc32c::init_value_t init_value;
        decode(init_value, *init_value_bl_it);
        Checksummer::calculate<Checksummer::crc32c>(
  	  init_value, csum_chunk_size, 0, read_bl.length(), read_bl,
	  &csum_data);
      }
      break;
    default:
      break;
    }
  }

  encode(csum_count, osd_op.outdata);
  osd_op.outdata.claim_append(csum);
  return 0;
}

struct C_ExtentCmpRead : public Context {
  PrimaryLogPG *primary_log_pg;
  OSDOp &osd_op;
  ceph_le64 read_length{};
  bufferlist read_bl;
  Context *fill_extent_ctx;

  C_ExtentCmpRead(PrimaryLogPG *primary_log_pg, OSDOp &osd_op,
		  std::optional<uint32_t> maybe_crc, uint64_t size,
		  OSDService *osd, hobject_t soid, uint32_t flags)
    : primary_log_pg(primary_log_pg), osd_op(osd_op),
      fill_extent_ctx(new FillInVerifyExtent(&read_length, &osd_op.rval,
					     &read_bl, maybe_crc, size,
					     osd, soid, flags)) {
  }
  ~C_ExtentCmpRead() override {
    delete fill_extent_ctx;
  }

  void finish(int r) override {
    if (r == -ENOENT) {
      osd_op.rval = 0;
      read_bl.clear();
      delete fill_extent_ctx;
    } else {
      fill_extent_ctx->complete(r);
    }
    fill_extent_ctx = nullptr;

    if (osd_op.rval >= 0) {
      osd_op.rval = primary_log_pg->finish_extent_cmp(osd_op, read_bl);
    }
  }
};

int PrimaryLogPG::do_extent_cmp(OpContext *ctx, OSDOp& osd_op)
{
  dout(20) << __func__ << dendl;
  ceph_osd_op& op = osd_op.op;

  auto& oi = ctx->new_obs.oi;
  uint64_t size = oi.size;
  if ((oi.truncate_seq < op.extent.truncate_seq) &&
      (op.extent.offset + op.extent.length > op.extent.truncate_size)) {
    size = op.extent.truncate_size;
  }

  if (op.extent.offset >= size) {
    op.extent.length = 0;
  } else if (op.extent.offset + op.extent.length > size) {
    op.extent.length = size - op.extent.offset;
  }

  if (op.extent.length == 0) {
    dout(20) << __func__ << " zero length extent" << dendl;
    return finish_extent_cmp(osd_op, bufferlist{});
  } else if (!ctx->obs->exists || ctx->obs->oi.is_whiteout()) {
    dout(20) << __func__ << " object DNE" << dendl;
    return finish_extent_cmp(osd_op, {});
  } else if (pool.info.is_erasure()) {
    // If there is a data digest and it is possible we are reading
    // entire object, pass the digest.
    std::optional<uint32_t> maybe_crc;
    if (oi.is_data_digest() && op.checksum.offset == 0 &&
        op.checksum.length >= oi.size) {
      maybe_crc = oi.data_digest;
    }

    // async read
    auto& soid = oi.soid;
    auto extent_cmp_ctx = new C_ExtentCmpRead(this, osd_op, maybe_crc, oi.size,
					      osd, soid, op.flags);
    ctx->pending_async_reads.push_back({
      {op.extent.offset, op.extent.length, op.flags},
      {&extent_cmp_ctx->read_bl, extent_cmp_ctx}});

    dout(10) << __func__ << ": async_read noted for " << soid << dendl;

    ctx->op_finishers[ctx->current_osd_subop_num].reset(
      new ReadFinisher(osd_op));
    return -EINPROGRESS;
  }

  // sync read
  vector<OSDOp> read_ops(1);
  OSDOp& read_op = read_ops[0];

  read_op.op.op = CEPH_OSD_OP_SYNC_READ;
  read_op.op.extent.offset = op.extent.offset;
  read_op.op.extent.length = op.extent.length;
  read_op.op.extent.truncate_seq = op.extent.truncate_seq;
  read_op.op.extent.truncate_size = op.extent.truncate_size;

  int result = do_osd_ops(ctx, read_ops);
  if (result < 0) {
    derr << __func__ << " failed " << result << dendl;
    return result;
  }
  return finish_extent_cmp(osd_op, read_op.outdata);
}

int PrimaryLogPG::finish_extent_cmp(OSDOp& osd_op, const bufferlist &read_bl)
{
  auto input_iter = osd_op.indata.begin();
  auto read_iter = read_bl.begin();
  uint64_t idx = 0;

  while (input_iter != osd_op.indata.end()) {
    char read_byte = (read_iter != read_bl.end() ? *read_iter : 0);
    if (*input_iter != read_byte) {
      return (-MAX_ERRNO - idx);
    }
    ++idx;
    ++input_iter;
    if (read_iter != read_bl.end()) {
      ++read_iter;
    }
  }

  return 0;
}

int PrimaryLogPG::do_read(OpContext *ctx, OSDOp& osd_op) {
  dout(20) << __func__ << dendl;
  auto& op = osd_op.op;
  auto& oi = ctx->new_obs.oi;
  auto& soid = oi.soid;
  __u32 seq = oi.truncate_seq;
  uint64_t size = oi.size;
  bool trimmed_read = false;

  dout(30) << __func__ << " oi.size: " << oi.size << dendl;
  dout(30) << __func__ << " oi.truncate_seq: " << oi.truncate_seq << dendl;
  dout(30) << __func__ << " op.extent.truncate_seq: " << op.extent.truncate_seq << dendl;
  dout(30) << __func__ << " op.extent.truncate_size: " << op.extent.truncate_size << dendl;

  // are we beyond truncate_size?
  if ( (seq < op.extent.truncate_seq) &&
       (op.extent.offset + op.extent.length > op.extent.truncate_size) &&
       (size > op.extent.truncate_size) )
    size = op.extent.truncate_size;

  if (op.extent.length == 0) //length is zero mean read the whole object
    op.extent.length = size;

  if (op.extent.offset >= size) {
    op.extent.length = 0;
    trimmed_read = true;
  } else if (op.extent.offset + op.extent.length > size) {
    op.extent.length = size - op.extent.offset;
    trimmed_read = true;
  }

  dout(30) << __func__ << "op.extent.length is now " << op.extent.length << dendl;

  // read into a buffer
  int result = 0;
  if (trimmed_read && op.extent.length == 0) {
    // read size was trimmed to zero and it is expected to do nothing
    // a read operation of 0 bytes does *not* do nothing, this is why
    // the trimmed_read boolean is needed
  } else if (pool.info.is_erasure()) {
    // The initialisation below is required to silence a false positive
    // -Wmaybe-uninitialized warning
    std::optional<uint32_t> maybe_crc;
    // If there is a data digest and it is possible we are reading
    // entire object, pass the digest.  FillInVerifyExtent will
    // will check the oi.size again.
    if (oi.is_data_digest() && op.extent.offset == 0 &&
        op.extent.length >= oi.size)
      maybe_crc = oi.data_digest;

    if (ctx->op->ec_direct_read()) {
      result = pgbackend->objects_read_sync(
        soid, op.extent.offset, op.extent.length, op.flags, &osd_op.outdata);

        dout(20) << " EC sync read for " << soid << " result=" << result << dendl;
    } else {
    ctx->pending_async_reads.push_back(
      make_pair(
        boost::make_tuple(op.extent.offset, op.extent.length, op.flags),
        make_pair(&osd_op.outdata,
		  new FillInVerifyExtent(&op.extent.length, &osd_op.rval,
					 &osd_op.outdata, maybe_crc, oi.size,
					 osd, soid, op.flags))));
    dout(10) << " async_read noted for " << soid << dendl;

    ctx->op_finishers[ctx->current_osd_subop_num].reset(
      new ReadFinisher(osd_op));
    }
  } else {
    int r = pgbackend->objects_read_sync(
      soid, op.extent.offset, op.extent.length, op.flags, &osd_op.outdata);
    // whole object?  can we verify the checksum?
    if (r >= 0 && op.extent.offset == 0 &&
        (uint64_t)r == oi.size && oi.is_data_digest()) {
      uint32_t crc = osd_op.outdata.crc32c(-1);
      if (oi.data_digest != crc) {
        osd->clog->error() << info.pgid << std::hex
                           << " full-object read crc 0x" << crc
                           << " != expected 0x" << oi.data_digest
                           << std::dec << " on " << soid;
        r = -EIO; // try repair later
      }
    }
    if (r == -EIO) {
      r = rep_repair_primary_object(soid, ctx);
    }
    if (r >= 0)
      op.extent.length = r;
    else if (r == -EAGAIN) {
      result = -EAGAIN;
    } else {
      result = r;
      op.extent.length = 0;
    }
    dout(10) << " read got " << r << " / " << op.extent.length
	     << " bytes from obj " << soid << dendl;
  }
  if (result >= 0) {
    ctx->delta_stats.num_rd_kb += shift_round_up(op.extent.length, 10);
    ctx->delta_stats.num_rd++;
  }
  return result;
}

int PrimaryLogPG::do_sparse_read(OpContext *ctx, OSDOp& osd_op) {
  dout(20) << __func__ << dendl;
  auto& op = osd_op.op;
  auto& oi = ctx->new_obs.oi;
  auto& soid = oi.soid;
  uint64_t size = oi.size;
  uint64_t offset = op.extent.offset;
  uint64_t length = op.extent.length;

  // are we beyond truncate_size?
  if ((oi.truncate_seq < op.extent.truncate_seq) &&
       (op.extent.offset + op.extent.length > op.extent.truncate_size) &&
       (size > op.extent.truncate_size)) {
    size = op.extent.truncate_size;
  }

  if (offset > size) {
    length = 0;
  } else if (offset + length > size) {
    length = size - offset;
  }

  ++ctx->num_read;
  if (pool.info.is_erasure() && !ctx->op->ec_direct_read()) {
    // translate sparse read to a normal one if not supported

    if (length > 0) {
      ctx->pending_async_reads.push_back(
        make_pair(
          boost::make_tuple(offset, length, op.flags),
          make_pair(
	    &osd_op.outdata,
	    new ToSparseReadResult(&osd_op.rval, &osd_op.outdata, offset,
				   &op.extent.length))));
      dout(10) << " async_read (was sparse_read) noted for " << soid << dendl;

      ctx->op_finishers[ctx->current_osd_subop_num].reset(
        new ReadFinisher(osd_op));
    } else {
      dout(10) << " sparse read ended up empty for " << soid << dendl;
      map<uint64_t, uint64_t> extents;
      encode(extents, osd_op.outdata);
      bufferlist data_bl;
      encode(data_bl, osd_op.outdata);
    }
  } else {
    // read into a buffer
    map<uint64_t, uint64_t> m;
    auto [shard_offset, shard_length] = pgbackend->extent_to_shard_extent(offset, length);
    int r = osd->store->fiemap(ch, ghobject_t(soid, ghobject_t::NO_GEN,
					      info.pgid.shard),
			       shard_offset, shard_length, m);
    if (r < 0)  {
      return r;
    }

    bufferlist data_bl;
    r = pgbackend->objects_readv_sync(soid, m, op.flags, &data_bl);
    if (r == -EIO) {
      r = rep_repair_primary_object(soid, ctx);
    }
    if (r < 0) {
      dout(10) << " sparse_read failed r=" << r << " from object " << soid << dendl;
      return r;
    }

    // Why SPARSE_READ need checksum? In fact, librbd always use sparse-read.
    // Maybe at first, there is no much whole objects. With continued use, more
    // and more whole object exist. So from this point, for spare-read add
    // checksum make sense.
    if ((uint64_t)r == oi.size && oi.is_data_digest()) {
      uint32_t crc = data_bl.crc32c(-1);
      if (oi.data_digest != crc) {
        osd->clog->error() << info.pgid << std::hex
          << " full-object read crc 0x" << crc
          << " != expected 0x" << oi.data_digest
          << std::dec << " on " << soid;
        r = rep_repair_primary_object(soid, ctx);
	if (r < 0) {
	  return r;
	}
      }
    }

    op.extent.length = r;

    encode(m, osd_op.outdata); // re-encode since it might be modified
    ::encode_destructively(data_bl, osd_op.outdata);

    dout(10) << " sparse_read got " << m.size() << " extents and " << r
             << " bytes from object " << soid << dendl;
  }

  ctx->delta_stats.num_rd_kb += shift_round_up(op.extent.length, 10);
  ctx->delta_stats.num_rd++;
  return 0;
}

int PrimaryLogPG::do_osd_ops(OpContext *ctx, vector<OSDOp>& ops)
{
  // 执行一个 MOSDOp 中按顺序排列的所有子操作。读操作直接填充各 OSDOp 的 outdata；
  // 写操作只更新 new_obs，并向 op_t 追加事务动作，此处不写磁盘。
  int result = 0;
  // obs/oi 是本次事务完成后的内存草稿，不是 OBC 当前已持久化的原始状态。
  SnapSetContext *ssc = ctx->obc->ssc;
  ObjectState& obs = ctx->new_obs;
  object_info_t& oi = obs.oi;
  const hobject_t& soid = oi.soid;
  const bool skip_data_digest = osd->store->has_builtin_csum() &&
    *osd->osd_skip_data_digest;

  // 同一请求的所有数据、属性和 omap 修改都汇集到这个 PGTransaction，
  // prepare_transaction() 返回后再由 finish_ctx() 补入对象元数据和 PG Log。
  PGTransaction* t = ctx->op_t.get();

  dout(10) << "do_osd_op " << soid << " " << ops << dendl;

  ctx->current_osd_subop_num = 0;
  // 复合请求严格按 vector 顺序执行；前一个子操作对 new_obs 的修改会成为
  // 后一个子操作看到的状态。遇到不可忽略的错误后停止执行剩余子操作。
  for (auto p = ops.begin(); p != ops.end(); ++p, ctx->current_osd_subop_num++, ctx->processed_subop_count++) {
    OSDOp& osd_op = *p;
    ceph_osd_op& op = osd_op.op;

    OpFinisher* op_finisher = nullptr;
    {
      // COPY_FROM、EC 异步读等操作可能让 execute_ctx() 稍后重入。
      // 首次进入负责启动异步工作，重入时由对应 finisher 完成同一个子操作。
      auto op_finisher_it = ctx->op_finishers.find(ctx->current_osd_subop_num);
      if (op_finisher_it != ctx->op_finishers.end()) {
        op_finisher = op_finisher_it->second.get();
      }
    }

    // TODO: check endianness (ceph_le32 vs uint32_t, etc.)
    // The fields in ceph_osd_op are little-endian (according to the definition in rados.h),
    // but the code in this function seems to treat them as native-endian.  What should the
    // tracepoints do?
    tracepoint(osd, do_osd_op_pre, soid.oid.name.c_str(), soid.snap.val, op.op, ceph_osd_op_name(op.op), op.flags);

    dout(10) << "do_osd_op  " << osd_op << dendl;

    auto bp = osd_op.indata.cbegin();

    // 判断本请求是否修改客户端可见的对象内容。user_modify 会让 finish_ctx() 推进 user_version；
    // cache、watch、manifest 管理等内部变化不应自动推进它。
    switch (op.op) {
      // non user-visible modifications
    case CEPH_OSD_OP_WATCH:
    case CEPH_OSD_OP_CACHE_EVICT:
    case CEPH_OSD_OP_CACHE_FLUSH:
    case CEPH_OSD_OP_CACHE_TRY_FLUSH:
    case CEPH_OSD_OP_UNDIRTY:
    case CEPH_OSD_OP_COPY_FROM:  // we handle user_version update explicitly
    case CEPH_OSD_OP_COPY_FROM2:
    case CEPH_OSD_OP_CACHE_PIN:
    case CEPH_OSD_OP_CACHE_UNPIN:
    case CEPH_OSD_OP_SET_REDIRECT:
    case CEPH_OSD_OP_SET_CHUNK:
    case CEPH_OSD_OP_TIER_PROMOTE:
    case CEPH_OSD_OP_TIER_FLUSH:
    case CEPH_OSD_OP_TIER_EVICT:
      break;
    default:
      if (op.op & CEPH_OSD_OP_MODE_WR)
	ctx->user_modify = true;
    }

    // 兼容旧协议对 truncate=-1 的编码，将其规范化成“不附带 truncate”。
    if (ceph_osd_op_uses_extent(op.op) &&
        op.extent.truncate_seq == 1 &&
        op.extent.truncate_size == (-1ULL)) {
      op.extent.truncate_size = 0;
      op.extent.truncate_seq = 0;
    }

    // ZERO 覆盖到对象末尾时等价于缩短对象，转换成 TRUNCATE 可避免写入大段零。
    // 不能转换成 DELETE，否则会连对象属性一起删除。
    if (op.op == CEPH_OSD_OP_ZERO &&
        obs.exists &&
        op.extent.offset < *osd->osd_max_object_size &&
        op.extent.length >= 1 &&
        op.extent.length <= *osd->osd_max_object_size &&
	op.extent.offset + op.extent.length >= oi.size) {
      if (op.extent.offset >= oi.size) {
        // no-op
	goto fail;
      }
      dout(10) << " munging ZERO " << op.extent.offset << "~" << op.extent.length
	       << " -> TRUNCATE " << op.extent.offset << " (old size is " << oi.size << ")" << dendl;
      op.op = CEPH_OSD_OP_TRUNCATE;
    }

    switch (op.op) {

      // --- 读取和条件检查 ---
      // 读取类操作从当前对象状态/ObjectStore 取数据写入 osd_op.outdata，
      // 通常不向 PGTransaction 添加持久化动作。

    case CEPH_OSD_OP_CMPEXT:
      ++ctx->num_read;
      tracepoint(osd, do_osd_op_pre_extent_cmp, soid.oid.name.c_str(),
		 soid.snap.val, oi.size, oi.truncate_seq, op.extent.offset,
		 op.extent.length, op.extent.truncate_size,
		 op.extent.truncate_seq);

      // 如果是比较对象内容的操作，有两种方式：
      // 1）如果是副本池对象，直接同步读出数据块并比较。
      // 2）如果是 erasure-coded 对象，因为怕阻塞，必须异步读出数据块并计算校验和；
      //       如果是异步，则会再次执行 execute_ctx() → do_osd_ops()，此时 op_finisher 不为空，直接调用 execute() 完成比较。
      if (op_finisher == nullptr) {
	result = do_extent_cmp(ctx, osd_op);
      } else {
	result = op_finisher->execute();
      }
      break;

    case CEPH_OSD_OP_SYNC_READ:
      // 同步读操作在 erasure-coded 池中不支持，必须使用异步读。
      if (pool.info.is_erasure()) {
	result = -EOPNOTSUPP;
	break;
      }
      // fall through
    case CEPH_OSD_OP_READ:
      ++ctx->num_read;
      tracepoint(osd, do_osd_op_pre_read, soid.oid.name.c_str(),
		 soid.snap.val, oi.size, oi.truncate_seq, op.extent.offset,
		 op.extent.length, op.extent.truncate_size,
		 op.extent.truncate_seq);
      if (op_finisher == nullptr) {
	if (!ctx->data_off) {
	  ctx->data_off = op.extent.offset;
	}
	result = do_read(ctx, osd_op);
      } else {
	result = op_finisher->execute();
      }
      break;

    case CEPH_OSD_OP_CHECKSUM:
      ++ctx->num_read;
      {
	tracepoint(osd, do_osd_op_pre_checksum, soid.oid.name.c_str(),
		   soid.snap.val, oi.size, oi.truncate_seq, op.checksum.type,
		   op.checksum.offset, op.checksum.length,
		   op.checksum.chunk_size);

	if (op_finisher == nullptr) {
	  result = do_checksum(ctx, osd_op, &bp);
	} else {
	  result = op_finisher->execute();
	}
      }
      break;

    /* map extents */
    case CEPH_OSD_OP_MAPEXT:
      // CEPH_OSD_OP_MAPEXT 是 Map Extents 操作，用于查询对象指定范围内，哪些区间实际分配了存储空间。
      tracepoint(osd, do_osd_op_pre_mapext, soid.oid.name.c_str(), soid.snap.val, op.extent.offset, op.extent.length);
      if (pool.info.is_erasure()) {
	result = -EOPNOTSUPP;
	break;
      }
      ++ctx->num_read;
      {
	// read into a buffer
	bufferlist bl;
	int r = osd->store->fiemap(ch, ghobject_t(soid, ghobject_t::NO_GEN,
						  info.pgid.shard),
				   op.extent.offset, op.extent.length, bl);
	auto bl_length = bl.length();
        osd_op.outdata = std::move(bl);
	if (r < 0)
	  result = r;
	else
	  ctx->delta_stats.num_rd_kb += shift_round_up(bl_length, 10);
	ctx->delta_stats.num_rd++;
	dout(10) << " map_extents done on object " << soid << dendl;
      }
      break;

    // CEPH_OSD_OP_SPARSE_READ 是稀疏读取：只返回指定范围中实际有数据的区间，并跳过空洞。
    /* map extents */
    case CEPH_OSD_OP_SPARSE_READ:
      tracepoint(osd, do_osd_op_pre_sparse_read, soid.oid.name.c_str(),
		 soid.snap.val, oi.size, oi.truncate_seq, op.extent.offset,
		 op.extent.length, op.extent.truncate_size,
		 op.extent.truncate_seq);
      if (op_finisher == nullptr) {
	result = do_sparse_read(ctx, osd_op);
      } else {
	result = op_finisher->execute();
      }
      break;

    // CEPH_OSD_OP_CALL 用于调用 Ceph 的 Object Class（对象类，简称 cls）方法。
    // 对这个对象调用：类名.方法名(输入参数)
    case CEPH_OSD_OP_CALL:
      {
	string cname, mname;
	bufferlist indata;
	try {
	  bp.copy(op.cls.class_len, cname);
	  bp.copy(op.cls.method_len, mname);
	  bp.copy(op.cls.indata_len, indata);
	} catch (ceph::buffer::error& e) {
	  dout(10) << "call unable to decode class + method + indata" << dendl;
	  dout(30) << "in dump: ";
	  osd_op.indata.hexdump(*_dout);
	  *_dout << dendl;
	  result = -EINVAL;
	  tracepoint(osd, do_osd_op_pre_call, soid.oid.name.c_str(), soid.snap.val, "???", "???");
	  break;
	}
	tracepoint(osd, do_osd_op_pre_call, soid.oid.name.c_str(), soid.snap.val, cname.c_str(), mname.c_str());

	ClassHandler::ClassData *cls;
	result = ClassHandler::get_instance().open_class(cname, &cls);
	ceph_assert(result == 0);   // init_op_flags() already verified this works.

	ClassHandler::ClassMethod *method = cls->get_method(mname);
	if (!method) {
	  dout(10) << "call method " << cname << "." << mname << " does not exist" << dendl;
	  result = -EOPNOTSUPP;
	  break;
	}

	int flags = method->get_flags();
	if (flags & CLS_METHOD_WR)
	  ctx->user_modify = true;

	bufferlist outdata;
	dout(10) << "call method " << cname << "." << mname << dendl;
	int prev_rd = ctx->num_read;
	int prev_wr = ctx->num_write;
	result = method->exec((cls_method_context_t)&ctx, indata, outdata);

	if (ctx->num_read > prev_rd && !(flags & CLS_METHOD_RD)) {
	  derr << "method " << cname << "." << mname << " tried to read object but is not marked RD" << dendl;
	  result = -EIO;
	  break;
	}
	if (ctx->num_write > prev_wr && !(flags & CLS_METHOD_WR)) {
	  derr << "method " << cname << "." << mname << " tried to update object but is not marked WR" << dendl;
	  result = -EIO;
	  break;
	}

	dout(10) << "method called response length=" << outdata.length() << dendl;
	op.extent.length = outdata.length();
	osd_op.outdata.claim_append(outdata);
	dout(30) << "out dump: ";
	osd_op.outdata.hexdump(*_dout);
	*_dout << dendl;
      }
      break;

    case CEPH_OSD_OP_STAT:
      // note: stat does not require RD
      {
	tracepoint(osd, do_osd_op_pre_stat, soid.oid.name.c_str(), soid.snap.val);

	if (obs.exists && !oi.is_whiteout()) {
	  encode(oi.size, osd_op.outdata);
	  encode(oi.mtime, osd_op.outdata);
	  dout(10) << "stat oi has " << oi.size << " " << oi.mtime << dendl;
	} else {
	  result = -ENOENT;
	  dout(10) << "stat oi object does not exist" << dendl;
	}

	ctx->delta_stats.num_rd++;
      }
      break;

    case CEPH_OSD_OP_ISDIRTY:
      ++ctx->num_read;
      {
	tracepoint(osd, do_osd_op_pre_isdirty, soid.oid.name.c_str(), soid.snap.val);
	bool is_dirty = obs.oi.is_dirty();
	encode(is_dirty, osd_op.outdata);
	ctx->delta_stats.num_rd++;
	result = 0;
      }
      break;

    case CEPH_OSD_OP_UNDIRTY:
      ++ctx->num_write;
      result = 0;
      {
	tracepoint(osd, do_osd_op_pre_undirty, soid.oid.name.c_str(), soid.snap.val);
	if (oi.is_dirty()) {
	  ctx->undirty = true;  // see make_writeable()
	  ctx->modify = true;
	  ctx->delta_stats.num_wr++;
	}
      }
      break;

    case CEPH_OSD_OP_CACHE_TRY_FLUSH:
      ++ctx->num_write;
      result = 0;
      {
	tracepoint(osd, do_osd_op_pre_try_flush, soid.oid.name.c_str(), soid.snap.val);
	if (ctx->lock_type != RWState::RWNONE) {
	  dout(10) << "cache-try-flush without SKIPRWLOCKS flag set" << dendl;
	  result = -EINVAL;
	  break;
	}
	if (pool.info.cache_mode == pg_pool_t::CACHEMODE_NONE || obs.oi.has_manifest()) {
	  result = -EINVAL;
	  break;
	}
	if (!obs.exists) {
	  result = 0;
	  break;
	}
	if (oi.is_cache_pinned()) {
	  dout(10) << "cache-try-flush on a pinned object, consider unpin this object first" << dendl;
	  result = -EPERM;
	  break;
	}
	if (oi.is_dirty()) {
	  result = start_flush(ctx->op, ctx->obc, false, NULL, std::nullopt);
	  if (result == -EINPROGRESS)
	    result = -EAGAIN;
	} else {
	  result = 0;
	}
      }
      break;

    case CEPH_OSD_OP_CACHE_FLUSH:
      ++ctx->num_write;
      result = 0;
      {
	tracepoint(osd, do_osd_op_pre_cache_flush, soid.oid.name.c_str(), soid.snap.val);
	if (ctx->lock_type == RWState::RWNONE) {
	  dout(10) << "cache-flush with SKIPRWLOCKS flag set" << dendl;
	  result = -EINVAL;
	  break;
	}
	if (pool.info.cache_mode == pg_pool_t::CACHEMODE_NONE || obs.oi.has_manifest()) {
	  result = -EINVAL;
	  break;
	}
	if (!obs.exists) {
	  result = 0;
	  break;
	}
	if (oi.is_cache_pinned()) {
	  dout(10) << "cache-flush on a pinned object, consider unpin this object first" << dendl;
	  result = -EPERM;
	  break;
	}
	hobject_t missing;
	if (oi.is_dirty()) {
	  result = start_flush(ctx->op, ctx->obc, true, &missing, std::nullopt);
	  if (result == -EINPROGRESS)
	    result = -EAGAIN;
	} else {
	  result = 0;
	}
	// Check special return value which has set missing_return
        if (result == -ENOENT) {
          dout(10) << __func__ << " CEPH_OSD_OP_CACHE_FLUSH got ENOENT" << dendl;
	  ceph_assert(!missing.is_min());
	  wait_for_unreadable_object(missing, ctx->op);
	  // Error code which is used elsewhere when wait_for_unreadable_object() is used
	  result = -EAGAIN;
	}
      }
      break;

    case CEPH_OSD_OP_CACHE_EVICT:
      ++ctx->num_write;
      result = 0;
      {
	tracepoint(osd, do_osd_op_pre_cache_evict, soid.oid.name.c_str(), soid.snap.val);
	if (pool.info.cache_mode == pg_pool_t::CACHEMODE_NONE || obs.oi.has_manifest()) {
	  result = -EINVAL;
	  break;
	}
	if (!obs.exists) {
	  result = 0;
	  break;
	}
	if (oi.is_cache_pinned()) {
	  dout(10) << "cache-evict on a pinned object, consider unpin this object first" << dendl;
	  result = -EPERM;
	  break;
	}
	if (oi.is_dirty()) {
	  result = -EBUSY;
	  break;
	}
	if (!oi.watchers.empty()) {
	  result = -EBUSY;
	  break;
	}
	if (soid.snap == CEPH_NOSNAP) {
	  result = _verify_no_head_clones(soid, ssc->snapset);
	  if (result < 0)
	    break;
	}
	result = _delete_oid(ctx, true, false);
	if (result >= 0) {
	  // mark that this is a cache eviction to avoid triggering normal
	  // make_writeable() clone creation in finish_ctx()
	  ctx->cache_operation = true;
	}
	osd->logger->inc(l_osd_tier_evict);
      }
      break;

    case CEPH_OSD_OP_GETXATTR:
      ++ctx->num_read;
      {
	string aname;
	bp.copy(op.xattr.name_len, aname);
	tracepoint(osd, do_osd_op_pre_getxattr, soid.oid.name.c_str(), soid.snap.val, aname.c_str());
	string name = "_" + aname;
	int r = getattr_maybe_cache(
	  ctx->obc,
	  name,
	  &(osd_op.outdata));
	if (r >= 0) {
	  op.xattr.value_len = osd_op.outdata.length();
	  result = 0;
	  ctx->delta_stats.num_rd_kb += shift_round_up(osd_op.outdata.length(), 10);
	} else
	  result = r;

	ctx->delta_stats.num_rd++;
      }
      break;

   case CEPH_OSD_OP_GETXATTRS:
      ++ctx->num_read;
      {
	tracepoint(osd, do_osd_op_pre_getxattrs, soid.oid.name.c_str(), soid.snap.val);
	map<string, bufferlist,less<>> out;
	result = getattrs_maybe_cache(
	  ctx->obc,
	  &out);

        bufferlist bl;
        encode(out, bl);
	ctx->delta_stats.num_rd_kb += shift_round_up(bl.length(), 10);
        ctx->delta_stats.num_rd++;
        osd_op.outdata.claim_append(bl);
      }
      break;

    case CEPH_OSD_OP_CMPXATTR:
      ++ctx->num_read;
      {
	string aname;
	bp.copy(op.xattr.name_len, aname);
	tracepoint(osd, do_osd_op_pre_cmpxattr, soid.oid.name.c_str(), soid.snap.val, aname.c_str());
	string name = "_" + aname;
	name[op.xattr.name_len + 1] = 0;

	bufferlist xattr;
	result = getattr_maybe_cache(
	  ctx->obc,
	  name,
	  &xattr);
	if (result < 0 && result != -EEXIST && result != -ENODATA)
	  break;

	ctx->delta_stats.num_rd++;
	ctx->delta_stats.num_rd_kb += shift_round_up(xattr.length(), 10);

	switch (op.xattr.cmp_mode) {
	case CEPH_OSD_CMPXATTR_MODE_STRING:
	  {
	    string val;
	    bp.copy(op.xattr.value_len, val);
	    val[op.xattr.value_len] = 0;
	    dout(10) << "CEPH_OSD_OP_CMPXATTR name=" << name << " val=" << val
		     << " op=" << (int)op.xattr.cmp_op << " mode=" << (int)op.xattr.cmp_mode << dendl;
	    result = do_xattr_cmp_str(op.xattr.cmp_op, val, xattr);
	  }
	  break;

        case CEPH_OSD_CMPXATTR_MODE_U64:
	  {
	    uint64_t u64val;
	    try {
	      decode(u64val, bp);
	    }
	    catch (ceph::buffer::error& e) {
	      result = -EINVAL;
	      goto fail;
	    }
	    dout(10) << "CEPH_OSD_OP_CMPXATTR name=" << name << " val=" << u64val
		     << " op=" << (int)op.xattr.cmp_op << " mode=" << (int)op.xattr.cmp_mode << dendl;
	    result = do_xattr_cmp_u64(op.xattr.cmp_op, u64val, xattr);
	  }
	  break;

	default:
	  dout(10) << "bad cmp mode " << (int)op.xattr.cmp_mode << dendl;
	  result = -EINVAL;
	}

	if (!result) {
	  dout(10) << "comparison returned false" << dendl;
	  result = -ECANCELED;
	  break;
	}
	if (result < 0) {
	  dout(10) << "comparison returned " << result << " " << cpp_strerror(-result) << dendl;
	  break;
	}

	dout(10) << "comparison returned true" << dendl;
      }
      break;

    case CEPH_OSD_OP_ASSERT_VER:
      ++ctx->num_read;
      {
	uint64_t ver = op.assert_ver.ver;
	tracepoint(osd, do_osd_op_pre_assert_ver, soid.oid.name.c_str(), soid.snap.val, ver);
	if (!ver) {
	  result = -EINVAL;
        } else if (ver < oi.user_version) {
	  result = -ERANGE;
        } else if (ver > oi.user_version) {
	  result = -EOVERFLOW;
        }
      }
      break;

    case CEPH_OSD_OP_LIST_WATCHERS:
      ++ctx->num_read;
      {
	tracepoint(osd, do_osd_op_pre_list_watchers, soid.oid.name.c_str(), soid.snap.val);
        obj_list_watch_response_t resp;

        map<pair<uint64_t, entity_name_t>, watch_info_t>::const_iterator oi_iter;
        for (oi_iter = oi.watchers.begin(); oi_iter != oi.watchers.end();
                                       ++oi_iter) {
          dout(20) << "key cookie=" << oi_iter->first.first
               << " entity=" << oi_iter->first.second << " "
               << oi_iter->second << dendl;
          ceph_assert(oi_iter->first.first == oi_iter->second.cookie);
          ceph_assert(oi_iter->first.second.is_client());

          watch_item_t wi(oi_iter->first.second, oi_iter->second.cookie,
		 oi_iter->second.timeout_seconds, oi_iter->second.addr);
          resp.entries.push_back(wi);
        }

        resp.encode(osd_op.outdata, ctx->get_features());
        result = 0;

        ctx->delta_stats.num_rd++;
        break;
      }

    case CEPH_OSD_OP_LIST_SNAPS:
      ++ctx->num_read;
      {
	tracepoint(osd, do_osd_op_pre_list_snaps, soid.oid.name.c_str(), soid.snap.val);
        obj_list_snap_response_t resp;

        if (!ssc) {
	  ssc = ctx->obc->ssc = get_snapset_context(soid, false);
        }
        ceph_assert(ssc);
	dout(20) << " snapset " << ssc->snapset << dendl;

        int clonecount = ssc->snapset.clones.size();
	clonecount++;  // for head
        resp.clones.reserve(clonecount);
        for (auto clone_iter = ssc->snapset.clones.begin();
	     clone_iter != ssc->snapset.clones.end(); ++clone_iter) {
          clone_info ci;
          ci.cloneid = *clone_iter;

	  hobject_t clone_oid = soid;
	  clone_oid.snap = *clone_iter;

	  auto p = ssc->snapset.clone_snaps.find(*clone_iter);
	  if (p == ssc->snapset.clone_snaps.end()) {
	    osd->clog->error() << "osd." << osd->whoami
			       << ": inconsistent clone_snaps found for oid "
			       << soid << " clone " << *clone_iter
			       << " snapset " << ssc->snapset;
	    result = -EINVAL;
	    break;
	  }
	  for (auto q = p->second.rbegin(); q != p->second.rend(); ++q) {
	    ci.snaps.push_back(*q);
	  }

          dout(20) << " clone " << *clone_iter << " snaps " << ci.snaps << dendl;

          map<snapid_t, interval_set<uint64_t> >::const_iterator coi;
          coi = ssc->snapset.clone_overlap.find(ci.cloneid);
          if (coi == ssc->snapset.clone_overlap.end()) {
            osd->clog->error() << "osd." << osd->whoami
			       << ": inconsistent clone_overlap found for oid "
			      << soid << " clone " << *clone_iter;
            result = -EINVAL;
            break;
          }
          const interval_set<uint64_t> &o = coi->second;
          ci.overlap.reserve(o.num_intervals());
          for (interval_set<uint64_t>::const_iterator r = o.begin();
               r != o.end(); ++r) {
            ci.overlap.push_back(pair<uint64_t,uint64_t>(r.get_start(),
							 r.get_len()));
          }

          map<snapid_t, uint64_t>::const_iterator si;
          si = ssc->snapset.clone_size.find(ci.cloneid);
          if (si == ssc->snapset.clone_size.end()) {
            osd->clog->error() << "osd." << osd->whoami
			       << ": inconsistent clone_size found for oid "
			       << soid << " clone " << *clone_iter;
            result = -EINVAL;
            break;
          }
          ci.size = si->second;

          resp.clones.push_back(ci);
        }
	if (result < 0) {
	  break;
	}
        if (!ctx->obc->obs.oi.is_whiteout()) {
          ceph_assert(obs.exists);
          clone_info ci;
          ci.cloneid = CEPH_NOSNAP;

          //Size for HEAD is oi.size
          ci.size = oi.size;

          resp.clones.push_back(ci);
        }
	resp.seq = ssc->snapset.seq;

        resp.encode(osd_op.outdata);
        result = 0;

        ctx->delta_stats.num_rd++;
        break;
      }

   case CEPH_OSD_OP_NOTIFY:
      ++ctx->num_read;
      {
	uint32_t timeout;
        bufferlist bl;

	try {
	  uint32_t ver; // obsolete
          decode(ver, bp);
	  decode(timeout, bp);
          decode(bl, bp);
	} catch (const ceph::buffer::error &e) {
	  timeout = 0;
	}
	tracepoint(osd, do_osd_op_pre_notify, soid.oid.name.c_str(), soid.snap.val, timeout);
	if (!timeout)
	  timeout = cct->_conf->osd_default_notify_timeout;

	notify_info_t n;
	n.timeout = timeout;
	n.notify_id = osd->get_next_id(get_osdmap_epoch());
	n.cookie = op.notify.cookie;
        n.bl = bl;
	ctx->notifies.push_back(n);

	// return our unique notify id to the client
	encode(n.notify_id, osd_op.outdata);
      }
      break;

    case CEPH_OSD_OP_NOTIFY_ACK:
      ++ctx->num_read;
      {
	try {
	  uint64_t notify_id = 0;
	  uint64_t watch_cookie = 0;
	  decode(notify_id, bp);
	  decode(watch_cookie, bp);
	  bufferlist reply_bl;
	  if (!bp.end()) {
	    decode(reply_bl, bp);
	  }
	  tracepoint(osd, do_osd_op_pre_notify_ack, soid.oid.name.c_str(), soid.snap.val, notify_id, watch_cookie, "Y");
	  OpContext::NotifyAck ack(notify_id, watch_cookie, reply_bl);
	  ctx->notify_acks.push_back(ack);
	} catch (const ceph::buffer::error &e) {
	  tracepoint(osd, do_osd_op_pre_notify_ack, soid.oid.name.c_str(), soid.snap.val, op.watch.cookie, 0, "N");
	  OpContext::NotifyAck ack(
	    // op.watch.cookie is actually the notify_id for historical reasons
	    op.watch.cookie
	    );
	  ctx->notify_acks.push_back(ack);
	}
      }
      break;

    case CEPH_OSD_OP_SETALLOCHINT:
      ++ctx->num_write;
      result = 0;
      {
	tracepoint(osd, do_osd_op_pre_setallochint, soid.oid.name.c_str(), soid.snap.val, op.alloc_hint.expected_object_size, op.alloc_hint.expected_write_size);
	maybe_create_new_object(ctx);
	oi.expected_object_size = op.alloc_hint.expected_object_size;
	oi.expected_write_size = op.alloc_hint.expected_write_size;
	oi.alloc_hint_flags = op.alloc_hint.flags;
        t->set_alloc_hint(soid, op.alloc_hint.expected_object_size,
                          op.alloc_hint.expected_write_size,
			  op.alloc_hint.flags);
      }
      break;


      // --- 写操作 ---

      // -- 对象数据 --

    case CEPH_OSD_OP_WRITE:
      ++ctx->num_write;
      result = 0;
      { // write
        // 普通范围写的主路径：校验参数及 truncate 顺序，必要时创建对象，
        // 向 op_t 添加 write，并同步更新对象大小、统计、摘要和脏区间。
        __u32 seq = oi.truncate_seq;
	tracepoint(osd, do_osd_op_pre_write, soid.oid.name.c_str(), soid.snap.val, oi.size, seq, op.extent.offset, op.extent.length, op.extent.truncate_size, op.extent.truncate_seq);
  // 检查请求声明的写入长度，是否等于实际携带的数据长度
	if (op.extent.length != osd_op.indata.length()) {
	  result = -EINVAL;
	  break;
	}

  // 如果 pool 配置了 write_fadvise_dontneed，则告诉底层存储：本次写入的数据预计近期不会再次访问，不必积极保留在缓存中。
	if (pool.info.has_flag(pg_pool_t::FLAG_WRITE_FADVISE_DONTNEED))
	  op.flags = op.flags | CEPH_OSD_OP_FLAG_FADVISE_DONTNEED;

  // 检查写入起始偏移是否满足 EC pool 的条带对齐要求
	if (pool.info.requires_aligned_append() &&
	    (op.extent.offset % pool.info.required_alignment() != 0)) {
	  result = -EOPNOTSUPP;
	  break;
	}

	if (!obs.exists) {
    // 新对象只能从 offset 0 开始
	  if (pool.info.requires_aligned_append() && op.extent.offset) {
	    result = -EOPNOTSUPP;
	    break;
	  }
	} else if (op.extent.offset != oi.size &&
		   pool.info.requires_aligned_append()) {
    // 已存在对象只能从当前末尾继续写
	  result = -EOPNOTSUPP;
	  break;
	}

    // truncate 是调整对象逻辑大小的操作，类似 Linux 的：truncate("file", new_size);
    // 这段处理的是：旧写请求在较新的 truncate 之后才到达 OSD，防止旧写重新扩展已经截短的对象。
    // seq                            // 对象当前记录的 truncate_seq
    // op.extent.truncate_seq         // 写请求携带的 truncate_seq
    // oi.size                        // 对象当前大小
    // op.extent.offset/length        // 本次写入范围
        if (seq && (seq > op.extent.truncate_seq) &&
            (op.extent.offset + op.extent.length > oi.size)) {
	  // old write, arrived after trimtrunc
	  op.extent.length = (op.extent.offset > oi.size ? 0 : oi.size - op.extent.offset);
	  dout(10) << " old truncate_seq " << op.extent.truncate_seq << " < current " << seq
		   << ", adjusting write length to " << op.extent.length << dendl;
	  bufferlist t;
    // 裁剪 length 和 indata 用于保留仍然有效的写入部分。
	  t.substr_of(osd_op.indata, 0, op.extent.length);
	  osd_op.indata.swap(t);
        }
	if (op.extent.truncate_seq > seq) {
	  // 写请求比对应 TRIMTRUNC 更早到达：先执行请求携带的 truncate，
	  // 保证乱序到达时仍得到与客户端操作顺序一致的对象状态。
	  if (obs.exists && !oi.is_whiteout()) {
	    dout(10) << " truncate_seq " << op.extent.truncate_seq << " > current " << seq
		     << ", truncating to " << op.extent.truncate_size << dendl;
	    t->truncate(soid, op.extent.truncate_size);
	    oi.truncate_seq = op.extent.truncate_seq;
	    oi.truncate_size = op.extent.truncate_size;
	    if (oi.size > op.extent.truncate_size) {
	      // 对象被缩短时，[truncate_size, old_size) 是本次删除的尾部范围。
	      // 将其记入 modified_ranges，供后续恢复/副本处理识别实际变化区间。
	      interval_set<uint64_t> trim;
	      trim.insert(op.extent.truncate_size,
		          oi.size - op.extent.truncate_size);
	      ctx->modified_ranges.union_of(trim);
	      // 该尾部范围不再与操作前数据一致，不能继续作为 clean region 使用。
	      ctx->clean_regions.mark_data_region_dirty(
		op.extent.truncate_size, oi.size - op.extent.truncate_size);
	      // 全对象摘要基于旧长度和旧内容计算；截短后已经失效，必须清除。
	      oi.clear_data_digest();
	    }
	    if (op.extent.truncate_size != oi.size) {
	      // 同时更新 new_obs 中的逻辑大小和 delta_stats.num_bytes；
	      // 这里只修改事务草稿，真正持久化仍在后续提交阶段完成。
	      truncate_update_size_and_usage(ctx->delta_stats,
		                             oi,
		                             op.extent.truncate_size);
	      // 扩展对象同样改变了长度，因此即使上面的“缩短”分支未执行，
	      // 旧的全对象摘要也不能保留。
	      oi.clear_data_digest();
	    }
	  } else {
	    dout(10) << " truncate_seq " << op.extent.truncate_seq << " > current " << seq
		     << ", but object is new" << dendl;
	    oi.truncate_seq = op.extent.truncate_seq;
	    oi.truncate_size = op.extent.truncate_size;
	  }
	}
	result = check_offset_and_length(
	  op.extent.offset, op.extent.length,
	  *osd->osd_max_object_size, get_dpp());
	if (result < 0)
	  break;

  /**
   * 用于确保本次 WRITE 的目标对象在事务草稿中处于“存在”状态。
   * RADOS 的 WRITE 具有隐式创建语义：
   * 对象存在     → 写入已有对象
   * 对象不存在   → 创建对象后再写入
   * 对象是白化项 → 恢复为普通对象后再写入
   */
	maybe_create_new_object(ctx);

	if (op.extent.length == 0) {
	  // 零长度写仍可能通过 offset 扩展对象；否则放入 nop，保留事务语义。
	  if (op.extent.offset > oi.size) {
	    if (seq && (seq > op.extent.truncate_seq)) {
	      //do nothing
	      //write arrived after truncate, we should not truncate to offset
	    } else {
	      t->truncate(
	        soid, op.extent.offset);
	      truncate_update_size_and_usage(ctx->delta_stats, oi,
	                                     op.extent.offset);
	      oi.clear_data_digest();
	    }
	  } else {
      // nop 是 No Operation，即“不修改对象内容的事务操作”。
	    t->nop(soid);
	  }
	} else {
	  t->write(
	    soid, op.extent.offset, op.extent.length, osd_op.indata, op.flags);
	}

  // data_digest 是对象完整数据内容的校验摘要。在这段代码中，它通常是 CRC32C。
	if (op.extent.offset == 0 && op.extent.length >= oi.size
            && !skip_data_digest) {
	  obs.oi.set_data_digest(osd_op.indata.crc32c(-1));
	} else if (op.extent.offset == oi.size && obs.oi.is_data_digest()) {
          if (skip_data_digest) {
            obs.oi.clear_data_digest();
          } else {
	    obs.oi.set_data_digest(osd_op.indata.crc32c(obs.oi.data_digest));
          }
	} else {
	  obs.oi.clear_data_digest();
        }
	// 同步维护 finish_ctx()/recovery 后续需要的三类草稿状态：
	// 对象大小和统计增量、实际修改范围，以及不能再视为 clean 的数据区域。
	write_update_size_and_usage(ctx->delta_stats, oi, ctx->modified_ranges,
				    op.extent.offset, op.extent.length);
	ctx->clean_regions.mark_data_region_dirty(op.extent.offset, op.extent.length);
	dout(10) << "clean_regions modified" << ctx->clean_regions << dendl;
      }
      break;

    case CEPH_OSD_OP_WRITEFULL:  // 用请求携带的数据完整替换整个对象。
      ++ctx->num_write;
      result = 0;
      { // write full object
	// 全量覆盖需要处理旧尾部：新内容更短时先截断，再从偏移 0 写入。
	tracepoint(osd, do_osd_op_pre_writefull, soid.oid.name.c_str(), soid.snap.val, oi.size, 0, op.extent.length);

	if (op.extent.length != osd_op.indata.length()) {
	  result = -EINVAL;
	  break;
	}
	result = check_offset_and_length(
	  0, op.extent.length,
          *osd->osd_max_object_size, get_dpp());
	if (result < 0)
	  break;

	if (pool.info.has_flag(pg_pool_t::FLAG_WRITE_FADVISE_DONTNEED))
	  op.flags = op.flags | CEPH_OSD_OP_FLAG_FADVISE_DONTNEED;

	maybe_create_new_object(ctx);
	if (pool.info.is_erasure()) {
	  t->truncate(soid, 0);
	} else if (obs.exists && op.extent.length < oi.size) {
	  t->truncate(soid, op.extent.length);
	}
	if (op.extent.length) {
	  t->write(soid, 0, op.extent.length, osd_op.indata, op.flags);
	}
        if (!skip_data_digest) {
	  obs.oi.set_data_digest(osd_op.indata.crc32c(-1));
        } else {
	  obs.oi.clear_data_digest();
	}
        ctx->clean_regions.mark_data_region_dirty(0,
          std::max((uint64_t)op.extent.length, oi.size));
	write_update_size_and_usage(ctx->delta_stats, oi, ctx->modified_ranges,
	    0, op.extent.length, true);
      }
      break;

    case CEPH_OSD_OP_WRITESAME:  // 将一小段输入数据重复多次，写满指定的对象范围。
      ++ctx->num_write;
      tracepoint(osd, do_osd_op_pre_writesame, soid.oid.name.c_str(), soid.snap.val, oi.size, op.writesame.offset, op.writesame.length, op.writesame.data_length);
      result = do_writesame(ctx, osd_op);
      break;

    case CEPH_OSD_OP_ROLLBACK :
      ++ctx->num_write;
      tracepoint(osd, do_osd_op_pre_rollback, soid.oid.name.c_str(), soid.snap.val);
      result = _rollback_to(ctx, osd_op);
      break;

    case CEPH_OSD_OP_ZERO:
      tracepoint(osd, do_osd_op_pre_zero, soid.oid.name.c_str(), soid.snap.val, op.extent.offset, op.extent.length);
      if (pool.info.requires_aligned_append()) {
	result = -EOPNOTSUPP;
	break;
      }
      ++ctx->num_write;
      { // zero
	result = check_offset_and_length(
	  op.extent.offset, op.extent.length,
          *osd->osd_max_object_size, get_dpp());
	if (result < 0)
	  break;

	if (op.extent.length && obs.exists && !oi.is_whiteout()) {
	  t->zero(soid, op.extent.offset, op.extent.length);
	  interval_set<uint64_t> ch;
	  ch.insert(op.extent.offset, op.extent.length);
	  ctx->modified_ranges.union_of(ch);
	  ctx->clean_regions.mark_data_region_dirty(op.extent.offset, op.extent.length);
	  ctx->delta_stats.num_wr++;
	  oi.clear_data_digest();
	} else {
	  // no-op
	}
      }
      break;
    case CEPH_OSD_OP_CREATE:
      ++ctx->num_write;
      result = 0;
      {
	tracepoint(osd, do_osd_op_pre_create, soid.oid.name.c_str(), soid.snap.val);
	if (obs.exists && !oi.is_whiteout() &&
	    (op.flags & CEPH_OSD_OP_FLAG_EXCL)) {
          result = -EEXIST; /* this is an exclusive create */
	} else {
	  if (osd_op.indata.length()) {
	    auto p = osd_op.indata.cbegin();
	    string category;
	    try {
	      decode(category, p);
	    }
	    catch (ceph::buffer::error& e) {
	      result = -EINVAL;
	      goto fail;
	    }
	    // category is no longer implemented.
	  }
	  maybe_create_new_object(ctx);
	  t->nop(soid);
	}
      }
      break;

    case CEPH_OSD_OP_TRIMTRUNC:
      op.extent.offset = op.extent.truncate_size;
      // falling through

    case CEPH_OSD_OP_TRUNCATE:
      tracepoint(osd, do_osd_op_pre_truncate, soid.oid.name.c_str(), soid.snap.val, oi.size, oi.truncate_seq, op.extent.offset, op.extent.length, op.extent.truncate_size, op.extent.truncate_seq);
      if (pool.info.requires_aligned_append()) {
	result = -EOPNOTSUPP;
	break;
      }
      ++ctx->num_write;
      result = 0;
      {
	// truncate
	if (!obs.exists || oi.is_whiteout()) {
	  dout(10) << " object dne, truncate is a no-op" << dendl;
	  break;
	}

        result = check_offset_and_length(
	  op.extent.offset, op.extent.length,
          *osd->osd_max_object_size, get_dpp());
        if (result < 0)
	  break;

	if (op.extent.truncate_seq) {
	  ceph_assert(op.extent.offset == op.extent.truncate_size);
	  if (op.extent.truncate_seq <= oi.truncate_seq) {
	    dout(10) << " truncate seq " << op.extent.truncate_seq << " <= current " << oi.truncate_seq
		     << ", no-op" << dendl;
	    break; // old
	  }
	  dout(10) << " truncate seq " << op.extent.truncate_seq << " > current " << oi.truncate_seq
		   << ", truncating" << dendl;
	  oi.truncate_seq = op.extent.truncate_seq;
	  oi.truncate_size = op.extent.truncate_size;
	}

	maybe_create_new_object(ctx);
	t->truncate(soid, op.extent.offset);
	if (oi.size > op.extent.offset) {
	  interval_set<uint64_t> trim;
	  trim.insert(op.extent.offset, oi.size-op.extent.offset);
	  ctx->modified_ranges.union_of(trim);
	  ctx->clean_regions.mark_data_region_dirty(op.extent.offset, oi.size - op.extent.offset);
	} else if (oi.size < op.extent.offset) {
          ctx->clean_regions.mark_data_region_dirty(oi.size, op.extent.offset - oi.size);
        }
	if (op.extent.offset != oi.size) {
          truncate_update_size_and_usage(ctx->delta_stats,
                                         oi,
                                         op.extent.offset);
	}
	ctx->delta_stats.num_wr++;
	// do no set exists, or we will break above DELETE -> TRUNCATE munging.

	oi.clear_data_digest();
      }
      break;

    case CEPH_OSD_OP_DELETE:
      ++ctx->num_write;
      result = 0;
      tracepoint(osd, do_osd_op_pre_delete, soid.oid.name.c_str(), soid.snap.val);
      {
	// _delete_oid() 将删除动作加入 op_t，并把 new_obs.exists 置为 false；
	// finish_ctx() 随后据此生成 DELETE 类型的 PG Log 条目。
	result = _delete_oid(ctx, false, ctx->ignore_cache);
      }
      break;

    // CEPH_OSD_OP_WATCH 用来让客户端在某个 RADOS 对象上注册、重连或取消 watcher。
    // 建立 watch 后，其他客户端可以对该对象发送 NOTIFY，OSD 会把通知转发给所有 watcher。
    case CEPH_OSD_OP_WATCH:
      ++ctx->num_write;
      result = 0;
      {
	// 持久化的 watcher 信息写入对象事务；实际建立/断开内存 Watch 和连接
	// 被记录到 ctx，等 repop 成功后由 do_osd_op_effects() 执行。
	tracepoint(osd, do_osd_op_pre_watch, soid.oid.name.c_str(), soid.snap.val,
		   op.watch.cookie, op.watch.op);
	if (!obs.exists) {
	  result = -ENOENT;
	  break;
	}
	result = 0;
        uint64_t cookie = op.watch.cookie;
        entity_name_t entity = ctx->reqid.name;
	ObjectContextRef obc = ctx->obc;

	dout(10) << "watch " << ceph_osd_watch_op_name(op.watch.op)
		 << ": ctx->obc=" << (void *)obc.get() << " cookie=" << cookie
		 << " oi.version=" << oi.version.version << " ctx->at_version=" << ctx->at_version << dendl;
	dout(10) << "watch: oi.user_version=" << oi.user_version<< dendl;
	dout(10) << "watch: peer_addr="
	  << ctx->op->get_req()->get_connection()->get_peer_addr() << dendl;

	uint32_t timeout = cct->_conf->osd_client_watch_timeout;
	if (op.watch.timeout != 0) {
	  timeout = op.watch.timeout;
	}

	watch_info_t w(cookie, timeout,
	  ctx->op->get_req()->get_connection()->get_peer_addr());
	if (op.watch.op == CEPH_OSD_WATCH_OP_WATCH ||
	    op.watch.op == CEPH_OSD_WATCH_OP_LEGACY_WATCH) {
	  if (oi.watchers.count(make_pair(cookie, entity))) {
	    dout(10) << " found existing watch " << w << " by " << entity << dendl;
	  } else {
	    dout(10) << " registered new watch " << w << " by " << entity << dendl;
	    oi.watchers[make_pair(cookie, entity)] = w;
	    t->nop(soid);  // make sure update the object_info on disk!
	  }
	  bool will_ping = (op.watch.op == CEPH_OSD_WATCH_OP_WATCH);
	  ctx->watch_connects.push_back(make_pair(w, will_ping));
        } else if (op.watch.op == CEPH_OSD_WATCH_OP_RECONNECT) {
	  if (!oi.watchers.count(make_pair(cookie, entity))) {
	    result = -ENOTCONN;
	    break;
	  }
	  dout(10) << " found existing watch " << w << " by " << entity << dendl;
	  ctx->watch_connects.push_back(make_pair(w, true));
        } else if (op.watch.op == CEPH_OSD_WATCH_OP_PING) {
	  /* Note: WATCH with PING doesn't cause may_write() to return true,
	   * so if there is nothing else in the transaction, this is going
	   * to run do_osd_op_effects, but not write out a log entry */
	  if (!oi.watchers.count(make_pair(cookie, entity))) {
	    result = -ENOTCONN;
	    break;
	  }
	  map<pair<uint64_t,entity_name_t>,WatchRef>::iterator p =
	    obc->watchers.find(make_pair(cookie, entity));
	  if (p == obc->watchers.end() ||
	      !p->second->is_connected()) {
	    // client needs to reconnect
	    result = -ETIMEDOUT;
	    break;
	  }
	  dout(10) << " found existing watch " << w << " by " << entity << dendl;
	  p->second->got_ping(ceph_clock_now());
	  result = 0;
        } else if (op.watch.op == CEPH_OSD_WATCH_OP_UNWATCH) {
	  map<pair<uint64_t, entity_name_t>, watch_info_t>::iterator oi_iter =
	    oi.watchers.find(make_pair(cookie, entity));
	  if (oi_iter != oi.watchers.end()) {
	    dout(10) << " removed watch " << oi_iter->second << " by "
		     << entity << dendl;
            oi.watchers.erase(oi_iter);
	    t->nop(soid);  // update oi on disk
	    ctx->watch_disconnects.push_back(
	      watch_disconnect_t(cookie, entity, false));
	  } else {
	    dout(10) << " can't remove: no watch by " << entity << dendl;
	  }
        }
      }
      break;

    case CEPH_OSD_OP_CACHE_PIN:
      tracepoint(osd, do_osd_op_pre_cache_pin, soid.oid.name.c_str(), soid.snap.val);
      if ((!pool.info.is_tier() ||
	  pool.info.cache_mode == pg_pool_t::CACHEMODE_NONE)) {
        result = -EINVAL;
        dout(10) << " pin object is only allowed on the cache tier " << dendl;
        break;
      }
      ++ctx->num_write;
      result = 0;
      {
	if (!obs.exists || oi.is_whiteout()) {
	  result = -ENOENT;
	  break;
	}

	if (!oi.is_cache_pinned()) {
	  oi.set_flag(object_info_t::FLAG_CACHE_PIN);
	  ctx->modify = true;
	  ctx->delta_stats.num_objects_pinned++;
	  ctx->delta_stats.num_wr++;
	}
      }
      break;

    case CEPH_OSD_OP_CACHE_UNPIN:
      tracepoint(osd, do_osd_op_pre_cache_unpin, soid.oid.name.c_str(), soid.snap.val);
      if ((!pool.info.is_tier() ||
	  pool.info.cache_mode == pg_pool_t::CACHEMODE_NONE)) {
        result = -EINVAL;
        dout(10) << " pin object is only allowed on the cache tier " << dendl;
        break;
      }
      ++ctx->num_write;
      result = 0;
      {
	if (!obs.exists || oi.is_whiteout()) {
	  result = -ENOENT;
	  break;
	}

	if (oi.is_cache_pinned()) {
	  oi.clear_flag(object_info_t::FLAG_CACHE_PIN);
	  ctx->modify = true;
	  ctx->delta_stats.num_objects_pinned--;
	  ctx->delta_stats.num_wr++;
	}
      }
      break;

    case CEPH_OSD_OP_SET_REDIRECT:
      ++ctx->num_write;
      result = 0;
      {
	if (pool.info.is_tier()) {
	  result = -EINVAL;
	  break;
	}
	if (!obs.exists) {
	  result = -ENOENT;
	  break;
	}
	if (get_osdmap()->require_osd_release < ceph_release_t::luminous) {
	  result = -EOPNOTSUPP;
	  break;
	}

	object_t target_name;
	object_locator_t target_oloc;
	snapid_t target_snapid = (uint64_t)op.copy_from.snapid;
	version_t target_version = op.copy_from.src_version;
	try {
	  decode(target_name, bp);
	  decode(target_oloc, bp);
	}
	catch (ceph::buffer::error& e) {
	  result = -EINVAL;
	  goto fail;
	}
	pg_t raw_pg;
	result = get_osdmap()->object_locator_to_pg(target_name, target_oloc, raw_pg);
	if (result < 0) {
	  dout(5) << " pool information is invalid: " << result << dendl;
	  break;
	}
	hobject_t target(target_name, target_oloc.key, target_snapid,
		raw_pg.ps(), raw_pg.pool(),
		target_oloc.nspace);
	if (target == soid) {
	  dout(20) << " set-redirect self is invalid" << dendl;
	  result = -EINVAL;
	  break;
	}

	bool need_reference = (osd_op.op.flags & CEPH_OSD_OP_FLAG_WITH_REFERENCE);
	bool has_reference = (oi.flags & object_info_t::FLAG_REDIRECT_HAS_REFERENCE);
	if (has_reference) {
	  result = -EINVAL;
	  dout(5) << " the object is already a manifest " << dendl;
	  break;
	}
	if (op_finisher == nullptr && need_reference) {
	  // start
	  ctx->op_finishers[ctx->current_osd_subop_num].reset(
	    new SetManifestFinisher(osd_op));
	  ManifestOpRef mop = std::make_shared<ManifestOp>(ctx->obc, new RefCountCallback(ctx, osd_op));
	  auto* fin = new C_SetManifestRefCountDone(this, soid, 0);
	  ceph_tid_t tid = refcount_manifest(soid, target, 
					      refcount_t::INCREMENT_REF, fin, std::nullopt);
	  fin->tid = tid;
	  mop->num_chunks++;
	  mop->tids[0] = tid;
	  manifest_ops[soid] = mop;
	  ctx->obc->start_block();
	  result = -EINPROGRESS;
	} else {
	  // finish
	  if (op_finisher) {
	    result = op_finisher->execute();
	    ceph_assert(result == 0);
	  }

	  if (!oi.has_manifest() && !oi.manifest.is_redirect())
	    ctx->delta_stats.num_objects_manifest++;

	  oi.set_flag(object_info_t::FLAG_MANIFEST);
	  oi.manifest.redirect_target = target;
	  oi.manifest.type = object_manifest_t::TYPE_REDIRECT;
	  t->truncate(soid, 0);
          ctx->clean_regions.mark_data_region_dirty(0, oi.size);
	  if (oi.is_omap() && pool.info.supports_omap()) {
	    t->omap_clear(soid);
	    obs.oi.clear_omap_digest();
	    obs.oi.clear_flag(object_info_t::FLAG_OMAP);
            ctx->clean_regions.mark_omap_dirty();
	  }
          write_update_size_and_usage(ctx->delta_stats, oi, ctx->modified_ranges,
	    0, oi.size, false);
	  ctx->delta_stats.num_bytes -= oi.size;
	  oi.size = 0;
	  oi.new_object();
	  oi.user_version = target_version;
	  ctx->user_at_version = target_version;
	  /* rm_attrs */
	  map<string,bufferlist,less<>> rmattrs;
	  result = getattrs_maybe_cache(ctx->obc, &rmattrs);
	  if (result < 0) {
	    dout(10) << __func__ << " error: " << cpp_strerror(result) << dendl;
	    return result;
	  }
	  map<string, bufferlist>::iterator iter;
	  for (iter = rmattrs.begin(); iter != rmattrs.end(); ++iter) {
	    const string& name = iter->first;
	    t->rmattr(soid, name);
	  }
	  if (!has_reference && need_reference) {
	    oi.set_flag(object_info_t::FLAG_REDIRECT_HAS_REFERENCE);
	  }
	  dout(10) << "set-redirect oid:" << oi.soid << " user_version: " << oi.user_version << dendl;
	  if (op_finisher) {
	    ctx->op_finishers.erase(ctx->current_osd_subop_num);
	  }
	}
      }

      break;

    case CEPH_OSD_OP_SET_CHUNK:
      ++ctx->num_write;
      result = 0;
      {
	if (pool.info.is_tier()) {
	  result = -EINVAL;
	  break;
	}
	if (!obs.exists) {
	  result = -ENOENT;
	  break;
	}
	if (get_osdmap()->require_osd_release < ceph_release_t::luminous) {
	  result = -EOPNOTSUPP;
	  break;
	}
	if (oi.manifest.is_redirect()) {
	  result = -EINVAL;
	  goto fail;
	}

	object_locator_t tgt_oloc;
	uint64_t src_offset, src_length, tgt_offset;
	object_t tgt_name;
	try {
	  decode(src_offset, bp);
	  decode(src_length, bp);
	  decode(tgt_oloc, bp);
	  decode(tgt_name, bp);
	  decode(tgt_offset, bp);
	}
	catch (ceph::buffer::error& e) {
	  result = -EINVAL;
	  goto fail;
	}

	if (!src_length) {
	  result = -EINVAL;
	  goto fail;
	}
	if (src_offset + src_length > oi.size) {
	  result = -ERANGE;
	  goto fail;
	}
	if (!(osd_op.op.flags & CEPH_OSD_OP_FLAG_WITH_REFERENCE)) {
	  result = -EOPNOTSUPP;
	  break;
	}
	if (pool.info.is_erasure()) {
	  result = -EOPNOTSUPP;
	  break;
	}

	for (auto &p : oi.manifest.chunk_map) {
	  interval_set<uint64_t> chunk;
	  chunk.insert(p.first, p.second.length);
	  if (chunk.intersects(src_offset, src_length)) {
	    dout(20) << __func__ << " overlapped !! offset: " << src_offset << " length: " << src_length
		    << " chunk_info: " << p << dendl;
	    result = -EOPNOTSUPP;
	    goto fail;
	  }
	}

	pg_t raw_pg;
	chunk_info_t chunk_info;
	result = get_osdmap()->object_locator_to_pg(tgt_name, tgt_oloc, raw_pg);
	if (result < 0) {
	  dout(5) << " pool information is invalid: " << result << dendl;
	  break;
	}
	hobject_t target(tgt_name, tgt_oloc.key, snapid_t(),
			 raw_pg.ps(), raw_pg.pool(),
			 tgt_oloc.nspace);
	bool has_reference = (oi.manifest.chunk_map.find(src_offset) != oi.manifest.chunk_map.end()) &&
			     (oi.manifest.chunk_map[src_offset].test_flag(chunk_info_t::FLAG_HAS_REFERENCE));
	if (has_reference) {
	  result = -EINVAL;
	  dout(5) << " the object is already a manifest " << dendl;
	  break;
	}
	chunk_info.oid = target;
	chunk_info.offset = tgt_offset;
	chunk_info.length = src_length;
	if (op_finisher == nullptr)  {
	  // start
	  ctx->op_finishers[ctx->current_osd_subop_num].reset(
	    new SetManifestFinisher(osd_op));
	  object_manifest_t set_chunk;
	  bool need_inc_ref = false;
	  set_chunk.chunk_map[src_offset] = chunk_info;
	  need_inc_ref = inc_refcount_by_set(ctx, set_chunk, osd_op);
	  if (need_inc_ref) {
	    result = -EINPROGRESS;
	    break;
	  }
	}
	if (op_finisher) {
	  result = op_finisher->execute();
	  ceph_assert(result == 0);
	}

	oi.manifest.chunk_map[src_offset] = chunk_info;
	if (!oi.has_manifest() && !oi.manifest.is_chunked())
	  ctx->delta_stats.num_objects_manifest++;
	oi.set_flag(object_info_t::FLAG_MANIFEST);
	oi.manifest.type = object_manifest_t::TYPE_CHUNKED;
	if (!has_reference) {
	  oi.manifest.chunk_map[src_offset].set_flag(chunk_info_t::FLAG_HAS_REFERENCE);
	}
	ctx->modify = true;
	ctx->cache_operation = true;

	dout(10) << "set-chunked oid:" << oi.soid << " user_version: " << oi.user_version
		 << " chunk_info: " << chunk_info << dendl;
	if (op_finisher) {
	  ctx->op_finishers.erase(ctx->current_osd_subop_num);
	}
      }

      break;

    case CEPH_OSD_OP_TIER_PROMOTE:
      ++ctx->num_write;
      result = 0;
      {
	if (pool.info.is_tier()) {
	  result = -EINVAL;
	  break;
	}
	if (!obs.exists) {
	  result = -ENOENT;
	  break;
	}
	if (get_osdmap()->require_osd_release < ceph_release_t::luminous) {
	  result = -EOPNOTSUPP;
	  break;
	}
	if (!obs.oi.has_manifest()) {
	  result = 0;
	  break;
	}

	if (op_finisher == nullptr) {
	  PromoteManifestCallback *cb;
	  object_locator_t my_oloc;
	  hobject_t src_hoid;

	  if (obs.oi.manifest.is_chunked()) {
	    src_hoid = obs.oi.soid;
	  } else if (obs.oi.manifest.is_redirect()) {
	    object_locator_t src_oloc(obs.oi.manifest.redirect_target);
	    my_oloc = src_oloc;
	    src_hoid = obs.oi.manifest.redirect_target;
	  } else {
	    ceph_abort_msg("unrecognized manifest type");
	  }
	  cb = new PromoteManifestCallback(ctx->obc, this, ctx);
          ctx->op_finishers[ctx->current_osd_subop_num].reset(
            new PromoteFinisher(cb));
	  unsigned flags = CEPH_OSD_COPY_FROM_FLAG_IGNORE_OVERLAY |
			   CEPH_OSD_COPY_FROM_FLAG_IGNORE_CACHE |
			   CEPH_OSD_COPY_FROM_FLAG_MAP_SNAP_CLONE |
			   CEPH_OSD_COPY_FROM_FLAG_RWORDERED;
	  unsigned src_fadvise_flags = LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL;
	  start_copy(cb, ctx->obc, src_hoid, my_oloc, 0, flags,
		     obs.oi.soid.snap == CEPH_NOSNAP,
		     src_fadvise_flags, 0);

	  dout(10) << "tier-promote oid:" << oi.soid << " manifest: " << obs.oi.manifest << dendl;
	  result = -EINPROGRESS;
	} else {
	  result = op_finisher->execute();
	  ceph_assert(result == 0);
	  ctx->op_finishers.erase(ctx->current_osd_subop_num);
	}
      }

      break;

    case CEPH_OSD_OP_TIER_FLUSH:
      ++ctx->num_write;
      result = 0;
      {
	if (pool.info.is_tier()) {
	  result = -EINVAL;
	  break;
	}
	if (!obs.exists) {
	  result = -ENOENT;
	  break;
	}
	if (get_osdmap()->require_osd_release < ceph_release_t::octopus) {
	  result = -EOPNOTSUPP;
	  break;
	}

	if (oi.is_dirty() || !obs.oi.has_manifest()) {
	  result = start_flush(ctx->op, ctx->obc, true, NULL, std::nullopt, true);
	  if (result == -EINPROGRESS)
	    result = -EAGAIN;
	} else {
	  result = 0;
	}
      }

      break;

    case CEPH_OSD_OP_TIER_EVICT:
      ++ctx->num_write;
      result = 0;
      {
	if (pool.info.is_tier()) {
	  result = -EINVAL;
	  break;
	}
	if (!obs.exists) {
	  result = -ENOENT;
	  break;
	}
	if (get_osdmap()->require_osd_release < ceph_release_t::octopus) {
	  result = -EOPNOTSUPP;
	  break;
	}
	if (!obs.oi.has_manifest()) {
	  result = -EINVAL;
	  break;
	}

	// The chunks already has a reference, so it is just enough to invoke truncate if necessary
	for (auto &p : obs.oi.manifest.chunk_map) {
	  p.second.set_flag(chunk_info_t::FLAG_MISSING);
	  // punch hole
	  t->zero(soid, p.first, p.second.length);
	  interval_set<uint64_t> ch;
	  ch.insert(p.first, p.second.length);
	  ctx->modified_ranges.union_of(ch);
	  ctx->clean_regions.mark_data_region_dirty(p.first, p.second.length);
	}
	oi.clear_data_digest();
	ctx->delta_stats.num_wr++;
	ctx->cache_operation = true;
	ctx->undirty = true;
	osd->logger->inc(l_osd_tier_evict);
      }

      break;

    case CEPH_OSD_OP_UNSET_MANIFEST:
      ++ctx->num_write;
      result = 0;
      {
	if (pool.info.is_tier()) {
	  result = -EINVAL;
	  break;
	}
	if (!obs.exists) {
	  result = -ENOENT;
	  break;
	}
	if (!oi.has_manifest()) {
	  result = -EOPNOTSUPP;
	  break;
	}
	if (get_osdmap()->require_osd_release < ceph_release_t::luminous) {
	  result = -EOPNOTSUPP;
	  break;
	}

	dec_all_refcount_manifest(oi, ctx);

	oi.clear_flag(object_info_t::FLAG_MANIFEST);
	oi.manifest = object_manifest_t();
	ctx->delta_stats.num_objects_manifest--;
	ctx->delta_stats.num_wr++;
	ctx->modify = true;
      }

      break;

      // -- 对象扩展属性 --
      // 用户 xattr 在 ObjectStore 中使用带 '_' 前缀的内部名称，和 Ceph 自己的
      // OI_ATTR、SS_ATTR 等保留属性区分开。

    case CEPH_OSD_OP_SETXATTR:
      ++ctx->num_write;
      result = 0;
      {
	if (cct->_conf->osd_max_attr_size > 0 &&
	    op.xattr.value_len > cct->_conf->osd_max_attr_size) {
	  tracepoint(osd, do_osd_op_pre_setxattr, soid.oid.name.c_str(), soid.snap.val, "???");
	  result = -EFBIG;
	  break;
	}
	unsigned max_name_len =
	  std::min<uint64_t>(osd->store->get_max_attr_name_length(),
			     cct->_conf->osd_max_attr_name_len);
	if (op.xattr.name_len > max_name_len) {
	  result = -ENAMETOOLONG;
	  break;
	}
	maybe_create_new_object(ctx);
	string aname;
	bp.copy(op.xattr.name_len, aname);
	tracepoint(osd, do_osd_op_pre_setxattr, soid.oid.name.c_str(), soid.snap.val, aname.c_str());
	string name = "_" + aname;
	bufferlist bl;
	bp.copy(op.xattr.value_len, bl);
	t->setattr(soid, name, bl);
 	ctx->delta_stats.num_wr++;
      }
      break;

    case CEPH_OSD_OP_RMXATTR:
      ++ctx->num_write;
      result = 0;
      {
	string aname;
	bp.copy(op.xattr.name_len, aname);
	tracepoint(osd, do_osd_op_pre_rmxattr, soid.oid.name.c_str(), soid.snap.val, aname.c_str());
	if (!obs.exists || oi.is_whiteout()) {
	  result = -ENOENT;
	  break;
	}
	string name = "_" + aname;
	t->rmattr(soid, name);
 	ctx->delta_stats.num_wr++;
      }
      break;


      // -- 复合写操作 --
      // APPEND/TMAP 等高层操作会改写成基础 READ/WRITE 子操作并递归复用本函数。
    case CEPH_OSD_OP_APPEND:
      {
	tracepoint(osd, do_osd_op_pre_append, soid.oid.name.c_str(), soid.snap.val, oi.size, oi.truncate_seq, op.extent.offset, op.extent.length, op.extent.truncate_size, op.extent.truncate_seq);
	// just do it inline; this works because we are happy to execute
	// fancy op on replicas as well.
	vector<OSDOp> nops(1);
	OSDOp& newop = nops[0];
	newop.op.op = CEPH_OSD_OP_WRITE;
	newop.op.extent.offset = oi.size;
	newop.op.extent.length = op.extent.length;
	newop.op.extent.truncate_seq = oi.truncate_seq;
        newop.indata = osd_op.indata;
	result = do_osd_ops(ctx, nops);
	osd_op.outdata = std::move(newop.outdata);
      }
      break;

    case CEPH_OSD_OP_STARTSYNC:
      result = 0;
      t->nop(soid);
      break;

      // -- 旧式 TMAP 操作 --
    case CEPH_OSD_OP_TMAPGET:
      tracepoint(osd, do_osd_op_pre_tmapget, soid.oid.name.c_str(), soid.snap.val);
      if (pool.info.is_erasure()) {
	result = -EOPNOTSUPP;
	break;
      }
      {
	vector<OSDOp> nops(1);
	OSDOp& newop = nops[0];
	newop.op.op = CEPH_OSD_OP_SYNC_READ;
	newop.op.extent.offset = 0;
	newop.op.extent.length = 0;
	result = do_osd_ops(ctx, nops);
	osd_op.outdata = std::move(newop.outdata);
      }
      break;

    case CEPH_OSD_OP_TMAPPUT:
      tracepoint(osd, do_osd_op_pre_tmapput, soid.oid.name.c_str(), soid.snap.val);
      if (pool.info.is_erasure()) {
	result = -EOPNOTSUPP;
	break;
      }
      {
	//_dout_lock.Lock();
	//osd_op.data.hexdump(*_dout);
	//_dout_lock.Unlock();

	// verify sort order
	bool unsorted = false;
	if (true) {
	  bufferlist header;
	  decode(header, bp);
	  uint32_t n;
	  decode(n, bp);
	  string last_key;
	  while (n--) {
	    string key;
	    decode(key, bp);
	    dout(10) << "tmapput key " << key << dendl;
	    bufferlist val;
	    decode(val, bp);
	    if (key < last_key) {
	      dout(10) << "TMAPPUT is unordered; resorting" << dendl;
	      unsorted = true;
	      break;
	    }
	    last_key = key;
	  }
	}

	// write it
	vector<OSDOp> nops(1);
	OSDOp& newop = nops[0];
	newop.op.op = CEPH_OSD_OP_WRITEFULL;
	newop.op.extent.offset = 0;
	newop.op.extent.length = osd_op.indata.length();
	newop.indata = osd_op.indata;

	if (unsorted) {
	  bp = osd_op.indata.begin();
	  bufferlist header;
	  map<string, bufferlist> m;
	  decode(header, bp);
	  decode(m, bp);
	  ceph_assert(bp.end());
	  bufferlist newbl;
	  encode(header, newbl);
	  encode(m, newbl);
	  newop.indata = newbl;
	}
	result = do_osd_ops(ctx, nops);
	ceph_assert(result == 0);
      }
      break;

    case CEPH_OSD_OP_TMAPUP:
      tracepoint(osd, do_osd_op_pre_tmapup, soid.oid.name.c_str(), soid.snap.val);
      if (pool.info.is_erasure()) {
	result = -EOPNOTSUPP;
	break;
      }
      ++ctx->num_write;
      result = do_tmapup(ctx, bp, osd_op);
      break;

    case CEPH_OSD_OP_TMAP2OMAP:
      ++ctx->num_write;
      tracepoint(osd, do_osd_op_pre_tmap2omap, soid.oid.name.c_str(), soid.snap.val);
      result = do_tmap2omap(ctx, op.tmap2omap.flags);
      break;

      // OMAP Read ops
    case CEPH_OSD_OP_OMAPGETKEYS:
      ++ctx->num_read;
      {
	string start_after;
	uint64_t max_return;
	try {
	  decode(start_after, bp);
	  decode(max_return, bp);
	}
	catch (ceph::buffer::error& e) {
	  result = -EINVAL;
	  tracepoint(osd, do_osd_op_pre_omapgetkeys, soid.oid.name.c_str(), soid.snap.val, "???", 0);
	  goto fail;
	}
	if (max_return > cct->_conf->osd_max_omap_entries_per_request) {
	  max_return = cct->_conf->osd_max_omap_entries_per_request;
	}
	tracepoint(osd, do_osd_op_pre_omapgetkeys, soid.oid.name.c_str(), soid.snap.val, start_after.c_str(), max_return);

	bufferlist bl;
	uint32_t num = 0;
	bool truncated = false;
	if (oi.is_omap()) {
          const auto result = osd->store->omap_iterate(
            ch, ghobject_t(soid),
            ObjectStore::omap_iter_seek_t{
              .seek_position = start_after,
              .seek_type = ObjectStore::omap_iter_seek_t::UPPER_BOUND
            },
            [&bl, &num, max_return,
	     max_bytes=cct->_conf->osd_max_omap_bytes_per_request]
            (std::string_view key, std::string_view value) mutable {
	      if (num >= max_return || bl.length() >= max_bytes) {
                return ObjectStore::omap_iter_ret_t::STOP;
	      }
	      encode(key, bl);
	      ++num;
              return ObjectStore::omap_iter_ret_t::NEXT;
            });
          if (result < 0) {
	    ceph_abort();
	  } else if (const auto more = static_cast<bool>(result); more) {
	    truncated = true;
	  }
	} // else return empty out_set
	encode(num, osd_op.outdata);
	osd_op.outdata.claim_append(bl);
	encode(truncated, osd_op.outdata);
	ctx->delta_stats.num_rd_kb += shift_round_up(osd_op.outdata.length(), 10);
	ctx->delta_stats.num_rd++;
      }
      break;

    case CEPH_OSD_OP_OMAPGETVALS:
      ++ctx->num_read;
      {
	string start_after;
	uint64_t max_return;
	string filter_prefix;
	try {
	  decode(start_after, bp);
	  decode(max_return, bp);
	  decode(filter_prefix, bp);
	}
	catch (ceph::buffer::error& e) {
	  result = -EINVAL;
	  tracepoint(osd, do_osd_op_pre_omapgetvals, soid.oid.name.c_str(), soid.snap.val, "???", 0, "???");
	  goto fail;
	}
	if (max_return > cct->_conf->osd_max_omap_entries_per_request) {
	  max_return = cct->_conf->osd_max_omap_entries_per_request;
	}
	tracepoint(osd, do_osd_op_pre_omapgetvals, soid.oid.name.c_str(), soid.snap.val, start_after.c_str(), max_return, filter_prefix.c_str());

	uint32_t num = 0;
	bool truncated = false;
	bufferlist bl;
	if (oi.is_omap()) {
	  using omap_iter_seek_t = ObjectStore::omap_iter_seek_t;
	  const auto result = osd->store->omap_iterate(
	    ch, ghobject_t(soid),
	    // try to seek as many keys-at-once as possible for the sake of performance.
	    // note complexity should be logarithmic, so seek(n/2) + seek(n/2) is worse
	    // than just seek(n).
	    ObjectStore::omap_iter_seek_t{
	      .seek_position = std::max(start_after, filter_prefix),
	      .seek_type = filter_prefix > start_after ? omap_iter_seek_t::LOWER_BOUND
						       : omap_iter_seek_t::UPPER_BOUND
	    },
	    [&bl, &truncated, &filter_prefix, &num, max_return,
	     max_bytes=cct->_conf->osd_max_omap_bytes_per_request]
	    (std::string_view key, std::string_view value) mutable {
	      if (key.substr(0, filter_prefix.size()) != filter_prefix) {
	        return ObjectStore::omap_iter_ret_t::STOP;
	      }
	      if (num >= max_return || bl.length() >= max_bytes) {
	        truncated = true;
	        return ObjectStore::omap_iter_ret_t::STOP;
	      }
	      encode(key, bl);
	      encode(value, bl);
	      ++num;
	      return ObjectStore::omap_iter_ret_t::NEXT;
	    });
	  if (result < 0) {
	    goto fail;
	  }
	} // else return empty out_set
	encode(num, osd_op.outdata);
	osd_op.outdata.claim_append(bl);
	encode(truncated, osd_op.outdata);
	ctx->delta_stats.num_rd_kb += shift_round_up(osd_op.outdata.length(), 10);
	ctx->delta_stats.num_rd++;
      }
      break;

    case CEPH_OSD_OP_OMAPGETHEADER:
      tracepoint(osd, do_osd_op_pre_omapgetheader, soid.oid.name.c_str(), soid.snap.val);
      if (!oi.is_omap()) {
	// return empty header
	break;
      }
      ++ctx->num_read;
      {
	osd->store->omap_get_header(ch, ghobject_t(soid), &osd_op.outdata);
	ctx->delta_stats.num_rd_kb += shift_round_up(osd_op.outdata.length(), 10);
	ctx->delta_stats.num_rd++;
      }
      break;

    case CEPH_OSD_OP_OMAPGETVALSBYKEYS:
      ++ctx->num_read;
      {
	set<string> keys_to_get;
	try {
	  decode(keys_to_get, bp);
	}
	catch (ceph::buffer::error& e) {
	  result = -EINVAL;
	  tracepoint(osd, do_osd_op_pre_omapgetvalsbykeys, soid.oid.name.c_str(), soid.snap.val, "???");
	  goto fail;
	}
	tracepoint(osd, do_osd_op_pre_omapgetvalsbykeys, soid.oid.name.c_str(), soid.snap.val, list_entries(keys_to_get).c_str());
	map<string, bufferlist> out;
	if (oi.is_omap()) {
	  osd->store->omap_get_values(ch, ghobject_t(soid), keys_to_get, &out);
	} // else return empty omap entries
	encode(out, osd_op.outdata);
	ctx->delta_stats.num_rd_kb += shift_round_up(osd_op.outdata.length(), 10);
	ctx->delta_stats.num_rd++;
      }
      break;

    case CEPH_OSD_OP_OMAP_CMP:
      ++ctx->num_read;
      {
	if (!obs.exists || oi.is_whiteout()) {
	  result = -ENOENT;
	  tracepoint(osd, do_osd_op_pre_omap_cmp, soid.oid.name.c_str(), soid.snap.val, "???");
	  break;
	}
	map<string, pair<bufferlist, int> > assertions;
	try {
	  decode(assertions, bp);
	}
	catch (ceph::buffer::error& e) {
	  result = -EINVAL;
	  tracepoint(osd, do_osd_op_pre_omap_cmp, soid.oid.name.c_str(), soid.snap.val, "???");
	  goto fail;
	}
	tracepoint(osd, do_osd_op_pre_omap_cmp, soid.oid.name.c_str(), soid.snap.val, list_keys(assertions).c_str());

	map<string, bufferlist> out;

	if (oi.is_omap()) {
	  set<string> to_get;
	  for (map<string, pair<bufferlist, int> >::iterator i = assertions.begin();
	       i != assertions.end();
	       ++i)
	    to_get.insert(i->first);
	  int r = osd->store->omap_get_values(ch, ghobject_t(soid),
					      to_get, &out);
	  if (r < 0) {
	    result = r;
	    break;
	  }
	} // else leave out empty

	//Should set num_rd_kb based on encode length of map
	ctx->delta_stats.num_rd++;

	int r = 0;
	bufferlist empty;
	for (map<string, pair<bufferlist, int> >::iterator i = assertions.begin();
	     i != assertions.end();
	     ++i) {
	  auto out_entry = out.find(i->first);
	  bufferlist &bl = (out_entry != out.end()) ?
	    out_entry->second : empty;
	  switch (i->second.second) {
	  case CEPH_OSD_CMPXATTR_OP_EQ:
	    if (!(bl == i->second.first)) {
	      r = -ECANCELED;
	    }
	    break;
	  case CEPH_OSD_CMPXATTR_OP_LT:
	    if (!(bl < i->second.first)) {
	      r = -ECANCELED;
	    }
	    break;
	  case CEPH_OSD_CMPXATTR_OP_GT:
	    if (!(bl > i->second.first)) {
	      r = -ECANCELED;
	    }
	    break;
	  default:
	    r = -EINVAL;
	    break;
	  }
	  if (r < 0)
	    break;
	}
	if (r < 0) {
	  result = r;
	}
      }
      break;

      /**
       * OMAP 是 Object Map，即附属于一个 RADOS 对象的持久化键值表。
       * 一个 RADOS 对象可以同时拥有：
       * 对象
       * ├── data        普通字节数据
       * ├── xattrs      少量扩展属性
       * └── omap
       *     ├── header
       *     └── key → value
       *
       * OMAP 常用于：
       *   RGW bucket index；
       *   RBD 元数据；
       *   CephFS 元数据对象；
       *   对象类的键值状态；
       *   队列和日志索引；
       *   分布式锁、版本信息。
       */

      // -- OMAP 写操作 --
      // 除向 op_t 添加 omap 动作外，还要标记 omap dirty、更新统计并清除旧摘要。
    case CEPH_OSD_OP_OMAPSETVALS:
      if (!pool.info.supports_omap()) {
	result = -EOPNOTSUPP;
	tracepoint(osd, do_osd_op_pre_omapsetvals, soid.oid.name.c_str(), soid.snap.val);
	break;
      }
      ++ctx->num_write;
      result = 0;
      {
	maybe_create_new_object(ctx);
	bufferlist to_set_bl;
	try {
	  decode_str_str_map_to_bl(bp, &to_set_bl);
	}
	catch (ceph::buffer::error& e) {
	  result = -EINVAL;
	  tracepoint(osd, do_osd_op_pre_omapsetvals, soid.oid.name.c_str(), soid.snap.val);
	  goto fail;
	}
	tracepoint(osd, do_osd_op_pre_omapsetvals, soid.oid.name.c_str(), soid.snap.val);
	if (cct->_conf->subsys.should_gather<dout_subsys, 20>()) {
	  dout(20) << "setting vals: " << dendl;
	  map<string,bufferlist> to_set;
	  bufferlist::const_iterator pt = to_set_bl.begin();
	  decode(to_set, pt);
	  for (map<string, bufferlist>::iterator i = to_set.begin();
	       i != to_set.end();
	       ++i) {
	    dout(20) << "\t" << i->first << dendl;
	  }
	}
	t->omap_setkeys(soid, to_set_bl);
	ctx->clean_regions.mark_omap_dirty();
	ctx->delta_stats.num_wr++;
        ctx->delta_stats.num_wr_kb += shift_round_up(to_set_bl.length(), 10);
      }
      obs.oi.set_flag(object_info_t::FLAG_OMAP);
      obs.oi.clear_omap_digest();
      break;

    case CEPH_OSD_OP_OMAPSETHEADER:
      tracepoint(osd, do_osd_op_pre_omapsetheader, soid.oid.name.c_str(), soid.snap.val);
      if (!pool.info.supports_omap()) {
	result = -EOPNOTSUPP;
	break;
      }
      ++ctx->num_write;
      result = 0;
      {
	maybe_create_new_object(ctx);
	t->omap_setheader(soid, osd_op.indata);
	ctx->clean_regions.mark_omap_dirty();
	ctx->delta_stats.num_wr++;
      }
      obs.oi.set_flag(object_info_t::FLAG_OMAP);
      obs.oi.clear_omap_digest();
      break;

    case CEPH_OSD_OP_OMAPCLEAR:
      tracepoint(osd, do_osd_op_pre_omapclear, soid.oid.name.c_str(), soid.snap.val);
      if (!pool.info.supports_omap()) {
	result = -EOPNOTSUPP;
	break;
      }
      ++ctx->num_write;
      result = 0;
      {
	if (!obs.exists || oi.is_whiteout()) {
	  result = -ENOENT;
	  break;
	}
	if (oi.is_omap()) {
	  t->omap_clear(soid);
	  ctx->clean_regions.mark_omap_dirty();
	  ctx->delta_stats.num_wr++;
	  obs.oi.clear_omap_digest();
	  obs.oi.clear_flag(object_info_t::FLAG_OMAP);
	}
      }
      break;

    case CEPH_OSD_OP_OMAPRMKEYS:
      if (!pool.info.supports_omap()) {
	result = -EOPNOTSUPP;
	tracepoint(osd, do_osd_op_pre_omaprmkeys, soid.oid.name.c_str(), soid.snap.val);
	break;
      }
      ++ctx->num_write;
      result = 0;
      {
	if (!obs.exists || oi.is_whiteout()) {
	  result = -ENOENT;
	  tracepoint(osd, do_osd_op_pre_omaprmkeys, soid.oid.name.c_str(), soid.snap.val);
	  break;
	}
	bufferlist to_rm_bl;
	try {
	  decode_str_set_to_bl(bp, &to_rm_bl);
	}
	catch (ceph::buffer::error& e) {
	  result = -EINVAL;
	  tracepoint(osd, do_osd_op_pre_omaprmkeys, soid.oid.name.c_str(), soid.snap.val);
	  goto fail;
	}
	tracepoint(osd, do_osd_op_pre_omaprmkeys, soid.oid.name.c_str(), soid.snap.val);
	t->omap_rmkeys(soid, to_rm_bl);
	ctx->clean_regions.mark_omap_dirty();
	ctx->delta_stats.num_wr++;
      }
      obs.oi.clear_omap_digest();
      break;

    case CEPH_OSD_OP_OMAPRMKEYRANGE:
      tracepoint(osd, do_osd_op_pre_omaprmkeyrange, soid.oid.name.c_str(), soid.snap.val);
      if (!pool.info.supports_omap()) {
	result = -EOPNOTSUPP;
	break;
      }
      ++ctx->num_write;
      result = 0;
      {
	if (!obs.exists || oi.is_whiteout()) {
	  result = -ENOENT;
	  break;
	}
	std::string key_begin, key_end;
	try {
	  decode(key_begin, bp);
	  decode(key_end, bp);
	} catch (ceph::buffer::error& e) {
	  result = -EINVAL;
	  goto fail;
	}
	t->omap_rmkeyrange(soid, key_begin, key_end);
        ctx->clean_regions.mark_omap_dirty();
	ctx->delta_stats.num_wr++;
      }
      obs.oi.clear_omap_digest();
      break;

    case CEPH_OSD_OP_COPY_GET:
      ++ctx->num_read;
      tracepoint(osd, do_osd_op_pre_copy_get, soid.oid.name.c_str(),
		 soid.snap.val);
      if (op_finisher == nullptr) {
	result = do_copy_get(ctx, bp, osd_op, ctx->obc);
      } else {
	result = op_finisher->execute();
      }
      break;

    case CEPH_OSD_OP_COPY_FROM:
    case CEPH_OSD_OP_COPY_FROM2:
      ++ctx->num_write;
      result = 0;
      {
	// COPY_FROM 跨对象读取源数据，无法在本次同步遍历中一次完成。首次执行
	// 启动 copy 并返回 EINPROGRESS；回调完成后用同一 ctx 重入本函数收尾。
	object_t src_name;
	object_locator_t src_oloc;
	uint32_t truncate_seq = 0;
	uint64_t truncate_size = 0;
	bool have_truncate = false;
	snapid_t src_snapid = (uint64_t)op.copy_from.snapid;
	version_t src_version = op.copy_from.src_version;

	if ((op.op == CEPH_OSD_OP_COPY_FROM2) &&
	    (op.copy_from.flags & ~CEPH_OSD_COPY_FROM_FLAGS)) {
	  dout(20) << "invalid copy-from2 flags 0x"
		  << std::hex << (int)op.copy_from.flags << std::dec << dendl;
	  result = -EINVAL;
	  break;
	}
	try {
	  decode(src_name, bp);
	  decode(src_oloc, bp);
	  // check if client sent us truncate_seq and truncate_size
	  if ((op.op == CEPH_OSD_OP_COPY_FROM2) &&
	      (op.copy_from.flags & CEPH_OSD_COPY_FROM_FLAG_TRUNCATE_SEQ)) {
	    decode(truncate_seq, bp);
	    decode(truncate_size, bp);
	    have_truncate = true;
	  }
	}
	catch (ceph::buffer::error& e) {
	  result = -EINVAL;
	  tracepoint(osd,
		     do_osd_op_pre_copy_from,
		     soid.oid.name.c_str(),
		     soid.snap.val,
		     "???",
		     0,
		     "???",
		     "???",
		     0,
		     src_snapid,
		     src_version);
	  goto fail;
	}
	tracepoint(osd,
		   do_osd_op_pre_copy_from,
		   soid.oid.name.c_str(),
		   soid.snap.val,
		   src_name.name.c_str(),
		   src_oloc.pool,
		   src_oloc.key.c_str(),
		   src_oloc.nspace.c_str(),
		   src_oloc.hash,
		   src_snapid,
		   src_version);
	if (op_finisher == nullptr) {
	  // 首次执行：保存 finisher 并启动异步复制。
	  pg_t raw_pg;
	  get_osdmap()->object_locator_to_pg(src_name, src_oloc, raw_pg);
	  hobject_t src(src_name, src_oloc.key, src_snapid,
			raw_pg.ps(), raw_pg.pool(),
			src_oloc.nspace);
	  if (src == soid) {
	    dout(20) << " copy from self is invalid" << dendl;
	    result = -EINVAL;
	    break;
	  }
	  CopyFromCallback *cb = new CopyFromCallback(ctx, osd_op);
	  if (have_truncate)
	    cb->set_truncate(truncate_seq, truncate_size);
          ctx->op_finishers[ctx->current_osd_subop_num].reset(
            new CopyFromFinisher(cb));
	  start_copy(cb, ctx->obc, src, src_oloc, src_version,
		     op.copy_from.flags,
		     false,
		     op.copy_from.src_fadvise_flags,
		     op.flags);
	  result = -EINPROGRESS;
	} else {
	  // 异步复制完成后的重入：把复制结果并入当前事务。
	  result = op_finisher->execute();
	  ceph_assert(result == 0);

          // COPY_FROM cannot be executed multiple times -- it must restart
          ctx->op_finishers.erase(ctx->current_osd_subop_num);
	}
      }
      break;

    default:
      tracepoint(osd, do_osd_op_pre_unknown, soid.oid.name.c_str(), soid.snap.val, op.op, ceph_osd_op_name(op.op));
      dout(1) << "unrecognized osd op " << op.op
	      << " " << ceph_osd_op_name(op.op)
	      << dendl;
      result = -EOPNOTSUPP;
    }

  fail:
    // 无论成功失败，都先保存该子操作的原始返回值，供 RETURNVEC 回复或 PG Log 去重记录使用。
    osd_op.rval = result;
    tracepoint(osd, do_osd_op_post, soid.oid.name.c_str(), soid.snap.val, op.op, ceph_osd_op_name(op.op), op.flags, result);
    if (result < 0 && (op.flags & CEPH_OSD_OP_FLAG_FAILOK) &&
        result != -EAGAIN && result != -EINPROGRESS)
      // FAILOK 只允许忽略普通子操作错误；EAGAIN/EINPROGRESS 表示整个上下文
      // 必须重试或等待异步完成，不能继续执行后续子操作。
      result = 0;

    // 未被 FAILOK 消化的错误终止复合请求，后续子操作不会执行。
    if (result < 0)
      break;
  }
  if (result < 0) {
    dout(10) << __func__ << " error: " << cpp_strerror(result) << dendl;
  }
  return result;
}

int PrimaryLogPG::_get_tmap(OpContext *ctx, bufferlist *header, bufferlist *vals)
{
  if (ctx->new_obs.oi.size == 0) {
    dout(20) << "unable to get tmap for zero sized " << ctx->new_obs.oi.soid << dendl;
    return -ENODATA;
  }
  vector<OSDOp> nops(1);
  OSDOp &newop = nops[0];
  newop.op.op = CEPH_OSD_OP_TMAPGET;
  do_osd_ops(ctx, nops);
  try {
    bufferlist::const_iterator i = newop.outdata.begin();
    decode(*header, i);
    (*vals).substr_of(newop.outdata, i.get_off(), i.get_remaining());
  } catch (...) {
    dout(20) << "unsuccessful at decoding tmap for " << ctx->new_obs.oi.soid
	     << dendl;
    return -EINVAL;
  }
  dout(20) << "successful at decoding tmap for " << ctx->new_obs.oi.soid
	   << dendl;
  return 0;
}

int PrimaryLogPG::_verify_no_head_clones(const hobject_t& soid,
					const SnapSet& ss)
{
  // verify that all clones have been evicted
  dout(20) << __func__ << " verifying clones are absent "
	   << ss << dendl;
  for (vector<snapid_t>::const_iterator p = ss.clones.begin();
       p != ss.clones.end();
       ++p) {
    hobject_t clone_oid = soid;
    clone_oid.snap = *p;
    if (is_missing_object(clone_oid))
      return -EBUSY;
    ObjectContextRef clone_obc = get_object_context(clone_oid, false);
    if (clone_obc && clone_obc->obs.exists) {
      dout(10) << __func__ << " cannot evict head before clone "
	       << clone_oid << dendl;
      return -EBUSY;
    }
    if (copy_ops.count(clone_oid)) {
      dout(10) << __func__ << " cannot evict head, pending promote on clone "
	       << clone_oid << dendl;
      return -EBUSY;
    }
  }
  return 0;
}

inline int PrimaryLogPG::_delete_oid(
  OpContext *ctx,
  bool no_whiteout,     // no whiteouts, no matter what.
  bool try_no_whiteout) // try not to whiteout
{
  SnapSet& snapset = ctx->new_snapset;
  ObjectState& obs = ctx->new_obs;
  object_info_t& oi = obs.oi;
  const hobject_t& soid = oi.soid;
  PGTransaction* t = ctx->op_t.get();

  // cache: cache: set whiteout on delete?
  bool whiteout = false;
  if (pool.info.cache_mode != pg_pool_t::CACHEMODE_NONE
      && !no_whiteout
      && !try_no_whiteout) {
    whiteout = true;
  }

  // in luminous or later, we can't delete the head if there are
  // clones. we trust the caller passing no_whiteout has already
  // verified they don't exist.
  if (should_whiteout(snapset, ctx->snapc)) {
    if (no_whiteout) {
      dout(20) << __func__ << " has or will have clones but no_whiteout=1"
	       << dendl;
    } else {
      dout(20) << __func__ << " has or will have clones; will whiteout"
	       << dendl;
      whiteout = true;
    }
  }
  dout(20) << __func__ << " " << soid << " whiteout=" << (int)whiteout
	   << " no_whiteout=" << (int)no_whiteout
	   << " try_no_whiteout=" << (int)try_no_whiteout
	   << dendl;
  if (!obs.exists || (obs.oi.is_whiteout() && whiteout))
    return -ENOENT;

  t->remove(soid);

  if (oi.size > 0) {
    interval_set<uint64_t> ch;
    ch.insert(0, oi.size);
    ctx->modified_ranges.union_of(ch);
    ctx->clean_regions.mark_data_region_dirty(0, oi.size);
  }

  ctx->clean_regions.mark_omap_dirty();
  ctx->delta_stats.num_wr++;
  if (soid.is_snap()) {
    ceph_assert(ctx->obc->ssc->snapset.clone_overlap.count(soid.snap));
    ctx->delta_stats.num_bytes -= ctx->obc->ssc->snapset.get_clone_bytes(soid.snap);
  } else {
    ctx->delta_stats.num_bytes -= oi.size;
  }
  oi.size = 0;
  oi.new_object();

  // disconnect all watchers
  for (map<pair<uint64_t, entity_name_t>, watch_info_t>::iterator p =
	 oi.watchers.begin();
       p != oi.watchers.end();
       ++p) {
    dout(20) << __func__ << " will disconnect watcher " << p->first << dendl;
    ctx->watch_disconnects.push_back(
      watch_disconnect_t(p->first.first, p->first.second, true));
  }
  oi.watchers.clear();

  if (whiteout) {
    dout(20) << __func__ << " setting whiteout on " << soid << dendl;
    oi.set_flag(object_info_t::FLAG_WHITEOUT);
    ctx->delta_stats.num_whiteouts++;
    t->create(soid);
    osd->logger->inc(l_osd_tier_whiteout);
    return 0;
  }

  if (oi.has_manifest()) {
    ctx->delta_stats.num_objects_manifest--;
    dec_all_refcount_manifest(oi, ctx);
  }

  // delete the head
  ctx->delta_stats.num_objects--;
  if (soid.is_snap())
    ctx->delta_stats.num_object_clones--;
  if (oi.is_whiteout()) {
    dout(20) << __func__ << " deleting whiteout on " << soid << dendl;
    ctx->delta_stats.num_whiteouts--;
    oi.clear_flag(object_info_t::FLAG_WHITEOUT);
  }
  if (oi.is_cache_pinned()) {
    ctx->delta_stats.num_objects_pinned--;
  }
  obs.exists = false;
  return 0;
}

int PrimaryLogPG::_rollback_to(OpContext *ctx, OSDOp& op)
{
  ObjectState& obs = ctx->new_obs;
  object_info_t& oi = obs.oi;
  const hobject_t& soid = oi.soid;
  snapid_t snapid = (uint64_t)op.op.snap.snapid;
  hobject_t missing_oid;

  dout(10) << "_rollback_to " << soid << " snapid " << snapid << dendl;

  ObjectContextRef rollback_to;

  int ret = find_object_context(
    hobject_t(soid.oid, soid.get_key(), snapid, soid.get_hash(), info.pgid.pool(),
	      soid.get_namespace()),
    &rollback_to, false, false, &missing_oid);
  if (ret == -EAGAIN) {
    /* clone must be missing */
    ceph_assert(is_degraded_or_backfilling_object(missing_oid) || is_degraded_on_async_recovery_target(missing_oid));
    dout(20) << "_rollback_to attempted to roll back to a missing or backfilling clone "
	     << missing_oid << " (requested snapid: ) " << snapid << dendl;
    block_write_on_degraded_snap(missing_oid, ctx->op);
    return ret;
  }
  /*
   * In rollback, if the head object is not manfest and the rollback_to is manifest,
   * the head object will become the manifest object. At this point,
   * we need to check adjacent clones beside the head object to calculate 
   * correct reference count for deduped chunks because the head object is now 
   * manifest. The reverse is also true---the head object is manifest, but the rollback_to
   * is not manifest.
   * Therefore, the following lines inserts the op to the waiting queue to wait until
   * unreadable object is recovered if either adjacent clones is 
   * unreadable to calculate chunk references.
   */
  auto block_write_if_unreadable = [this](ObjectContextRef obc, OpRequestRef op) {
    snapid_t sid = do_recover_adjacent_clones(obc, op);
    if (sid != snapid_t()) {
      hobject_t oid = obc->obs.oi.soid; 
      oid.snap = sid;
      block_write_on_unreadable_snap(oid, op);
      return -EAGAIN;
    } 
    return 0;
  };
  if (oi.has_manifest() && oi.manifest.is_chunked()) {
    int r = block_write_if_unreadable(ctx->obc, ctx->op);
    if (r < 0) {
      return r;
    }
  }
  if (rollback_to && rollback_to->obs.oi.has_manifest() &&
      rollback_to->obs.oi.manifest.is_chunked()) {
    int r = block_write_if_unreadable(rollback_to, ctx->op);
    if (r < 0) {
      return r;
    }
  }
  {
    ObjectContextRef promote_obc;
    cache_result_t tier_mode_result;
    if (obs.exists && obs.oi.has_manifest()) {
      /* 
       * In the case of manifest object, the object_info exists on the base tier at all time,
       * so promote_obc should be equal to rollback_to 
       * */
      promote_obc = rollback_to;
      tier_mode_result =
	maybe_handle_manifest_detail(
	  ctx->op,
	  true,
	  rollback_to);
    } else {
      tier_mode_result =
	maybe_handle_cache_detail(
	  ctx->op,
	  true,
	  rollback_to,
	  ret,
	  missing_oid,
	  true,
	  false,
	  &promote_obc);
    }
    switch (tier_mode_result) {
    case cache_result_t::NOOP:
      break;
    case cache_result_t::BLOCKED_PROMOTE:
      ceph_assert(promote_obc);
      block_write_on_snap_rollback(soid, promote_obc, ctx->op);
      return -EAGAIN;
    case cache_result_t::BLOCKED_FULL:
      block_write_on_full_cache(soid, ctx->op);
      return -EAGAIN;
    case cache_result_t::REPLIED_WITH_EAGAIN:
      ceph_abort_msg("this can't happen, no rollback on replica");
    default:
      ceph_abort_msg("must promote was set, other values are not valid");
      return -EAGAIN;
    }
  }

  if (ret == -ENOENT || (rollback_to && rollback_to->obs.oi.is_whiteout())) {
    // there's no snapshot here, or there's no object.
    // if there's no snapshot, we delete the object; otherwise, do nothing.
    dout(20) << "_rollback_to deleting head on " << soid.oid
	     << " because got ENOENT|whiteout on find_object_context" << dendl;
    if (ctx->obc->obs.oi.watchers.size()) {
      // Cannot delete an object with watchers
      ret = -EBUSY;
    } else {
      _delete_oid(ctx, false, false);
      ret = 0;
    }
  } else if (ret) {
    // ummm....huh? It *can't* return anything else at time of writing.
    ceph_abort_msg("unexpected error code in _rollback_to");
  } else { //we got our context, let's use it to do the rollback!
    hobject_t& rollback_to_sobject = rollback_to->obs.oi.soid;
    if (is_degraded_or_backfilling_object(rollback_to_sobject) ||
	is_degraded_on_async_recovery_target(rollback_to_sobject)) {
      dout(20) << "_rollback_to attempted to roll back to a degraded object "
	       << rollback_to_sobject << " (requested snapid: ) " << snapid << dendl;
      block_write_on_degraded_snap(rollback_to_sobject, ctx->op);
      ret = -EAGAIN;
    } else if (rollback_to->obs.oi.soid.snap == CEPH_NOSNAP) {
      // rolling back to the head; we just need to clone it.
      ctx->modify = true;
    } else {
      if (rollback_to->obs.oi.has_manifest() && rollback_to->obs.oi.manifest.is_chunked()) {
	/*
	 * looking at the following case, the foo head needs the reference of chunk4 and chunk5
	 * in case snap[1] is removed.
	 * 
	 * Before rollback to snap[1]:
	 *
	 * foo snap[1]:          [chunk4]          [chunk5]
	 * foo snap[0]: [                  chunk2                   ]
	 * foo head   :          [chunk1]                    [chunk3]
	 *
	 * After:
	 *
	 * foo snap[1]:          [chunk4]          [chunk5]
	 * foo snap[0]: [                  chunk2                   ]
	 * foo head   :          [chunk4]          [chunk5] 
	 *
	 */
	OpFinisher* op_finisher = nullptr;
	auto op_finisher_it = ctx->op_finishers.find(ctx->current_osd_subop_num);
	if (op_finisher_it != ctx->op_finishers.end()) {
	  op_finisher = op_finisher_it->second.get();
	}
	if (!op_finisher) {
	  bool need_inc_ref = inc_refcount_by_set(ctx, rollback_to->obs.oi.manifest, op);
	  if (need_inc_ref) {
	    ceph_assert(op_finisher_it == ctx->op_finishers.end());
	    ctx->op_finishers[ctx->current_osd_subop_num].reset(
		new SetManifestFinisher(op));
	    return -EINPROGRESS;
	  }
	} else {
	  op_finisher->execute();
	  ctx->op_finishers.erase(ctx->current_osd_subop_num);
	}
      }
      _do_rollback_to(ctx, rollback_to, op);
    }
  }
  return ret;
}

void PrimaryLogPG::_do_rollback_to(OpContext *ctx, ObjectContextRef rollback_to,
				    OSDOp& op)
{
  SnapSet& snapset = ctx->new_snapset;
  ObjectState& obs = ctx->new_obs;
  object_info_t& oi = obs.oi;
  const hobject_t& soid = oi.soid;
  PGTransaction* t = ctx->op_t.get();
  snapid_t snapid = (uint64_t)op.op.snap.snapid;
  hobject_t& rollback_to_sobject = rollback_to->obs.oi.soid;

  /* 1) Delete current head
   * 2) Clone correct snapshot into head
   * 3) Calculate clone_overlaps by following overlaps
   *    forward from rollback snapshot */
  dout(10) << "_do_rollback_to deleting " << soid.oid
	   << " and rolling back to old snap" << dendl;

  if (obs.exists) {
    t->remove(soid);
    if (obs.oi.has_manifest()) {
      dec_all_refcount_manifest(obs.oi, ctx);
      oi.manifest.clear();
      oi.manifest.type = object_manifest_t::TYPE_NONE;
      oi.clear_flag(object_info_t::FLAG_MANIFEST);
      ctx->delta_stats.num_objects_manifest--;
      ctx->cache_operation = true; // do not trigger to call ref function to calculate refcount
    }
  }
  t->clone(soid, rollback_to_sobject);
  t->add_obc(rollback_to);

  map<snapid_t, interval_set<uint64_t> >::iterator iter =
    snapset.clone_overlap.lower_bound(snapid);
  ceph_assert(iter != snapset.clone_overlap.end());
  interval_set<uint64_t> overlaps = iter->second;
  for ( ;
	iter != snapset.clone_overlap.end();
	++iter)
    overlaps.intersection_of(iter->second);

  if (obs.oi.size > 0) {
    interval_set<uint64_t> modified;
    modified.insert(0, obs.oi.size);
    overlaps.intersection_of(modified);
    modified.subtract(overlaps);
    ctx->modified_ranges.union_of(modified);
  }

  // Adjust the cached objectcontext
  maybe_create_new_object(ctx, true);
  ctx->delta_stats.num_bytes -= obs.oi.size;
  ctx->delta_stats.num_bytes += rollback_to->obs.oi.size;
  ctx->clean_regions.mark_data_region_dirty(0, std::max(obs.oi.size, rollback_to->obs.oi.size));
  ctx->clean_regions.mark_omap_dirty();
  obs.oi.size = rollback_to->obs.oi.size;
  if (rollback_to->obs.oi.is_data_digest())
    obs.oi.set_data_digest(rollback_to->obs.oi.data_digest);
  else
    obs.oi.clear_data_digest();
  if (rollback_to->obs.oi.is_omap_digest())
    obs.oi.set_omap_digest(rollback_to->obs.oi.omap_digest);
  else
    obs.oi.clear_omap_digest();

  if (rollback_to->obs.oi.has_manifest() && rollback_to->obs.oi.manifest.is_chunked()) {
    obs.oi.set_flag(object_info_t::FLAG_MANIFEST);
    obs.oi.manifest.type = rollback_to->obs.oi.manifest.type;
    obs.oi.manifest.chunk_map = rollback_to->obs.oi.manifest.chunk_map;
    ctx->cache_operation = true; 
    ctx->delta_stats.num_objects_manifest++;
  }

  if (rollback_to->obs.oi.is_omap()) {
    dout(10) << __func__ << " setting omap flag on " << obs.oi.soid << dendl;
    obs.oi.set_flag(object_info_t::FLAG_OMAP);
  } else {
    dout(10) << __func__ << " clearing omap flag on " << obs.oi.soid << dendl;
    obs.oi.clear_flag(object_info_t::FLAG_OMAP);
  }
}

void PrimaryLogPG::_make_clone(
  OpContext *ctx,
  PGTransaction* t,
  ObjectContextRef clone_obc,
  const hobject_t& head, const hobject_t& coid,
  object_info_t *poi)
{
  bufferlist bv;
  encode(*poi, bv, get_osdmap()->get_features(CEPH_ENTITY_TYPE_OSD, nullptr));

  t->clone(coid, head);
  setattr_maybe_cache(clone_obc, t, OI_ATTR, bv);
  rmattr_maybe_cache(clone_obc, t, SS_ATTR);
}

void PrimaryLogPG::make_writeable(OpContext *ctx)
{
  const hobject_t& soid = ctx->obs->oi.soid;
  SnapContext& snapc = ctx->snapc;

  // clone?
  ceph_assert(soid.snap == CEPH_NOSNAP);
  dout(20) << "make_writeable " << soid << " snapset=" << ctx->new_snapset
	   << "  snapc=" << snapc << dendl;

  bool was_dirty = ctx->obc->obs.oi.is_dirty();
  if (ctx->new_obs.exists) {
    // we will mark the object dirty
    if (ctx->undirty && was_dirty) {
      dout(20) << " clearing DIRTY flag" << dendl;
      ceph_assert(ctx->new_obs.oi.is_dirty());
      ctx->new_obs.oi.clear_flag(object_info_t::FLAG_DIRTY);
      --ctx->delta_stats.num_objects_dirty;
      osd->logger->inc(l_osd_tier_clean);
    } else if (!was_dirty && !ctx->undirty) {
      dout(20) << " setting DIRTY flag" << dendl;
      ctx->new_obs.oi.set_flag(object_info_t::FLAG_DIRTY);
      ++ctx->delta_stats.num_objects_dirty;
      osd->logger->inc(l_osd_tier_dirty);
    }
  } else {
    if (was_dirty) {
      dout(20) << " deletion, decrementing num_dirty and clearing flag" << dendl;
      ctx->new_obs.oi.clear_flag(object_info_t::FLAG_DIRTY);
      --ctx->delta_stats.num_objects_dirty;
    }
  }

  if ((ctx->new_obs.exists &&
       ctx->new_obs.oi.is_omap()) &&
      (!ctx->obc->obs.exists ||
       !ctx->obc->obs.oi.is_omap())) {
    ++ctx->delta_stats.num_objects_omap;
  }
  if ((!ctx->new_obs.exists ||
       !ctx->new_obs.oi.is_omap()) &&
      (ctx->obc->obs.exists &&
       ctx->obc->obs.oi.is_omap())) {
    --ctx->delta_stats.num_objects_omap;
  }

  if (ctx->new_snapset.seq > snapc.seq) {
    dout(10) << " op snapset is old" << dendl;
  }

  if ((ctx->obs->exists && !ctx->obs->oi.is_whiteout()) && // head exist(ed)
      snapc.snaps.size() &&                 // there are snaps
      !ctx->cache_operation &&
      snapc.snaps[0] > ctx->new_snapset.seq) {  // existing object is old
    // clone
    hobject_t coid = soid;
    coid.snap = snapc.seq;

    const auto snaps = [&] {
      auto last = find_if_not(
        begin(snapc.snaps), end(snapc.snaps),
        [&](snapid_t snap_id) { return snap_id > ctx->new_snapset.seq; });
      return vector<snapid_t>{begin(snapc.snaps), last};
    }();

    // prepare clone
    object_info_t static_snap_oi(coid);
    object_info_t *snap_oi;
    if (is_primary()) {
      ctx->clone_obc = object_contexts.lookup_or_create(static_snap_oi.soid);
      ctx->clone_obc->destructor_callback =
	new C_PG_ObjectContext(this, ctx->clone_obc.get());
      ctx->clone_obc->obs.oi = static_snap_oi;
      ctx->clone_obc->obs.exists = true;
      ctx->clone_obc->ssc = ctx->obc->ssc;
      ctx->clone_obc->ssc->ref++;
      if (pool.info.is_erasure())
	ctx->clone_obc->attr_cache = ctx->obc->attr_cache;
      snap_oi = &ctx->clone_obc->obs.oi;
      if (ctx->obc->obs.oi.has_manifest()) {
	if ((ctx->obc->obs.oi.flags & object_info_t::FLAG_REDIRECT_HAS_REFERENCE) &&
	    ctx->obc->obs.oi.manifest.is_redirect()) {
	  snap_oi->set_flag(object_info_t::FLAG_MANIFEST);
	  snap_oi->manifest.type = object_manifest_t::TYPE_REDIRECT;
	  snap_oi->manifest.redirect_target = ctx->obc->obs.oi.manifest.redirect_target;
	} else if (ctx->obc->obs.oi.manifest.is_chunked()) {
	  snap_oi->set_flag(object_info_t::FLAG_MANIFEST);
	  snap_oi->manifest.type = object_manifest_t::TYPE_CHUNKED;
	  snap_oi->manifest.chunk_map = ctx->obc->obs.oi.manifest.chunk_map;
	} else {
	  ceph_abort_msg("unrecognized manifest type");
	}
      }
      bool got = ctx->lock_manager.get_write_greedy(
	coid,
	ctx->clone_obc,
	ctx->op);
      ceph_assert(got);
      dout(20) << " got greedy write on clone_obc " << *ctx->clone_obc << dendl;
    } else {
      snap_oi = &static_snap_oi;
    }
    snap_oi->version = ctx->at_version;
    snap_oi->prior_version = ctx->obs->oi.version;
    snap_oi->copy_user_bits(ctx->obs->oi);

    _make_clone(ctx, ctx->op_t.get(), ctx->clone_obc, soid, coid, snap_oi);

    ctx->delta_stats.num_objects++;
    if (snap_oi->is_dirty()) {
      ctx->delta_stats.num_objects_dirty++;
      osd->logger->inc(l_osd_tier_dirty);
    }
    if (snap_oi->is_omap())
      ctx->delta_stats.num_objects_omap++;
    if (snap_oi->is_cache_pinned())
      ctx->delta_stats.num_objects_pinned++;
    if (snap_oi->has_manifest())
      ctx->delta_stats.num_objects_manifest++;
    ctx->delta_stats.num_object_clones++;
    ctx->new_snapset.clones.push_back(coid.snap);
    ctx->new_snapset.clone_size[coid.snap] = ctx->obs->oi.size;
    ctx->new_snapset.clone_snaps[coid.snap] = snaps;

    // clone_overlap should contain an entry for each clone
    // (an empty interval_set if there is no overlap)
    ctx->new_snapset.clone_overlap[coid.snap];
    if (ctx->obs->oi.size) {
      ctx->new_snapset.clone_overlap[coid.snap].insert(0, ctx->obs->oi.size);
    }

    // log clone
    dout(10) << " cloning v " << ctx->obs->oi.version
	     << " to " << coid << " v " << ctx->at_version
	     << " snaps=" << snaps
	     << " snapset=" << ctx->new_snapset << dendl;
    ctx->log.push_back(pg_log_entry_t(
			 pg_log_entry_t::CLONE, coid, ctx->at_version,
			 ctx->obs->oi.version,
			 ctx->obs->oi.user_version,
			 osd_reqid_t(), ctx->new_obs.oi.mtime, 0));
    encode(snaps, ctx->log.back().snaps);

    ctx->at_version.version++;
  }

  // update most recent clone_overlap and usage stats
  if (ctx->new_snapset.clones.size() > 0) {
    // the clone_overlap is difference of range between head and clones.
    // we need to check whether the most recent clone exists, if it's
    // been evicted, it's not included in the stats, but the clone_overlap
    // is still exist in the snapset, so we should update the
    // clone_overlap to make it sense.
    hobject_t last_clone_oid = soid;
    last_clone_oid.snap = ctx->new_snapset.clone_overlap.rbegin()->first;
    interval_set<uint64_t> &newest_overlap =
      ctx->new_snapset.clone_overlap.rbegin()->second;
    ctx->modified_ranges.intersection_of(newest_overlap);
    if (is_present_clone(last_clone_oid)) {
      // modified_ranges is still in use by the clone
      ctx->delta_stats.num_bytes += ctx->modified_ranges.size();
    }
    newest_overlap.subtract(ctx->modified_ranges);
  }

  if (snapc.seq > ctx->new_snapset.seq) {
    // update snapset with latest snap context
    ctx->new_snapset.seq = snapc.seq;
  }
  dout(20) << "make_writeable " << soid
	   << " done, snapset=" << ctx->new_snapset << dendl;
}


void PrimaryLogPG::write_update_size_and_usage(object_stat_sum_t& delta_stats, object_info_t& oi,
					       interval_set<uint64_t>& modified, uint64_t offset,
					       uint64_t length, bool write_full)
{
  interval_set<uint64_t> ch;
  if (write_full) {
    if (oi.size)
      ch.insert(0, oi.size);
  } else if (length)
    ch.insert(offset, length);
  modified.union_of(ch);
  if (write_full ||
      (offset + length > oi.size && length)) {
    uint64_t new_size = offset + length;
    delta_stats.num_bytes -= oi.size;
    delta_stats.num_bytes += new_size;
    oi.size = new_size;
  }

  delta_stats.num_wr++;
  delta_stats.num_wr_kb += shift_round_up(length, 10);
}

void PrimaryLogPG::truncate_update_size_and_usage(
  object_stat_sum_t& delta_stats,
  object_info_t& oi,
  uint64_t truncate_size)
{
  if (oi.size != truncate_size) {
    delta_stats.num_bytes -= oi.size;
    delta_stats.num_bytes += truncate_size;
    oi.size = truncate_size;
  }
}

void PrimaryLogPG::complete_disconnect_watches(
  ObjectContextRef obc,
  const list<watch_disconnect_t> &to_disconnect)
{
  for (list<watch_disconnect_t>::const_iterator i =
	 to_disconnect.begin();
       i != to_disconnect.end();
       ++i) {
    pair<uint64_t, entity_name_t> watcher(i->cookie, i->name);
    auto watchers_entry = obc->watchers.find(watcher);
    if (watchers_entry != obc->watchers.end()) {
      WatchRef watch = watchers_entry->second;
      dout(10) << "do_osd_op_effects disconnect watcher " << watcher << dendl;
      obc->watchers.erase(watcher);
      watch->remove(i->send_disconnect);
    } else {
      dout(10) << "do_osd_op_effects disconnect failed to find watcher "
	       << watcher << dendl;
    }
  }
}

void PrimaryLogPG::do_osd_op_effects(OpContext *ctx, const ConnectionRef& conn)
{
  entity_name_t entity = ctx->reqid.name;
  dout(15) << "do_osd_op_effects " << entity << " con " << conn.get() << dendl;

  // disconnects first
  complete_disconnect_watches(ctx->obc, ctx->watch_disconnects);

  ceph_assert(conn);

  auto session = conn->get_priv();

  for (list<pair<watch_info_t,bool> >::iterator i = ctx->watch_connects.begin();
       i != ctx->watch_connects.end();
       ++i) {
    pair<uint64_t, entity_name_t> watcher(i->first.cookie, entity);
    dout(15) << "do_osd_op_effects applying watch connect on session "
	     << (session ? session.get() : nullptr) << " watcher " << watcher
	     << dendl;
    WatchRef watch;
    if (ctx->obc->watchers.count(watcher)) {
      dout(15) << "do_osd_op_effects found existing watch watcher " << watcher
	       << dendl;
      watch = ctx->obc->watchers[watcher];
    } else {
      dout(15) << "do_osd_op_effects new watcher " << watcher
	       << dendl;
      watch = Watch::makeWatchRef(
	this, osd, ctx->obc, i->first.timeout_seconds,
	i->first.cookie, entity, conn->get_peer_addr());
      ctx->obc->watchers.insert(
	make_pair(
	  watcher,
	  watch));
    }
    watch->connect(conn, i->second);
  }

  for (list<notify_info_t>::iterator p = ctx->notifies.begin();
       p != ctx->notifies.end();
       ++p) {
    dout(10) << "do_osd_op_effects, notify " << *p << dendl;
    NotifyRef notif(
      Notify::makeNotifyRef(
	conn,
	ctx->reqid.name.num(),
	p->bl,
	p->timeout,
	p->cookie,
	p->notify_id,
	ctx->obc->obs.oi.user_version,
	osd));
    for (map<pair<uint64_t, entity_name_t>, WatchRef>::iterator i =
	   ctx->obc->watchers.begin();
	 i != ctx->obc->watchers.end();
	 ++i) {
      dout(10) << "starting notify on watch " << i->first << dendl;
      i->second->start_notify(notif);
    }
    notif->init();
  }

  for (list<OpContext::NotifyAck>::iterator p = ctx->notify_acks.begin();
       p != ctx->notify_acks.end();
       ++p) {
    if (p->watch_cookie)
      dout(10) << "notify_ack " << make_pair(*(p->watch_cookie), p->notify_id) << dendl;
    else
      dout(10) << "notify_ack " << make_pair("NULL", p->notify_id) << dendl;
    for (map<pair<uint64_t, entity_name_t>, WatchRef>::iterator i =
	   ctx->obc->watchers.begin();
	 i != ctx->obc->watchers.end();
	 ++i) {
      if (i->first.second != entity) continue;
      if (p->watch_cookie &&
	  *(p->watch_cookie) != i->first.first) continue;
      dout(10) << "acking notify on watch " << i->first << dendl;
      i->second->notify_ack(p->notify_id, p->reply_bl);
    }
  }
}

hobject_t PrimaryLogPG::generate_temp_object(const hobject_t& target)
{
  ostringstream ss;
  ss << "temp_" << info.pgid << "_" << get_role()
     << "_" << osd->monc->get_global_id() << "_" << (++temp_seq);
  hobject_t hoid = target.make_temp_hobject(ss.str());
  dout(20) << __func__ << " " << hoid << dendl;
  return hoid;
}

hobject_t PrimaryLogPG::get_temp_recovery_object(
  const hobject_t& target,
  eversion_t version)
{
  ostringstream ss;
  ss << "temp_recovering_" << info.pgid  // (note this includes the shardid)
     << "_" << version
     << "_" << info.history.same_interval_since
     << "_" << target.snap;
  // pgid + version + interval + snapid is unique, and short
  hobject_t hoid = target.make_temp_hobject(ss.str());
  dout(20) << __func__ << " " << hoid << dendl;
  return hoid;
}

int PrimaryLogPG::prepare_transaction(OpContext *ctx)
{
  // 将一个客户端请求中的 OSDOp 转换为读结果或待提交的 PGTransaction。
  // 本函数只准备修改及 PG Log，真正写入主 OSD 和副本发生在 issue_repop()。
  ceph_assert(!ctx->ops->empty());

  // snapc 描述本次写入需要保护的快照集合；无效的序列/列表组合不能用于
  // 创建 snap clone，因此在执行任何子操作前直接拒绝。
  if (!ctx->snapc.is_valid()) {
    dout(10) << " invalid snapc " << ctx->snapc << dendl;
    return -EINVAL;
  }

  // 依次执行请求中的所有子操作。读操作填充 outdata；
  // 写操作修改 new_obs，并向 ctx->op_t 追加操作，但此时尚未写入 ObjectStore。
  int result = do_osd_ops(ctx, *ctx->ops);
  if (result < 0) {
    // 写请求已经得到确定错误时，仍需把 reqid 和错误码写入 PG Log。
    // 客户端重试相同 reqid 时即可返回原结果，而不会因对象状态变化重新执行。
    if (ctx->op->may_write() &&
	get_osdmap()->require_osd_release >= ceph_release_t::kraken) {
      ctx->update_log_only = true;
    }
    // execute_ctx() 根据 update_log_only 决定记录 ERROR 条目，或按普通失败回复。
    return result;
  }

  // 没有事务动作且未被强制标记为修改，说明这是纯读，或者写操作最终是 no-op。
  if (ctx->op_t->empty() && !ctx->modify) {
    // 异步读尚未结束时统计值还不完整，完成回调重入后再累计。
    if (ctx->pending_async_reads.empty())
      unstable_stats.add(ctx->delta_stats);
    // no-op 写也要记录成功结果用于 reqid 去重；
    // 纯读虽然进入该分支，但 may_write() 为 false，不会设置 update_log_only。
    if (ctx->op->may_write() &&
	get_osdmap()->require_osd_release >= ceph_release_t::kraken) {
      ctx->update_log_only = true;
    }
    return result;
  }

  // 只有本次事务会增加对象数或数据字节数时才检查 pool full；
  // 覆盖等不增长空间的修改允许继续。key/omap 的空间增量尚未纳入该判断。
  if ((ctx->delta_stats.num_bytes > 0 ||
       ctx->delta_stats.num_objects > 0) &&  // FIXME: keys?
      pool.info.has_flag(pg_pool_t::FLAG_FULL)) {
    auto m = ctx->op->get_req<MOSDOp>();
    // MDS 写和显式 FULL_FORCE 请求允许绕过 full，用于受控的元数据操作或清理。
    if (ctx->reqid.name.is_mds() ||   // FIXME: ignore MDS for now
	m->has_flag(CEPH_OSD_FLAG_FULL_FORCE)) {
      dout(20) << __func__ << " full, but proceeding due to FULL_FORCE or MDS"
	       << dendl;
    } else if (m->has_flag(CEPH_OSD_FLAG_FULL_TRY)) {
      // FULL_TRY 表示客户端接受明确失败：配额导致 full 返回 EDQUOT，
      // 其余容量不足返回 ENOSPC。
      dout(20) << __func__ << " full, replying to FULL_TRY op" << dendl;
      return pool.info.has_flag(pg_pool_t::FLAG_FULL_QUOTA) ? -EDQUOT : -ENOSPC;
    } else {
      // 普通客户端理论上应在 OSDMap 的 full 标志处停止写入。
      // 这里返回 EAGAIN，execute_ctx() 会释放当前 ctx 而不回复，让请求稍后基于新状态重试。
      dout(20) << __func__ << " full, dropping request (bad client)" << dendl;
      return -EAGAIN;
    }
  }

  const hobject_t& soid = ctx->obs->oi.soid;
  // 写 head 对象前，根据 snapc/SnapSet 判断是否需要先把旧版本保存为 clone。
  // 对已有 snap clone 的直接操作不走该步骤。
  // CEPH_NOSNAP：表示对象的 head（当前版本）
  if (soid.snap == CEPH_NOSNAP)
    make_writeable(ctx);

  // 完成事务的对象元数据、统计和 PG Log 条目：操作后对象仍存在记 MODIFY，
  // 已被删除则记 DELETE；这些内容随后与对象事务一起复制和提交。
  finish_ctx(ctx,
	     ctx->new_obs.exists ? pg_log_entry_t::MODIFY :
	     pg_log_entry_t::DELETE,
	     result);

  return result;
}

void PrimaryLogPG::finish_ctx(OpContext *ctx, int log_op_type, int result)
{
  // 收尾 do_osd_ops() 生成的对象修改：补齐版本和持久化元数据，构造 PG Log 条目，
  // 并推进 primary 的内存投影。所有磁盘修改仍只存在于 ctx->op_t 中。
  const hobject_t& soid = ctx->obs->oi.soid;
  dout(20) << __func__ << " " << soid << " " << ctx
	   << " op " << pg_log_entry_t::get_op_name(log_op_type)
	   << dendl;
  utime_t now = ceph_clock_now();


  //   类型	                  含义
  // 普通对象	            数据直接存储在自身
  // Chunked manifest	   不同范围可以映射到多个 chunk，支持共享和去重
  // Redirect manifest	 整个对象重定向到另一个对象

  // 普通写入使 chunked manifest 对象变脏时，更新 chunk map，并释放被覆盖的 dedup chunk 引用。
  // cache 操作和 promote 有各自的引用处理，不能在此重复执行。
  if (ctx->new_obs.oi.is_dirty() &&
    (ctx->obs->oi.has_manifest() && ctx->obs->oi.manifest.is_chunked()) &&
    !ctx->cache_operation &&
    log_op_type != pg_log_entry_t::PROMOTE) {
    update_chunk_map_by_dirty(ctx);
    // 创建快照 clone 时旧数据仍由 clone 引用，不能按普通覆盖写减少 chunk 引用。
    if (!ctx->delta_stats.num_object_clones) {
      dec_refcount_by_dirty(ctx);
    }
  }

  // 阶段 1：为客户端可见的修改分配 user_version。
  // watch 等内部状态变化虽然可能修改对象元数据，但不应推进客户端观察到的对象内容版本。
  if (ctx->user_modify) {
    // 同时参考 PG 和对象当前的 user_version，防止版本倒退，然后加一。
    ctx->user_at_version = std::max(info.last_user_version, ctx->new_obs.oi.user_version) + 1;
    /* In order for new clients and old clients to interoperate properly
     * when exchanging versions, we need to lower bound the user_version
     * (which our new clients pay proper attention to)
     * by the at_version (which is all the old clients can ever see). */
    if (ctx->at_version.version > ctx->user_at_version)
      ctx->user_at_version = ctx->at_version.version;
    ctx->new_obs.oi.user_version = ctx->user_at_version;
  }
  // 从事务草稿统计实际写入的数据量，提交后用于 OSD 性能计数。
  ctx->bytes_written = ctx->op_t->get_bytes_written();

  if (ctx->new_obs.exists) {
    // 阶段 2：补齐操作后的 object_info。
    // version 是本次 PG Log 版本，prior_version 指向修改前版本，last_reqid 用于识别最后一次对象修改。
    ctx->new_obs.oi.version = ctx->at_version;
    ctx->new_obs.oi.prior_version = ctx->obs->oi.version;
    ctx->new_obs.oi.last_reqid = ctx->reqid;
    if (ctx->mtime != utime_t()) {
      // mtime 是客户端指定的对象时间；local_mtime 记录 OSD 本地处理时间。
      ctx->new_obs.oi.mtime = ctx->mtime;
      dout(10) << " set mtime to " << ctx->new_obs.oi.mtime << dendl;
      ctx->new_obs.oi.local_mtime = now;
    } else {
      dout(10) << " mtime unchanged at " << ctx->new_obs.oi.mtime << dendl;
    }

    // 将 object_info 编码为 OI_ATTR，并加入与数据修改相同的 PGTransaction。
    // setattrs() 此时仅追加事务动作，并未单独把元数据写入磁盘。
    map <string, bufferlist, less<>> attrs;
    bufferlist bv(sizeof(ctx->new_obs.oi));
    encode(ctx->new_obs.oi, bv,
	     get_osdmap()->get_features(CEPH_ENTITY_TYPE_OSD, nullptr));
    attrs[OI_ATTR] = std::move(bv);

    // SnapSet 是 Ceph OSD 为一个对象维护的快照族谱，记录该对象的 head 和历史 clone 之间的关系。
    // SnapSet 存在 head 对象的 SS_ATTR 中；clone 自身不重复保存整份 SnapSet。
    if (soid.snap == CEPH_NOSNAP) {
      dout(10) << " final snapset " << ctx->new_snapset
	       << " in " << soid << dendl;
      bufferlist bss;
      encode(ctx->new_snapset, bss);
      attrs[SS_ATTR] = std::move(bss);
    } else {
      dout(10) << " no snapset (this is a clone)" << dendl;
    }
    // OI_ATTR、SS_ATTR 和此前加入 op_t 的数据写入将在 ObjectStore 中原子提交。
    ctx->op_t->setattrs(soid, attrs);
  } else {
    // 删除后的内存状态不应继续携带旧 object_info，仅保留对象标识。
    ctx->new_obs.oi = object_info_t(ctx->obc->obs.oi.soid);
  }

  // 阶段 3：构造本次修改的 PG Log 条目，记录操作类型、对象、新旧版本、
  // user_version 和 reqid，供 peering、recovery 及重复请求检测使用。
  ctx->log.push_back(
    pg_log_entry_t(log_op_type, soid, ctx->at_version,
		   ctx->obs->oi.version,
		   ctx->user_at_version, ctx->reqid,
		   ctx->mtime,
		   (ctx->op && ctx->op->allows_returnvec()) ? result : 0));
  if (ctx->op && ctx->op->allows_returnvec()) {
    // 客户端要求 RETURNVEC 时，连同每个子操作的返回值一起持久化；
    // 相同 reqid 重试时才能复现首次回复。
    ctx->log.back().set_op_returns(*ctx->ops);
    dout(20) << __func__ << " op_returns " << ctx->log.back().op_returns
	     << dendl;
  }

  // clean_regions 描述此次修改后仍可信的数据区域，供增量恢复和校验使用。
  ctx->log.back().clean_regions = ctx->clean_regions;
  dout(20) << __func__ << " object " << soid <<  " marks clean_regions " << ctx->log.back().clean_regions << dendl;

  if (soid.snap < CEPH_NOSNAP) {
    // clone 的 MODIFY/PROMOTE/CLEAN 日志还需携带该 clone 覆盖的 snap 集合，
    // 使其他副本在日志合并或恢复时能重建快照归属关系。
    switch (log_op_type) {
    case pg_log_entry_t::MODIFY:
    case pg_log_entry_t::PROMOTE:
    case pg_log_entry_t::CLEAN:
      dout(20) << __func__ << " encoding snaps from " << ctx->new_snapset
	       << dendl;
      encode(ctx->new_snapset.clone_snaps[soid.snap], ctx->log.back().snaps);
      break;
    default:
      break;
    }
  }

  if (!ctx->extra_reqids.empty()) {
    // COPY_FROM 等操作可能代表多个历史请求；
    // 把这些 reqid 及返回码并入当前日志条目，使它们也能参与重复请求检测。
    dout(20) << __func__ << "  extra_reqids " << ctx->extra_reqids << " "
             << ctx->extra_reqid_return_codes << dendl;
    ctx->log.back().extra_reqids.swap(ctx->extra_reqids);
    ctx->log.back().extra_reqid_return_codes.swap(ctx->extra_reqid_return_codes);
  }

  // 阶段 4：提前推进 primary 的 OBC 内存投影，让后续有序操作看到本次写入成功后应有的状态。
  // 对象锁和 RepGather 会保护该投影，持久化仍由后续 issue_repop() 提交同一个数据、元数据和 PG Log 事务。
  ctx->obc->obs = ctx->new_obs;

  // head 被删除时，其 SnapSetContext 也标记为不存在并清空；
  // 其他情况下，将本次计算出的 SnapSet 同步到内存缓存。
  if (soid.is_head() && !ctx->obc->obs.exists) {
    ctx->obc->ssc->exists = false;
    ctx->obc->ssc->snapset = SnapSet();
  } else {
    ctx->obc->ssc->exists = true;
    ctx->obc->ssc->snapset = ctx->new_snapset;
  }
}

void PrimaryLogPG::apply_stats(
  const hobject_t &soid,
  const object_stat_sum_t &delta_stats) {

  recovery_state.apply_op_stats(soid, delta_stats);
  for (set<pg_shard_t>::const_iterator i = get_backfill_targets().begin();
       i != get_backfill_targets().end();
       ++i) {
    pg_shard_t bt = *i;
    const pg_info_t& pinfo = recovery_state.get_peer_info(bt);
    if (soid > pinfo.last_backfill && soid <= last_backfill_started) {
      pending_backfill_updates[soid].stats.add(delta_stats);
    }
  }

  m_scrubber->stats_of_handled_objects(delta_stats, soid);
}

void PrimaryLogPG::complete_read_ctx(int result, OpContext *ctx)
{
  auto m = ctx->op->get_req<MOSDOp>();
  ceph_assert(ctx->async_reads_complete());

  for (auto p = ctx->ops->begin();
    p != ctx->ops->end() && result >= 0; ++p) {
    if (p->rval < 0 && !(p->op.flags & CEPH_OSD_OP_FLAG_FAILOK)) {
      result = p->rval;
      break;
    }
    ctx->bytes_read += p->outdata.length();
  }
  ctx->reply->get_header().data_off = (ctx->data_off ? *ctx->data_off : 0);

  MOSDOpReply *reply = ctx->reply;
  ctx->reply = nullptr;

  if (result >= 0) {
    if (!ctx->ignore_log_op_stats) {
      log_op_stats(*ctx->op, ctx->bytes_written, ctx->bytes_read);

      publish_stats_to_osd();
    }

    // on read, return the current object version
    if (ctx->obs) {
      reply->set_reply_versions(eversion_t(), ctx->obs->oi.user_version);
    } else {
      reply->set_reply_versions(eversion_t(), ctx->user_at_version);
    }
  } else if (result == -ENOENT) {
    // on ENOENT, set a floor for what the next user version will be.
    reply->set_enoent_reply_versions(info.last_update, info.last_user_version);
  }

  reply->set_result(result);
  reply->add_flags(CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK);
  osd->send_message_osd_client(reply, m->get_connection());
  close_op_ctx(ctx);
}

// ========================================================================
// copyfrom

struct C_Copyfrom : public Context {
  PrimaryLogPGRef pg;
  hobject_t oid;
  epoch_t last_peering_reset;
  ceph_tid_t tid;
  PrimaryLogPG::CopyOpRef cop;	// used for keeping the cop alive
  C_Copyfrom(PrimaryLogPG *p, hobject_t o, epoch_t lpr,
	     const PrimaryLogPG::CopyOpRef& c)
    : pg(p), oid(o), last_peering_reset(lpr),
      tid(0), cop(c)
  {}
  void finish(int r) override {
    if (r == -ECANCELED)
      return;
    std::scoped_lock l{*pg};
    if (last_peering_reset == pg->get_last_peering_reset()) {
      pg->process_copy_chunk(oid, tid, r);
      cop.reset();
    }
  }
};

struct C_CopyFrom_AsyncReadCb : public Context {
  OSDOp *osd_op;
  object_copy_data_t reply_obj;
  uint64_t features;
  size_t len;
  C_CopyFrom_AsyncReadCb(OSDOp *osd_op, uint64_t features) :
    osd_op(osd_op), features(features), len(0) {}
  void finish(int r) override {
    osd_op->rval = r;
    if (r < 0) {
      return;
    }

    ceph_assert(len > 0);
    ceph_assert(len <= reply_obj.data.length());
    bufferlist bl;
    bl.substr_of(reply_obj.data, 0, len);
    reply_obj.data.swap(bl);
    encode(reply_obj, osd_op->outdata, features);
  }
};

struct C_CopyChunk : public Context {
  PrimaryLogPGRef pg;
  hobject_t oid;
  epoch_t last_peering_reset;
  ceph_tid_t tid;
  PrimaryLogPG::CopyOpRef cop;	// used for keeping the cop alive
  uint64_t offset = 0;
  C_CopyChunk(PrimaryLogPG *p, hobject_t o, epoch_t lpr,
	     const PrimaryLogPG::CopyOpRef& c)
    : pg(p), oid(o), last_peering_reset(lpr),
      tid(0), cop(c)
  {}
  void finish(int r) override {
    if (r == -ECANCELED)
      return;
    std::scoped_lock l{*pg};
    if (last_peering_reset == pg->get_last_peering_reset()) {
      pg->process_copy_chunk_manifest(oid, tid, r, offset);
      cop.reset();
    }
  }
};

int PrimaryLogPG::do_copy_get(OpContext *ctx, bufferlist::const_iterator& bp,
			      OSDOp& osd_op, ObjectContextRef &obc)
{
  object_info_t& oi = obc->obs.oi;
  hobject_t& soid = oi.soid;
  int result = 0;
  object_copy_cursor_t cursor;
  uint64_t out_max;
  try {
    decode(cursor, bp);
    decode(out_max, bp);
  }
  catch (ceph::buffer::error& e) {
    result = -EINVAL;
    return result;
  }

  const MOSDOp *op = reinterpret_cast<const MOSDOp*>(ctx->op->get_req());
  uint64_t features = op->get_features();

  bool async_read_started = false;
  object_copy_data_t _reply_obj;
  C_CopyFrom_AsyncReadCb *cb = nullptr;
  if (pool.info.is_erasure()) {
    cb = new C_CopyFrom_AsyncReadCb(&osd_op, features);
  }
  object_copy_data_t &reply_obj = cb ? cb->reply_obj : _reply_obj;
  // size, mtime
  reply_obj.size = oi.size;
  reply_obj.mtime = oi.mtime;
  ceph_assert(obc->ssc);
  if (soid.snap < CEPH_NOSNAP) {
    auto p = obc->ssc->snapset.clone_snaps.find(soid.snap);
    ceph_assert(p != obc->ssc->snapset.clone_snaps.end()); // warn?
    reply_obj.snaps = p->second;
  } else {
    reply_obj.snap_seq = obc->ssc->snapset.seq;
  }
  if (oi.is_data_digest()) {
    reply_obj.flags |= object_copy_data_t::FLAG_DATA_DIGEST;
    reply_obj.data_digest = oi.data_digest;
  }
  if (oi.is_omap_digest()) {
    reply_obj.flags |= object_copy_data_t::FLAG_OMAP_DIGEST;
    reply_obj.omap_digest = oi.omap_digest;
  }
  reply_obj.truncate_seq = oi.truncate_seq;
  reply_obj.truncate_size = oi.truncate_size;

  // attrs
  map<string,bufferlist,less<>>& out_attrs = reply_obj.attrs;
  if (!cursor.attr_complete) {
    result = getattrs_maybe_cache(
      ctx->obc,
      &out_attrs);
    if (result < 0) {
      if (cb) {
        delete cb;
      }
      return result;
    }
    cursor.attr_complete = true;
    dout(20) << " got attrs" << dendl;
  }

  int64_t left = out_max - osd_op.outdata.length();

  // data
  bufferlist& bl = reply_obj.data;
  if (left > 0 && !cursor.data_complete) {
    if (cursor.data_offset < oi.size) {
      uint64_t max_read = std::min(oi.size - cursor.data_offset, (uint64_t)left);
      if (cb) {
	async_read_started = true;
	ctx->pending_async_reads.push_back(
	  make_pair(
	    boost::make_tuple(cursor.data_offset, max_read, osd_op.op.flags),
	    make_pair(&bl, cb)));
	cb->len = max_read;

        ctx->op_finishers[ctx->current_osd_subop_num].reset(
          new ReadFinisher(osd_op));
	result = -EINPROGRESS;

	dout(10) << __func__ << ": async_read noted for " << soid << dendl;
      } else {
	result = pgbackend->objects_read_sync(
	  oi.soid, cursor.data_offset, max_read, osd_op.op.flags, &bl);
	if (result < 0)
	  return result;
      }
      left -= max_read;
      cursor.data_offset += max_read;
    }
    if (cursor.data_offset == oi.size) {
      cursor.data_complete = true;
      dout(20) << " got data" << dendl;
    }
    ceph_assert(cursor.data_offset <= oi.size);
  }

  // omap
  uint32_t omap_keys = 0;
  if (!pool.info.supports_omap() || !oi.is_omap()) {
    cursor.omap_complete = true;
  } else {
    if (left > 0 && !cursor.omap_complete) {
      ceph_assert(cursor.data_complete);
      if (cursor.omap_offset.empty()) {
	osd->store->omap_get_header(ch, ghobject_t(oi.soid),
				    &reply_obj.omap_header);
      }
      bufferlist omap_data;
      const auto result = osd->store->omap_iterate(
        ch, ghobject_t(oi.soid),
        ObjectStore::omap_iter_seek_t{
          .seek_position = cursor.omap_offset,
          .seek_type = ObjectStore::omap_iter_seek_t::UPPER_BOUND
        },
        [&omap_data, &omap_keys, &left, &cursor]
        (std::string_view key, std::string_view value) mutable {
	  ++omap_keys;
	  encode(key, omap_data);
	  encode(value, omap_data);
	  left -= key.length() + 4 + value.length() + 4;
	  if (left <= 0) {
	    cursor.omap_offset = key;
            return ObjectStore::omap_iter_ret_t::STOP;
	  }
          return ObjectStore::omap_iter_ret_t::NEXT;
        });
      if (result < 0) {
	ceph_abort();
      } else if (const auto more = static_cast<bool>(result); !more) {
	cursor.omap_complete = true;
	dout(20) << " got omap" << dendl;
      }
      if (omap_keys) {
	encode(omap_keys, reply_obj.omap_data);
	reply_obj.omap_data.claim_append(omap_data);
      }
    }
  }

  if (cursor.is_complete()) {
    // include reqids only in the final step.  this is a bit fragile
    // but it works...
    recovery_state.get_pg_log().get_log().get_object_reqids(ctx->obc->obs.oi.soid, 10,
                                       &reply_obj.reqids,
                                       &reply_obj.reqid_return_codes);
    dout(20) << " got reqids" << dendl;
  }

  dout(20) << " cursor.is_complete=" << cursor.is_complete()
	   << " " << out_attrs.size() << " attrs"
	   << " " << bl.length() << " bytes"
	   << " " << reply_obj.omap_header.length() << " omap header bytes"
	   << " " << reply_obj.omap_data.length() << " omap data bytes in "
	   << omap_keys << " keys"
	   << " " << reply_obj.reqids.size() << " reqids"
	   << dendl;
  reply_obj.cursor = cursor;
  if (!async_read_started) {
    encode(reply_obj, osd_op.outdata, features);
  }
  if (cb && !async_read_started) {
    delete cb;
  }

  if (result > 0) {
    result = 0;
  }
  return result;
}

void PrimaryLogPG::fill_in_copy_get_noent(OpRequestRef& op, hobject_t oid,
                                          OSDOp& osd_op)
{
  const MOSDOp *m = static_cast<const MOSDOp*>(op->get_req());
  uint64_t features = m->get_features();
  object_copy_data_t reply_obj;

  recovery_state.get_pg_log().get_log().get_object_reqids(oid, 10, &reply_obj.reqids,
                                     &reply_obj.reqid_return_codes);
  dout(20) << __func__ << " got reqids " << reply_obj.reqids << dendl;
  encode(reply_obj, osd_op.outdata, features);
  osd_op.rval = -ENOENT;
  MOSDOpReply *reply = new MOSDOpReply(m, 0, get_osdmap_epoch(), 0, false);
  reply->set_result(-ENOENT);
  reply->add_flags(CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK);
  osd->send_message_osd_client(reply, m->get_connection());
}

void PrimaryLogPG::start_copy(CopyCallback *cb, ObjectContextRef obc,
			      hobject_t src, object_locator_t oloc,
			      version_t version, unsigned flags,
			      bool mirror_snapset,
			      unsigned src_obj_fadvise_flags,
			      unsigned dest_obj_fadvise_flags)
{
  const hobject_t& dest = obc->obs.oi.soid;
  dout(10) << __func__ << " " << dest
	   << " from " << src << " " << oloc << " v" << version
	   << " flags " << flags
	   << (mirror_snapset ? " mirror_snapset" : "")
	   << dendl;

  ceph_assert(!mirror_snapset || src.snap == CEPH_NOSNAP);

  // cancel a previous in-progress copy?
  if (copy_ops.count(dest)) {
    // FIXME: if the src etc match, we could avoid restarting from the
    // beginning.
    CopyOpRef cop = copy_ops[dest];
    vector<ceph_tid_t> tids;
    cancel_copy(cop, false, &tids);
    osd->objecter->op_cancel(tids, -ECANCELED);
  }

  CopyOpRef cop(std::make_shared<CopyOp>(cb, obc, src, oloc, version, flags,
			   mirror_snapset, src_obj_fadvise_flags,
			   dest_obj_fadvise_flags));
  copy_ops[dest] = cop;
  dout(20) << fmt::format("{}: blocking {}", __func__, dest) << dendl;
  obc->start_block();

  if (!obc->obs.oi.has_manifest()) {
    _copy_some(obc, cop);
  } else {
    if (obc->obs.oi.manifest.is_redirect()) {
      _copy_some(obc, cop);
    } else if (obc->obs.oi.manifest.is_chunked()) {
      auto p = obc->obs.oi.manifest.chunk_map.begin();
      _copy_some_manifest(obc, cop, p->first);
    } else {
      ceph_abort_msg("unrecognized manifest type");
    }
  }
}

void PrimaryLogPG::_copy_some(ObjectContextRef obc, CopyOpRef cop)
{
  dout(10) << __func__ << " " << *obc << " " << cop << dendl;

  unsigned flags = 0;
  if (cop->flags & CEPH_OSD_COPY_FROM_FLAG_FLUSH)
    flags |= CEPH_OSD_FLAG_FLUSH;
  if (cop->flags & CEPH_OSD_COPY_FROM_FLAG_IGNORE_CACHE)
    flags |= CEPH_OSD_FLAG_IGNORE_CACHE;
  if (cop->flags & CEPH_OSD_COPY_FROM_FLAG_IGNORE_OVERLAY)
    flags |= CEPH_OSD_FLAG_IGNORE_OVERLAY;
  if (cop->flags & CEPH_OSD_COPY_FROM_FLAG_MAP_SNAP_CLONE)
    flags |= CEPH_OSD_FLAG_MAP_SNAP_CLONE;
  if (cop->flags & CEPH_OSD_COPY_FROM_FLAG_RWORDERED)
    flags |= CEPH_OSD_FLAG_RWORDERED;

  C_GatherBuilder gather(cct);

  if (cop->cursor.is_initial() && cop->mirror_snapset) {
    // list snaps too.
    ceph_assert(cop->src.snap == CEPH_NOSNAP);
    ObjectOperation op;
    op.list_snaps(&cop->results.snapset, NULL);
    ceph_tid_t tid = osd->objecter->read(cop->src.oid, cop->oloc, op,
				    CEPH_SNAPDIR, NULL,
				    flags, gather.new_sub(), NULL);
    cop->objecter_tid2 = tid;
  }

  ObjectOperation op;
  if (cop->results.user_version) {
    op.assert_version(cop->results.user_version);
  } else {
    // we should learn the version after the first chunk, if we didn't know
    // it already!
    ceph_assert(cop->cursor.is_initial());
  }
  op.copy_get(&cop->cursor, get_copy_chunk_size(),
	      &cop->results.object_size, &cop->results.mtime,
	      &cop->attrs, &cop->data, &cop->omap_header, &cop->omap_data,
	      &cop->results.snaps, &cop->results.snap_seq,
	      &cop->results.flags,
	      &cop->results.source_data_digest,
	      &cop->results.source_omap_digest,
	      &cop->results.reqids,
	      &cop->results.reqid_return_codes,
	      &cop->results.truncate_seq,
	      &cop->results.truncate_size,
	      &cop->rval);
  op.set_last_op_flags(cop->src_obj_fadvise_flags);

  C_Copyfrom *fin = new C_Copyfrom(this, obc->obs.oi.soid,
				   get_last_peering_reset(), cop);
  gather.set_finisher(new C_OnFinisher(fin,
				       osd->get_objecter_finisher(get_pg_shard())));

  ceph_tid_t tid = osd->objecter->read(cop->src.oid, cop->oloc, op,
				  cop->src.snap, NULL,
				  flags,
				  gather.new_sub(),
				  // discover the object version if we don't know it yet
				  cop->results.user_version ? NULL : &cop->results.user_version);
  fin->tid = tid;
  cop->objecter_tid = tid;
  gather.activate();
}

void PrimaryLogPG::_copy_some_manifest(ObjectContextRef obc, CopyOpRef cop, uint64_t start_offset)
{
  dout(10) << __func__ << " " << *obc << " " << cop << dendl;

  unsigned flags = 0;
  if (cop->flags & CEPH_OSD_COPY_FROM_FLAG_FLUSH)
    flags |= CEPH_OSD_FLAG_FLUSH;
  if (cop->flags & CEPH_OSD_COPY_FROM_FLAG_IGNORE_CACHE)
    flags |= CEPH_OSD_FLAG_IGNORE_CACHE;
  if (cop->flags & CEPH_OSD_COPY_FROM_FLAG_IGNORE_OVERLAY)
    flags |= CEPH_OSD_FLAG_IGNORE_OVERLAY;
  if (cop->flags & CEPH_OSD_COPY_FROM_FLAG_MAP_SNAP_CLONE)
    flags |= CEPH_OSD_FLAG_MAP_SNAP_CLONE;
  if (cop->flags & CEPH_OSD_COPY_FROM_FLAG_RWORDERED)
    flags |= CEPH_OSD_FLAG_RWORDERED;

  int num_chunks = 0;
  uint64_t last_offset = 0, chunks_size = 0;
  object_manifest_t *manifest = &obc->obs.oi.manifest;
  map<uint64_t, chunk_info_t>::iterator iter = manifest->chunk_map.find(start_offset);
  for (;iter != manifest->chunk_map.end(); ++iter) {
    num_chunks++;
    chunks_size += iter->second.length;
    last_offset = iter->first;
    if (get_copy_chunk_size() < chunks_size) {
      break;
    }
  }

  cop->num_chunk = num_chunks;
  cop->start_offset = start_offset;
  cop->last_offset = last_offset;
  dout(20) << __func__ << " oid " << obc->obs.oi.soid << " num_chunks: " << num_chunks
	  << " start_offset: " << start_offset << " chunks_size: " << chunks_size
	  << " last_offset: " << last_offset << dendl;

  iter = manifest->chunk_map.find(start_offset);
  for (;iter != manifest->chunk_map.end(); ++iter) {
    uint64_t obj_offset = iter->first;
    uint64_t length = manifest->chunk_map[iter->first].length;
    hobject_t soid = manifest->chunk_map[iter->first].oid;
    object_locator_t oloc(soid);
    CopyCallback * cb = NULL;
    CopyOpRef sub_cop(std::make_shared<CopyOp>(cb, ObjectContextRef(), cop->src, oloc,
		       cop->results.user_version, cop->flags, cop->mirror_snapset,
		       cop->src_obj_fadvise_flags, cop->dest_obj_fadvise_flags));
    sub_cop->cursor.data_offset = obj_offset;
    cop->chunk_cops[obj_offset] = sub_cop;

    int s = sub_cop->chunk_ops.size();
    sub_cop->chunk_ops.resize(s+1);
    sub_cop->chunk_ops[s].op.op =  CEPH_OSD_OP_READ;
    sub_cop->chunk_ops[s].op.extent.offset = manifest->chunk_map[iter->first].offset;
    sub_cop->chunk_ops[s].op.extent.length = length;

    ObjectOperation op;
    op.dup(sub_cop->chunk_ops);

    if (cop->results.user_version) {
      op.assert_version(cop->results.user_version);
    } else {
      // we should learn the version after the first chunk, if we didn't know
      // it already!
      ceph_assert(cop->cursor.is_initial());
    }
    op.set_last_op_flags(cop->src_obj_fadvise_flags);

    C_CopyChunk *fin = new C_CopyChunk(this, obc->obs.oi.soid,
				     get_last_peering_reset(), cop);
    fin->offset = obj_offset;

    ceph_tid_t tid = osd->objecter->read(
      soid.oid, oloc, op,
      sub_cop->src.snap, NULL,
      flags,
      new C_OnFinisher(fin, osd->get_objecter_finisher(get_pg_shard())),
      // discover the object version if we don't know it yet
      sub_cop->results.user_version ? NULL : &sub_cop->results.user_version);
    fin->tid = tid;
    sub_cop->objecter_tid = tid;

    dout(20) << __func__ << " tgt_oid: " << soid.oid << " tgt_offset: "
	    << manifest->chunk_map[iter->first].offset
	    << " length: " << length << " pool id: " << oloc.pool
	    << " tid: " << tid << dendl;

    if (last_offset <= iter->first) {
      break;
    }
  }
}

void PrimaryLogPG::process_copy_chunk(hobject_t oid, ceph_tid_t tid, int r)
{
  dout(10) << __func__ << " " << oid << " tid " << tid
	   << " " << cpp_strerror(r) << dendl;
  map<hobject_t,CopyOpRef>::iterator p = copy_ops.find(oid);
  if (p == copy_ops.end()) {
    dout(10) << __func__ << " no copy_op found" << dendl;
    return;
  }
  CopyOpRef cop = p->second;
  if (tid != cop->objecter_tid) {
    dout(10) << __func__ << " tid " << tid << " != cop " << cop
	     << " tid " << cop->objecter_tid << dendl;
    return;
  }

  if (cop->omap_data.length() || cop->omap_header.length())
    cop->results.has_omap = true;

  if (r >= 0 && !pool.info.supports_omap() &&
      (cop->omap_data.length() || cop->omap_header.length())) {
    r = -EOPNOTSUPP;
  }
  cop->objecter_tid = 0;
  cop->objecter_tid2 = 0;  // assume this ordered before us (if it happened)
  ObjectContextRef& cobc = cop->obc;

  if (r < 0)
    goto out;

  ceph_assert(cop->rval >= 0);

  if (oid.snap < CEPH_NOSNAP && !cop->results.snaps.empty()) {
    // verify snap hasn't been deleted
    vector<snapid_t>::iterator p = cop->results.snaps.begin();
    while (p != cop->results.snaps.end()) {
      // make best effort to sanitize snaps/clones.
      if (get_osdmap()->in_removed_snaps_queue(info.pgid.pgid.pool(), *p)) {
	dout(10) << __func__ << " clone snap " << *p << " has been deleted"
		 << dendl;
	for (vector<snapid_t>::iterator q = p + 1;
	     q != cop->results.snaps.end();
	     ++q)
	  *(q - 1) = *q;
	cop->results.snaps.resize(cop->results.snaps.size() - 1);
      } else {
	++p;
      }
    }
    if (cop->results.snaps.empty()) {
      dout(10) << __func__ << " no more snaps for " << oid << dendl;
      r = -ENOENT;
      goto out;
    }
  }

  ceph_assert(cop->rval >= 0);

  if (!cop->temp_cursor.data_complete) {
    cop->results.data_digest = cop->data.crc32c(cop->results.data_digest);
  }
  if (pool.info.supports_omap() && !cop->temp_cursor.omap_complete) {
    if (cop->omap_header.length()) {
      cop->results.omap_digest =
	cop->omap_header.crc32c(cop->results.omap_digest);
    }
    if (cop->omap_data.length()) {
      bufferlist keys;
      keys.substr_of(cop->omap_data, 4, cop->omap_data.length() - 4);
      cop->results.omap_digest = keys.crc32c(cop->results.omap_digest);
    }
  }

  if (!cop->temp_cursor.attr_complete) {
    for (map<string,bufferlist>::iterator p = cop->attrs.begin();
	 p != cop->attrs.end();
	 ++p) {
      cop->results.attrs[string("_") + p->first] = p->second;
    }
    cop->attrs.clear();
  }

  if (!cop->cursor.is_complete()) {
    // write out what we have so far
    if (cop->temp_cursor.is_initial()) {
      ceph_assert(!cop->results.started_temp_obj);
      cop->results.started_temp_obj = true;
      cop->results.temp_oid = generate_temp_object(oid);
      dout(20) << __func__ << " using temp " << cop->results.temp_oid << dendl;
    }
    ObjectContextRef tempobc = get_object_context(cop->results.temp_oid, true);
    OpContextUPtr ctx = simple_opc_create(tempobc);
    if (cop->temp_cursor.is_initial()) {
      ctx->new_temp_oid = cop->results.temp_oid;
    }
    _write_copy_chunk(cop, ctx->op_t.get());
    simple_opc_submit(std::move(ctx));
    dout(10) << __func__ << " fetching more" << dendl;
    _copy_some(cobc, cop);
    return;
  }

  // verify digests?
  if (cop->results.is_data_digest() || cop->results.is_omap_digest()) {
    dout(20) << __func__ << std::hex
      << " got digest: rx data 0x" << cop->results.data_digest
      << " omap 0x" << cop->results.omap_digest
      << ", source: data 0x" << cop->results.source_data_digest
      << " omap 0x" <<  cop->results.source_omap_digest
      << std::dec
      << " flags " << cop->results.flags
      << dendl;
  }
  if (cop->results.is_data_digest() &&
      cop->results.data_digest != cop->results.source_data_digest) {
    derr << __func__ << std::hex << " data digest 0x" << cop->results.data_digest
	 << " != source 0x" << cop->results.source_data_digest << std::dec
	 << dendl;
    osd->clog->error() << info.pgid << " copy from " << cop->src
		       << " to " << cop->obc->obs.oi.soid << std::hex
		       << " data digest 0x" << cop->results.data_digest
		       << " != source 0x" << cop->results.source_data_digest
		       << std::dec;
    r = -EIO;
    goto out;
  }
  if (cop->results.is_omap_digest() &&
      cop->results.omap_digest != cop->results.source_omap_digest) {
    derr << __func__ << std::hex
	 << " omap digest 0x" << cop->results.omap_digest
	 << " != source 0x" << cop->results.source_omap_digest
	 << std::dec << dendl;
    osd->clog->error() << info.pgid << " copy from " << cop->src
		       << " to " << cop->obc->obs.oi.soid << std::hex
		       << " omap digest 0x" << cop->results.omap_digest
		       << " != source 0x" << cop->results.source_omap_digest
		       << std::dec;
    r = -EIO;
    goto out;
  }
  if (cct->_conf->osd_debug_inject_copyfrom_error) {
    derr << __func__ << " injecting copyfrom failure" << dendl;
    r = -EIO;
    goto out;
  }

  cop->results.fill_in_final_tx = std::function<void(PGTransaction*)>(
    [this, &cop /* avoid ref cycle */](PGTransaction *t) {
      ObjectState& obs = cop->obc->obs;
      if (cop->temp_cursor.is_initial()) {
	dout(20) << "fill_in_final_tx: writing "
		 << "directly to final object" << dendl;
	// write directly to final object
	cop->results.temp_oid = obs.oi.soid;
	_write_copy_chunk(cop, t);
      } else {
	// finish writing to temp object, then move into place
	dout(20) << "fill_in_final_tx: writing to temp object" << dendl;
	if (obs.oi.has_manifest() && obs.oi.manifest.is_redirect() && obs.exists) {
	  /* In redirect manifest case, the object exists in the upper tier.
	   * So, to avoid a conflict when rename() is called, remove existing
	   * object first
	   */
	  t->remove(obs.oi.soid);
	}
	_write_copy_chunk(cop, t);
	t->rename(obs.oi.soid, cop->results.temp_oid);
      }
      t->setattrs(obs.oi.soid, cop->results.attrs);
    });

  dout(20) << __func__ << " success; committing" << dendl;

 out:
  dout(20) << __func__ << " complete r = " << cpp_strerror(r) << dendl;
  CopyCallbackResults results(r, &cop->results);
  cop->cb->complete(results);

  copy_ops.erase(cobc->obs.oi.soid);
  cobc->stop_block();

  if (r < 0 && cop->results.started_temp_obj) {
    dout(10) << __func__ << " deleting partial temp object "
	     << cop->results.temp_oid << dendl;
    ObjectContextRef tempobc = get_object_context(cop->results.temp_oid, true);
    OpContextUPtr ctx = simple_opc_create(tempobc);
    ctx->op_t->remove(cop->results.temp_oid);
    ctx->discard_temp_oid = cop->results.temp_oid;
    simple_opc_submit(std::move(ctx));
  }

  // cancel and requeue proxy ops on this object
  if (!r) {
    cancel_and_requeue_proxy_ops(cobc->obs.oi.soid);
  }

  kick_object_context_blocked(cobc);
}

void PrimaryLogPG::process_copy_chunk_manifest(hobject_t oid, ceph_tid_t tid, int r, uint64_t offset)
{
  dout(10) << __func__ << " " << oid << " tid " << tid
	   << " " << cpp_strerror(r) << dendl;
  map<hobject_t,CopyOpRef>::iterator p = copy_ops.find(oid);
  if (p == copy_ops.end()) {
    dout(10) << __func__ << " no copy_op found" << dendl;
    return;
  }
  CopyOpRef obj_cop = p->second;
  CopyOpRef chunk_cop = obj_cop->chunk_cops[offset];

  if (tid != chunk_cop->objecter_tid) {
    dout(10) << __func__ << " tid " << tid << " != cop " << chunk_cop
	     << " tid " << chunk_cop->objecter_tid << dendl;
    return;
  }

  if (chunk_cop->omap_data.length() || chunk_cop->omap_header.length()) {
    r = -EOPNOTSUPP;
  }

  chunk_cop->objecter_tid = 0;
  chunk_cop->objecter_tid2 = 0;  // assume this ordered before us (if it happened)
  ObjectContextRef& cobc = obj_cop->obc;
  OSDOp &chunk_data = chunk_cop->chunk_ops[0];

  if (r < 0) {
    obj_cop->failed = true;
    goto out;
  }

  if (obj_cop->failed) {
    return;
  }
  if (!chunk_data.outdata.length()) {
    r = -EIO;
    obj_cop->failed = true;
    goto out;
  }

  obj_cop->num_chunk--;

  /* check all of the copyop are completed */
  if (obj_cop->num_chunk) {
    dout(20) << __func__ << " num_chunk: " << obj_cop->num_chunk << dendl;
    return;
  }

  {
    OpContextUPtr ctx = simple_opc_create(obj_cop->obc);
    if (!ctx->lock_manager.take_write_lock(
	  obj_cop->obc->obs.oi.soid,
	  obj_cop->obc)) {
      // recovery op can take read lock.
      // so need to wait for recovery completion
      r = -EAGAIN;
      obj_cop->failed = true;
      close_op_ctx(ctx.release());
      goto out;
    }
    dout(20) << __func__ << " took lock on obc, " << obj_cop->obc->rwstate << dendl;

    PGTransaction *t = ctx->op_t.get();
    ObjectState& obs = ctx->new_obs;
    for (auto p : obj_cop->chunk_cops) {
      OSDOp &sub_chunk = p.second->chunk_ops[0];
      t->write(cobc->obs.oi.soid,
	      p.second->cursor.data_offset,
	      sub_chunk.outdata.length(),
	      sub_chunk.outdata,
	      p.second->dest_obj_fadvise_flags);
      dout(20) << __func__ << " offset: " << p.second->cursor.data_offset
	      << " length: " << sub_chunk.outdata.length() << dendl;
      write_update_size_and_usage(ctx->delta_stats, obs.oi, ctx->modified_ranges,
				  p.second->cursor.data_offset, sub_chunk.outdata.length());
      obs.oi.manifest.chunk_map[p.second->cursor.data_offset].clear_flag(chunk_info_t::FLAG_MISSING);
      ctx->clean_regions.mark_data_region_dirty(p.second->cursor.data_offset, sub_chunk.outdata.length());
      sub_chunk.outdata.clear();
    }
    obs.oi.clear_data_digest();
    ctx->at_version = get_next_version();
    finish_ctx(ctx.get(), pg_log_entry_t::PROMOTE);
    simple_opc_submit(std::move(ctx));
    obj_cop->chunk_cops.clear();

    auto p = cobc->obs.oi.manifest.chunk_map.rbegin();
    /* check remaining work */
    if (p != cobc->obs.oi.manifest.chunk_map.rend()) {
      if (obj_cop->last_offset < p->first) {
	for (auto &en : cobc->obs.oi.manifest.chunk_map) {
	  if (obj_cop->last_offset < en.first) {
	    _copy_some_manifest(cobc, obj_cop, en.first);
	    return;
	  }
	}
      }
    }
  }

 out:
  dout(20) << __func__ << " complete r = " << cpp_strerror(r) << dendl;
  CopyCallbackResults results(r, &obj_cop->results);
  obj_cop->cb->complete(results);

  copy_ops.erase(cobc->obs.oi.soid);
  cobc->stop_block();

  // cancel and requeue proxy ops on this object
  if (!r) {
    cancel_and_requeue_proxy_ops(cobc->obs.oi.soid);
  }

  kick_object_context_blocked(cobc);
}

void PrimaryLogPG::cancel_and_requeue_proxy_ops(hobject_t oid) {
  vector<ceph_tid_t> tids;
  for (map<ceph_tid_t, ProxyReadOpRef>::iterator it = proxyread_ops.begin();
      it != proxyread_ops.end();) {
    if (it->second->soid == oid) {
      cancel_proxy_read((it++)->second, &tids);
    } else {
      ++it;
    }
  }
  for (map<ceph_tid_t, ProxyWriteOpRef>::iterator it = proxywrite_ops.begin();
       it != proxywrite_ops.end();) {
    if (it->second->soid == oid) {
      cancel_proxy_write((it++)->second, &tids);
    } else {
      ++it;
    }
  }
  osd->objecter->op_cancel(tids, -ECANCELED);
  kick_proxy_ops_blocked(oid);
}

void PrimaryLogPG::_write_copy_chunk(CopyOpRef cop, PGTransaction *t)
{
  dout(20) << __func__ << " " << cop
	   << " " << cop->attrs.size() << " attrs"
	   << " " << cop->data.length() << " bytes"
	   << " " << cop->omap_header.length() << " omap header bytes"
	   << " " << cop->omap_data.length() << " omap data bytes"
	   << dendl;
  if (!cop->temp_cursor.attr_complete) {
    t->create(cop->results.temp_oid);
  }
  if (!cop->temp_cursor.data_complete) {
    ceph_assert(cop->data.length() + cop->temp_cursor.data_offset ==
	   cop->cursor.data_offset);
    if (pool.info.required_alignment() &&
	!cop->cursor.data_complete) {
      /**
       * Trim off the unaligned bit at the end, we'll adjust cursor.data_offset
       * to pick it up on the next pass.
       */
      ceph_assert(cop->temp_cursor.data_offset %
	     pool.info.required_alignment() == 0);
      if (cop->data.length() % pool.info.required_alignment() != 0) {
	uint64_t to_trim =
	  cop->data.length() % pool.info.required_alignment();
	bufferlist bl;
	bl.substr_of(cop->data, 0, cop->data.length() - to_trim);
	cop->data.swap(bl);
	cop->cursor.data_offset -= to_trim;
	ceph_assert(cop->data.length() + cop->temp_cursor.data_offset ==
	       cop->cursor.data_offset);
      }
    }
    if (cop->data.length()) {
      t->write(
	cop->results.temp_oid,
	cop->temp_cursor.data_offset,
	cop->data.length(),
	cop->data,
	cop->dest_obj_fadvise_flags);
    }
    cop->data.clear();
  }
  if (pool.info.supports_omap()) {
    if (!cop->temp_cursor.omap_complete) {
      if (cop->omap_header.length()) {
	t->omap_setheader(
	  cop->results.temp_oid,
	  cop->omap_header);
	cop->omap_header.clear();
      }
      if (cop->omap_data.length()) {
	map<string,bufferlist> omap;
	bufferlist::const_iterator p = cop->omap_data.begin();
	decode(omap, p);
	t->omap_setkeys(cop->results.temp_oid, omap);
	cop->omap_data.clear();
      }
    }
  } else {
    ceph_assert(cop->omap_header.length() == 0);
    ceph_assert(cop->omap_data.length() == 0);
  }
  cop->temp_cursor = cop->cursor;
}

void PrimaryLogPG::finish_copyfrom(CopyFromCallback *cb)
{
  OpContext *ctx = cb->ctx;
  dout(20) << "finish_copyfrom on " << ctx->obs->oi.soid << dendl;

  ObjectState& obs = ctx->new_obs;
  if (obs.exists) {
    dout(20) << __func__ << ": exists, removing" << dendl;
    ctx->op_t->remove(obs.oi.soid);
  } else {
    ctx->delta_stats.num_objects++;
    obs.exists = true;
  }
  if (cb->is_temp_obj_used()) {
    ctx->discard_temp_oid = cb->results->temp_oid;
  }
  cb->results->fill_in_final_tx(ctx->op_t.get());

  // CopyFromCallback fills this in for us
  obs.oi.user_version = ctx->user_at_version;

  if (cb->results->is_data_digest()) {
    obs.oi.set_data_digest(cb->results->data_digest);
  } else {
    obs.oi.clear_data_digest();
  }
  if (cb->results->is_omap_digest()) {
    obs.oi.set_omap_digest(cb->results->omap_digest);
  } else {
    obs.oi.clear_omap_digest();
  }

  obs.oi.truncate_seq = cb->truncate_seq;
  obs.oi.truncate_size = cb->truncate_size;

  obs.oi.mtime = ceph::real_clock::to_timespec(cb->results->mtime);
  ctx->mtime = utime_t();

  ctx->extra_reqids = cb->results->reqids;
  ctx->extra_reqid_return_codes = cb->results->reqid_return_codes;

  // cache: clear whiteout?
  if (obs.oi.is_whiteout()) {
    dout(10) << __func__ << " clearing whiteout on " << obs.oi.soid << dendl;
    obs.oi.clear_flag(object_info_t::FLAG_WHITEOUT);
    --ctx->delta_stats.num_whiteouts;
  }

  if (cb->results->has_omap) {
    dout(10) << __func__ << " setting omap flag on " << obs.oi.soid << dendl;
    obs.oi.set_flag(object_info_t::FLAG_OMAP);
    ctx->clean_regions.mark_omap_dirty();
  } else {
    dout(10) << __func__ << " clearing omap flag on " << obs.oi.soid << dendl;
    obs.oi.clear_flag(object_info_t::FLAG_OMAP);
  }

  interval_set<uint64_t> ch;
  if (obs.oi.size > 0)
    ch.insert(0, obs.oi.size);
  ctx->modified_ranges.union_of(ch);
  ctx->clean_regions.mark_data_region_dirty(0, std::max(obs.oi.size, cb->get_data_size()));

  if (cb->get_data_size() != obs.oi.size) {
    ctx->delta_stats.num_bytes -= obs.oi.size;
    obs.oi.size = cb->get_data_size();
    ctx->delta_stats.num_bytes += obs.oi.size;
  }
  ctx->delta_stats.num_wr++;
  ctx->delta_stats.num_wr_kb += shift_round_up(obs.oi.size, 10);

  osd->logger->inc(l_osd_copyfrom);
}

void PrimaryLogPG::finish_promote(int r, CopyResults *results,
				  ObjectContextRef obc)
{
  const hobject_t& soid = obc->obs.oi.soid;
  dout(10) << __func__ << " " << soid << " r=" << r
	   << " uv" << results->user_version << dendl;

  if (r == -ECANCELED) {
    return;
  }

  if (r != -ENOENT && soid.is_snap()) {
    if (results->snaps.empty()) {
      // we must have read "snap" content from the head object in the
      // base pool.  use snap_seq to construct what snaps should be
      // for this clone (what is was before we evicted the clean clone
      // from this pool, and what it will be when we flush and the
      // clone eventually happens in the base pool).  we want to use
      // snaps in (results->snap_seq,soid.snap]
      SnapSet& snapset = obc->ssc->snapset;
      for (auto p = snapset.clone_snaps.rbegin();
	   p != snapset.clone_snaps.rend();
	   ++p) {
	for (auto snap : p->second) {
	  if (snap > soid.snap) {
	    continue;
	  }
	  if (snap <= results->snap_seq) {
	    break;
	  }
	  results->snaps.push_back(snap);
	}
      }
    }

    dout(20) << __func__ << " snaps " << results->snaps << dendl;
    filter_snapc(results->snaps);

    dout(20) << __func__ << " filtered snaps " << results->snaps << dendl;
    if (results->snaps.empty()) {
      dout(20) << __func__
	       << " snaps are empty, clone is invalid,"
	       << " setting r to ENOENT" << dendl;
      r = -ENOENT;
    }
  }

  if (r < 0 && results->started_temp_obj) {
    dout(10) << __func__ << " abort; will clean up partial work" << dendl;
    ObjectContextRef tempobc = get_object_context(results->temp_oid, false);
    ceph_assert(tempobc);
    OpContextUPtr ctx = simple_opc_create(tempobc);
    ctx->op_t->remove(results->temp_oid);
    simple_opc_submit(std::move(ctx));
    results->started_temp_obj = false;
  }

  if (r == -ENOENT && soid.is_snap()) {
    dout(10) << __func__
	     << ": enoent while trying to promote clone, " << soid
	     << " must have been trimmed, removing from snapset"
	     << dendl;
    hobject_t head(soid.get_head());
    ObjectContextRef obc = get_object_context(head, false);
    ceph_assert(obc);

    OpContextUPtr tctx = simple_opc_create(obc);
    tctx->at_version = get_next_version();
    vector<snapid_t> new_clones;
    map<snapid_t, vector<snapid_t>> new_clone_snaps;
    for (vector<snapid_t>::iterator i = tctx->new_snapset.clones.begin();
	 i != tctx->new_snapset.clones.end();
	 ++i) {
      if (*i != soid.snap) {
	new_clones.push_back(*i);
	auto p = tctx->new_snapset.clone_snaps.find(*i);
	if (p != tctx->new_snapset.clone_snaps.end()) {
	  new_clone_snaps[*i] = p->second;
	}
      }
    }
    tctx->new_snapset.clones.swap(new_clones);
    tctx->new_snapset.clone_overlap.erase(soid.snap);
    tctx->new_snapset.clone_size.erase(soid.snap);
    tctx->new_snapset.clone_snaps.swap(new_clone_snaps);

    // take RWWRITE lock for duration of our local write.  ignore starvation.
    if (!tctx->lock_manager.take_write_lock(
	  head,
	  obc)) {
      ceph_abort_msg("problem!");
    }
    dout(20) << __func__ << " took lock on obc, " << obc->rwstate << dendl;

    finish_ctx(tctx.get(), pg_log_entry_t::PROMOTE);

    simple_opc_submit(std::move(tctx));
    return;
  }

  bool whiteout = false;
  if (r == -ENOENT) {
    ceph_assert(soid.snap == CEPH_NOSNAP); // snap case is above
    dout(10) << __func__ << " whiteout " << soid << dendl;
    whiteout = true;
  }

  if (r < 0 && !whiteout) {
    derr << __func__ << " unexpected promote error " << cpp_strerror(r) << dendl;
    // pass error to everyone blocked on this object
    // FIXME: this is pretty sloppy, but at this point we got
    // something unexpected and don't have many other options.
    map<hobject_t,list<OpRequestRef>>::iterator blocked_iter =
      waiting_for_blocked_object.find(soid);
    if (blocked_iter != waiting_for_blocked_object.end()) {
      while (!blocked_iter->second.empty()) {
	osd->reply_op_error(blocked_iter->second.front(), r);
	blocked_iter->second.pop_front();
      }
      waiting_for_blocked_object.erase(blocked_iter);
    }
    return;
  }

  osd->promote_finish(results->object_size);

  OpContextUPtr tctx =  simple_opc_create(obc);
  tctx->at_version = get_next_version();

  if (!obc->obs.oi.has_manifest()) {
    ++tctx->delta_stats.num_objects;
  }
  if (soid.snap < CEPH_NOSNAP)
    ++tctx->delta_stats.num_object_clones;
  tctx->new_obs.exists = true;

  tctx->extra_reqids = results->reqids;
  tctx->extra_reqid_return_codes = results->reqid_return_codes;

  if (obc->obs.oi.has_manifest() && obc->obs.oi.manifest.is_redirect()) {
    tctx->new_obs.oi.manifest.type = object_manifest_t::TYPE_NONE;
    tctx->new_obs.oi.clear_flag(object_info_t::FLAG_REDIRECT_HAS_REFERENCE);
    tctx->new_obs.oi.clear_flag(object_info_t::FLAG_MANIFEST);
    tctx->new_obs.oi.manifest.redirect_target = hobject_t();
    tctx->delta_stats.num_objects_manifest--;
    if (obc->obs.oi.test_flag(object_info_t::FLAG_REDIRECT_HAS_REFERENCE)) {
      dec_all_refcount_manifest(obc->obs.oi, tctx.get());
    }
  }

  if (whiteout) {
    // create a whiteout
    tctx->op_t->create(soid);
    tctx->new_obs.oi.set_flag(object_info_t::FLAG_WHITEOUT);
    ++tctx->delta_stats.num_whiteouts;
    dout(20) << __func__ << " creating whiteout on " << soid << dendl;
    osd->logger->inc(l_osd_tier_whiteout);
  } else {
    if (results->has_omap) {
      dout(10) << __func__ << " setting omap flag on " << soid << dendl;
      tctx->new_obs.oi.set_flag(object_info_t::FLAG_OMAP);
      ++tctx->delta_stats.num_objects_omap;
    }

    results->fill_in_final_tx(tctx->op_t.get());
    if (results->started_temp_obj) {
      tctx->discard_temp_oid = results->temp_oid;
    }
    tctx->new_obs.oi.size = results->object_size;
    tctx->new_obs.oi.user_version = results->user_version;
    tctx->new_obs.oi.mtime = ceph::real_clock::to_timespec(results->mtime);
    tctx->mtime = utime_t();
    if (results->is_data_digest()) {
      tctx->new_obs.oi.set_data_digest(results->data_digest);
    } else {
      tctx->new_obs.oi.clear_data_digest();
    }
    if (results->object_size)
      tctx->clean_regions.mark_data_region_dirty(0, results->object_size);
    if (results->is_omap_digest()) {
      tctx->new_obs.oi.set_omap_digest(results->omap_digest);
    } else {
      tctx->new_obs.oi.clear_omap_digest();
    }
    if (results->has_omap)
        tctx->clean_regions.mark_omap_dirty();
    tctx->new_obs.oi.truncate_seq = results->truncate_seq;
    tctx->new_obs.oi.truncate_size = results->truncate_size;

    if (soid.snap != CEPH_NOSNAP) {
      ceph_assert(obc->ssc->snapset.clone_snaps.count(soid.snap));
      ceph_assert(obc->ssc->snapset.clone_size.count(soid.snap));
      ceph_assert(obc->ssc->snapset.clone_size[soid.snap] ==
	     results->object_size);
      ceph_assert(obc->ssc->snapset.clone_overlap.count(soid.snap));

      tctx->delta_stats.num_bytes += obc->ssc->snapset.get_clone_bytes(soid.snap);
    } else {
      tctx->delta_stats.num_bytes += results->object_size;
    }
  }

  if (results->mirror_snapset) {
    ceph_assert(tctx->new_obs.oi.soid.snap == CEPH_NOSNAP);
    tctx->new_snapset.from_snap_set(
      results->snapset,
      get_osdmap()->require_osd_release < ceph_release_t::luminous);
  }
  dout(20) << __func__ << " new_snapset " << tctx->new_snapset << dendl;

  // take RWWRITE lock for duration of our local write.  ignore starvation.
  if (!tctx->lock_manager.take_write_lock(
	obc->obs.oi.soid,
	obc)) {
    ceph_abort_msg("problem!");
  }
  dout(20) << __func__ << " took lock on obc, " << obc->rwstate << dendl;

  finish_ctx(tctx.get(), pg_log_entry_t::PROMOTE);

  simple_opc_submit(std::move(tctx));

  osd->logger->inc(l_osd_tier_promote);

  if (agent_state &&
      agent_state->is_idle())
    agent_choose_mode();
}

void PrimaryLogPG::finish_promote_manifest(int r, CopyResults *results,
					    ObjectContextRef obc)
{
  const hobject_t& soid = obc->obs.oi.soid;
  dout(10) << __func__ << " " << soid << " r=" << r
	   << " uv" << results->user_version << dendl;

  if (r == -ECANCELED || r == -EAGAIN) {
    return;
  }

  if (r < 0) {
    derr << __func__ << " unexpected promote error " << cpp_strerror(r) << dendl;
    // pass error to everyone blocked on this object
    // FIXME: this is pretty sloppy, but at this point we got
    // something unexpected and don't have many other options.
    map<hobject_t,list<OpRequestRef>>::iterator blocked_iter =
      waiting_for_blocked_object.find(soid);
    if (blocked_iter != waiting_for_blocked_object.end()) {
      while (!blocked_iter->second.empty()) {
	osd->reply_op_error(blocked_iter->second.front(), r);
	blocked_iter->second.pop_front();
      }
      waiting_for_blocked_object.erase(blocked_iter);
    }
    return;
  }

  osd->promote_finish(results->object_size);
  osd->logger->inc(l_osd_tier_promote);

  if (agent_state &&
      agent_state->is_idle())
    agent_choose_mode();
}

void PrimaryLogPG::cancel_copy(CopyOpRef cop, bool requeue,
			       vector<ceph_tid_t> *tids)
{
  dout(10) << __func__ << " " << cop->obc->obs.oi.soid
	   << " from " << cop->src << " " << cop->oloc
	   << " v" << cop->results.user_version << dendl;

  // cancel objecter op, if we can
  if (cop->objecter_tid) {
    tids->push_back(cop->objecter_tid);
    cop->objecter_tid = 0;
    if (cop->objecter_tid2) {
      tids->push_back(cop->objecter_tid2);
      cop->objecter_tid2 = 0;
    }
  }

  copy_ops.erase(cop->obc->obs.oi.soid);
  cop->obc->stop_block();

  kick_object_context_blocked(cop->obc);
  cop->results.should_requeue = requeue;
  CopyCallbackResults result(-ECANCELED, &cop->results);
  cop->cb->complete(result);

  // There may still be an objecter callback referencing this copy op.
  // That callback will not need the obc since it's been canceled, and
  // we need the obc reference to go away prior to flush.
  cop->obc = ObjectContextRef();
}

void PrimaryLogPG::cancel_copy_ops(bool requeue, vector<ceph_tid_t> *tids)
{
  dout(10) << __func__ << dendl;
  map<hobject_t,CopyOpRef>::iterator p = copy_ops.begin();
  while (p != copy_ops.end()) {
    // requeue this op? can I queue up all of them?
    cancel_copy((p++)->second, requeue, tids);
  }
}

struct C_gather : public Context {
  PrimaryLogPGRef pg;
  hobject_t oid;
  epoch_t last_peering_reset;
  OSDOp *osd_op;
  C_gather(PrimaryLogPG *pg_, hobject_t oid_, epoch_t lpr_, OSDOp *osd_op_) :
    pg(pg_), oid(oid_), last_peering_reset(lpr_), osd_op(osd_op_) {}
  void finish(int r) override {
    if (r == -ECANCELED)
      return;
    std::scoped_lock locker{*pg};
    auto p = pg->cls_gather_ops.find(oid);
    if (p == pg->cls_gather_ops.end()) {
      // op was cancelled
      return;
    }
    if (last_peering_reset != pg->get_last_peering_reset()) {
      return;
    }
    osd_op->rval = r;
    PrimaryLogPG::OpContext *ctx = p->second.ctx;
    pg->cls_gather_ops.erase(p);
    pg->execute_ctx(ctx);
  }
};

int PrimaryLogPG::start_cls_gather(OpContext *ctx, std::map<std::string, bufferlist> *src_obj_buffs, const std::string& pool,
				   const char *cls, const char *method, bufferlist& inbl)
{
  OpRequestRef op = ctx->op;
  MOSDOp *m = static_cast<MOSDOp*>(op->get_nonconst_req());

  auto pool_id = osd->objecter->with_osdmap(std::mem_fn(&OSDMap::lookup_pg_pool_name), pool);
  object_locator_t oloc(pool_id);

  ObjectState& obs = ctx->new_obs;
  object_info_t& oi = obs.oi;
  const hobject_t& soid = oi.soid;

  ObjectContextRef obc = get_object_context(soid, false);
  C_GatherBuilder gather(cct);

  auto [iter, inserted] = cls_gather_ops.emplace(soid, CLSGatherOp(ctx, obc, op));
  ceph_assert(inserted);
  auto &cgop = iter->second;
  for (std::map<std::string, bufferlist>::iterator it = src_obj_buffs->begin(); it != src_obj_buffs->end(); it++) {
    std::string oid = it->first;
    ObjectOperation obj_op;
    obj_op.call(cls, method, inbl);
    uint32_t flags = 0;
    ceph_tid_t tid = osd->objecter->read(
					 object_t(oid), oloc, obj_op,
					 m->get_snapid(), &it->second,
					 flags, gather.new_sub());
    cgop.objecter_tids.push_back(tid);
    dout(10) << __func__ << " src=" << oid << ", tgt=" << soid << dendl;
  }

  C_gather *fin = new C_gather(this, soid, get_last_peering_reset(), &(*ctx->ops)[ctx->current_osd_subop_num]);
  gather.set_finisher(new C_OnFinisher(fin,
				       osd->get_objecter_finisher(get_pg_shard())));
  gather.activate();

  return -EINPROGRESS;
}

// ========================================================================
// flush
//
// Flush a dirty object in the cache tier by writing it back to the
// base tier.  The sequence looks like:
//
//  * send a copy-from operation to the base tier to copy the current
//    version of the object
//  * base tier will pull the object via (perhaps multiple) copy-get(s)
//  * on completion, we check if the object has been modified.  if so,
//    just reply with -EAGAIN.
//  * try to take a write lock so we can clear the dirty flag.  if this
//    fails, wait and retry
//  * start a repop that clears the bit.
//
// If we have to wait, we will retry by coming back through the
// start_flush method.  We check if a flush is already in progress
// and, if so, try to finish it by rechecking the version and trying
// to clear the dirty bit.
//
// In order for the cache-flush (a write op) to not block the copy-get
// from reading the object, the client *must* set the SKIPRWLOCKS
// flag.
//
// NOTE: normally writes are strictly ordered for the client, but
// flushes are special in that they can be reordered with respect to
// other writes.  In particular, we can't have a flush request block
// an update to the cache pool object!

struct C_Flush : public Context {
  PrimaryLogPGRef pg;
  hobject_t oid;
  epoch_t last_peering_reset;
  ceph_tid_t tid;
  utime_t start;
  C_Flush(PrimaryLogPG *p, hobject_t o, epoch_t lpr)
    : pg(p), oid(o), last_peering_reset(lpr),
      tid(0), start(ceph_clock_now())
  {}
  void finish(int r) override {
    if (r == -ECANCELED)
      return;
    std::scoped_lock locker{*pg};
    if (last_peering_reset == pg->get_last_peering_reset()) {
      pg->finish_flush(oid, tid, r);
      pg->osd->logger->tinc(l_osd_tier_flush_lat, ceph_clock_now() - start);
    }
  }
};

int PrimaryLogPG::start_dedup(OpRequestRef op, ObjectContextRef obc)
{
  const object_info_t& oi = obc->obs.oi;
  const hobject_t& soid = oi.soid;

  ceph_assert(obc->is_blocked());
  if (oi.size == 0) {
    // evicted 
    return 0;
  }
  if (pool.info.get_fingerprint_type() == pg_pool_t::TYPE_FINGERPRINT_NONE) {
    dout(0) << " fingerprint algorithm is not set " << dendl;
    return -EINVAL;
  }
  if (pool.info.get_dedup_tier() <= 0) {
    dout(10) << " dedup tier is not set " << dendl;
    return -EINVAL;
  }

  /*
   * The operations to make dedup chunks are tracked by a ManifestOp.
   * This op will be finished if all the operations are completed.
   */
  ManifestOpRef mop(std::make_shared<ManifestOp>(obc, nullptr));

  // cdc
  std::map<uint64_t, bufferlist> chunks; 
  int r = do_cdc(oi, mop->new_manifest.chunk_map, chunks);
  if (r < 0) {
    return r;
  }
  if (!chunks.size()) {
    return 0;
  }

  // chunks issued here are different with chunk_map newly generated
  // because the same chunks in previous snap will not be issued
  // So, we need two data structures; the first is the issued chunk list to track
  // issued operations, and the second is the new chunk_map to update chunk_map after 
  // all operations are finished
  object_ref_delta_t refs;
  ObjectContextRef obc_l, obc_g;
  get_adjacent_clones(obc, obc_l, obc_g);
  // skip if the same content exits in prev snap at same offset
  mop->new_manifest.calc_refs_to_inc_on_set(
    obc_l ? &(obc_l->obs.oi.manifest) : nullptr,
    obc_g ? &(obc_g->obs.oi.manifest) : nullptr,
    refs);

  for (const auto& p : chunks) {
    hobject_t target = mop->new_manifest.chunk_map[p.first].oid;
    if (refs.find(target) == refs.end()) {
      continue;
    }
    C_SetDedupChunks *fin = new C_SetDedupChunks(this, soid, get_last_peering_reset(), p.first);
    ceph_tid_t tid = refcount_manifest(soid, target, refcount_t::CREATE_OR_GET_REF, 
			    fin, std::move(chunks[p.first]));
    mop->chunks[target] = make_pair(p.first, p.second.length());
    mop->num_chunks++;
    mop->tids[p.first] = tid;
    fin->tid = tid;
    dout(10) << __func__ << " oid: " << soid << " tid: " << tid
	    << " target: " << target << " offset: " << p.first
	    << " length: " << p.second.length() << dendl;
  }

  if (mop->tids.size()) {
    manifest_ops[soid] = mop;
    manifest_ops[soid]->op = op;
  } else {
    // size == 0
    return 0;
  }

  return -EINPROGRESS;
}

int PrimaryLogPG::do_cdc(const object_info_t& oi, 
			 std::map<uint64_t, chunk_info_t>& chunk_map,
			 std::map<uint64_t, bufferlist>& chunks)
{
  string chunk_algo = pool.info.get_dedup_chunk_algorithm_name();
  int64_t chunk_size = pool.info.get_dedup_cdc_chunk_size();
  uint64_t total_length = 0;

  std::unique_ptr<CDC> cdc = CDC::create(chunk_algo, cbits(chunk_size)-1);
  if (!cdc) {
    dout(0) << __func__ << " unrecognized chunk-algorithm " << dendl;
    return -EINVAL;
  }

  bufferlist bl;
  /**
   * We disable EC pool as a base tier of distributed dedup.
   * The reason why we disallow erasure code pool here is that the EC pool does not support objects_read_sync(). 
   * Therefore, we should change the current implementation totally to make EC pool compatible. 
   * As s result, we leave this as a future work.
   */
  int r = pgbackend->objects_read_sync(
      oi.soid, 0, oi.size, 0, &bl);
  if (r < 0) {
    dout(0) << __func__ << " read fail " << oi.soid
            << " len: " << oi.size << " r: " << r << dendl;
    return r;
  }
  if (bl.length() != oi.size) {
    dout(0) << __func__ << " bl.length: " << bl.length() << " != oi.size: "
	    << oi.size << " during chunking " << dendl;
    return -EIO;
  }

  dout(10) << __func__ << " oid: " << oi.soid << " len: " << bl.length() 
	   << " oi.size: " << oi.size   
	   << " chunk_size: " << chunk_size << dendl;

  vector<pair<uint64_t, uint64_t>> cdc_chunks;
  cdc->calc_chunks(bl, &cdc_chunks);

  // get fingerprint 
  for (auto p : cdc_chunks) {
    bufferlist chunk;
    chunk.substr_of(bl, p.first, p.second);
    auto [ret, target] = get_fpoid_from_chunk(oi.soid, chunk);
    if (ret < 0) {
      return ret;
    }
    chunks[p.first] = std::move(chunk);
    chunk_map[p.first] = chunk_info_t(0, p.second, target);
    total_length += p.second;
  }
  return total_length;
}

std::pair<int, hobject_t> PrimaryLogPG::get_fpoid_from_chunk(
  const hobject_t soid, bufferlist& chunk)
{
  pg_pool_t::fingerprint_t fp_algo = pool.info.get_fingerprint_type();
  if (fp_algo == pg_pool_t::TYPE_FINGERPRINT_NONE) {
    return make_pair(-EINVAL, hobject_t());
  }
  object_t fp_oid = [&fp_algo, &chunk]() -> string {
    switch (fp_algo) {
      case pg_pool_t::TYPE_FINGERPRINT_SHA1:
	return ceph::crypto::digest<ceph::crypto::SHA1>(chunk).to_str();
      case pg_pool_t::TYPE_FINGERPRINT_SHA256:
	return ceph::crypto::digest<ceph::crypto::SHA256>(chunk).to_str();
      case pg_pool_t::TYPE_FINGERPRINT_SHA512:
	return ceph::crypto::digest<ceph::crypto::SHA512>(chunk).to_str();
      default:
	ceph_assert(0 == "unrecognized fingerprint type");
	return {};
    }
  }();    

  pg_t raw_pg;
  object_locator_t oloc(soid);
  oloc.pool = pool.info.get_dedup_tier();
  // check if dedup_tier isn't set
  ceph_assert(oloc.pool > 0);
  int ret = get_osdmap()->object_locator_to_pg(fp_oid, oloc, raw_pg);
  if (ret < 0) {
    return make_pair(ret, hobject_t());
  }
  hobject_t target(fp_oid, oloc.key, snapid_t(),
		    raw_pg.ps(), raw_pg.pool(),
		    oloc.nspace);
  return make_pair(0, target);
}

int PrimaryLogPG::finish_set_dedup(hobject_t oid, int r, ceph_tid_t tid, uint64_t offset)
{
  dout(10) << __func__ << " " << oid << " tid " << tid
	   << " " << cpp_strerror(r) << dendl;
  map<hobject_t,ManifestOpRef>::iterator p = manifest_ops.find(oid);
  if (p == manifest_ops.end()) {
    dout(10) << __func__ << " no manifest_op found" << dendl;
    return -EINVAL;
  }
  ManifestOpRef mop = p->second;
  mop->results[offset] = r;
  if (r < 0) {
    // if any failure occurs, put a mark on the results to recognize the failure
    mop->results[0] = r;
  }
  if (mop->num_chunks != mop->results.size()) {
    // there are on-going works
    return -EINPROGRESS;
  }
  ObjectContextRef obc = mop->obc;
  ceph_assert(obc);
  ceph_assert(obc->is_blocked());
  obc->stop_block();
  kick_object_context_blocked(obc);
  if (mop->results[0] < 0) {
    // check if the previous op returns fail
    ceph_assert(mop->num_chunks == mop->results.size());
    manifest_ops.erase(oid);
    osd->reply_op_error(mop->op, mop->results[0]);
    return -EIO;
  }

  if (mop->chunks.size()) {
    OpContextUPtr ctx = simple_opc_create(obc);
    ceph_assert(ctx);
    if (ctx->lock_manager.get_lock_type(
	  RWState::RWWRITE,
	  oid,
	  obc,
	  mop->op)) {
      dout(20) << __func__ << " took write lock" << dendl;
    } else if (mop->op) {
      dout(10) << __func__ << " waiting on write lock " << mop->op << dendl;
      close_op_ctx(ctx.release());
      return -EAGAIN;    
    }

    ctx->at_version = get_next_version();
    ctx->new_obs = obc->obs;
    ctx->new_obs.oi.clear_flag(object_info_t::FLAG_DIRTY);
    --ctx->delta_stats.num_objects_dirty;
    if (!ctx->obs->oi.has_manifest()) {
      ctx->delta_stats.num_objects_manifest++;
      ctx->new_obs.oi.set_flag(object_info_t::FLAG_MANIFEST);
      ctx->new_obs.oi.manifest.type = object_manifest_t::TYPE_CHUNKED;
    }

    /* 
    * Let's assume that there is a manifest snapshotted object, and we issue tier_flush() to head.
    * head: [0, 2) aaa <-- tier_flush()
    * 20:   [0, 2) ddd, [6, 2) bbb, [8, 2) ccc
    * 
    * In this case, if the new chunk_map is as follows,
    * new_chunk_map : [0, 2) ddd, [6, 2) bbb, [8, 2) ccc
    * we should drop aaa from head by using calc_refs_to_drop_on_removal().
    * So, the precedure is 
    * 	1. calc_refs_to_drop_on_removal()
    * 	2. register old references to drop after tier_flush() is committed
    * 	3. update new chunk_map
    */

    ObjectCleanRegions c_regions = ctx->clean_regions;
    ObjectContextRef cobc = get_prev_clone_obc(obc);
    c_regions.mark_fully_dirty(); 
    // CDC was done on entire range of manifest object,
    // so the first thing we should do here is to drop the reference to old chunks
    ObjectContextRef obc_l, obc_g;
    get_adjacent_clones(obc, obc_l, obc_g);
    // clear all old references
    object_ref_delta_t refs;
    ctx->obs->oi.manifest.calc_refs_to_drop_on_removal(
      obc_l ? &(obc_l->obs.oi.manifest) : nullptr,
      obc_g ? &(obc_g->obs.oi.manifest) : nullptr,
      refs);
    if (!refs.is_empty()) {
      ctx->register_on_commit(
        [oid, this, refs](){
          dec_refcount(oid, refs);
        });
    }

    // set new references
    ctx->new_obs.oi.manifest.chunk_map = mop->new_manifest.chunk_map;

    finish_ctx(ctx.get(), pg_log_entry_t::CLEAN);
    simple_opc_submit(std::move(ctx));
  }
  if (mop->op)
    osd->reply_op_error(mop->op, r);

  manifest_ops.erase(oid);
  return 0;
}

int PrimaryLogPG::finish_set_manifest_refcount(hobject_t oid, int r, ceph_tid_t tid, uint64_t offset)
{
  dout(10) << __func__ << " " << oid << " tid " << tid
	   << " " << cpp_strerror(r) << dendl;
  map<hobject_t,ManifestOpRef>::iterator p = manifest_ops.find(oid);
  if (p == manifest_ops.end()) {
    dout(10) << __func__ << " no manifest_op found" << dendl;
    return -EINVAL;
  }
  ManifestOpRef mop = p->second;
  mop->results[offset] = r;
  if (r < 0) {
    // if any failure occurs, put a mark on the results to recognize the failure
    mop->results[0] = r;
  }
  if (mop->num_chunks != mop->results.size()) {
    // there are on-going works
    return -EINPROGRESS;
  }

  if (mop->cb) {
    mop->cb->complete(r);
  }

  manifest_ops.erase(p);
  mop.reset();

  return 0; 
}

int PrimaryLogPG::start_flush(
  OpRequestRef op, ObjectContextRef obc,
  bool blocking, hobject_t *pmissing,
  std::optional<std::function<void()>> &&on_flush,
  bool force_dedup)
{
  const object_info_t& oi = obc->obs.oi;
  const hobject_t& soid = oi.soid;
  dout(10) << __func__ << " " << soid
	   << " v" << oi.version
	   << " uv" << oi.user_version
	   << " " << (blocking ? "blocking" : "non-blocking/best-effort")
	   << dendl;

  const SnapSet& snapset = obc->ssc->snapset;

  if ((obc->obs.oi.has_manifest() && obc->obs.oi.manifest.is_chunked())
      || force_dedup) {
    // current dedup tier only supports blocking operation
    if (!blocking) {
      return -EOPNOTSUPP;
    }
  }

  // verify there are no (older) check for dirty clones
  {
    dout(20) << " snapset " << snapset << dendl;
    vector<snapid_t>::const_reverse_iterator p = snapset.clones.rbegin();
    while (p != snapset.clones.rend() && *p >= soid.snap)
      ++p;
    if (p != snapset.clones.rend()) {
      hobject_t next = soid;
      next.snap = *p;
      ceph_assert(next.snap < soid.snap);
      if (recovery_state.get_pg_log().get_missing().is_missing(next)) {
	dout(10) << __func__ << " missing clone is " << next << dendl;
	if (pmissing)
	  *pmissing = next;
	return -ENOENT;
      }
      ObjectContextRef older_obc = get_object_context(next, false);
      if (older_obc) {
	dout(20) << __func__ << " next oldest clone is " << older_obc->obs.oi
		 << dendl;
	if (older_obc->obs.oi.is_dirty()) {
	  dout(10) << __func__ << " next oldest clone is dirty: "
		   << older_obc->obs.oi << dendl;
	  return -EBUSY;
	}
      } else {
	dout(20) << __func__ << " next oldest clone " << next
		 << " is not present; implicitly clean" << dendl;
      }
    } else {
      dout(20) << __func__ << " no older clones" << dendl;
    }
  }

  if (blocking) {
    dout(20) << fmt::format("{}: blocking {}", __func__, soid) << dendl;
    obc->start_block();
  }

  map<hobject_t,FlushOpRef>::iterator p = flush_ops.find(soid);
  if (p != flush_ops.end()) {
    FlushOpRef fop = p->second;
    if (fop->op == op) {
      // we couldn't take the write lock on a cache-try-flush before;
      // now we are trying again for the lock.
      return try_flush_mark_clean(fop);
    }
    if (fop->flushed_version == obc->obs.oi.user_version &&
	(fop->blocking || !blocking)) {
      // nonblocking can join anything
      // blocking can only join a blocking flush
      dout(20) << __func__ << " piggybacking on existing flush " << dendl;
      if (op)
	fop->dup_ops.push_back(op);
      return -EAGAIN;   // clean up this ctx; op will retry later
    }

    // cancel current flush since it will fail anyway, or because we
    // are blocking and the existing flush is nonblocking.
    dout(20) << __func__ << " canceling previous flush; it will fail" << dendl;
    if (fop->op)
      osd->reply_op_error(fop->op, -EBUSY);
    while (!fop->dup_ops.empty()) {
      osd->reply_op_error(fop->dup_ops.front(), -EBUSY);
      fop->dup_ops.pop_front();
    }
    vector<ceph_tid_t> tids;
    cancel_flush(fop, false, &tids);
    osd->objecter->op_cancel(tids, -ECANCELED);
  }

  if ((obc->obs.oi.has_manifest() && obc->obs.oi.manifest.is_chunked())
      || force_dedup) {
    int r = start_dedup(op, obc);
    if (r != -EINPROGRESS) {
      if (blocking)
	obc->stop_block();
    }
    return r;
  }

  /**
   * In general, we need to send a delete and a copyfrom.
   * Consider snapc 10:[10, 9, 8, 4, 3, 2]:[10(10, 9), 4(4,3,2)]
   * where 4 is marked as clean.  To flush 10, we have to:
   * 1) delete 4:[4,3,2] -- Logically, the object does not exist after 4
   * 2) copyfrom 8:[8,4,3,2] -- flush object after snap 8
   *
   * There is a complicating case.  Supposed there had been a clone 7
   * for snaps [7, 6] which has been trimmed since they no longer exist.
   * In the base pool, we'd have 5:[4,3,2]:[4(4,3,2)]+head.  When we submit
   * the delete, the snap will be promoted to 5, and the head will become
   * a whiteout.  When the copy-from goes through, we'll end up with
   * 8:[8,4,3,2]:[4(4,3,2)]+head.
   *
   * Another complication is the case where there is an interval change
   * after doing the delete and the flush but before marking the object
   * clean.  We'll happily delete head and then recreate it at the same
   * sequence number, which works out ok.
   */

  SnapContext snapc, dsnapc;
  if (snapset.seq != 0) {
    if (soid.snap == CEPH_NOSNAP) {
      snapc = snapset.get_ssc_as_of(snapset.seq);
    } else {
      snapid_t min_included_snap;
      auto p = snapset.clone_snaps.find(soid.snap);
      ceph_assert(p != snapset.clone_snaps.end());
      min_included_snap = p->second.back();
      snapc = snapset.get_ssc_as_of(min_included_snap - 1);
    }

    snapid_t prev_snapc = 0;
    for (vector<snapid_t>::const_reverse_iterator citer = snapset.clones.rbegin();
	 citer != snapset.clones.rend();
	 ++citer) {
      if (*citer < soid.snap) {
	prev_snapc = *citer;
	break;
      }
    }

    dsnapc = snapset.get_ssc_as_of(prev_snapc);
  }

  object_locator_t base_oloc(soid);
  base_oloc.pool = pool.info.tier_of;

  if (dsnapc.seq < snapc.seq) {
    ObjectOperation o;
    o.remove();
    osd->objecter->mutate(
      soid.oid,
      base_oloc,
      o,
      dsnapc,
      ceph::real_clock::from_ceph_timespec(oi.mtime),
      (CEPH_OSD_FLAG_IGNORE_OVERLAY |
       CEPH_OSD_FLAG_ENFORCE_SNAPC),
      NULL /* no callback, we'll rely on the ordering w.r.t the next op */);
  }

  FlushOpRef fop(std::make_shared<FlushOp>());
  fop->obc = obc;
  fop->flushed_version = oi.user_version;
  fop->blocking = blocking;
  fop->on_flush = std::move(on_flush);
  fop->op = op;

  ObjectOperation o;
  if (oi.is_whiteout()) {
    fop->removal = true;
    o.remove();
  } else {
    object_locator_t oloc(soid);
    o.copy_from(soid.oid.name, soid.snap, oloc, oi.user_version,
		CEPH_OSD_COPY_FROM_FLAG_FLUSH |
		CEPH_OSD_COPY_FROM_FLAG_IGNORE_OVERLAY |
		CEPH_OSD_COPY_FROM_FLAG_IGNORE_CACHE |
		CEPH_OSD_COPY_FROM_FLAG_MAP_SNAP_CLONE,
		LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL|LIBRADOS_OP_FLAG_FADVISE_NOCACHE);

    //mean the base tier don't cache data after this
    if (agent_state && agent_state->evict_mode != TierAgentState::EVICT_MODE_FULL)
      o.set_last_op_flags(LIBRADOS_OP_FLAG_FADVISE_DONTNEED);
  }
  C_Flush *fin = new C_Flush(this, soid, get_last_peering_reset());

  ceph_tid_t tid = osd->objecter->mutate(
    soid.oid, base_oloc, o, snapc,
    ceph::real_clock::from_ceph_timespec(oi.mtime),
    CEPH_OSD_FLAG_IGNORE_OVERLAY | CEPH_OSD_FLAG_ENFORCE_SNAPC,
    new C_OnFinisher(fin,
		     osd->get_objecter_finisher(get_pg_shard())));
  /* we're under the pg lock and fin->finish() is grabbing that */
  fin->tid = tid;
  fop->objecter_tid = tid;

  flush_ops[soid] = fop;

  recovery_state.update_stats(
    [&oi](auto &history, auto &stats) {
      stats.stats.sum.num_flush++;
      stats.stats.sum.num_flush_kb += shift_round_up(oi.size, 10);
      return false;
    });
  return -EINPROGRESS;
}

void PrimaryLogPG::finish_flush(hobject_t oid, ceph_tid_t tid, int r)
{
  dout(10) << __func__ << " " << oid << " tid " << tid
	   << " " << cpp_strerror(r) << dendl;
  map<hobject_t,FlushOpRef>::iterator p = flush_ops.find(oid);
  if (p == flush_ops.end()) {
    dout(10) << __func__ << " no flush_op found" << dendl;
    return;
  }
  FlushOpRef fop = p->second;
  if (tid != fop->objecter_tid && !fop->obc->obs.oi.has_manifest()) {
    dout(10) << __func__ << " tid " << tid << " != fop " << fop
	     << " tid " << fop->objecter_tid << dendl;
    return;
  }
  ObjectContextRef obc = fop->obc;
  fop->objecter_tid = 0;

  if (r < 0 && !(r == -ENOENT && fop->removal)) {
    if (fop->op)
      osd->reply_op_error(fop->op, -EBUSY);
    if (fop->blocking) {
      obc->stop_block();
      kick_object_context_blocked(obc);
    }

    if (!fop->dup_ops.empty()) {
      dout(20) << __func__ << " requeueing dups" << dendl;
      requeue_ops(fop->dup_ops);
    }
    if (fop->on_flush) {
      (*(fop->on_flush))();
      fop->on_flush = std::nullopt;
    }
    flush_ops.erase(oid);
    return;
  }

  r = try_flush_mark_clean(fop);
  if (r == -EBUSY && fop->op) {
    osd->reply_op_error(fop->op, r);
  }
}

int PrimaryLogPG::try_flush_mark_clean(FlushOpRef fop)
{
  ObjectContextRef obc = fop->obc;
  const hobject_t& oid = obc->obs.oi.soid;

  if (fop->blocking) {
    obc->stop_block();
    kick_object_context_blocked(obc);
  }

  if (fop->flushed_version != obc->obs.oi.user_version ||
      !obc->obs.exists) {
    if (obc->obs.exists)
      dout(10) << __func__ << " flushed_version " << fop->flushed_version
	       << " != current " << obc->obs.oi.user_version
	       << dendl;
    else
      dout(10) << __func__ << " object no longer exists" << dendl;

    if (!fop->dup_ops.empty()) {
      dout(20) << __func__ << " requeueing dups" << dendl;
      requeue_ops(fop->dup_ops);
    }
    if (fop->on_flush) {
      (*(fop->on_flush))();
      fop->on_flush = std::nullopt;
    }
    flush_ops.erase(oid);
    if (fop->blocking)
      osd->logger->inc(l_osd_tier_flush_fail);
    else
      osd->logger->inc(l_osd_tier_try_flush_fail);
    return -EBUSY;
  }

  if (!fop->blocking &&
      m_scrubber->write_blocked_by_scrub(oid)) {
    if (fop->op) {
      dout(10) << __func__ << " blocked by scrub" << dendl;
      requeue_op(fop->op);
      requeue_ops(fop->dup_ops);
      return -EAGAIN;    // will retry
    } else {
      osd->logger->inc(l_osd_tier_try_flush_fail);
      vector<ceph_tid_t> tids;
      cancel_flush(fop, false, &tids);
      osd->objecter->op_cancel(tids, -ECANCELED);
      return -ECANCELED;
    }
  }

  // successfully flushed, can we evict this object?
  if (!obc->obs.oi.has_manifest() && !fop->op &&
      agent_state && agent_state->evict_mode != TierAgentState::EVICT_MODE_IDLE &&
      agent_maybe_evict(obc, true)) {
    osd->logger->inc(l_osd_tier_clean);
    if (fop->on_flush) {
      (*(fop->on_flush))();
      fop->on_flush = std::nullopt;
    }
    flush_ops.erase(oid);
    return 0;
  }

  dout(10) << __func__ << " clearing DIRTY flag for " << oid << dendl;
  OpContextUPtr ctx = simple_opc_create(fop->obc);

  // successfully flushed; can we clear the dirty bit?
  // try to take the lock manually, since we don't
  // have a ctx yet.
  if (ctx->lock_manager.get_lock_type(
	RWState::RWWRITE,
	oid,
	obc,
	fop->op)) {
    dout(20) << __func__ << " took write lock" << dendl;
  } else if (fop->op) {
    dout(10) << __func__ << " waiting on write lock " << fop->op << " "
	     << fop->dup_ops << dendl;
    // fop->op is now waiting on the lock; get fop->dup_ops to wait too.
    for (auto op : fop->dup_ops) {
      bool locked = ctx->lock_manager.get_lock_type(
	RWState::RWWRITE,
	oid,
	obc,
	op);
      ceph_assert(!locked);
    }
    close_op_ctx(ctx.release());
    return -EAGAIN;    // will retry
  } else {
    dout(10) << __func__ << " failed write lock, no op; failing" << dendl;
    close_op_ctx(ctx.release());
    osd->logger->inc(l_osd_tier_try_flush_fail);
    vector<ceph_tid_t> tids;
    cancel_flush(fop, false, &tids);
    osd->objecter->op_cancel(tids, -ECANCELED);
    return -ECANCELED;
  }

  if (fop->on_flush) {
    ctx->register_on_finish(*(fop->on_flush));
    fop->on_flush = std::nullopt;
  }

  ctx->at_version = get_next_version();

  ctx->new_obs = obc->obs;
  ctx->new_obs.oi.clear_flag(object_info_t::FLAG_DIRTY);
  --ctx->delta_stats.num_objects_dirty;
  if (fop->obc->obs.oi.has_manifest()) {
    ceph_assert(obc->obs.oi.manifest.is_chunked());
    PGTransaction* t = ctx->op_t.get();
    uint64_t chunks_size = 0;
    for (auto &p : ctx->new_obs.oi.manifest.chunk_map) {
      chunks_size += p.second.length;
    }
    if (ctx->new_obs.oi.is_omap() && pool.info.supports_omap()) {
      t->omap_clear(oid);
      ctx->new_obs.oi.clear_omap_digest();
      ctx->new_obs.oi.clear_flag(object_info_t::FLAG_OMAP);
      ctx->clean_regions.mark_omap_dirty();
    }
    if (obc->obs.oi.size == chunks_size) {
      t->truncate(oid, 0);
      interval_set<uint64_t> trim;
      trim.insert(0, ctx->new_obs.oi.size);
      ctx->modified_ranges.union_of(trim);
      truncate_update_size_and_usage(ctx->delta_stats,
				     ctx->new_obs.oi,
				     0);
      ctx->clean_regions.mark_data_region_dirty(0, ctx->new_obs.oi.size);
      ctx->new_obs.oi.new_object();
      for (auto &p : ctx->new_obs.oi.manifest.chunk_map) {
	p.second.set_flag(chunk_info_t::FLAG_MISSING);
      }
    } else {
      for (auto &p : ctx->new_obs.oi.manifest.chunk_map) {
	dout(20) << __func__ << " offset: " << p.second.offset
		<< " length: " << p.second.length << dendl;
	p.second.clear_flag(chunk_info_t::FLAG_MISSING); // CLEAN
      }
    }
  }

  finish_ctx(ctx.get(), pg_log_entry_t::CLEAN);

  osd->logger->inc(l_osd_tier_clean);

  if (!fop->dup_ops.empty() || fop->op) {
    dout(20) << __func__ << " requeueing for " << ctx->at_version << dendl;
    list<OpRequestRef> ls;
    if (fop->op)
      ls.push_back(fop->op);
    ls.splice(ls.end(), fop->dup_ops);
    requeue_ops(ls);
  }

  simple_opc_submit(std::move(ctx));

  flush_ops.erase(oid);

  if (fop->blocking)
    osd->logger->inc(l_osd_tier_flush);
  else
    osd->logger->inc(l_osd_tier_try_flush);

  return -EINPROGRESS;
}

void PrimaryLogPG::cancel_flush(FlushOpRef fop, bool requeue,
				vector<ceph_tid_t> *tids)
{
  dout(10) << __func__ << " " << fop->obc->obs.oi.soid << " tid "
	   << fop->objecter_tid << dendl;
  if (fop->objecter_tid) {
    tids->push_back(fop->objecter_tid);
    fop->objecter_tid = 0;
  }
  if (fop->io_tids.size()) {
    for (auto &p : fop->io_tids) {
      tids->push_back(p.second);
      p.second = 0;
    }
  }
  if (fop->blocking && fop->obc->is_blocked()) {
    fop->obc->stop_block();
    kick_object_context_blocked(fop->obc);
  }
  if (requeue) {
    if (fop->op)
      requeue_op(fop->op);
    requeue_ops(fop->dup_ops);
  }
  if (fop->on_flush) {
    (*(fop->on_flush))();
    fop->on_flush = std::nullopt;
  }
  flush_ops.erase(fop->obc->obs.oi.soid);
}

void PrimaryLogPG::cancel_flush_ops(bool requeue, vector<ceph_tid_t> *tids)
{
  dout(10) << __func__ << dendl;
  map<hobject_t,FlushOpRef>::iterator p = flush_ops.begin();
  while (p != flush_ops.end()) {
    cancel_flush((p++)->second, requeue, tids);
  }
}

bool PrimaryLogPG::is_present_clone(hobject_t coid)
{
  if (!pool.info.allow_incomplete_clones())
    return true;
  if (is_missing_object(coid))
    return true;
  ObjectContextRef obc = get_object_context(coid, false);
  return obc && obc->obs.exists;
}

// ========================================================================
// cls gather
//

void PrimaryLogPG::cancel_cls_gather(map<hobject_t,CLSGatherOp>::iterator iter, bool requeue,
				     vector<ceph_tid_t> *tids)
{
  auto &cgop = iter->second;
  for (std::vector<ceph_tid_t>::iterator p = cgop.objecter_tids.begin(); p != cgop.objecter_tids.end(); p++) {
    tids->push_back(*p);
    dout(10) << __func__ << " " << cgop.obc->obs.oi.soid << " tid " << *p << dendl;
  }
  cgop.objecter_tids.clear();
  close_op_ctx(cgop.ctx);
  cgop.ctx = NULL;
  if (requeue) {
    if (cgop.op)
      requeue_op(cgop.op);
  }
  cls_gather_ops.erase(iter);
}

void PrimaryLogPG::cancel_cls_gather_ops(bool requeue, vector<ceph_tid_t> *tids)
{
  dout(10) << __func__ << dendl;
  map<hobject_t,CLSGatherOp>::iterator p = cls_gather_ops.begin();
  while (p != cls_gather_ops.end()) {
    cancel_cls_gather(p++, requeue, tids);
  }
}

// ========================================================================
// rep op gather

class C_OSD_RepopCommit : public Context {
  PrimaryLogPGRef pg;
  boost::intrusive_ptr<PrimaryLogPG::RepGather> repop;
public:
  C_OSD_RepopCommit(PrimaryLogPG *pg, PrimaryLogPG::RepGather *repop)
    : pg(pg), repop(repop) {}
  void finish(int) override {
    pg->repop_all_committed(repop.get());
  }
};

void PrimaryLogPG::repop_all_committed(RepGather *repop)
{
  dout(10) << __func__ << ": repop tid " << repop->rep_tid << " all committed "
	   << dendl;
  repop->all_committed = true;
  if (!repop->rep_aborted) {
    if (repop->v != eversion_t()) {
      recovery_state.complete_write(repop->v, repop->pg_local_last_complete);
    }
    eval_repop(repop);
  }
}

void PrimaryLogPG::op_applied(const eversion_t &applied_version)
{
  dout(10) << "op_applied version " << applied_version << dendl;
  ceph_assert(applied_version != eversion_t());
  ceph_assert(applied_version <= info.last_update);
  recovery_state.local_write_applied(applied_version);

  if (is_primary() && m_scrubber) {
    // if there's a scrub operation waiting for the selected chunk to be fully updated -
    // allow it to continue
    m_scrubber->on_applied_when_primary(recovery_state.get_last_update_applied());
  }
}

// 评估一次复制写是否可以完成客户端回复和有序收尾。
// 该函数既会在首次 issue 后调用，也会在 backend 汇总完 primary/副本提交结果后再次调用。
void PrimaryLogPG::eval_repop(RepGather *repop)
{
  dout(10) << "eval_repop " << *repop
    << (repop->op && repop->op->get_req<MOSDOp>() ? "" : " (no op)") << dendl;

  // 只有 backend 确认所有要求参与的 OSD 均已提交，才能宣告 ONDISK。
  // 首次 issue_repop() 后通常尚未满足条件，本函数会直接返回等待回调。
  if (repop->all_committed) {
    dout(10) << " commit: " << *repop << dendl;
    // 执行提交阶段回调。
    // 普通客户端写在这里统计 IO、添加 ACK|ONDISK，并发送最终回复；逐个删除可保证回调不会被重复执行。
    for (auto p = repop->on_committed.begin();
	 p != repop->on_committed.end();
	 repop->on_committed.erase(p++)) {
      (*p)();
    }
    // 相同 reqid 的重复请求若在原写尚未落盘时到达，会挂在 waiting_for_ondisk[v]。
    // 现在复用原操作的版本和结果统一回复它们。
    auto it = waiting_for_ondisk.find(repop->v);
    if (it != waiting_for_ondisk.end()) {
      // 重复写回复也必须按版本推进；当前完成版本应是等待表中最早的一项。
      ceph_assert(waiting_for_ondisk.begin()->first == repop->v);
      for (auto& i : it->second) {
        // repop 自身失败时返回该失败；否则使用首次请求记录的返回码，保证重复请求得到与原请求相同的结果。
        int return_code = repop->r;
        if (return_code >= 0) {
          return_code = std::get<2>(i);
        }
        osd->reply_op_error(std::get<0>(i), return_code, repop->v,
                            std::get<1>(i), std::get<3>(i));
      }
      waiting_for_ondisk.erase(it);
    }

    // 提交已稳定，可以把最新 PG 统计发布给 OSD service/monitor 上报路径。
    publish_stats_to_osd();

    dout(10) << " removing " << *repop << dendl;
    ceph_assert(!repop_queue.empty());
    dout(20) << "   q front is " << *repop_queue.front() << dendl;
    // repop 的物理提交可能乱序完成，但 on_success、锁释放和上下文销毁必须保持 PG 写入顺序。
    // 非队头完成时只标记 all_committed，暂不移除。
    if (repop_queue.front() == repop) {
      RepGather *to_remove = nullptr;
      // 从队头连续处理所有已经提交的 repop；遇到第一个未提交项立即停止，
      // 即使其后的写已经完成，也不能越过它执行成功副作用。
      while (!repop_queue.empty() &&
		     (to_remove = repop_queue.front())->all_committed) {
	repop_queue.pop_front();
	// on_success 包括 watch/notify 等依赖事务成功且要求有序的内存/网络动作。
	for (auto p = to_remove->on_success.begin();
	     p != to_remove->on_success.end();
	     to_remove->on_success.erase(p++)) {
	  (*p)();
	}
	// 执行 on_finish、释放对象锁和队列引用，最终结束该 RepGather 生命周期。
	remove_repop(to_remove);
      }
    }
  }
}

// 将 execute_ctx()/finish_ctx() 准备好的事务交给 PGBackend：
// backend 会提交 primary 本地 ObjectStore 事务，并把等价修改发送给 acting 副本。
// RepGather 负责在 primary 上保存这次写的身份、顺序、锁和完成回调，直到所需 OSD 全部提交。
void PrimaryLogPG::issue_repop(RepGather *repop, OpContext *ctx)
{
  FUNCTRACE(cct);
  const hobject_t& soid = ctx->obs->oi.soid;
  dout(7) << "issue_repop rep_tid " << repop->rep_tid
          << " o " << soid
          << dendl;
  // RepGather 以本次 PG Log 版本标识这次复制写，后续提交完成、重复请求
  // 等待队列及 last_complete 推进都使用该版本。
  repop->v = ctx->at_version;

  // 把事务涉及的 OBC 关联到 PGTransaction。除目标对象外，快照写可能同时
  // 创建 clone 或更新 head/SnapSet，这些上下文需要一起受事务生命周期保护。
  ctx->op_t->add_obc(ctx->obc);
  if (ctx->clone_obc) {
    ctx->op_t->add_obc(ctx->clone_obc);
  }
  if (ctx->head_obc) {
    ctx->op_t->add_obc(ctx->head_obc);
  }

  // backend 在 primary 和所需副本均提交后执行该回调，
  // 最终进入 repop_all_committed()，再由 eval_repop() 触发客户端提交回复。
  Context *on_all_commit = new C_OSD_RepopCommit(this, repop);

  // projected_last_update/projected_log 表示已经发出但可能尚未落盘的写入。
  // 后续请求和 peering 逻辑必须看到这些投影，不能只依据稳定 PG Log。
  if (!(ctx->log.empty())) {
    ceph_assert(ctx->at_version >= projected_last_update);
    projected_last_update = ctx->at_version;
  }
  for (auto &&entry: ctx->log) {
    projected_log.add(entry);
  }

  // 在真正提交前通知 PeeringState：预先推进各 peer 的投影版本，
  // 并为异步恢复目标更新 peer_missing/missing_loc，避免恢复判断落后于在途写入。
  recovery_state.pre_submit_op(
    soid,
    ctx->log,
    ctx->at_version);
  // 所有权在此转交给具体 backend。
  // ReplicatedBackend 会生成本地 ObjectStore::Transaction 并向副本发送 MOSDRepOp；
  // ECBackend 则负责编码和 shard 写入。数据、对象元数据和 PG Log 作为同一事务提交。
  pgbackend->submit_transaction(
    soid,
    ctx->delta_stats,
    ctx->at_version,
    std::move(ctx->op_t),  // 转交对象数据/属性修改事务
    recovery_state.get_pg_trim_to(),
    recovery_state.get_pg_committed_to(),
    std::move(ctx->log),   // 转交本次 PG Log 条目
    ctx->updated_hset_history,
    on_all_commit,         // 所有参与 OSD 提交后的聚合回调
    repop->rep_tid,        // primary 与副本匹配本次复制操作的内部 tid
    ctx->reqid,            // 客户端请求 ID，用于日志和重复请求检测
    ctx->op);              // 保留原请求引用，供跟踪、统计及完成处理
}

PrimaryLogPG::RepGather *PrimaryLogPG::new_repop(
  OpContext *ctx,
  ceph_tid_t rep_tid)
{
  if (ctx->op)
    dout(10) << "new_repop rep_tid " << rep_tid << " on " << *ctx->op->get_req() << dendl;
  else
    dout(10) << "new_repop rep_tid " << rep_tid << " (no op)" << dendl;

  RepGather *repop = new RepGather(
    ctx, rep_tid, info.last_complete);

  repop->start = ceph_clock_now();

  repop_queue.push_back(&repop->queue_item);
  repop->get();

  osd->logger->inc(l_osd_op_wip);

  dout(10) << __func__ << ": " << *repop << dendl;
  return repop;
}

boost::intrusive_ptr<PrimaryLogPG::RepGather> PrimaryLogPG::new_repop(
  eversion_t version,
  int r,
  ObcLockManager &&manager,
  OpRequestRef &&op,
  std::optional<std::function<void(void)> > &&on_complete)
{
  RepGather *repop = new RepGather(
    std::move(manager),
    std::move(op),
    std::move(on_complete),
    osd->get_tid(),
    info.last_complete,
    r);
  repop->v = version;

  repop->start = ceph_clock_now();

  repop_queue.push_back(&repop->queue_item);

  osd->logger->inc(l_osd_op_wip);

  dout(10) << __func__ << ": " << *repop << dendl;
  return boost::intrusive_ptr<RepGather>(repop);
}

void PrimaryLogPG::remove_repop(RepGather *repop)
{
  dout(20) << __func__ << " " << *repop << dendl;

  for (auto p = repop->on_finish.begin();
       p != repop->on_finish.end();
       repop->on_finish.erase(p++)) {
    (*p)();
  }

  release_object_locks(
    repop->lock_manager);
  repop->put();

  osd->logger->dec(l_osd_op_wip);
}

PrimaryLogPG::OpContextUPtr PrimaryLogPG::simple_opc_create(ObjectContextRef obc)
{
  dout(20) << __func__ << " " << obc->obs.oi.soid << dendl;
  ceph_tid_t rep_tid = osd->get_tid();
  osd_reqid_t reqid(osd->get_cluster_msgr_name(), 0, rep_tid);
  OpContextUPtr ctx(new OpContext(OpRequestRef(), reqid, nullptr, obc, this));
  ctx->op_t.reset(new PGTransaction());
  ctx->mtime = ceph_clock_now();
  return ctx;
}

void PrimaryLogPG::simple_opc_submit(OpContextUPtr ctx)
{
  RepGather *repop = new_repop(ctx.get(), ctx->reqid.tid);
  dout(20) << __func__ << " " << repop << dendl;
  issue_repop(repop, ctx.get());
  eval_repop(repop);
  recovery_state.update_trim_to();
  repop->put();
}


void PrimaryLogPG::submit_log_entries(
  const mempool::osd_pglog::list<pg_log_entry_t> &entries,
  ObcLockManager &&manager,
  std::optional<std::function<void(void)> > &&_on_complete,
  OpRequestRef op,
  int r)
{
  dout(10) << __func__ << " " << entries << dendl;
  ceph_assert(is_primary());

  eversion_t version;
  if (!entries.empty()) {
    ceph_assert(entries.rbegin()->version >= projected_last_update);
    version = projected_last_update = entries.rbegin()->version;
  }

  boost::intrusive_ptr<RepGather> repop;
  std::optional<std::function<void(void)> > on_complete;
  if (get_osdmap()->require_osd_release >= ceph_release_t::jewel) {
    repop = new_repop(
      version,
      r,
      std::move(manager),
      std::move(op),
      std::move(_on_complete));
  } else {
    on_complete = std::move(_on_complete);
  }

  pgbackend->call_write_ordered(
    [this, entries, repop, on_complete]() mutable {
      ObjectStore::Transaction t;
      eversion_t old_last_update = info.last_update;
      recovery_state.merge_new_log_entries(
	entries, t, recovery_state.get_pg_trim_to(),
	recovery_state.get_pg_committed_to());

      set<pg_shard_t> waiting_on;
      for (set<pg_shard_t>::const_iterator i = get_acting_recovery_backfill().begin();
	   i != get_acting_recovery_backfill().end();
	   ++i) {
	pg_shard_t peer(*i);
	if (peer == pg_whoami) continue;
	ceph_assert(recovery_state.get_peer_missing().count(peer));
	ceph_assert(recovery_state.has_peer_info(peer));
	if (get_osdmap()->require_osd_release >= ceph_release_t::jewel) {
	  ceph_assert(repop);
	  MOSDPGUpdateLogMissing *m = new MOSDPGUpdateLogMissing(
	    entries,
	    spg_t(info.pgid.pgid, i->shard),
	    pg_whoami.shard,
	    get_osdmap_epoch(),
	    get_last_peering_reset(),
	    repop->rep_tid,
	    recovery_state.get_pg_trim_to(),
	    recovery_state.get_pg_committed_to());
	  osd->send_message_osd_cluster(
	    peer.osd, m, get_osdmap_epoch());
	  waiting_on.insert(peer);
	} else {
	  MOSDPGLog *m = new MOSDPGLog(
	    peer.shard, pg_whoami.shard,
	    info.last_update.epoch,
	    info, get_last_peering_reset());
	  m->log.log = entries;
	  m->log.tail = old_last_update;
	  m->log.head = info.last_update;
	  osd->send_message_osd_cluster(
	    peer.osd, m, get_osdmap_epoch());
	}
      }
      ceph_tid_t rep_tid = repop->rep_tid;
      waiting_on.insert(pg_whoami);
      log_entry_update_waiting_on.insert(
	make_pair(
	  rep_tid,
	  LogUpdateCtx{std::move(repop), std::move(waiting_on)}
	  ));
      struct OnComplete : public Context {
	PrimaryLogPGRef pg;
	ceph_tid_t rep_tid;
	epoch_t epoch;
	OnComplete(
	  PrimaryLogPGRef pg,
	  ceph_tid_t rep_tid,
	  epoch_t epoch)
	  : pg(pg), rep_tid(rep_tid), epoch(epoch) {}
	void finish(int) override {
	  std::scoped_lock l{*pg};
	  if (!pg->pg_has_reset_since(epoch)) {
	    auto it = pg->log_entry_update_waiting_on.find(rep_tid);
	    ceph_assert(it != pg->log_entry_update_waiting_on.end());
	    auto it2 = it->second.waiting_on.find(pg->pg_whoami);
	    ceph_assert(it2 != it->second.waiting_on.end());
	    it->second.waiting_on.erase(it2);
	    if (it->second.waiting_on.empty()) {
	      pg->repop_all_committed(it->second.repop.get());
	      pg->log_entry_update_waiting_on.erase(it);
	    }
	  }
	}
      };
      t.register_on_commit(
	new OnComplete{this, rep_tid, get_osdmap_epoch()});
      int r = osd->store->queue_transaction(ch, std::move(t), NULL);
      ceph_assert(r == 0);
      op_applied(info.last_update);
    });

  recovery_state.update_trim_to();
}

void PrimaryLogPG::cancel_log_updates()
{
  // get rid of all the LogUpdateCtx so their references to repops are
  // dropped
  log_entry_update_waiting_on.clear();
}

// -------------------------------------------------------

void PrimaryLogPG::get_watchers(list<obj_watch_item_t> *ls)
{
  std::scoped_lock l{*this};
  pair<hobject_t, ObjectContextRef> i;
  while (object_contexts.get_next(i.first, &i)) {
    ObjectContextRef obc(i.second);
    get_obc_watchers(obc, *ls);
  }
}

void PrimaryLogPG::get_obc_watchers(ObjectContextRef obc, list<obj_watch_item_t> &pg_watchers)
{
  for (map<pair<uint64_t, entity_name_t>, WatchRef>::iterator j =
	 obc->watchers.begin();
	j != obc->watchers.end();
	++j) {
    obj_watch_item_t owi;

    owi.obj = obc->obs.oi.soid;
    owi.wi.addr = j->second->get_peer_addr();
    owi.wi.name = j->second->get_entity();
    owi.wi.cookie = j->second->get_cookie();
    owi.wi.timeout_seconds = j->second->get_timeout();

    dout(30) << "watch: Found oid=" << owi.obj << " addr=" << owi.wi.addr
      << " name=" << owi.wi.name << " cookie=" << owi.wi.cookie << dendl;

    pg_watchers.push_back(owi);
  }
}

void PrimaryLogPG::check_blocklisted_watchers()
{
  dout(20) << "PrimaryLogPG::check_blocklisted_watchers for pg " << get_pgid() << dendl;
  pair<hobject_t, ObjectContextRef> i;
  while (object_contexts.get_next(i.first, &i))
    check_blocklisted_obc_watchers(i.second);
}

void PrimaryLogPG::check_blocklisted_obc_watchers(ObjectContextRef obc)
{
  dout(20) << "PrimaryLogPG::check_blocklisted_obc_watchers for obc " << obc->obs.oi.soid << dendl;
  for (map<pair<uint64_t, entity_name_t>, WatchRef>::iterator k =
	 obc->watchers.begin();
	k != obc->watchers.end();
	) {
    //Advance iterator now so handle_watch_timeout() can erase element
    map<pair<uint64_t, entity_name_t>, WatchRef>::iterator j = k++;
    dout(30) << "watch: Found " << j->second->get_entity() << " cookie " << j->second->get_cookie() << dendl;
    entity_addr_t ea = j->second->get_peer_addr();
    dout(30) << "watch: Check entity_addr_t " << ea << dendl;
    if (get_osdmap()->is_blocklisted(ea)) {
      dout(10) << "watch: Found blocklisted watcher for " << ea << dendl;
      ceph_assert(j->second->get_pg() == this);
      j->second->unregister_cb();
      handle_watch_timeout(j->second);
    }
  }
}

void PrimaryLogPG::populate_obc_watchers(ObjectContextRef obc)
{
  ceph_assert(is_primary() && is_active());
  auto it_objects = recovery_state.get_pg_log().get_log().objects.find(obc->obs.oi.soid);
  ceph_assert((recovering.count(obc->obs.oi.soid) ||
	  !is_missing_object(obc->obs.oi.soid)) ||
	 (it_objects != recovery_state.get_pg_log().get_log().objects.end() && // or this is a revert... see recover_primary()
	  it_objects->second->op ==
	    pg_log_entry_t::LOST_REVERT &&
	  it_objects->second->reverting_to ==
	    obc->obs.oi.version));

  dout(10) << "populate_obc_watchers " << obc->obs.oi.soid << dendl;
  ceph_assert(obc->watchers.empty());
  // populate unconnected_watchers
  for (map<pair<uint64_t, entity_name_t>, watch_info_t>::iterator p =
	obc->obs.oi.watchers.begin();
       p != obc->obs.oi.watchers.end();
       ++p) {
    utime_t expire = info.stats.last_became_active;
    expire += p->second.timeout_seconds;
    dout(10) << "  unconnected watcher " << p->first << " will expire " << expire << dendl;
    WatchRef watch(
      Watch::makeWatchRef(
	this, osd, obc, p->second.timeout_seconds, p->first.first,
	p->first.second, p->second.addr));
    watch->disconnect();
    obc->watchers.insert(
      make_pair(
	make_pair(p->first.first, p->first.second),
	watch));
  }
  // Look for watchers from blocklisted clients and drop
  check_blocklisted_obc_watchers(obc);
}

void PrimaryLogPG::handle_watch_timeout(WatchRef watch)
{
  ObjectContextRef obc = watch->get_obc(); // handle_watch_timeout owns this ref
  dout(10) << "handle_watch_timeout obc " << *obc << dendl;

  if (!is_active()) {
    dout(10) << "handle_watch_timeout not active, no-op" << dendl;
    return;
  }
  if (!obc->obs.exists) {
    dout(10) << __func__ << " object " << obc->obs.oi.soid << " dne" << dendl;
    return;
  }
  if (is_degraded_or_backfilling_object(obc->obs.oi.soid)) {
    callbacks_for_degraded_object[obc->obs.oi.soid].push_back(
      watch->get_delayed_cb()
      );
    dout(10) << "handle_watch_timeout waiting for degraded on obj "
	     << obc->obs.oi.soid
	     << dendl;
    return;
  }

  if (m_scrubber->write_blocked_by_scrub(obc->obs.oi.soid)) {
    dout(10) << "handle_watch_timeout waiting for scrub on obj "
	     << obc->obs.oi.soid
	     << dendl;
    m_scrubber->add_callback(
      watch->get_delayed_cb() // This callback!
      );
    return;
  }

  OpContextUPtr ctx = simple_opc_create(obc);
  ctx->at_version = get_next_version();

  object_info_t& oi = ctx->new_obs.oi;
  oi.watchers.erase(make_pair(watch->get_cookie(),
			      watch->get_entity()));

  osd->logger->inc(l_osd_watch_timeouts);
  dout(3) << __func__ << " watcher " << watch->get_peer_addr()
	  << " object " << obc->obs.oi.soid << dendl;

  list<watch_disconnect_t> watch_disconnects = {
    watch_disconnect_t(watch->get_cookie(), watch->get_entity(), true)
  };
  ctx->register_on_success(
    [this, obc, watch_disconnects]() {
      complete_disconnect_watches(obc, watch_disconnects);
    });


  PGTransaction *t = ctx->op_t.get();
  ctx->log.push_back(pg_log_entry_t(pg_log_entry_t::MODIFY, obc->obs.oi.soid,
				    ctx->at_version,
				    oi.version,
				    0,
				    osd_reqid_t(), ctx->mtime, 0));

  oi.prior_version = obc->obs.oi.version;
  oi.version = ctx->at_version;
  bufferlist bl;
  encode(oi, bl, get_osdmap()->get_features(CEPH_ENTITY_TYPE_OSD, nullptr));
  t->setattr(obc->obs.oi.soid, OI_ATTR, bl);

  // apply new object state.
  ctx->obc->obs = ctx->new_obs;

  // no ctx->delta_stats
  simple_opc_submit(std::move(ctx));
}

ObjectContextRef PrimaryLogPG::create_object_context(const object_info_t& oi,
						     SnapSetContext *ssc)
{
  ObjectContextRef obc(object_contexts.lookup_or_create(oi.soid));
  ceph_assert(obc->destructor_callback == NULL);
  obc->destructor_callback = new C_PG_ObjectContext(this, obc.get());
  obc->obs.oi = oi;
  obc->obs.exists = false;
  obc->ssc = ssc;
  if (ssc)
    register_snapset_context(ssc);
  dout(10) << "create_object_context " << (void*)obc.get() << " " << oi.soid << " " << dendl;
  if (is_active())
    populate_obc_watchers(obc);
  return obc;
}

ObjectContextRef PrimaryLogPG::get_object_context(
  const hobject_t& soid,
  bool can_create,
  const map<string, bufferlist, less<>> *attrs)
{
  auto it_objects = recovery_state.get_pg_log().get_log().objects.find(soid);
  ceph_assert(
    attrs || !recovery_state.get_pg_log().get_missing().is_missing(soid) ||
    // or this is a revert... see recover_primary()
    (it_objects != recovery_state.get_pg_log().get_log().objects.end() &&
      it_objects->second->op ==
      pg_log_entry_t::LOST_REVERT));
  ObjectContextRef obc = object_contexts.lookup(soid);
  osd->logger->inc(l_osd_object_ctx_cache_total);
  if (obc) {
    osd->logger->inc(l_osd_object_ctx_cache_hit);
    dout(10) << __func__ << ": found obc in cache: " << *obc
	     << dendl;
  } else {
    dout(10) << __func__ << ": obc NOT found in cache: " << soid << dendl;
    // check disk
    bufferlist bv;
    if (attrs) {
      auto it_oi = attrs->find(OI_ATTR);
      ceph_assert(it_oi != attrs->end());
      bv = it_oi->second;
    } else {
      int r = pgbackend->objects_get_attr(soid, OI_ATTR, &bv);
      if (r < 0) {
	if (!can_create) {
	  dout(10) << __func__ << ": no obc for soid "
		   << soid << " and !can_create"
		   << dendl;
	  return ObjectContextRef();   // -ENOENT!
	}

	dout(10) << __func__ << ": no obc for soid "
		 << soid << " but can_create"
		 << dendl;
	// new object.
	object_info_t oi(soid);
	SnapSetContext *ssc = get_snapset_context(
	  soid, true, 0, false);
        ceph_assert(ssc);
	obc = create_object_context(oi, ssc);
	dout(10) << __func__ << ": " << *obc
		 << " oi: " << obc->obs.oi
		 << " " << *obc->ssc << dendl;
	return obc;
      }
    }

    object_info_t oi;
    try {
      bufferlist::const_iterator bliter = bv.begin();
      decode(oi, bliter);
    } catch (...) {
      dout(0) << __func__ << ": obc corrupt: " << soid << dendl;
      return ObjectContextRef();   // -ENOENT!
    }

    ceph_assert(oi.soid.pool == (int64_t)info.pgid.pool());

    obc = object_contexts.lookup_or_create(oi.soid);
    obc->destructor_callback = new C_PG_ObjectContext(this, obc.get());
    obc->obs.oi = oi;
    obc->obs.exists = true;

    obc->ssc = get_snapset_context(
      soid, true,
      soid.has_snapset() ? attrs : 0);

    if (is_primary() && is_active())
      populate_obc_watchers(obc);

    if (pool.info.is_erasure()) {
      if (attrs) {
	obc->attr_cache = *attrs;
      } else {
	int r = pgbackend->objects_get_attrs(
	  soid,
	  &obc->attr_cache);
	ceph_assert(r == 0);
      }
    }

    dout(10) << __func__ << ": creating obc from disk: " << *obc
	     << dendl;
  }

  // XXX: Caller doesn't expect this
  if (obc->ssc == NULL) {
    derr << __func__ << ": obc->ssc not available, not returning context" << dendl;
    return ObjectContextRef();   // -ENOENT!
  }

  dout(10) << __func__ << ": " << *obc
	   << " oi: " << obc->obs.oi
	   << " exists: " << (int)obc->obs.exists
	   << " " << *obc->ssc << dendl;
  return obc;
}

void PrimaryLogPG::context_registry_on_change()
{
  pair<hobject_t, ObjectContextRef> i;
  while (object_contexts.get_next(i.first, &i)) {
    ObjectContextRef obc(i.second);
    if (obc) {
      for (map<pair<uint64_t, entity_name_t>, WatchRef>::iterator j =
	     obc->watchers.begin();
	   j != obc->watchers.end();
	   obc->watchers.erase(j++)) {
	j->second->discard();
      }
    }
  }
}


/*
 * If we return an error, and set *pmissing, then promoting that
 * object may help.
 *
 * If we return -EAGAIN, we will always set *pmissing to the missing
 * object to wait for.
 *
 * If we return an error but do not set *pmissing, then we know the
 * object does not exist.
 */
int PrimaryLogPG::find_object_context(const hobject_t& oid,
				      ObjectContextRef *pobc,
				      bool can_create,
				      bool map_snapid_to_clone,
				      hobject_t *pmissing)
{
  FUNCTRACE(cct);
  ceph_assert(oid.pool == static_cast<int64_t>(info.pgid.pool()));
  // want the head?
  if (oid.snap == CEPH_NOSNAP) {
    ObjectContextRef obc = get_object_context(oid, can_create);
    if (!obc) {
      if (pmissing)
        *pmissing = oid;
      return -ENOENT;
    }
    dout(10) << __func__ << " " << oid
       << " @" << oid.snap
       << " oi=" << obc->obs.oi
       << dendl;
    *pobc = obc;

    return 0;
  }

  // we want a snap

  hobject_t head = oid.get_head();
  SnapSetContext *ssc = get_snapset_context(oid, can_create);
  if (!ssc || !(ssc->exists || can_create)) {
    dout(20) << __func__ << " " << oid << " no snapset" << dendl;
    if (pmissing)
      *pmissing = head;  // start by getting the head
    if (ssc)
      put_snapset_context(ssc);
    return -ENOENT;
  }

  if (map_snapid_to_clone) {
    dout(10) << __func__ << " " << oid << " @" << oid.snap
	     << " snapset " << ssc->snapset
	     << " map_snapid_to_clone=true" << dendl;
    if (oid.snap > ssc->snapset.seq) {
      // already must be readable
      ObjectContextRef obc = get_object_context(head, false);
      dout(10) << __func__ << " " << oid << " @" << oid.snap
	       << " snapset " << ssc->snapset
	       << " maps to head" << dendl;
      *pobc = obc;
      put_snapset_context(ssc);
      return (obc && obc->obs.exists) ? 0 : -ENOENT;
    } else {
      vector<snapid_t>::const_iterator citer = std::find(
	ssc->snapset.clones.begin(),
	ssc->snapset.clones.end(),
	oid.snap);
      if (citer == ssc->snapset.clones.end()) {
	dout(10) << __func__ << " " << oid << " @" << oid.snap
		 << " snapset " << ssc->snapset
		 << " maps to nothing" << dendl;
	put_snapset_context(ssc);
	return -ENOENT;
      }

      dout(10) << __func__ << " " << oid << " @" << oid.snap
	       << " snapset " << ssc->snapset
	       << " maps to " << oid << dendl;

      if (recovery_state.get_pg_log().get_missing().is_missing(oid)) {
	dout(10) << __func__ << " " << oid << " @" << oid.snap
		 << " snapset " << ssc->snapset
		 << " " << oid << " is missing" << dendl;
	if (pmissing)
	  *pmissing = oid;
	put_snapset_context(ssc);
	return -EAGAIN;
      }

      ObjectContextRef obc = get_object_context(oid, false);
      if (!obc || !obc->obs.exists) {
	dout(10) << __func__ << " " << oid << " @" << oid.snap
		 << " snapset " << ssc->snapset
		 << " " << oid << " is not present" << dendl;
	if (pmissing)
	  *pmissing = oid;
	put_snapset_context(ssc);
	return -ENOENT;
      }
      dout(10) << __func__ << " " << oid << " @" << oid.snap
	       << " snapset " << ssc->snapset
	       << " " << oid << " HIT" << dendl;
      *pobc = obc;
      put_snapset_context(ssc);
      return 0;
    }
    ceph_abort(); //unreachable
  }

  dout(10) << __func__ << " " << oid << " @" << oid.snap
	   << " snapset " << ssc->snapset << dendl;

  // head?
  if (oid.snap > ssc->snapset.seq) {
    ObjectContextRef obc = get_object_context(head, false);
    dout(10) << __func__ << " " << head
	     << " want " << oid.snap << " > snapset seq " << ssc->snapset.seq
	     << " -- HIT " << obc->obs
	     << dendl;
    if (!obc->ssc)
      obc->ssc = ssc;
    else {
      ceph_assert(ssc == obc->ssc);
      put_snapset_context(ssc);
    }
    *pobc = obc;
    return 0;
  }

  // which clone would it be?
  unsigned k = 0;
  while (k < ssc->snapset.clones.size() &&
	 ssc->snapset.clones[k] < oid.snap)
    k++;
  if (k == ssc->snapset.clones.size()) {
    dout(10) << __func__ << " no clones with last >= oid.snap "
	     << oid.snap << " -- DNE" << dendl;
    put_snapset_context(ssc);
    return -ENOENT;
  }
  hobject_t soid(oid.oid, oid.get_key(), ssc->snapset.clones[k], oid.get_hash(),
		 info.pgid.pool(), oid.get_namespace());

  if (recovery_state.get_pg_log().get_missing().is_missing(soid)) {
    dout(20) << __func__ << " " << soid << " missing, try again later"
	     << dendl;
    if (pmissing)
      *pmissing = soid;
    put_snapset_context(ssc);
    return -EAGAIN;
  }

  ObjectContextRef obc = get_object_context(soid, false);
  if (!obc || !obc->obs.exists) {
    if (pmissing)
      *pmissing = soid;
    put_snapset_context(ssc);
    if (is_primary()) {
      if (is_degraded_or_backfilling_object(soid)) {
	dout(20) << __func__ << " clone is degraded or backfilling " << soid << dendl;
	return -EAGAIN;
      } else if (is_degraded_on_async_recovery_target(soid)) {
	dout(20) << __func__ << " clone is recovering " << soid << dendl;
	return -EAGAIN;
      } else {
	dout(20) << __func__ << " missing clone " << soid << dendl;
	return -ENOENT;
      }
    } else {
      dout(20) << __func__ << " replica missing clone" << soid << dendl;
      return -ENOENT;
    }
  }

  if (!obc->ssc) {
    obc->ssc = ssc;
  } else {
    ceph_assert(obc->ssc == ssc);
    put_snapset_context(ssc);
  }
  ssc = 0;

  // clone
  dout(20) << __func__ << " " << soid
	   << " snapset " << obc->ssc->snapset
	   << dendl;
  auto p = obc->ssc->snapset.clone_snaps.find(soid.snap);
  ceph_assert(p != obc->ssc->snapset.clone_snaps.end());
  if (p->second.empty()) {
    dout(1) << __func__ << " " << soid << " empty snapset -- DNE" << dendl;
    ceph_assert(!cct->_conf->osd_debug_verify_snaps);
    return -ENOENT;
  }
  if (std::find(p->second.begin(), p->second.end(), oid.snap) ==
      p->second.end()) {
    dout(20) << __func__ << " " << soid << " clone_snaps " << p->second
	     << " does not contain " << oid.snap << " -- DNE" << dendl;
    return -ENOENT;
  }
  if (get_osdmap()->in_removed_snaps_queue(info.pgid.pgid.pool(), oid.snap)) {
    dout(20) << __func__ << " " << soid << " snap " << oid.snap
	     << " in removed_snaps_queue" << " -- DNE" << dendl;
    return -ENOENT;
  }
  dout(20) << __func__ << " " << soid << " clone_snaps " << p->second
	   << " contains " << oid.snap << " -- HIT " << obc->obs << dendl;
  *pobc = obc;
  return 0;
}

void PrimaryLogPG::object_context_destructor_callback(ObjectContext *obc)
{
  if (obc->ssc)
    put_snapset_context(obc->ssc);
}

void PrimaryLogPG::add_object_context_to_pg_stat(ObjectContextRef obc, pg_stat_t *pgstat)
{
  object_info_t& oi = obc->obs.oi;

  dout(10) << __func__ << " " << oi.soid << dendl;
  ceph_assert(!oi.soid.is_snapdir());

  object_stat_sum_t stat;
  stat.num_objects++;
  if (oi.is_dirty())
    stat.num_objects_dirty++;
  if (oi.is_whiteout())
    stat.num_whiteouts++;
  if (oi.is_omap())
    stat.num_objects_omap++;
  if (oi.is_cache_pinned())
    stat.num_objects_pinned++;
  if (oi.has_manifest())
    stat.num_objects_manifest++;

  if (oi.soid.is_snap()) {
    stat.num_object_clones++;

    if (!obc->ssc)
      obc->ssc = get_snapset_context(oi.soid, false);
    ceph_assert(obc->ssc);
    stat.num_bytes += obc->ssc->snapset.get_clone_bytes(oi.soid.snap);
  } else {
    stat.num_bytes += oi.size;
  }

  // add it in
  pgstat->stats.sum.add(stat);
}

void PrimaryLogPG::requeue_op_blocked_by_object(const hobject_t &soid) {
  map<hobject_t, list<OpRequestRef>>::iterator p = waiting_for_blocked_object.find(soid);
  if (p != waiting_for_blocked_object.end()) {
    list<OpRequestRef>& ls = p->second;
    dout(10) << __func__ << " " << soid << " requeuing " << ls.size() << " requests" << dendl;
    requeue_ops(ls);
    waiting_for_blocked_object.erase(p);
  }
}

void PrimaryLogPG::kick_object_context_blocked(ObjectContextRef obc)
{
  const hobject_t& soid = obc->obs.oi.soid;
  if (obc->is_blocked()) {
    dout(10) << __func__ << " " << soid << " still blocked" << dendl;
    return;
  }

  requeue_op_blocked_by_object(soid);

  map<hobject_t, ObjectContextRef>::iterator i =
    objects_blocked_on_snap_promotion.find(obc->obs.oi.soid.get_head());
  if (i != objects_blocked_on_snap_promotion.end()) {
    ceph_assert(i->second == obc);
    ObjectContextRef head_obc = get_object_context(i->first, false);
    head_obc->stop_block();
    // kick blocked ops (head)
    requeue_op_blocked_by_object(i->first);
    objects_blocked_on_snap_promotion.erase(i);
  }

  if (obc->requeue_scrub_on_unblock) {

    obc->requeue_scrub_on_unblock = false;

    dout(20) << __func__ << " requeuing if still active: " << (is_active() ? "yes" : "no") << dendl;

    // only requeue if we are still active: we may be unblocking
    // because we are resetting for a new peering interval
    if (is_active()) {
      osd->queue_scrub_unblocking(this, is_scrub_blocking_ops());
    }
  }
}

SnapSetContext *PrimaryLogPG::get_snapset_context(
  const hobject_t& oid,
  bool can_create,
  const map<string, bufferlist, less<>> *attrs,
  bool oid_existed)
{
  std::lock_guard l(snapset_contexts_lock);
  SnapSetContext *ssc;
  map<hobject_t, SnapSetContext*>::iterator p = snapset_contexts.find(
    oid.get_snapdir());
  if (p != snapset_contexts.end()) {
    if (can_create || p->second->exists) {
      ssc = p->second;
    } else {
      return NULL;
    }
  } else {
    bufferlist bv;
    if (!attrs) {
      int r = -ENOENT;
      if (!(oid.is_head() && !oid_existed)) {
	r = pgbackend->objects_get_attr(oid.get_head(), SS_ATTR, &bv);
      }
      if (r < 0 && !can_create)
	return NULL;
    } else {
      auto it_ss = attrs->find(SS_ATTR);
      ceph_assert(it_ss != attrs->end());
      bv = it_ss->second;
    }
    ssc = new SnapSetContext(oid.get_snapdir());
    _register_snapset_context(ssc);
    if (bv.length()) {
      bufferlist::const_iterator bvp = bv.begin();
      try {
	ssc->snapset.decode(bvp);
      } catch (const ceph::buffer::error& e) {
        dout(0) << __func__ << " Can't decode snapset: " << e.what() << dendl;
	return NULL;
      }
      ssc->exists = true;
    } else {
      ssc->exists = false;
    }
  }
  ceph_assert(ssc);
  ssc->ref++;
  return ssc;
}

void PrimaryLogPG::put_snapset_context(SnapSetContext *ssc)
{
  std::lock_guard l(snapset_contexts_lock);
  --ssc->ref;
  if (ssc->ref == 0) {
    if (ssc->registered)
      snapset_contexts.erase(ssc->oid);
    delete ssc;
  }
}

/*
 * Return values:
 *  NONE  - didn't pull anything
 *  YES   - pulled what the caller wanted
 *  HEAD  - needed to pull head first
 */
enum { PULL_NONE, PULL_HEAD, PULL_YES };

int PrimaryLogPG::recover_missing(
  const hobject_t &soid, eversion_t v,
  int priority,
  PGBackend::RecoveryHandle *h)
{
  dout(10) << fmt::format(
		  "{} sar: {}", __func__,
		  m_scrubber->is_after_repair_required())
	   << dendl;

  if (recovery_state.get_missing_loc().is_unfound(soid)) {
    dout(7) << __func__ << " " << soid
	    << " v " << v
	    << " but it is unfound" << dendl;
    return PULL_NONE;
  }

  if (recovery_state.get_missing_loc().is_deleted(soid)) {
    start_recovery_op(soid);
    ceph_assert(!recovering.count(soid));
    recovering.insert(make_pair(soid, ObjectContextRef()));
    epoch_t cur_epoch = get_osdmap_epoch();
    remove_missing_object(soid, v, new LambdaContext(
     [=, this](int) {
       std::scoped_lock locker{*this};
       if (!pg_has_reset_since(cur_epoch)) {
	 bool object_missing = false;
	 for (const auto& shard : get_acting_recovery_backfill()) {
	   if (shard == pg_whoami)
	     continue;
	   if (recovery_state.get_peer_missing(shard).is_missing(soid)) {
	     dout(20) << __func__ << ": soid " << soid << " needs to be deleted from replica " << shard << dendl;
	     object_missing = true;
	     break;
	   }
	 }
	 if (!object_missing) {
	   object_stat_sum_t stat_diff;
	   stat_diff.num_objects_recovered = 1;
	   if (m_scrubber->is_after_repair_required())
	     stat_diff.num_objects_repaired = 1;
	   on_global_recover(soid, stat_diff, true);
	 } else {
	   auto recovery_handle = pgbackend->open_recovery_op();
	   pgbackend->recover_delete_object(soid, v, recovery_handle);
	   pgbackend->run_recovery_op(recovery_handle, priority);
	 }
       }
     }));
    return PULL_YES;
  }

  // is this a snapped object?  if so, consult the snapset.. we may not need the entire object!
  ObjectContextRef obc;
  ObjectContextRef head_obc;
  if (soid.snap && soid.snap < CEPH_NOSNAP) {
    // do we have the head?
    hobject_t head = soid.get_head();
    if (recovery_state.get_pg_log().get_missing().is_missing(head)) {
      if (recovering.count(head)) {
	dout(10) << " missing but already recovering head " << head << dendl;
	return PULL_NONE;
      } else {
	int r = recover_missing(
	  head, recovery_state.get_pg_log().get_missing().get_items().find(head)->second.need, priority,
	  h);
	if (r != PULL_NONE)
	  return PULL_HEAD;
	return PULL_NONE;
      }
    }
    head_obc = get_object_context(
      head,
      false,
      0);
    ceph_assert(head_obc);
  }
  start_recovery_op(soid);
  ceph_assert(!recovering.count(soid));
  recovering.insert(make_pair(soid, obc));
  int r = pgbackend->recover_object(
    soid,
    v,
    head_obc,
    obc,
    h);
  // This is only a pull which shouldn't return an error
  ceph_assert(r >= 0);
  return PULL_YES;
}

void PrimaryLogPG::remove_missing_object(const hobject_t &soid,
					 eversion_t v, Context *on_complete)
{
  dout(20) << __func__ << " " << soid << " " << v << dendl;
  ceph_assert(on_complete != nullptr);
  // delete locally
  ObjectStore::Transaction t;
  remove_snap_mapped_object(t, soid);

  ObjectRecoveryInfo recovery_info;
  recovery_info.soid = soid;
  recovery_info.version = v;

  epoch_t cur_epoch = get_osdmap_epoch();
  t.register_on_complete(new LambdaContext(
     [=, this](int) {
       std::unique_lock locker{*this};
       if (!pg_has_reset_since(cur_epoch)) {
	 ObjectStore::Transaction t2;
	 on_local_recover(soid, recovery_info, ObjectContextRef(), true, &t2);
	 t2.register_on_complete(on_complete);
	 int r = osd->store->queue_transaction(ch, std::move(t2), nullptr);
	 ceph_assert(r == 0);
	 locker.unlock();
       } else {
	 locker.unlock();
	 on_complete->complete(-EAGAIN);
       }
     }));
  int r = osd->store->queue_transaction(ch, std::move(t), nullptr);
  ceph_assert(r == 0);
}

void PrimaryLogPG::finish_degraded_object(const hobject_t oid)
{
  dout(10) << __func__ << " " << oid << dendl;
  if (callbacks_for_degraded_object.count(oid)) {
    list<Context*> contexts;
    contexts.swap(callbacks_for_degraded_object[oid]);
    callbacks_for_degraded_object.erase(oid);
    for (list<Context*>::iterator i = contexts.begin();
	 i != contexts.end();
	 ++i) {
      (*i)->complete(0);
    }
  }
  map<hobject_t, snapid_t>::iterator i = objects_blocked_on_degraded_snap.find(
    oid.get_head());
  if (i != objects_blocked_on_degraded_snap.end() &&
      i->second == oid.snap)
    objects_blocked_on_degraded_snap.erase(i);
}

void PrimaryLogPG::finish_unreadable_object(const hobject_t oid)
{
  dout(10) << __func__ << " " << oid << dendl;
  map<hobject_t, snapid_t>::iterator i = objects_blocked_on_unreadable_snap.find(
    oid.get_head());
  if (i != objects_blocked_on_unreadable_snap.end() &&
      i->second == oid.snap)
    objects_blocked_on_unreadable_snap.erase(i);
}

void PrimaryLogPG::_committed_pushed_object(
  epoch_t epoch, eversion_t last_complete)
{
  std::scoped_lock locker{*this};
  if (!pg_has_reset_since(epoch)) {
    recovery_state.recovery_committed_to(last_complete);
  } else {
    dout(10) << __func__
	     << " pg has changed, not touching last_complete_ondisk" << dendl;
  }
}

void PrimaryLogPG::_applied_recovered_object(ObjectContextRef obc)
{
  dout(20) << __func__ << dendl;
  if (obc) {
    dout(20) << "obc = " << *obc << dendl;
  }
  ceph_assert(active_pushes >= 1);
  --active_pushes;

  // requeue an active chunky scrub waiting on recovery ops
  if (!recovery_state.is_deleting() && active_pushes == 0 &&
      is_scrub_active()) {

    osd->queue_scrub_pushes_update(this, is_scrub_blocking_ops());
  }
}

void PrimaryLogPG::_applied_recovered_object_replica()
{
  dout(20) << __func__ << dendl;
  ceph_assert(active_pushes >= 1);
  --active_pushes;

  // requeue an active scrub waiting on recovery ops
  if (!recovery_state.is_deleting() && active_pushes == 0 &&
      is_scrub_active()) {

    osd->queue_scrub_replica_pushes(this, m_scrubber->replica_op_priority());
  }
}

void PrimaryLogPG::on_failed_pull(
  const set<pg_shard_t> &from,
  const hobject_t &soid,
  const eversion_t &v)
{
  dout(20) << __func__ << ": " << soid << dendl;
  ceph_assert(recovering.count(soid));
  auto obc = recovering[soid];
  if (obc) {
    list<OpRequestRef> blocked_ops;
    obc->drop_recovery_read(&blocked_ops);
    requeue_ops(blocked_ops);
  }
  recovering.erase(soid);
  for (auto&& i : from) {
    if (i != pg_whoami) { // we'll get it below in primary_error
      recovery_state.force_object_missing(i, soid, v);
    }
  }

  dout(0) << __func__ << " " << soid << " from shard " << from
	  << ", reps on " << recovery_state.get_missing_loc().get_locations(soid)
	  << " unfound? " << recovery_state.get_missing_loc().is_unfound(soid)
	  << dendl;
  finish_recovery_op(soid);  // close out this attempt,
  finish_degraded_object(soid);

  if (from.count(pg_whoami)) {
    dout(0) << " primary missing oid " << soid << " version " << v << dendl;
    primary_error(soid, v);
    backfills_in_flight.erase(soid);
  }
}

eversion_t PrimaryLogPG::pick_newest_available(const hobject_t& oid)
{
  eversion_t v;
  pg_missing_item pmi;
  bool is_missing = recovery_state.get_pg_log().get_missing().is_missing(oid, &pmi);
  ceph_assert(is_missing);
  v = pmi.have;
  dout(10) << "pick_newest_available " << oid << " " << v << " on osd." << osd->whoami << " (local)" << dendl;

  ceph_assert(!get_acting_recovery_backfill().empty());
  for (set<pg_shard_t>::iterator i = get_acting_recovery_backfill().begin();
       i != get_acting_recovery_backfill().end();
       ++i) {
    if (*i == get_primary()) continue;
    pg_shard_t peer = *i;
    if (!recovery_state.get_peer_missing(peer).is_missing(oid)) {
      continue;
    }
    eversion_t h = recovery_state.get_peer_missing(peer).get_items().at(oid).have;
    dout(10) << "pick_newest_available " << oid << " " << h << " on osd." << peer << dendl;
    if (h > v)
      v = h;
  }

  dout(10) << "pick_newest_available " << oid << " " << v << " (newest)" << dendl;
  return v;
}

void PrimaryLogPG::do_update_log_missing(OpRequestRef &op)
{
  const MOSDPGUpdateLogMissing *m = static_cast<const MOSDPGUpdateLogMissing*>(
    op->get_req());
  ceph_assert(m->get_type() == MSG_OSD_PG_UPDATE_LOG_MISSING);
  ObjectStore::Transaction t;
  std::optional<eversion_t> op_trim_to, op_pg_committed_to;
  if (m->pg_trim_to != eversion_t())
    op_trim_to = m->pg_trim_to;
  if (m->pg_committed_to != eversion_t())
    op_pg_committed_to = m->pg_committed_to;

  dout(20) << __func__
	   << " op_trim_to = " << op_trim_to << " op_pg_committed_to = "
	   << op_pg_committed_to << dendl;

  recovery_state.append_log_entries_update_missing(
    m->entries, t, op_trim_to, op_pg_committed_to);
  eversion_t new_lcod = info.last_complete;

  Context *complete = new LambdaContext(
    [=, this](int) {
      const MOSDPGUpdateLogMissing *msg = static_cast<const MOSDPGUpdateLogMissing*>(
	op->get_req());
      std::scoped_lock locker{*this};
      if (!pg_has_reset_since(msg->get_epoch())) {
	update_last_complete_ondisk(new_lcod);
	MOSDPGUpdateLogMissingReply *reply =
	  new MOSDPGUpdateLogMissingReply(
	    spg_t(info.pgid.pgid, primary_shard().shard),
	    pg_whoami.shard,
	    msg->get_epoch(),
	    msg->min_epoch,
	    msg->get_tid(),
	    new_lcod);
	reply->set_priority(CEPH_MSG_PRIO_HIGH);
	msg->get_connection()->send_message(reply);
      }
    });

  if (get_osdmap()->require_osd_release >= ceph_release_t::kraken) {
    t.register_on_commit(complete);
  } else {
    /* Hack to work around the fact that ReplicatedBackend sends
     * ack+commit if commit happens first
     *
     * This behavior is no longer necessary, but we preserve it so old
     * primaries can keep their repops in order */
    if (pool.info.is_erasure()) {
      t.register_on_complete(complete);
    } else {
      t.register_on_commit(complete);
    }
  }
  int tr = osd->store->queue_transaction(
    ch,
    std::move(t),
    nullptr);
  ceph_assert(tr == 0);
  op_applied(info.last_update);
}

void PrimaryLogPG::do_update_log_missing_reply(OpRequestRef &op)
{
  const MOSDPGUpdateLogMissingReply *m =
    static_cast<const MOSDPGUpdateLogMissingReply*>(
    op->get_req());
  dout(20) << __func__ << " got reply from "
	   << m->get_from() << dendl;

  auto it = log_entry_update_waiting_on.find(m->get_tid());
  if (it != log_entry_update_waiting_on.end()) {
    if (it->second.waiting_on.count(m->get_from())) {
      it->second.waiting_on.erase(m->get_from());
      if (m->last_complete_ondisk != eversion_t()) {
	update_peer_last_complete_ondisk(m->get_from(), m->last_complete_ondisk);
      }
    } else {
      osd->clog->error()
	<< info.pgid << " got reply "
	<< *m << " from shard we are not waiting for "
	<< m->get_from();
    }

    if (it->second.waiting_on.empty()) {
      repop_all_committed(it->second.repop.get());
      log_entry_update_waiting_on.erase(it);
    }
  } else {
    osd->clog->error()
      << info.pgid << " got reply "
      << *m << " on unknown tid " << m->get_tid();
  }
}

/* Mark all unfound objects as lost.
 */
void PrimaryLogPG::mark_all_unfound_lost(
  int what,
  asok_finisher on_finish)
{
  dout(3) << __func__ << " " << pg_log_entry_t::get_op_name(what) << dendl;
  list<hobject_t> oids;

  dout(30) << __func__ << ": log before:\n";
  recovery_state.get_pg_log().get_log().print(*_dout);
  *_dout << dendl;

  mempool::osd_pglog::list<pg_log_entry_t> log_entries;

  utime_t mtime = ceph_clock_now();
  map<hobject_t, pg_missing_item>::const_iterator m =
    recovery_state.get_missing_loc().get_needs_recovery().begin();
  map<hobject_t, pg_missing_item>::const_iterator mend =
    recovery_state.get_missing_loc().get_needs_recovery().end();

  ObcLockManager manager;
  eversion_t v = get_next_version();
  v.epoch = get_osdmap_epoch();
  uint64_t num_unfound = recovery_state.get_missing_loc().num_unfound();
  while (m != mend) {
    const hobject_t &oid(m->first);
    if (!recovery_state.get_missing_loc().is_unfound(oid)) {
      // We only care about unfound objects
      ++m;
      continue;
    }

    ObjectContextRef obc;
    eversion_t prev;

    switch (what) {
    case pg_log_entry_t::LOST_MARK:
      ceph_abort_msg("actually, not implemented yet!");
      break;

    case pg_log_entry_t::LOST_REVERT:
      prev = pick_newest_available(oid);
      if (prev > eversion_t()) {
	// log it
	pg_log_entry_t e(
	  pg_log_entry_t::LOST_REVERT, oid, v,
	  m->second.need, 0, osd_reqid_t(), mtime, 0);
	e.reverting_to = prev;
	e.mark_unrollbackable();
	log_entries.push_back(e);
	dout(10) << e << dendl;

	// we are now missing the new version; recovery code will sort it out.
	++v.version;
	++m;
	break;
      }

    case pg_log_entry_t::LOST_DELETE:
      {
	pg_log_entry_t e(pg_log_entry_t::LOST_DELETE, oid, v, m->second.need,
			 0, osd_reqid_t(), mtime, 0);
	if (get_osdmap()->require_osd_release >= ceph_release_t::jewel) {
	  if (pool.info.require_rollback()) {
	    e.mod_desc.try_rmobject(v.version);
	  } else {
	    e.mark_unrollbackable();
	  }
	} // otherwise, just do what we used to do
	dout(10) << e << dendl;
	log_entries.push_back(e);
        oids.push_back(oid);

	// If context found mark object as deleted in case
	// of racing with new creation.  This can happen if
	// object lost and EIO at primary.
	obc = object_contexts.lookup(oid);
	if (obc)
	  obc->obs.exists = false;

	++v.version;
	++m;
      }
      break;

    default:
      ceph_abort();
    }
  }

  recovery_state.update_stats(
    [](auto &history, auto &stats) {
      stats.stats_invalid = true;
      return false;
    });

  submit_log_entries(
    log_entries,
    std::move(manager),
    std::optional<std::function<void(void)> >(
      [this, oids, num_unfound, on_finish]() {
	if (recovery_state.perform_deletes_during_peering()) {
	  for (auto oid : oids) {
	    // clear old locations - merge_new_log_entries will have
	    // handled rebuilding missing_loc for each of these
	    // objects if we have the RECOVERY_DELETES flag
	    recovery_state.object_recovered(oid, object_stat_sum_t());
	  }
	}

	if (is_recovery_unfound()) {
	  queue_peering_event(
	    PGPeeringEventRef(
	      std::make_shared<PGPeeringEvent>(
	      get_osdmap_epoch(),
	      get_osdmap_epoch(),
	      PeeringState::DoRecovery())));
	} else if (is_backfill_unfound()) {
	  queue_peering_event(
	    PGPeeringEventRef(
	      std::make_shared<PGPeeringEvent>(
	      get_osdmap_epoch(),
	      get_osdmap_epoch(),
	      PeeringState::RequestBackfill())));
	} else {
	  queue_recovery();
	}

	stringstream ss;
	ss << "pg has " << num_unfound
	   << " objects unfound and apparently lost marking";
	string rs = ss.str();
	dout(0) << "do_command r=" << 0 << " " << rs << dendl;
	osd->clog->info() << rs;
	bufferlist empty;
	on_finish(0, rs, empty);
      }),
    OpRequestRef());
}

void PrimaryLogPG::_split_into(pg_t child_pgid, PG *child, unsigned split_bits)
{
  ceph_assert(repop_queue.empty());
}

/*
 * pg status change notification
 */

void PrimaryLogPG::apply_and_flush_repops(bool requeue)
{
  list<OpRequestRef> rq;

  // apply all repops
  while (!repop_queue.empty()) {
    RepGather *repop = repop_queue.front();
    repop_queue.pop_front();
    dout(10) << " canceling repop tid " << repop->rep_tid << dendl;
    repop->rep_aborted = true;
    repop->on_committed.clear();
    repop->on_success.clear();

    if (requeue) {
      if (repop->op) {
	dout(10) << " requeuing " << *repop->op->get_req() << dendl;
	rq.push_back(repop->op);
	repop->op = OpRequestRef();
      }

      // also requeue any dups, interleaved into position
      auto p = waiting_for_ondisk.find(repop->v);
      if (p != waiting_for_ondisk.end()) {
	dout(10) << " also requeuing ondisk waiters " << p->second << dendl;
	for (auto& i : p->second) {
	  rq.push_back(std::get<0>(i));
	}
	waiting_for_ondisk.erase(p);
      }
    }

    remove_repop(repop);
  }

  ceph_assert(repop_queue.empty());

  if (requeue) {
    requeue_ops(rq);
    if (!waiting_for_ondisk.empty()) {
      for (auto& i : waiting_for_ondisk) {
        for (auto& j : i.second) {
          derr << __func__ << ": op " << *(std::get<0>(j)->get_req())
               << " waiting on " << i.first << dendl;
        }
      }
      ceph_assert(waiting_for_ondisk.empty());
    }
  }

  waiting_for_ondisk.clear();
}

void PrimaryLogPG::on_flushed()
{
  requeue_ops(waiting_for_flush);
  if (!is_peered() || !is_primary()) {
    pair<hobject_t, ObjectContextRef> i;
    while (object_contexts.get_next(i.first, &i)) {
      derr << __func__ << ": object " << i.first << " obc still alive" << dendl;
    }
    ceph_assert(object_contexts.empty());
  }
}

void PrimaryLogPG::on_removal(ObjectStore::Transaction &t)
{
  dout(10) << __func__ << dendl;

  on_shutdown();

  // starting PG deletion, num_objects can be 1
  // do_delete_work will update num_objects
  t.register_on_commit(new C_DeleteMore(this, get_osdmap_epoch(), 1));
}

void PrimaryLogPG::clear_async_reads()
{
  dout(10) << __func__ << dendl;
  for(auto& i : in_progress_async_reads) {
    dout(10) << "clear ctx: "
             << "OpRequestRef " << i.first
             << " OpContext " << i.second
             << dendl;
    close_op_ctx(i.second);
  }
}

void PrimaryLogPG::clear_cache()
{
  object_contexts.clear();
}

void PrimaryLogPG::on_shutdown()
{
  dout(10) << __func__ << dendl;

  if (recovery_queued) {
    recovery_queued = false;
    osd->clear_queued_recovery(this);
  }

  m_scrubber->on_new_interval();

  vector<ceph_tid_t> tids;
  cancel_copy_ops(false, &tids);
  cancel_flush_ops(false, &tids);
  cancel_proxy_ops(false, &tids);
  cancel_manifest_ops(false, &tids);
  cancel_cls_gather_ops(false, &tids);
  osd->objecter->op_cancel(tids, -ECANCELED);

  apply_and_flush_repops(false);
  cancel_log_updates();
  // we must remove PGRefs, so do this this prior to release_backoffs() callers
  clear_backoffs();
  // clean up snap trim references
  snap_trimmer_machine.process_event(Reset());

  pgbackend->on_change();

  context_registry_on_change();
  object_contexts.clear();

  clear_async_reads();

  osd->remote_reserver.cancel_reservation(info.pgid);
  osd->local_reserver.cancel_reservation(info.pgid);

  clear_primary_state();
  cancel_recovery();

  if (is_primary()) {
    osd->clear_ready_to_merge(this);
  }
}

/**
 * PG 完成 activation 后的收尾：唤醒已等待 peering 的请求，
 * 并把后续 recovery/backfill 工作作为新的 peering 事件排队处理。
 */
void PrimaryLogPG::on_activate_complete()
{
  check_local();
  // 如果旧事务的 flush 已完成，waiting_for_peered 中的请求可以重新入队；
  // 否则先转移到 waiting_for_flush，避免新 interval 的请求越过旧事务。
  if (!recovery_state.needs_flush()) {
    requeue_ops(waiting_for_peered);
  } else if (!waiting_for_peered.empty()) {
    dout(10) << __func__ << " flushes in progress, moving "
	     << waiting_for_peered.size()
	     << " items to waiting_for_flush"
	     << dendl;
    ceph_assert(waiting_for_flush.empty());
    waiting_for_flush.swap(waiting_for_peered);
  }
  // activation 只表示副本已完成激活，不代表副本内容已经完全同步。
  // needs_recovery() 检查 PG log/missing 中是否还有需要逐对象恢复的数据。
  if (needs_recovery()) {
    dout(10) << "activate not all replicas are up-to-date, queueing recovery" << dendl;
    // 使用当前 OSDMap epoch 构造事件，并通过 queue_peering_event() 异步交给 OSD peering 工作队列；
    queue_peering_event(
      PGPeeringEventRef(
	std::make_shared<PGPeeringEvent>(
	  get_osdmap_epoch(),
	  get_osdmap_epoch(),
	  PeeringState::DoRecovery())));
  } else if (needs_backfill()) {
    // 没有普通 log-based recovery，但仍有需要补齐的 backfill 区间/对象。
    dout(10) << "activate queueing backfill" << dendl;
    queue_peering_event(
      PGPeeringEventRef(
	std::make_shared<PGPeeringEvent>(
	  get_osdmap_epoch(),
	  get_osdmap_epoch(),
	  PeeringState::RequestBackfill())));
  } else {
    // recovery 和 backfill 都不需要，通知状态机所有副本已经恢复完成。
    dout(10) << "activate all replicas clean, no recovery" << dendl;
    queue_peering_event(
      PGPeeringEventRef(
	std::make_shared<PGPeeringEvent>(
	  get_osdmap_epoch(),
	  get_osdmap_epoch(),
	  PeeringState::AllReplicasRecovered())));
  }

  publish_stats_to_osd();

  // 在 activation 完成、backfill 即将异步启动时，固定本轮 backfill 的初始边界和内部状态。
  // 记录本轮 backfill 的起点，并为后续 backfill 调度标记 new_backfill。
  if (get_backfill_targets().size()) {
    last_backfill_started = recovery_state.earliest_backfill();
    new_backfill = true;
    ceph_assert(!last_backfill_started.is_max());
    dout(5) << __func__ << ": bft=" << get_backfill_targets()
	   << " from " << last_backfill_started << dendl;
    for (set<pg_shard_t>::const_iterator i = get_backfill_targets().begin();
	 i != get_backfill_targets().end();
	 ++i) {
      dout(5) << "target shard " << *i
	     << " from " << recovery_state.get_peer_info(*i).last_backfill
	     << dendl;
    }
  }

  hit_set_setup();
  agent_setup();
}

void PrimaryLogPG::on_change(ObjectStore::Transaction &t)
{
  dout(10) << __func__ << dendl;

  if (hit_set && hit_set->insert_count() == 0) {
    dout(20) << " discarding empty hit_set" << dendl;
    hit_set_clear();
  }

  if (recovery_queued) {
    recovery_queued = false;
    osd->clear_queued_recovery(this);
  }

  // requeue everything in the reverse order they should be
  // reexamined.
  requeue_ops(waiting_for_peered);
  requeue_ops(waiting_for_flush);
  requeue_ops(waiting_for_active);
  requeue_ops(waiting_for_readable);

  vector<ceph_tid_t> tids;
  cancel_copy_ops(is_primary(), &tids);
  cancel_flush_ops(is_primary(), &tids);
  cancel_proxy_ops(is_primary(), &tids);
  cancel_manifest_ops(is_primary(), &tids);
  cancel_cls_gather_ops(is_primary(), &tids);
  osd->objecter->op_cancel(tids, -ECANCELED);

  // requeue object waiters
  for (auto& p : waiting_for_unreadable_object) {
    release_backoffs(p.first);
  }
  if (is_primary()) {
    requeue_object_waiters(waiting_for_unreadable_object);
  } else {
    waiting_for_unreadable_object.clear();
  }
  for (map<hobject_t,list<OpRequestRef>>::iterator p = waiting_for_degraded_object.begin();
       p != waiting_for_degraded_object.end();
       waiting_for_degraded_object.erase(p++)) {
    release_backoffs(p->first);
    if (is_primary())
      requeue_ops(p->second);
    else
      p->second.clear();
    finish_degraded_object(p->first);
  }

  ceph_assert(waiting_for_scrub.empty());

  for (auto p = waiting_for_blocked_object.begin();
       p != waiting_for_blocked_object.end();
       waiting_for_blocked_object.erase(p++)) {
    if (is_primary())
      requeue_ops(p->second);
    else
      p->second.clear();
  }
  for (auto i = callbacks_for_degraded_object.begin();
       i != callbacks_for_degraded_object.end();
    ) {
    finish_degraded_object((i++)->first);
  }
  ceph_assert(callbacks_for_degraded_object.empty());

  if (is_primary()) {
    requeue_ops(waiting_for_cache_not_full);
  } else {
    waiting_for_cache_not_full.clear();
  }
  objects_blocked_on_cache_full.clear();

  for (list<pair<OpRequestRef, OpContext*> >::iterator i =
         in_progress_async_reads.begin();
       i != in_progress_async_reads.end();
       in_progress_async_reads.erase(i++)) {
    close_op_ctx(i->second);
    if (is_primary())
      requeue_op(i->first);
  }

  // this will requeue ops we were working on but didn't finish, and
  // any dups
  apply_and_flush_repops(is_primary());
  cancel_log_updates();

  // do this *after* apply_and_flush_repops so that we catch any newly
  // registered watches.
  context_registry_on_change();

  pgbackend->on_change_cleanup(&t);
  m_scrubber->cleanup_store(&t);
  pgbackend->on_change();

  // clear snap_trimmer state
  snap_trimmer_machine.process_event(Reset());

  debug_op_order.clear();
  unstable_stats.clear();

  // we don't want to cache object_contexts through the interval change
  // NOTE: we actually assert that all currently live references are dead
  // by the time the flush for the next interval completes.
  object_contexts.clear();

  // should have been cleared above by finishing all of the degraded objects
  ceph_assert(objects_blocked_on_degraded_snap.empty());
}

void PrimaryLogPG::plpg_on_role_change()
{
  dout(10) << __func__ << dendl;
  if (get_role() != 0 && hit_set) {
    dout(10) << " clearing hit set" << dendl;
    hit_set_clear();
  }
}

void PrimaryLogPG::plpg_on_pool_change()
{
  dout(10) << __func__ << dendl;
  // requeue cache full waiters just in case the cache_mode is
  // changing away from writeback mode.  note that if we are not
  // active the normal requeuing machinery is sufficient (and properly
  // ordered).
  if (is_active() &&
      pool.info.cache_mode != pg_pool_t::CACHEMODE_WRITEBACK &&
      !waiting_for_cache_not_full.empty()) {
    dout(10) << __func__ << " requeuing full waiters (not in writeback) "
	     << dendl;
    requeue_ops(waiting_for_cache_not_full);
    objects_blocked_on_cache_full.clear();
  }
  hit_set_setup();
  agent_setup();
}

// clear state.  called on recovery completion AND cancellation.
void PrimaryLogPG::_clear_recovery_state()
{
#ifdef DEBUG_RECOVERY_OIDS
  recovering_oids.clear();
#endif
  dout(15) << __func__ << dendl;

  last_backfill_started = hobject_t();
  set<hobject_t>::iterator i = backfills_in_flight.begin();
  while (i != backfills_in_flight.end()) {
    backfills_in_flight.erase(i++);
  }

  list<OpRequestRef> blocked_ops;
  for (map<hobject_t, ObjectContextRef>::iterator i = recovering.begin();
       i != recovering.end();
       recovering.erase(i++)) {
    if (i->second) {
      i->second->drop_recovery_read(&blocked_ops);
      requeue_ops(blocked_ops);
    }
  }
  ceph_assert(backfills_in_flight.empty());
  pending_backfill_updates.clear();
  ceph_assert(recovering.empty());
  pgbackend->clear_recovery_state();
}

void PrimaryLogPG::cancel_pull(const hobject_t &soid)
{
  dout(20) << __func__ << ": " << soid << dendl;
  ceph_assert(recovering.count(soid));
  ObjectContextRef obc = recovering[soid];
  if (obc) {
    list<OpRequestRef> blocked_ops;
    obc->drop_recovery_read(&blocked_ops);
    requeue_ops(blocked_ops);
  }
  recovering.erase(soid);
  finish_recovery_op(soid);
  release_backoffs(soid);
  if (waiting_for_degraded_object.count(soid)) {
    dout(20) << " kicking degraded waiters on " << soid << dendl;
    requeue_ops(waiting_for_degraded_object[soid]);
    waiting_for_degraded_object.erase(soid);
  }
  if (waiting_for_unreadable_object.count(soid)) {
    dout(20) << " kicking unreadable waiters on " << soid << dendl;
    requeue_ops(waiting_for_unreadable_object[soid]);
    waiting_for_unreadable_object.erase(soid);
  }
  if (is_missing_object(soid))
    recovery_state.set_last_requested(0);
  finish_degraded_object(soid);
  finish_unreadable_object(soid);
}

void PrimaryLogPG::check_recovery_sources(const OSDMapRef& osdmap)
{
  pgbackend->check_recovery_sources(osdmap);
}

/**
 * 在一次 recovery 调度配额内选择并启动对象恢复或 backfill 操作；
 * 返回 true 表示仍有 unfound 对象，需要由调用者继续查询其可能位置。
 */
bool PrimaryLogPG::start_recovery_ops(
  uint64_t max,
  ThreadPool::TPHandle &handle,
  uint64_t *ops_started)
{
  // started 记录本轮已经启动的对象操作数量；其上限由 reserved_pushes 决定。
  uint64_t& started = *ops_started;
  started = 0;
  // work_in_progress 表示本轮已启动 recovery/backfill；
  // recovery_started 用于区分 recover_replicas 是否实际产生了工作。
  bool work_in_progress = false;
  bool recovery_started = false;
  // recovery 必须运行在 primary、peered 且未删除的 PG 上。
  ceph_assert(is_primary());
  ceph_assert(is_peered());
  ceph_assert(!recovery_state.is_deleting());

  // 当前 PGRecovery 调度项已经出队，允许后续仍有工作时再次排队。
  ceph_assert(recovery_queued);
  recovery_queued = false;

  // 状态机可能在任务排队后发生转换；若 PG 已不在 recovery/backfill 状态，
  // 本轮任务已经过时，只保留 unfound 查询结果。
  if (!state_test(PG_STATE_RECOVERING) &&
      !state_test(PG_STATE_BACKFILLING)) {
    /* TODO: I think this case is broken and will make do_recovery()
     * unhappy since we're returning false */
    dout(10) << "recovery raced and were queued twice, ignoring!" << dendl;
    return have_unfound();
  }

  const auto &missing = recovery_state.get_pg_log().get_missing();

  // 记录本轮开始前的 unfound 数量，用于后面判断查询是否发现了新的位置。
  uint64_t num_unfound = get_num_unfound();

  if (!recovery_state.have_missing()) {
    // primary 本地没有 missing，标记本地 recovery 已完成，后续可专注于副本同步。
    recovery_state.local_recovery_complete();
  }

  // primary 没有 missing，或 primary 的 missing 全部是 unfound 时，
  // 优先恢复仍可从 primary 获取的副本对象。
  if (!missing.have_missing() ||
      recovery_state.all_missing_unfound()) {
    started = recover_replicas(max, handle, &recovery_started);
  }
  if (!started) {
    // 如果前一步没有启动操作，再尝试从副本取回 primary 缺失的对象。
    started += recover_primary(max, handle);
  }
  if (!started && num_unfound != get_num_unfound()) {
    // unfound 数量发生变化后重新尝试副本恢复，利用刚发现的新位置。
    started = recover_replicas(max, handle, &recovery_started);
  }

  if (started || recovery_started)
    work_in_progress = true;

  bool deferred_backfill = false;
  // 只有当前没有对象 recovery、PG 正在 backfilling、且 primary missing 已清空时，
  // 才能在本轮剩余配额内推进 backfill。
  if (recovering.empty() &&
      state_test(PG_STATE_BACKFILLING) &&
      !get_backfill_targets().empty() && started < max &&
      missing.num_missing() == 0 &&
      waiting_on_backfill.empty()) {
    if (get_osdmap()->test_flag(CEPH_OSDMAP_NOBACKFILL)) {
      // 集群设置 NOBACKFILL，保留任务状态，等待后续重新调度。
      dout(10) << "deferring backfill due to NOBACKFILL" << dendl;
      deferred_backfill = true;
    } else if (get_osdmap()->test_flag(CEPH_OSDMAP_NOREBALANCE) &&
	       !is_degraded())  {
      // 非 degraded PG 受 NOREBALANCE 限制时，暂不执行均衡型 backfill。
      dout(10) << "deferring backfill due to NOREBALANCE" << dendl;
      deferred_backfill = true;
    } else if (!recovery_state.is_backfill_reserved()) {
      /* DNMNOTE I think this branch is dead */
      dout(10) << "deferring backfill due to !backfill_reserved" << dendl;
      if (!backfill_reserving) {
	// 重新进入状态机申请 backfill 资源，避免直接执行未完成 reservation 的 backfill。
	dout(10) << "queueing RequestBackfill" << dendl;
	backfill_reserving = true;
	queue_peering_event(
	  PGPeeringEventRef(
	    std::make_shared<PGPeeringEvent>(
	      get_osdmap_epoch(),
	      get_osdmap_epoch(),
	      PeeringState::RequestBackfill())));
      }
      deferred_backfill = true;
    } else {
      // recovery 已满足前置条件，使用剩余配额执行本轮 backfill。
      started += recover_backfill(max - started, handle, &work_in_progress);
    }
  }

  dout(10) << " started " << started << dendl;
  osd->logger->inc(l_osd_rop, started);

  // 仍有对象在进行、刚启动了工作、已有 active recovery op，或 backfill 被延迟时，
  // 先返回，等待完成回调或下一次调度继续推进。
  if (!recovering.empty() ||
      work_in_progress || recovery_ops_active > 0 || deferred_backfill)
    return !work_in_progress && have_unfound();

  ceph_assert(recovering.empty());
  ceph_assert(recovery_ops_active == 0);

  dout(10) << __func__ << " needs_recovery: "
	   << recovery_state.get_missing_loc().get_needs_recovery()
	   << dendl;
  dout(10) << __func__ << " missing_loc: "
	   << recovery_state.get_missing_loc().get_missing_locs()
	   << dendl;
  int unfound = get_num_unfound();
  if (unfound) {
    // 没有活动 recovery，但仍有 unfound；返回 true 让 OSD 查询更多来源。
    dout(10) << " still have " << unfound << " unfound" << dendl;
    return true;
  }

  if (missing.num_missing() > 0) {
    // this shouldn't happen!
    osd->clog->error() << info.pgid << " Unexpected Error: recovery ending with "
		       << missing.num_missing() << ": " << missing.get_items();
    return false;
  }

  if (needs_recovery()) {
    // this shouldn't happen!
    // We already checked num_missing() so we must have missing replicas
    osd->clog->error() << info.pgid
                       << " Unexpected Error: recovery ending with missing replicas";
    return false;
  }

  if (state_test(PG_STATE_RECOVERING)) {
    // 普通 recovery 已经完成；若仍有 backfill target，转入 backfill，否则通知状态机所有副本已恢复完成
    state_clear(PG_STATE_RECOVERING);
    state_clear(PG_STATE_FORCED_RECOVERY);
    if (needs_backfill()) {
      dout(10) << "recovery done, queuing backfill" << dendl;
      queue_peering_event(
        PGPeeringEventRef(
          std::make_shared<PGPeeringEvent>(
            get_osdmap_epoch(),
            get_osdmap_epoch(),
            PeeringState::RequestBackfill())));
    } else {
      dout(10) << "recovery done, no backfill" << dendl;
      state_clear(PG_STATE_FORCED_BACKFILL);
      queue_peering_event(
        PGPeeringEventRef(
          std::make_shared<PGPeeringEvent>(
            get_osdmap_epoch(),
            get_osdmap_epoch(),
            PeeringState::AllReplicasRecovered())));
    }
  } else { // backfilling
    // backfill 也已完成，通知状态机进入 Recovered。
    state_clear(PG_STATE_BACKFILLING);
    state_clear(PG_STATE_FORCED_BACKFILL);
    state_clear(PG_STATE_FORCED_RECOVERY);
    dout(10) << "recovery done, backfill done" << dendl;
    queue_peering_event(
      PGPeeringEventRef(
        std::make_shared<PGPeeringEvent>(
          get_osdmap_epoch(),
          get_osdmap_epoch(),
          PeeringState::Backfilled())));
  }

  return false;
}

/**
 * 恢复 primary 自己缺失的对象：按 missing 版本顺序选择对象，从已知副本准备 pull 操作；
 * 同时处理 LOST_REVERT 等需要恢复旧版本的特殊日志项。
 */
uint64_t PrimaryLogPG::recover_primary(uint64_t max, ThreadPool::TPHandle &handle)
{
  // 只有 primary 负责选择来源并恢复本地缺失对象。
  ceph_assert(is_primary());

  // primary 本地 PG log 中的 missing 集合，是本函数的恢复候选来源。
  const auto &missing = recovery_state.get_pg_log().get_missing();

  dout(10) << __func__ << " recovering " << recovering.size()
           << " in pg,"
           << " missing " << missing << dendl;

  dout(25) << __func__ << " " << missing.get_items() << dendl;

  // latest 指向该对象的最新 PG log 项；started 受本轮 recovery 配额 max 限制；
  // skipped 用于避免跳过对象后仍错误推进 last_requested。
  pg_log_entry_t *latest = 0;
  unsigned started = 0;
  int skipped = 0;

  // 将本轮多个 pull 操作累计到同一个 backend recovery handle。
  PGBackend::RecoveryHandle *h = pgbackend->open_recovery_op();
  // 从上次成功请求的位置继续按版本顺序扫描，避免每轮都从头遍历 missing 集合。
  map<eversion_t, hobject_t>::const_iterator p =
    missing.get_rmissing().lower_bound(eversion_t(0, recovery_state.get_pg_log().get_log().last_requested));
  while (p != missing.get_rmissing().end()) {
    handle.reset_tp_timeout();
    hobject_t soid;
    eversion_t v = p->first;

    // PG log 中存在该对象时，使用最新日志项中的 soid；否则直接使用 missing 索引的对象。
    auto it_objects = recovery_state.get_pg_log().get_log().objects.find(p->second);
    if (it_objects != recovery_state.get_pg_log().get_log().objects.end()) {
      latest = it_objects->second;
      ceph_assert(latest->is_update() || latest->is_delete());
      soid = latest->soid;
    } else {
      latest = 0;
      soid = p->second;
    }
    // item 记录 primary 当前已有版本 have 与需要恢复到的版本 need。
    const pg_missing_item& item = missing.get_items().find(p->second)->second;
    ++p;

    hobject_t head = soid.get_head();

    eversion_t need = item.need;

    dout(10) << __func__ << " "
             << soid << " " << item.need
	     << (missing.is_missing(soid) ? " (missing)":"")
	     << (missing.is_missing(head) ? " (missing head)":"")
             << (recovering.count(soid) ? " (recovering)":"")
	     << (recovering.count(head) ? " (recovering head)":"")
             << dendl;

    if (latest) {
      switch (latest->op) {
      case pg_log_entry_t::CLONE:
	// 当前没有针对 CLONE 的特殊恢复处理，交给下面的通用 missing recovery 路径。
	/*
	 * Handling for this special case removed for now, until we
	 * can correctly construct an accurate SnapSet from the old
	 * one.
	 */
	break;

      case pg_log_entry_t::LOST_REVERT:
	{
	  // LOST_REVERT 要把对象回退到较早版本：若本地已有目标版本，直接完成本地回退；
	  // 否则先找拥有 reverting_to 版本的副本，作为后续 pull 来源。
	  if (item.have == latest->reverting_to) {
	    ObjectContextRef obc = get_object_context(soid, true);

	    if (obc->obs.oi.version == latest->version) {
	      // 本地已经在执行同一回退，不重复提交事务。
	      dout(10) << " already reverting " << soid << dendl;
	    } else {
	      // 更新对象版本并通过本地事务记录 recovery 完成；
	      // 其 applied/commit 回调继续推进 on_local_recover 和副本确认。
	      dout(10) << " reverting " << soid << " to " << latest->prior_version << dendl;
	      obc->obs.oi.version = latest->version;

	      ObjectStore::Transaction t;
	      bufferlist b2;
	      obc->obs.oi.encode(
		b2,
		get_osdmap()->get_features(CEPH_ENTITY_TYPE_OSD, nullptr));
	      ceph_assert(!pool.info.require_rollback());
	      t.setattr(coll, ghobject_t(soid), OI_ATTR, b2);

	      recovery_state.recover_got(
		soid,
		latest->version,
		false,
		t);

	      ++active_pushes;

	      t.register_on_applied(new C_OSD_AppliedRecoveredObject(this, obc));
	      t.register_on_commit(new C_OSD_CommittedPushedObject(
				     this,
				     get_osdmap_epoch(),
				     info.last_complete));
	      osd->store->queue_transaction(ch, std::move(t));
	      continue;
	    }
	  } else {
	    /*
	     * Pull the old version of the object.  Update missing_loc here to have the location
	     * of the version we want.
	     *
	     * This doesn't use the usual missing_loc paths, but that's okay:
	     *  - if we have it locally, we hit the case above, and go from there.
	     *  - if we don't, we always pass through this case during recovery and set up the location
	     *    properly.
	     *  - this way we don't need to mangle the missing code to be general about needing an old
	     *    version...
	     */
	    // 当前本地没有目标旧版本，挑选副本中 have == reverting_to 的 shard 作为来源。
	    eversion_t alternate_need = latest->reverting_to;
	    dout(10) << " need to pull prior_version " << alternate_need << " for revert " << item << dendl;

	    set<pg_shard_t> good_peers;
	    for (auto p = recovery_state.get_peer_missing().begin();
		 p != recovery_state.get_peer_missing().end();
		 ++p) {
	      if (p->second.is_missing(soid, need) &&
		  p->second.get_items().at(soid).have == alternate_need) {
		good_peers.insert(p->first);
	      }
	    }
	    // 将这些候选来源写入 missing_loc，随后走通用 recover_missing() pull 流程。
	    recovery_state.set_revert_with_targets(
	      soid,
	      good_peers);
	    dout(10) << " will pull " << alternate_need << " or " << need
		     << " from one of "
		     << recovery_state.get_missing_loc().get_locations(soid)
		     << dendl;
	  }
	}
	break;
      }
    }

    // 同一对象或其 head 已在恢复时不能重复启动；snap clone 需要先完成 head 的恢复。
    if (!recovering.count(soid)) {
      if (recovering.count(head)) {
	++skipped;
      } else {
	int r = recover_missing(
	  soid, need, recovery_state.get_recovery_op_priority(), h);
	switch (r) {
	case PULL_YES:
	  // 已为当前对象准备 pull。
	  ++started;
	  break;
	case PULL_HEAD:
	  // 当前 clone 依赖的 head 被启动恢复；它同样占用一个 recovery 配额。
	  ++started;
	case PULL_NONE:
	  // PULL_HEAD 故意贯穿到这里：当前 clone 本身尚未恢复，记录为 skipped。
	  ++skipped;
	  break;
	default:
	  ceph_abort();
	}
	if (started >= max)
	  break;
      }
    }

    // 只有扫描过程中没有跳过对象，才能推进断点，避免下轮遗漏仍未能启动的对象。
    if (!skipped)
      recovery_state.set_last_requested(v.version);
  }

  // 将本轮累计的 pull 操作一次性交给 backend 发送给候选副本。
  pgbackend->run_recovery_op(h, recovery_state.get_recovery_op_priority());
  return started;
}

bool PrimaryLogPG::primary_error(
  const hobject_t& soid, eversion_t v)
{
  recovery_state.force_object_missing(pg_whoami, soid, v);
  bool uhoh = recovery_state.get_missing_loc().is_unfound(soid);
  if (uhoh)
    osd->clog->error() << info.pgid << " missing primary copy of "
		       << soid << ", unfound";
  else
    osd->clog->error() << info.pgid << " missing primary copy of "
		       << soid
		       << ", will try copies on "
		       << recovery_state.get_missing_loc().get_locations(soid);
  return uhoh;
}

int PrimaryLogPG::prep_object_replica_deletes(
  const hobject_t& soid, eversion_t v,
  PGBackend::RecoveryHandle *h,
  bool *work_started)
{
  ceph_assert(is_primary());
  dout(10) << __func__ << ": on " << soid << dendl;

  ObjectContextRef obc = get_object_context(soid, false);
  if (obc) {
    if (!obc->get_recovery_read()) {
      dout(20) << "replica delete delayed on " << soid
	       << "; could not get rw_manager lock" << dendl;
      *work_started = true;
      return 0;
    } else {
      dout(20) << "replica delete got recovery read lock on " << soid
	       << dendl;
    }
  }

  start_recovery_op(soid);
  ceph_assert(!recovering.count(soid));
  if (!obc)
    recovering.insert(make_pair(soid, ObjectContextRef()));
  else
    recovering.insert(make_pair(soid, obc));

  pgbackend->recover_delete_object(soid, v, h);
  return 1;
}

int PrimaryLogPG::prep_object_replica_pushes(
  const hobject_t& soid, eversion_t v,
  PGBackend::RecoveryHandle *h,
  bool *work_started)
{
  ceph_assert(is_primary());
  dout(10) << __func__ << ": on " << soid << dendl;

  if (soid.snap && soid.snap < CEPH_NOSNAP) {
    // do we have the head and/or snapdir?
    hobject_t head = soid.get_head();
    if (recovery_state.get_pg_log().get_missing().is_missing(head)) {
      if (recovering.count(head)) {
	dout(10) << " missing but already recovering head " << head << dendl;
	return 0;
      } else {
	int r = recover_missing(
	    head, recovery_state.get_pg_log().get_missing().get_items().find(head)->second.need,
	    recovery_state.get_recovery_op_priority(), h);
	if (r != PULL_NONE)
	  return 1;
	return 0;
      }
    }
  }

  // NOTE: we know we will get a valid oloc off of disk here.
  ObjectContextRef obc = get_object_context(soid, false);
  if (!obc) {
    primary_error(soid, v);
    return 0;
  }

  if (!obc->get_recovery_read()) {
    dout(20) << "recovery delayed on " << soid
	     << "; could not get rw_manager lock" << dendl;
    *work_started = true;
    return 0;
  } else {
    dout(20) << "recovery got recovery read lock on " << soid
	     << dendl;
  }

  start_recovery_op(soid);
  ceph_assert(!recovering.count(soid));
  recovering.insert(make_pair(soid, obc));

  int r = pgbackend->recover_object(
    soid,
    v,
    ObjectContextRef(),
    obc, // has snapset context
    h);
  if (r < 0) {
    dout(0) << __func__ << " Error " << r << " on oid " << soid << dendl;
    on_failed_pull({ pg_whoami }, soid, v);
    return 0;
  }
  return 1;
}

/**
 * 恢复 acting 集中仍缺少对象的副本：从副本 missing 集合中选择对象，
 * 为删除对象准备 delete 操作，为普通对象准备从 primary push 的 recovery 操作。
 */
uint64_t PrimaryLogPG::recover_replicas(uint64_t max, ThreadPool::TPHandle &handle,
  bool *work_started)
{
  dout(10) << __func__ << "(" << max << ")" << dendl;
  // started 记录本轮已准备的副本 recovery 操作数量。
  uint64_t started = 0;

  // 打开一次 backend recovery handle，汇总本轮准备的多个对象操作。
  PGBackend::RecoveryHandle *h = pgbackend->open_recovery_op();

  // 当前排序策略并非全局最优，但优先处理 missing 较少的副本，可以更快地让它恢复到正常状态。
  ceph_assert(!get_acting_recovery_backfill().empty());
  std::vector<std::pair<unsigned int, pg_shard_t>> replicas_by_num_missing,
    async_by_num_missing;
  // 为普通 acting 副本和 async recovery target 分开收集待恢复副本。
  replicas_by_num_missing.reserve(get_acting_recovery_backfill().size() - 1);
  for (auto &p: get_acting_recovery_backfill()) {
    if (p == get_primary()) {
      continue;
    }
    auto pm = recovery_state.get_peer_missing().find(p);
    ceph_assert(pm != recovery_state.get_peer_missing().end());
    auto nm = pm->second.num_missing();
    if (nm != 0) {
      // 只处理确实有 missing 对象的副本；async target 放到第二组，稍后处理。
      if (is_async_recovery_target(p)) {
        async_by_num_missing.push_back(make_pair(nm, p));
      } else {
        replicas_by_num_missing.push_back(make_pair(nm, p));
      }
    }
  }
  // 每组都按 missing 对象数量升序排列，优先恢复缺口最小的副本。
  auto func = [](const std::pair<unsigned int, pg_shard_t> &lhs,
                 const std::pair<unsigned int, pg_shard_t> &rhs) {
    return lhs.first < rhs.first;
  };
  // 普通 acting 副本优先于 async recovery target。
  std::sort(replicas_by_num_missing.begin(), replicas_by_num_missing.end(), func);
  // then async_recovery_targets
  std::sort(async_by_num_missing.begin(), async_by_num_missing.end(), func);
  replicas_by_num_missing.insert(replicas_by_num_missing.end(),
    async_by_num_missing.begin(), async_by_num_missing.end());
  for (auto &replica: replicas_by_num_missing) {
    pg_shard_t &peer = replica.second;
    ceph_assert(peer != get_primary());
    auto pm = recovery_state.get_peer_missing().find(peer);
    ceph_assert(pm != recovery_state.get_peer_missing().end());
    size_t m_sz = pm->second.num_missing();

    dout(10) << " peer osd." << peer << " missing " << m_sz << " objects." << dendl;
    // This log statement is verbose - a PG with 8000 missing objects can
    // generate 1M+ character log entries and several GB of log output per recovery
    dout(25) << " peer osd." << peer << " missing " << pm->second.get_items() << dendl;

    // 同一副本内按最早版本优先处理 missing 对象。
    const pg_missing_t &m(pm->second);
    for (map<eversion_t, hobject_t>::const_iterator p = m.get_rmissing().begin();
	 p != m.get_rmissing().end() && started < max;
	   ++p) {
      handle.reset_tp_timeout();
      const hobject_t soid(p->second);

      if (recovery_state.get_missing_loc().is_unfound(soid)) {
	// primary 也找不到该对象时，暂时不能向副本推送它。
	dout(10) << __func__ << ": " << soid << " still unfound" << dendl;
	continue;
      }

      const pg_info_t &pi = recovery_state.get_peer_info(peer);
      if (soid > pi.last_backfill) {
	// 该对象属于 backfill 区间，不能走普通 replica recovery；
	// recovering 集合中应已有对应的 backfill 记录。
	if (!recovering.count(soid)) {
          derr << __func__ << ": object " << soid << " last_backfill "
	       << pi.last_backfill << dendl;
	  derr << __func__ << ": object added to missing set for backfill, but "
	       << "is not in recovering, error!" << dendl;
	  ceph_abort();
	}
	continue;
      }

      if (recovering.count(soid)) {
	// 对象已经有 recovery 操作在进行，避免重复准备。
	dout(10) << __func__ << ": already recovering " << soid << dendl;
	continue;
      }

      if (recovery_state.get_missing_loc().is_deleted(soid)) {
	// 对象在 primary 侧已被判定为删除，向副本准备删除操作而不是 push 数据。
	dout(10) << __func__ << ": " << soid << " is a delete, removing" << dendl;
	map<hobject_t,pg_missing_item>::const_iterator r = m.get_items().find(soid);
	started += prep_object_replica_deletes(soid, r->second.need, h, work_started);
	continue;
      }

      if (soid.is_snap() &&
	  recovery_state.get_pg_log().get_missing().is_missing(
	    soid.get_head())) {
	// snap clone 依赖的 head 仍在 primary missing 中，先等待 head 恢复。
	dout(10) << __func__ << ": " << soid.get_head()
		 << " still missing on primary" << dendl;
	continue;
      }

      if (recovery_state.get_pg_log().get_missing().is_missing(soid)) {
	// primary 自己仍缺该对象，当前不能把它作为数据源推送给副本。
	dout(10) << __func__ << ": " << soid << " still missing on primary" << dendl;
	continue;
      }

      dout(10) << __func__ << ": recover_object_replicas(" << soid << ")" << dendl;
      map<hobject_t,pg_missing_item>::const_iterator r = m.get_items().find(soid);
      // 为副本准备从 primary 推送对象的 recovery 操作。
      started += prep_object_replica_pushes(soid, r->second.need, h, work_started);
    }
  }

  // 将本轮累计的 delete/push 操作一次性交给 backend 执行。
  pgbackend->run_recovery_op(h, recovery_state.get_recovery_op_priority());
  return started;
}

hobject_t PrimaryLogPG::earliest_peer_backfill() const
{
  hobject_t e = hobject_t::get_max();
  for (const pg_shard_t& peer : get_backfill_targets()) {
    const auto iter = peer_backfill_info.find(peer);
    ceph_assert(iter != peer_backfill_info.end());
    e = std::min(e, iter->second.begin);
  }
  return e;
}

bool PrimaryLogPG::all_peer_done() const
{
  // Primary hasn't got any more objects
  ceph_assert(backfill_info.empty());

  for (const pg_shard_t& bt : get_backfill_targets()) {
    const auto piter = peer_backfill_info.find(bt);
    ceph_assert(piter != peer_backfill_info.end());
    const ReplicaBackfillInterval& pbi = piter->second;
    // See if peer has more to process
    if (!pbi.extends_to_end() || !pbi.empty())
	return false;
  }
  return true;
}

/**
 * recover_backfill
 *
 * Invariants:
 *
 * backfilled: fully pushed to replica or present in replica's missing set (both
 * our copy and theirs).
 *
 * All objects on a backfill_target in
 * [MIN,peer_backfill_info[backfill_target].begin) are valid; logically-removed
 * objects have been actually deleted and all logically-valid objects are replicated.
 * There may be PG objects in this interval yet to be backfilled.
 *
 * All objects in PG in [MIN,backfill_info.begin) have been backfilled to all
 * backfill_targets.  There may be objects on backfill_target(s) yet to be deleted.
 *
 * For a backfill target, all objects < std::min(peer_backfill_info[target].begin,
 *     backfill_info.begin) in PG are backfilled.  No deleted objects in this
 * interval remain on the backfill target.
 *
 * For a backfill target, all objects <= peer_info[target].last_backfill
 * have been backfilled to target
 *
 * There *MAY* be missing/outdated objects between last_backfill_started and
 * std::min(peer_backfill_info[*].begin, backfill_info.begin) in the event that client
 * io created objects since the last scan.  For this reason, we call
 * update_range() again before continuing backfill.
 *
 * primary 扫描本地与各个 backfill target 的对象区间：
 * 副本多出的对象发送删除请求，primary 或副本版本较新的对象则准备 push；
 * 只有越过所有 in-flight 操作的连续前缀才会推进各副本的 last_backfill。
 */
uint64_t PrimaryLogPG::recover_backfill(
  uint64_t max,
  ThreadPool::TPHandle &handle, bool *work_started)
{
  dout(10) << __func__ << " (" << max << ")"
           << " bft=" << get_backfill_targets()
	   << " last_backfill_started " << last_backfill_started
	   << (new_backfill ? " new_backfill":"")
	   << dendl;
  ceph_assert(!get_backfill_targets().empty());

  // Initialize from prior backfill state
  // new_backfill 在 activation 完成时被设为 true。因此首次进入 recover_backfill() 时，执行一次
  if (new_backfill) {
    // on_activate() was called prior to getting here
    // 确认本轮起点仍是所有 backfill target 中最落后的 last_backfill。
    ceph_assert(last_backfill_started == recovery_state.earliest_backfill());
    new_backfill = false;

    // 每个 target 从其已确认的 last_backfill 开始扫描；本地也从本轮起点开始。
    for (set<pg_shard_t>::const_iterator i = get_backfill_targets().begin();
	 i != get_backfill_targets().end();
	 ++i) {
      peer_backfill_info[*i].reset(
	recovery_state.get_peer_info(*i).last_backfill);
    }
    backfill_info.reset(last_backfill_started);

    // 新一轮不继承先前批次的在途对象和待提交统计。
    backfills_in_flight.clear();
    pending_backfill_updates.clear();
  }

  for (set<pg_shard_t>::const_iterator i = get_backfill_targets().begin();
       i != get_backfill_targets().end();
       ++i) {
    dout(10) << "peer osd." << *i
	   << " info " << recovery_state.get_peer_info(*i)
	   << " interval " << peer_backfill_info[*i].begin
	   << "-" << peer_backfill_info[*i].end
	   << " " << peer_backfill_info[*i].objects.size() << " objects"
	   << dendl;
  }

  // 每轮重新扫描本地区间，纳入上轮扫描后客户端 I/O 新增或修改的对象。
  backfill_info.begin = last_backfill_started;
  update_range(&backfill_info, handle);  // 把 primary 这一侧“待对账的对象区间”更新成当前可信的对象清单。

  unsigned ops = 0;
  vector<boost::tuple<hobject_t, eversion_t, pg_shard_t> > to_remove;
  set<hobject_t> add_to_stat;

  // 已确认的前缀无需重复比较，收缩各侧区间到本轮尚未完成的位置。
  for (set<pg_shard_t>::const_iterator i = get_backfill_targets().begin();
       i != get_backfill_targets().end();
       ++i) {
    peer_backfill_info[*i].trim_to(
      std::max(
	recovery_state.get_peer_info(*i).last_backfill,
	last_backfill_started));
  }
  backfill_info.trim_to(last_backfill_started);

  // 将本轮准备出的多次 push 汇总到同一个 backend recovery handle。
  PGBackend::RecoveryHandle *h = pgbackend->open_recovery_op();
  // recover_backfill() 不能一次扫完整个 PG，否则一个很大的 PG 会长期占用 recovery 工作线程和 recovery reservation。
  // 它每次最多启动 max 个需要等待完成的 backfill 操作，然后返回，后续由调度/完成回调再次进入。
  // 一次扫描并参与对比的对象区间大小
  //  由配置控制：
  //  osd_backfill_scan_min = 64
  //  osd_backfill_scan_max = 512
  while (ops < max) {
    // 重置起点，继续对比
    if (backfill_info.begin <= earliest_peer_backfill() &&
	!backfill_info.extends_to_end() && backfill_info.empty()) {
      hobject_t next = backfill_info.end;
      backfill_info.reset(next);
      backfill_info.end = hobject_t::get_max();
      update_range(&backfill_info, handle);
      backfill_info.trim();
    }

    dout(20) << "   my backfill interval " << backfill_info << dendl;

    bool sent_scan = false;
    for (set<pg_shard_t>::const_iterator i = get_backfill_targets().begin();
	 i != get_backfill_targets().end();
	 ++i) {
      pg_shard_t bt = *i;
      ReplicaBackfillInterval& pbi = peer_backfill_info[bt];

      dout(20) << " peer shard " << bt << " backfill " << pbi << dendl;
      // target 的已知区间耗尽时，先异步请求它继续扫描；结果回来前不能比较新范围。
      if (pbi.begin <= backfill_info.begin &&
	  !pbi.extends_to_end() && pbi.empty()) {
	dout(10) << " scanning peer osd." << bt << " from " << pbi.end << dendl;
	epoch_t e = get_osdmap_epoch();
	MOSDPGScan *m = new MOSDPGScan(
	  MOSDPGScan::OP_SCAN_GET_DIGEST, pg_whoami, e, get_last_peering_reset(),
	  spg_t(info.pgid.pgid, bt.shard),
	  pbi.end, hobject_t());

	if (cct->_conf->osd_op_queue == "mclock_scheduler") {
	  /* This guard preserves legacy WeightedPriorityQueue behavior for
	   * now, but should be removed after Reef */
	  m->set_priority(recovery_state.get_recovery_op_priority());
	}
	osd->send_message_osd_cluster(bt.osd, m, get_osdmap_epoch());
	ceph_assert(waiting_on_backfill.find(bt) == waiting_on_backfill.end());
	waiting_on_backfill.insert(bt);
        sent_scan = true;
      }
    }

    // 同轮发往多个 target 的 scan 共同占用一个 recovery 配额，并等待回复推进区间。
    if (sent_scan) {
      ops++;
      start_recovery_op(hobject_t::get_max()); // XXX: was pbi.end
      break;
    }

    if (backfill_info.empty() && all_peer_done()) {
      dout(10) << " reached end for both local and all peers" << dendl;
      break;
    }

    // 选择 primary/targets 中最靠前的对象位置，并找出需要对此对象执行操作的 target。
    hobject_t check = earliest_peer_backfill();

    if (check < backfill_info.begin) {
      // 某个 target 当前最靠前的待对账对象比 primary 当前最靠前的待对账对象还小
      // 该对象只出现在 target 区间：primary 已无此对象，应从对应副本删除。

      set<pg_shard_t> check_targets;  // 哪些 target 当前正停在这个多余对象 check 上
      for (set<pg_shard_t>::const_iterator i = get_backfill_targets().begin();
	   i != get_backfill_targets().end();
	   ++i) {
        pg_shard_t bt = *i;
        ReplicaBackfillInterval& pbi = peer_backfill_info[bt];
        if (pbi.begin == check)
          check_targets.insert(bt);
      }
      ceph_assert(!check_targets.empty());

      dout(20) << " BACKFILL removing " << check
	       << " from peers " << check_targets << dendl;
      for (set<pg_shard_t>::iterator i = check_targets.begin();
	   i != check_targets.end();
	   ++i) {
        // 记录待删除请求
        pg_shard_t bt = *i;
        ReplicaBackfillInterval& pbi = peer_backfill_info[bt];
        ceph_assert(pbi.begin == check);

        to_remove.push_back(boost::make_tuple(check, pbi.objects.begin()->second, bt));
        pbi.pop_front();
      }

      // 删除请求不等待 recovery-op 回复，但扫描游标可以继续向前比较。
      last_backfill_started = check;

      // Don't increment ops here because deletions
      // are cheap and not replied to unlike real recovery_ops,
      // and we can't increment ops without requeueing ourself
      // for recovery.
    } else {
      // primary 当前有对象：判断哪些 target 需要它

      // 从 primary 的 backfill_info 中，取出当前第一个对象 hoid 的全部版本记录，整理成后续比较/推送需要的两个结果。
      auto it = backfill_info.objects.begin();
      const hobject_t& hoid = it->first;  // 当前要处理的对象名
      eversion_t obj_v;  // 该对象的最高版本，后续 push 时使用
      std::map<shard_id_t,eversion_t> versions;  // 该对象在不同 shard 上应有的版本
      // 副本池通常只有一条：
      //  object B -> NO_SHARD: 10'5
      //  EC 部分写时可能有多条：
      //  object B -> NO_SHARD: 10'5
      //  object B -> shard 1: 10'3
      while (it != backfill_info.objects.end() && it->first == hoid) {
	obj_v = std::max(obj_v, it->second.second);
	versions[it->second.first] = it->second.second;
	++it;
      }
      // 将 target 分为版本不符、缺对象、版本正确、尚未追到比较线四类。
      vector<pg_shard_t> need_ver_targs, missing_targs, keep_ver_targs, skip_targs;
      for (set<pg_shard_t>::const_iterator i = get_backfill_targets().begin();
	   i != get_backfill_targets().end();
	   ++i) {
	pg_shard_t bt = *i;
	ReplicaBackfillInterval& pbi = peer_backfill_info[bt];
	if (check == backfill_info.begin && check == pbi.begin) {
    // 两侧游标都指向该对象时，直接比较 target 已报告的版本。
	  eversion_t replicaobj_v;
	  if (versions.contains(bt.shard)) {
	    replicaobj_v = versions.at(bt.shard);
	  } else {
	    replicaobj_v = versions.at(shard_id_t::NO_SHARD);
	  }
	  if (pbi.objects.begin()->second != replicaobj_v) {
	    need_ver_targs.push_back(bt);
	  } else {
	    keep_ver_targs.push_back(bt);
	  }
        } else {
	  const pg_info_t& pinfo = recovery_state.get_peer_info(bt);

          // 仅对已追到该位置的 target 判定“缺对象”；
          // 游标还在后面的 target 尚未完成扫描，不能据此认为它缺少此对象。
          if (backfill_info.begin > pinfo.last_backfill)
	    missing_targs.push_back(bt);
	  else
	    skip_targs.push_back(bt);
	}
      }

      if (!keep_ver_targs.empty()) {
        // These peers have version obj_v
	dout(20) << " BACKFILL keeping " << check
		 << " with ver " << obj_v
		 << " on peers " << keep_ver_targs << dendl;
	//ceph_assert(!waiting_for_degraded_object.count(check));
      }
      if (!need_ver_targs.empty() || !missing_targs.empty()) {
	ObjectContextRef obc = get_object_context(backfill_info.begin, false);
	ceph_assert(obc);
	if (obc->get_recovery_read()) {
	  if (!need_ver_targs.empty()) {
	    dout(20) << " BACKFILL replacing " << check
		   << " with ver " << obj_v
		   << " to peers " << need_ver_targs << dendl;
	  }
	  if (!missing_targs.empty()) {
	    dout(20) << " BACKFILL pushing " << backfill_info.begin
	         << " with ver " << obj_v
	         << " to peers " << missing_targs << dendl;
	  }
          // 对版本不符和缺对象的 target 发送同一次对象 push。
          vector<pg_shard_t> all_push = need_ver_targs;
	  all_push.insert(all_push.end(), missing_targs.begin(), missing_targs.end());

	  handle.reset_tp_timeout();
	  int r = prep_backfill_object_push(backfill_info.begin, obj_v, obc, all_push, h);
	  if (r < 0) {
	    *work_started = true;
	    dout(0) << __func__ << " Error " << r << " trying to backfill " << backfill_info.begin << dendl;
	    break;
	  }
          // 该对象的 push 将异步完成；其完成回调才会允许 last_backfill 跨过此位置。
          ops++;
	} else {
	  *work_started = true;
	  dout(20) << "backfill blocking on " << backfill_info.begin
		   << "; could not get rw_manager lock" << dendl;
	  break;
	}
      }
      dout(20) << "need_ver_targs=" << need_ver_targs
	       << " keep_ver_targs=" << keep_ver_targs << dendl;
      dout(20) << "backfill_targets=" << get_backfill_targets()
	       << " missing_targs=" << missing_targs
	       << " skip_targs=" << skip_targs << dendl;

      // 在“当前对象已经完成对账决策后”，推进内存中的扫描游标，但不代表对象已经真正 backfill 完成。
      last_backfill_started = backfill_info.begin;
      add_to_stat.insert(backfill_info.begin); // XXX: Only one for all pushes?
      backfill_info.pop_front();
      vector<pg_shard_t> check_targets = need_ver_targs;
      check_targets.insert(check_targets.end(), keep_ver_targs.begin(), keep_ver_targs.end());
      for (vector<pg_shard_t>::iterator i = check_targets.begin();
	   i != check_targets.end();
	   ++i) {
        pg_shard_t bt = *i;
        ReplicaBackfillInterval& pbi = peer_backfill_info[bt];
        pbi.pop_front();
      }
    }
  }

  // 统计与 last_backfill 必须随对象实际完成推进，因此先保存在 pending 表中。
  for (set<hobject_t>::iterator i = add_to_stat.begin();
       i != add_to_stat.end();
       ++i) {
    ObjectContextRef obc = get_object_context(*i, false);
    ceph_assert(obc);
    pg_stat_t stat;
    add_object_context_to_pg_stat(obc, &stat);
    pending_backfill_updates[*i] = stat;
  }
  // 将同一 target 的多个删除项合并为一条 BackfillRemove 消息。
  map<pg_shard_t,MOSDPGBackfillRemove*> reqs;
  for (unsigned i = 0; i < to_remove.size(); ++i) {
    handle.reset_tp_timeout();
    const hobject_t& oid = to_remove[i].get<0>();
    eversion_t v = to_remove[i].get<1>();
    pg_shard_t peer = to_remove[i].get<2>();
    MOSDPGBackfillRemove *m;
    auto it = reqs.find(peer);
    if (it != reqs.end()) {
      m = it->second;
    } else {
      m = reqs[peer] = new MOSDPGBackfillRemove(
	spg_t(info.pgid.pgid, peer.shard),
	get_osdmap_epoch());
      if (cct->_conf->osd_op_queue == "mclock_scheduler") {
	/* This guard preserves legacy WeightedPriorityQueue behavior for
	   * now, but should be removed after Reef */
	m->set_priority(recovery_state.get_recovery_op_priority());
      }
    }
    m->ls.push_back(make_pair(oid, v));

    if (oid <= last_backfill_started)
      pending_backfill_updates[oid]; // add empty stat!
  }
  for (auto p : reqs) {
    osd->send_message_osd_cluster(p.first.osd, p.second,
				  get_osdmap_epoch());
  }

  // 提交本轮累计的对象 push；实际数据传输和完成通知在 backend 中异步进行。
  pgbackend->run_recovery_op(h, recovery_state.get_recovery_op_priority());


  hobject_t backfill_pos =
    std::min(backfill_info.begin, earliest_peer_backfill());
  dout(5) << "backfill_pos is " << backfill_pos << dendl;
  for (set<hobject_t>::iterator i = backfills_in_flight.begin();
       i != backfills_in_flight.end();
       ++i) {
    dout(20) << *i << " is still in flight" << dendl;
  }

  // 在途对象之前的连续区间才可确认完成，不能跨越最靠前的 in-flight 对象推进游标。
  // backfills_in_flight 已经发起对象 push、但尚未完成全局 recovery 的 backfill 对象。
  // pending_backfill_updates 已经处理到、但还不能正式计入 target PG 统计的对象
  hobject_t next_backfill_to_complete = backfills_in_flight.empty() ?
    backfill_pos : *(backfills_in_flight.begin());
  hobject_t new_last_backfill = recovery_state.earliest_backfill();
  dout(10) << "starting new_last_backfill at " << new_last_backfill << dendl;
  for (map<hobject_t, pg_stat_t>::iterator i =
	 pending_backfill_updates.begin();
       i != pending_backfill_updates.end() &&
	 i->first < next_backfill_to_complete;
       pending_backfill_updates.erase(i++)) {
    dout(20) << " pending_backfill_update " << i->first << dendl;
    ceph_assert(i->first > new_last_backfill);
    // 该项来自已结束的先前批次；现在可以把对象统计并入 peer 的完成前缀。
    recovery_state.update_complete_backfill_object_stats(
      i->first,
      i->second);
    new_last_backfill = i->first;
  }
  dout(10) << "possible new_last_backfill at " << new_last_backfill << dendl;

  ceph_assert(!pending_backfill_updates.empty() ||
	 new_last_backfill == last_backfill_started);
  if (pending_backfill_updates.empty() &&
      backfill_pos.is_max()) {
    ceph_assert(backfills_in_flight.empty());
    new_last_backfill = backfill_pos;
    last_backfill_started = backfill_pos;
  }
  dout(10) << "final new_last_backfill at " << new_last_backfill << dendl;

  // 若 new_last_backfill 到达 MAX，向 target 发送 OP_BACKFILL_FINISH；否则发送进度更新。
  // If new_last_backfill == MAX, then we will send OP_BACKFILL_FINISH to
  // all the backfill targets.  Otherwise, we will move last_backfill up on
  // those targets need it and send OP_BACKFILL_PROGRESS to them.
  for (set<pg_shard_t>::const_iterator i = get_backfill_targets().begin();
       i != get_backfill_targets().end();
       ++i) {
    pg_shard_t bt = *i;
    const pg_info_t& pinfo = recovery_state.get_peer_info(bt);

    if (new_last_backfill > pinfo.last_backfill) {
      // 先更新内存中的 peer_info，再把新的进度和统计同步给对应 target。
      recovery_state.update_peer_last_backfill(bt, new_last_backfill);
      epoch_t e = get_osdmap_epoch();
      MOSDPGBackfill *m = NULL;
      if (pinfo.last_backfill.is_max()) {
        m = new MOSDPGBackfill(
	  MOSDPGBackfill::OP_BACKFILL_FINISH,
	  e,
	  get_last_peering_reset(),
	  spg_t(info.pgid.pgid, bt.shard));
        // Use default priority here, must match sub_op priority
        start_recovery_op(hobject_t::get_max());
      } else {
        m = new MOSDPGBackfill(
	  MOSDPGBackfill::OP_BACKFILL_PROGRESS,
	  e,
	  get_last_peering_reset(),
	  spg_t(info.pgid.pgid, bt.shard));
        // Use default priority here, must match sub_op priority
      }
      m->last_backfill = pinfo.last_backfill;
      m->stats = pinfo.stats;

      if (cct->_conf->osd_op_queue == "mclock_scheduler") {
	/* This guard preserves legacy WeightedPriorityQueue behavior for
	 * now, but should be removed after Reef */
	m->set_priority(recovery_state.get_recovery_op_priority());
      }

      osd->send_message_osd_cluster(bt.osd, m, get_osdmap_epoch());
      dout(10) << " peer " << bt
	       << " num_objects now " << pinfo.stats.stats.sum.num_objects
	       << " / " << info.stats.stats.sum.num_objects << dendl;
    }
  }

  if (ops)
    *work_started = true;
  return ops;
}

/**
 * 为一个对象准备 backfill push：记录对象和目标副本的在途状态，
 * 更新 recovery missing 信息，并把实际数据恢复请求交给 PGBackend。
 */
int PrimaryLogPG::prep_backfill_object_push(
  hobject_t oid, eversion_t v,
  ObjectContextRef obc,
  vector<pg_shard_t> peers,
  PGBackend::RecoveryHandle *h)
{
  dout(10) << __func__ << " " << oid << " v " << v << " to peers " << peers << dendl;
  ceph_assert(!peers.empty());

  // 在对象真正完成 push 前，将其作为 backfill 在途对象记录下来，阻止完成边界越过它。
  backfills_in_flight.insert(oid);
  // 记录该对象的目标版本以及需要接收它的 target，供 recovery 完成回调更新状态。
  recovery_state.prepare_backfill_for_missing(oid, v, peers);

  ceph_assert(!recovering.count(oid));

  // 为对象建立一个 active recovery 操作，并保存其上下文，便于完成或失败时收尾。
  start_recovery_op(oid);
  recovering.insert(make_pair(oid, obc));

  // backfill 时 primary 持有对象 obc，因此 backend 会准备向 peers 推送对象数据。
  int r = pgbackend->recover_object(
    oid,
    v,
    ObjectContextRef(),
    obc,
    h);
  if (r < 0) {
    // backend 准备失败时立即走失败路径，清理本次 recovery 的状态。
    dout(0) << __func__ << " Error " << r << " on oid " << oid << dendl;
    on_failed_pull({ pg_whoami }, oid, v);
  }
  return r;
}

void PrimaryLogPG::update_range(
  PrimaryBackfillInterval *bi,
  ThreadPool::TPHandle &handle)
{
  int local_min = cct->_conf->osd_backfill_scan_min;
  int local_max = cct->_conf->osd_backfill_scan_max;
  const std::set<pg_shard_t>& backfill_targets = get_backfill_targets();

  if (bi->version < info.log_tail) {
    dout(10) << __func__<< ": bi is old, rescanning local backfill_info"
	     << dendl;
    bi->version = info.last_update;
    scan_range_primary(local_min, local_max, bi, handle, backfill_targets);
  }

  if (bi->version >= projected_last_update) {
    dout(10) << __func__<< ": bi is current " << dendl;
    ceph_assert(bi->version == projected_last_update);
  } else if (bi->version >= info.log_tail) {
    if (recovery_state.get_pg_log().get_log().empty() && projected_log.empty()) {
      /* Because we don't move log_tail on split, the log might be
       * empty even if log_tail != last_update.  However, the only
       * way to get here with an empty log is if log_tail is actually
       * eversion_t(), because otherwise the entry which changed
       * last_update since the last scan would have to be present.
       */
      ceph_assert(bi->version == eversion_t());
      return;
    }

    dout(10) << __func__<< ": bi is old, (" << bi->version
	     << ") can be updated with log to projected_last_update "
	     << projected_last_update << dendl;

    auto func = [&](const pg_log_entry_t &e) {
      dout(10) << __func__ << ": updating from version " << e.version
               << dendl;
      const hobject_t &soid = e.soid;
      if (soid >= bi->begin &&
	  soid < bi->end) {
	if (e.is_update()) {
	  dout(10) << __func__ << ": " << e.soid << " updated to version "
		   << e.version << dendl;
	  if (e.written_shards.empty()) {
	    // Log entry updates all shards, replace all entries for e.soid
	    bi->objects.erase(e.soid);
	    bi->objects.insert(make_pair(e.soid,
					 make_pair(shard_id_t::NO_SHARD,
						   e.version)));
	  } else {
	    // Update backfill interval for shards modified by log entry
	    std::map<shard_id_t,eversion_t> versions;
	    // Create map from existing entries in backfill entry
	    const auto & [begin, end] = bi->objects.equal_range(e.soid);
	    for (const auto & entry : std::ranges::subrange(begin, end)) {
	      const auto & [shard, version] = entry.second;
	      versions[shard] = version;
	    }
	    // Update entries in map that are modified by log entry
	    bool uses_default = false;
	    for (const auto & shard : backfill_targets) {
	      if (e.is_written_shard(shard.shard)) {
		versions.erase(shard.shard);
		uses_default = true;
	      } else {
		if (!versions.contains(shard.shard)) {
		  versions[shard.shard] = e.prior_version;
		}
		//Else: keep existing version
	      }
	    }
	    if (uses_default) {
	      versions[shard_id_t::NO_SHARD] = e.version;
	    } else {
	      versions.erase(shard_id_t::NO_SHARD);
	    }
	    // Erase and recreate backfill interval for e.soid using map
	    bi->objects.erase(e.soid);
	    for (auto & [shard, version] : versions) {
	      bi->objects.insert(make_pair(e.soid, make_pair(shard, version)));
	    }
	  }
	} else if (e.is_delete()) {
	  dout(10) << __func__ << ": " << e.soid << " removed" << dendl;
	  bi->objects.erase(e.soid); // Erase all entries for e.soid
	}
      }
    };
    dout(10) << "scanning pg log first" << dendl;
    recovery_state.get_pg_log().get_log().scan_log_after(bi->version, func);
    dout(10) << "scanning projected log" << dendl;
    projected_log.scan_log_after(bi->version, func);
    bi->version = projected_last_update;
  } else {
    ceph_abort_msg("scan_range_primary should have raised bi->version past log_tail");
  }
}

void PrimaryLogPG::scan_range_primary(
  int min, int max, PrimaryBackfillInterval *bi,
  ThreadPool::TPHandle &handle,
  const std::set<pg_shard_t> &backfill_targets)
{
  ceph_assert(is_locked());
  dout(10) << "scan_range_primary from " << bi->begin <<
              " backfill_targets " << backfill_targets << dendl;
  bi->clear_objects();

  vector<hobject_t> ls;
  ls.reserve(max);
  int r = pgbackend->objects_list_partial(bi->begin, min, max, &ls, &bi->end);
  ceph_assert(r >= 0);
  dout(10) << " got " << ls.size() << " items, next " << bi->end << dendl;
  dout(20) << ls << dendl;

  for (vector<hobject_t>::iterator p = ls.begin(); p != ls.end(); ++p) {
    handle.reset_tp_timeout();

    ceph_assert(is_primary());

    eversion_t version;
    std::map<shard_id_t,eversion_t> shard_versions;
    ObjectContextRef obc = object_contexts.lookup(*p);

    if (obc) {
      if (!obc->obs.exists) {
	/* If the object does not exist here, it must have been removed
	 * between the collection_list_partial and here.  This can happen
	 * for the first item in the range, which is usually last_backfill.
	 */
	continue;
      }
      version = obc->obs.oi.version;
      shard_versions = obc->obs.oi.shard_versions;
    } else {
      bufferlist bl;
      int r = pgbackend->objects_get_attr(*p, OI_ATTR, &bl);
      /* If the object does not exist here, it must have been removed
       * between the collection_list_partial and here.  This can happen
       * for the first item in the range, which is usually last_backfill.
       */
      if (r == -ENOENT)
	continue;

      ceph_assert(r >= 0);
      object_info_t oi(bl);
      version = oi.version;
      shard_versions = oi.shard_versions;
    }
    dout(20) << "  " << *p << " " << version << dendl;
    if (shard_versions.empty()) {
      bi->objects.insert(make_pair(*p, std::make_pair(shard_id_t::NO_SHARD,
						      version)));
    } else {
      bool added_default = false;
      for (auto & shard: backfill_targets) {
	if (shard_versions.contains(shard.shard)) {
	  auto shard_version = shard_versions.at(shard.shard);
	  bi->objects.insert(make_pair(*p, std::make_pair(shard.shard,
							  shard_version)));
	} else if (!added_default) {
	  bi->objects.insert(make_pair(*p, std::make_pair(shard_id_t::NO_SHARD,
							  version)));
	  added_default = true;
	}
      }
    }
  }
}

void PrimaryLogPG::scan_range_replica(
  int min, int max, ReplicaBackfillInterval *bi,
  ThreadPool::TPHandle &handle)
{
  ceph_assert(is_locked());
  dout(10) << "scan_range_replica from " << bi->begin << dendl;
  bi->clear_objects();

  vector<hobject_t> ls;
  ls.reserve(max);
  int r = pgbackend->objects_list_partial(bi->begin, min, max, &ls, &bi->end);
  ceph_assert(r >= 0);
  dout(10) << " got " << ls.size() << " items, next " << bi->end << dendl;
  dout(20) << ls << dendl;

  for (vector<hobject_t>::iterator p = ls.begin(); p != ls.end(); ++p) {
    handle.reset_tp_timeout();

    ceph_assert(!is_primary());
    bufferlist bl;
    int r = pgbackend->objects_get_attr(*p, OI_ATTR, &bl);
    /* If the object does not exist here, it must have been removed
     * between the collection_list_partial and here.  This can happen
     * for the first item in the range, which is usually last_backfill.
     */
    if (r == -ENOENT)
      continue;

    ceph_assert(r >= 0);
    object_info_t oi(bl);
    bi->objects[*p] = oi.version;
    dout(20) << "  " << *p << " " << oi.version << dendl;
  }
}

/** check_local
 *
 * verifies that stray objects have been deleted
 */
void PrimaryLogPG::check_local()
{
  dout(10) << __func__ << dendl;

  ceph_assert(
    info.last_update >=
    recovery_state.get_pg_log().get_tail());  // otherwise we need some help!

  if (!cct->_conf->osd_debug_verify_stray_on_activate)
    return;

  // just scan the log.
  set<hobject_t> did;
  for (list<pg_log_entry_t>::const_reverse_iterator p = recovery_state.get_pg_log().get_log().log.rbegin();
       p != recovery_state.get_pg_log().get_log().log.rend();
       ++p) {
    if (did.count(p->soid))
      continue;
    did.insert(p->soid);

    if (p->is_delete() && !is_missing_object(p->soid)) {
      dout(10) << " checking " << p->soid
	       << " at " << p->version << dendl;
      struct stat st;
      int r = osd->store->stat(
	ch,
	ghobject_t(p->soid, ghobject_t::NO_GEN, pg_whoami.shard),
	&st);
      if (r != -ENOENT) {
	derr << __func__ << " " << p->soid << " exists, but should have been "
	     << "deleted" << dendl;
	ceph_abort_msg("erroneously present object");
      }
    } else {
      // ignore old(+missing) objects
    }
  }
}



// ===========================
// hit sets

hobject_t PrimaryLogPG::get_hit_set_current_object(utime_t stamp)
{
  ostringstream ss;
  ss << "hit_set_" << info.pgid.pgid << "_current_" << stamp;
  hobject_t hoid(sobject_t(ss.str(), CEPH_NOSNAP), "",
		 info.pgid.ps(), info.pgid.pool(),
		 cct->_conf->osd_hit_set_namespace);
  dout(20) << __func__ << " " << hoid << dendl;
  return hoid;
}

hobject_t PrimaryLogPG::get_hit_set_archive_object(utime_t start,
						   utime_t end,
						   bool using_gmt)
{
  ostringstream ss;
  ss << "hit_set_" << info.pgid.pgid << "_archive_";
  if (using_gmt) {
    start.gmtime(ss, true /* legacy pre-octopus form */) << "_";
    end.gmtime(ss, true /* legacy pre-octopus form */);
  } else {
    start.localtime(ss, true /* legacy pre-octopus form */) << "_";
    end.localtime(ss, true /* legacy pre-octopus form */);
  }
  hobject_t hoid(sobject_t(ss.str(), CEPH_NOSNAP), "",
		 info.pgid.ps(), info.pgid.pool(),
		 cct->_conf->osd_hit_set_namespace);
  dout(20) << __func__ << " " << hoid << dendl;
  return hoid;
}

void PrimaryLogPG::hit_set_clear()
{
  dout(20) << __func__ << dendl;
  hit_set.reset();
  hit_set_start_stamp = utime_t();
}

void PrimaryLogPG::hit_set_setup()
{
  if (!is_active() ||
      !is_primary()) {
    hit_set_clear();
    return;
  }

  if (is_active() && is_primary() &&
      (!pool.info.hit_set_count ||
       !pool.info.hit_set_period ||
       pool.info.hit_set_params.get_type() == HitSet::TYPE_NONE)) {
    hit_set_clear();

    // only primary is allowed to remove all the hit set objects
    hit_set_remove_all();
    return;
  }

  // FIXME: discard any previous data for now
  hit_set_create();

  // include any writes we know about from the pg log.  this doesn't
  // capture reads, but it is better than nothing!
  hit_set_apply_log();
}

void PrimaryLogPG::hit_set_remove_all()
{
  // If any archives are degraded we skip this
  for (auto p = info.hit_set.history.begin();
       p != info.hit_set.history.end();
       ++p) {
    hobject_t aoid = get_hit_set_archive_object(p->begin, p->end, p->using_gmt);

    // Once we hit a degraded object just skip
    if (is_degraded_or_backfilling_object(aoid))
      return;
    if (m_scrubber->write_blocked_by_scrub(aoid))
      return;
  }

  if (!info.hit_set.history.empty()) {
    auto p = info.hit_set.history.rbegin();
    ceph_assert(p != info.hit_set.history.rend());
    hobject_t oid = get_hit_set_archive_object(p->begin, p->end, p->using_gmt);
    ceph_assert(!is_degraded_or_backfilling_object(oid));
    ObjectContextRef obc = get_object_context(oid, false);
    ceph_assert(obc);

    OpContextUPtr ctx = simple_opc_create(obc);
    ctx->at_version = get_next_version();
    ctx->updated_hset_history = info.hit_set;
    utime_t now = ceph_clock_now();
    ctx->mtime = now;
    hit_set_trim(ctx, 0);
    simple_opc_submit(std::move(ctx));
  }

  recovery_state.update_hset(pg_hit_set_history_t());
  if (agent_state) {
    agent_state->discard_hit_sets();
  }
}

void PrimaryLogPG::hit_set_create()
{
  utime_t now = ceph_clock_now();
  // make a copy of the params to modify
  HitSet::Params params(pool.info.hit_set_params);

  dout(20) << __func__ << " " << params << dendl;
  if (pool.info.hit_set_params.get_type() == HitSet::TYPE_BLOOM) {
    BloomHitSet::Params *p =
      static_cast<BloomHitSet::Params*>(params.impl.get());

    // convert false positive rate so it holds up across the full period
    p->set_fpp(p->get_fpp() / pool.info.hit_set_count);
    if (p->get_fpp() <= 0.0)
      p->set_fpp(.01);  // fpp cannot be zero!

    // if we don't have specified size, estimate target size based on the
    // previous bin!
    if (p->target_size == 0 && hit_set) {
      utime_t dur = now - hit_set_start_stamp;
      unsigned unique = hit_set->approx_unique_insert_count();
      dout(20) << __func__ << " previous set had approx " << unique
	       << " unique items over " << dur << " seconds" << dendl;
      p->target_size = (double)unique * (double)pool.info.hit_set_period
		     / (double)dur;
    }
    if (p->target_size <
	static_cast<uint64_t>(cct->_conf->osd_hit_set_min_size))
      p->target_size = cct->_conf->osd_hit_set_min_size;

    if (p->target_size
	> static_cast<uint64_t>(cct->_conf->osd_hit_set_max_size))
      p->target_size = cct->_conf->osd_hit_set_max_size;

    p->seed = now.sec();

    dout(10) << __func__ << " target_size " << p->target_size
	     << " fpp " << p->get_fpp() << dendl;
  }
  hit_set.reset(new HitSet(params));
  hit_set_start_stamp = now;
}

/**
 * apply log entries to set
 *
 * this would only happen after peering, to at least capture writes
 * during an interval that was potentially lost.
 */
bool PrimaryLogPG::hit_set_apply_log()
{
  if (!hit_set)
    return false;

  eversion_t to = info.last_update;
  eversion_t from = info.hit_set.current_last_update;
  if (to <= from) {
    dout(20) << __func__ << " no update" << dendl;
    return false;
  }

  dout(20) << __func__ << " " << to << " .. " << info.last_update << dendl;
  list<pg_log_entry_t>::const_reverse_iterator p =
    recovery_state.get_pg_log().get_log().log.rbegin();
  while (p != recovery_state.get_pg_log().get_log().log.rend() && p->version > to)
    ++p;
  while (p != recovery_state.get_pg_log().get_log().log.rend() && p->version > from) {
    hit_set->insert(p->soid);
    ++p;
  }

  return true;
}

void PrimaryLogPG::hit_set_persist()
{
  dout(10) << __func__  << dendl;
  bufferlist bl;
  unsigned max = pool.info.hit_set_count;

  utime_t now = ceph_clock_now();
  hobject_t oid;

  // If any archives are degraded we skip this persist request
  // account for the additional entry being added below
  for (auto p = info.hit_set.history.begin();
       p != info.hit_set.history.end();
       ++p) {
    hobject_t aoid = get_hit_set_archive_object(p->begin, p->end, p->using_gmt);

    // Once we hit a degraded object just skip further trim
    if (is_degraded_or_backfilling_object(aoid))
      return;
    if (m_scrubber->write_blocked_by_scrub(aoid))
      return;
  }

  // If backfill is in progress and we could possibly overlap with the
  // hit_set_* objects, back off.  Since these all have
  // hobject_t::hash set to pgid.ps(), and those sort first, we can
  // look just at that.  This is necessary because our transactions
  // may include a modify of the new hit_set *and* a delete of the
  // old one, and this may span the backfill boundary.
  for (set<pg_shard_t>::const_iterator p = get_backfill_targets().begin();
       p != get_backfill_targets().end();
       ++p) {
    const pg_info_t& pi = recovery_state.get_peer_info(*p);
    if (pi.last_backfill == hobject_t() ||
	pi.last_backfill.get_hash() == info.pgid.ps()) {
      dout(10) << __func__ << " backfill target osd." << *p
	       << " last_backfill has not progressed past pgid ps"
	       << dendl;
      return;
    }
  }


  pg_hit_set_info_t new_hset = pg_hit_set_info_t(pool.info.use_gmt_hitset);
  new_hset.begin = hit_set_start_stamp;
  new_hset.end = now;
  oid = get_hit_set_archive_object(
    new_hset.begin,
    new_hset.end,
    new_hset.using_gmt);

  // If the current object is degraded we skip this persist request
  if (m_scrubber->write_blocked_by_scrub(oid))
    return;

  hit_set->seal();
  encode(*hit_set, bl);
  dout(20) << __func__ << " archive " << oid << dendl;

  if (agent_state) {
    agent_state->add_hit_set(new_hset.begin, hit_set);
    uint32_t size = agent_state->hit_set_map.size();
    if (size >= pool.info.hit_set_count) {
      size = pool.info.hit_set_count > 0 ? pool.info.hit_set_count - 1: 0;
    }
    hit_set_in_memory_trim(size);
  }

  ObjectContextRef obc = get_object_context(oid, true);
  OpContextUPtr ctx = simple_opc_create(obc);

  ctx->at_version = get_next_version();
  ctx->updated_hset_history = info.hit_set;
  pg_hit_set_history_t &updated_hit_set_hist = *(ctx->updated_hset_history);

  updated_hit_set_hist.current_last_update = info.last_update;
  new_hset.version = ctx->at_version;

  updated_hit_set_hist.history.push_back(new_hset);
  hit_set_create();

  // fabricate an object_info_t and SnapSet
  obc->obs.oi.version = ctx->at_version;
  obc->obs.oi.mtime = now;
  obc->obs.oi.size = bl.length();
  obc->obs.exists = true;
  obc->obs.oi.set_data_digest(bl.crc32c(-1));

  ctx->new_obs = obc->obs;

  ctx->new_snapset = obc->ssc->snapset;

  ctx->delta_stats.num_objects++;
  ctx->delta_stats.num_objects_hit_set_archive++;

  ctx->delta_stats.num_bytes += bl.length();
  ctx->delta_stats.num_bytes_hit_set_archive += bl.length();

  bufferlist bss;
  encode(ctx->new_snapset, bss);
  bufferlist boi(sizeof(ctx->new_obs.oi));
  encode(ctx->new_obs.oi, boi,
	   get_osdmap()->get_features(CEPH_ENTITY_TYPE_OSD, nullptr));

  ctx->op_t->create(oid);
  if (bl.length()) {
    ctx->op_t->write(oid, 0, bl.length(), bl, 0);
    write_update_size_and_usage(ctx->delta_stats, obc->obs.oi, ctx->modified_ranges,
        0, bl.length());
    ctx->clean_regions.mark_data_region_dirty(0, bl.length());
  }
  map<string, bufferlist, std::less<>> attrs = {
    {OI_ATTR, std::move(boi)},
    {SS_ATTR, std::move(bss)}
  };
  setattrs_maybe_cache(ctx->obc, ctx->op_t.get(), attrs);
  ctx->log.push_back(
    pg_log_entry_t(
      pg_log_entry_t::MODIFY,
      oid,
      ctx->at_version,
      eversion_t(),
      0,
      osd_reqid_t(),
      ctx->mtime,
      0)
    );
  ctx->log.back().clean_regions = ctx->clean_regions;

  hit_set_trim(ctx, max);

  simple_opc_submit(std::move(ctx));
}

void PrimaryLogPG::hit_set_trim(OpContextUPtr &ctx, unsigned max)
{
  ceph_assert(ctx->updated_hset_history);
  pg_hit_set_history_t &updated_hit_set_hist =
    *(ctx->updated_hset_history);
  for (unsigned num = updated_hit_set_hist.history.size(); num > max; --num) {
    list<pg_hit_set_info_t>::iterator p = updated_hit_set_hist.history.begin();
    ceph_assert(p != updated_hit_set_hist.history.end());
    hobject_t oid = get_hit_set_archive_object(p->begin, p->end, p->using_gmt);

    ceph_assert(!is_degraded_or_backfilling_object(oid));

    dout(20) << __func__ << " removing " << oid << dendl;
    ++ctx->at_version.version;
    ctx->log.push_back(
        pg_log_entry_t(pg_log_entry_t::DELETE,
		       oid,
		       ctx->at_version,
		       p->version,
		       0,
		       osd_reqid_t(),
		       ctx->mtime,
		       0));

    ctx->op_t->remove(oid);
    updated_hit_set_hist.history.pop_front();

    ObjectContextRef obc = get_object_context(oid, false);
    ceph_assert(obc);
    --ctx->delta_stats.num_objects;
    --ctx->delta_stats.num_objects_hit_set_archive;
    ctx->delta_stats.num_bytes -= obc->obs.oi.size;
    ctx->delta_stats.num_bytes_hit_set_archive -= obc->obs.oi.size;
  }
}

void PrimaryLogPG::hit_set_in_memory_trim(uint32_t max_in_memory)
{
  while (agent_state->hit_set_map.size() > max_in_memory) {
    agent_state->remove_oldest_hit_set();
  }
}


// =======================================
// cache agent

void PrimaryLogPG::agent_setup()
{
  ceph_assert(is_locked());
  if (!is_active() ||
      !is_primary() ||
      state_test(PG_STATE_PREMERGE) ||
      pool.info.cache_mode == pg_pool_t::CACHEMODE_NONE ||
      pool.info.tier_of < 0 ||
      !get_osdmap()->have_pg_pool(pool.info.tier_of)) {
    agent_clear();
    return;
  }
  if (!agent_state) {
    agent_state.reset(new TierAgentState);

    // choose random starting position
    agent_state->position = hobject_t();
    agent_state->position.pool = info.pgid.pool();
    agent_state->position.set_hash(pool.info.get_random_pg_position(
      info.pgid.pgid,
      rand()));
    agent_state->start = agent_state->position;

    dout(10) << __func__ << " allocated new state, position "
	     << agent_state->position << dendl;
  } else {
    dout(10) << __func__ << " keeping existing state" << dendl;
  }

  if (info.stats.stats_invalid) {
    osd->clog->warn() << "pg " << info.pgid << " has invalid (post-split) stats; must scrub before tier agent can activate";
  }

  agent_choose_mode();
}

void PrimaryLogPG::agent_clear()
{
  agent_stop();
  agent_state.reset(NULL);
}

// Return false if no objects operated on since start of object hash space
bool PrimaryLogPG::agent_work(int start_max, int agent_flush_quota)
{
  std::scoped_lock locker{*this};
  if (!agent_state) {
    dout(10) << __func__ << " no agent state, stopping" << dendl;
    return true;
  }

  ceph_assert(!recovery_state.is_deleting());

  if (agent_state->is_idle()) {
    dout(10) << __func__ << " idle, stopping" << dendl;
    return true;
  }

  osd->logger->inc(l_osd_agent_wake);

  dout(10) << __func__
	   << " max " << start_max
	   << ", flush " << agent_state->get_flush_mode_name()
	   << ", evict " << agent_state->get_evict_mode_name()
	   << ", pos " << agent_state->position
	   << dendl;
  ceph_assert(is_primary());
  ceph_assert(is_active());

  agent_load_hit_sets();

  const pg_pool_t *base_pool = get_osdmap()->get_pg_pool(pool.info.tier_of);
  ceph_assert(base_pool);

  int ls_min = 1;
  int ls_max = cct->_conf->osd_pool_default_cache_max_evict_check_size;

  // list some objects.  this conveniently lists clones (oldest to
  // newest) before heads... the same order we want to flush in.
  //
  // NOTE: do not flush the Sequencer.  we will assume that the
  // listing we get back is imprecise.
  vector<hobject_t> ls;
  hobject_t next;
  int r = pgbackend->objects_list_partial(agent_state->position, ls_min, ls_max,
					  &ls, &next);
  ceph_assert(r >= 0);
  dout(20) << __func__ << " got " << ls.size() << " objects" << dendl;
  int started = 0;
  for (vector<hobject_t>::iterator p = ls.begin();
       p != ls.end();
       ++p) {
    if (p->nspace == cct->_conf->osd_hit_set_namespace) {
      dout(20) << __func__ << " skip (hit set) " << *p << dendl;
      osd->logger->inc(l_osd_agent_skip);
      continue;
    }
    if (is_degraded_or_backfilling_object(*p)) {
      dout(20) << __func__ << " skip (degraded) " << *p << dendl;
      osd->logger->inc(l_osd_agent_skip);
      continue;
    }
    if (is_missing_object(p->get_head())) {
      dout(20) << __func__ << " skip (missing head) " << *p << dendl;
      osd->logger->inc(l_osd_agent_skip);
      continue;
    }
    ObjectContextRef obc = get_object_context(*p, false, NULL);
    if (!obc) {
      // we didn't flush; we may miss something here.
      dout(20) << __func__ << " skip (no obc) " << *p << dendl;
      osd->logger->inc(l_osd_agent_skip);
      continue;
    }
    if (!obc->obs.exists) {
      dout(20) << __func__ << " skip (dne) " << obc->obs.oi.soid << dendl;
      osd->logger->inc(l_osd_agent_skip);
      continue;
    }
    if (m_scrubber->range_intersects_scrub(obc->obs.oi.soid,
			       obc->obs.oi.soid.get_head())) {
      dout(20) << __func__ << " skip (scrubbing) " << obc->obs.oi << dendl;
      osd->logger->inc(l_osd_agent_skip);
      continue;
    }
    if (obc->is_blocked()) {
      dout(20) << __func__ << " skip (blocked) " << obc->obs.oi << dendl;
      osd->logger->inc(l_osd_agent_skip);
      continue;
    }
    if (obc->is_request_pending()) {
      dout(20) << __func__ << " skip (request pending) " << obc->obs.oi << dendl;
      osd->logger->inc(l_osd_agent_skip);
      continue;
    }

    // be careful flushing omap to an EC pool.
    if (!base_pool->supports_omap() &&
	obc->obs.oi.is_omap()) {
      dout(20) << __func__ << " skip (omap to EC) " << obc->obs.oi << dendl;
      osd->logger->inc(l_osd_agent_skip);
      continue;
    }

    if (agent_state->evict_mode != TierAgentState::EVICT_MODE_IDLE &&
	agent_maybe_evict(obc, false))
      ++started;
    else if (agent_state->flush_mode != TierAgentState::FLUSH_MODE_IDLE &&
             agent_flush_quota > 0 && agent_maybe_flush(obc)) {
      ++started;
      --agent_flush_quota;
    }
    if (started >= start_max) {
      // If finishing early, set "next" to the next object
      if (++p != ls.end())
	next = *p;
      break;
    }
  }

  if (++agent_state->hist_age > cct->_conf->osd_agent_hist_halflife) {
    dout(20) << __func__ << " resetting atime and temp histograms" << dendl;
    agent_state->hist_age = 0;
    agent_state->temp_hist.decay();
  }

  // Total objects operated on so far
  int total_started = agent_state->started + started;
  bool need_delay = false;

  dout(20) << __func__ << " start pos " << agent_state->position
    << " next start pos " << next
    << " started " << total_started << dendl;

  // See if we've made a full pass over the object hash space
  // This might check at most ls_max objects a second time to notice that
  // we've checked every objects at least once.
  if (agent_state->position < agent_state->start &&
      next >= agent_state->start) {
    dout(20) << __func__ << " wrap around " << agent_state->start << dendl;
    if (total_started == 0)
      need_delay = true;
    else
      total_started = 0;
    agent_state->start = next;
  }
  agent_state->started = total_started;

  // See if we are starting from beginning
  if (next.is_max())
    agent_state->position = hobject_t();
  else
    agent_state->position = next;

  // Discard old in memory HitSets
  hit_set_in_memory_trim(pool.info.hit_set_count);

  if (need_delay) {
    ceph_assert(agent_state->delaying == false);
    agent_delay();
    return false;
  }
  agent_choose_mode();
  return true;
}

void PrimaryLogPG::agent_load_hit_sets()
{
  if (agent_state->evict_mode == TierAgentState::EVICT_MODE_IDLE) {
    return;
  }

  if (agent_state->hit_set_map.size() < info.hit_set.history.size()) {
    dout(10) << __func__ << dendl;
    for (auto p = info.hit_set.history.begin();
	 p != info.hit_set.history.end(); ++p) {
      if (agent_state->hit_set_map.count(p->begin.sec()) == 0) {
	dout(10) << __func__ << " loading " << p->begin << "-"
		 << p->end << dendl;
	if (!pool.info.is_replicated()) {
	  // FIXME: EC not supported here yet
	  derr << __func__ << " on non-replicated pool" << dendl;
	  break;
	}

	hobject_t oid = get_hit_set_archive_object(p->begin, p->end, p->using_gmt);
	if (is_unreadable_object(oid)) {
	  dout(10) << __func__ << " unreadable " << oid << ", waiting" << dendl;
	  break;
	}

	ObjectContextRef obc = get_object_context(oid, false);
	if (!obc) {
	  derr << __func__ << ": could not load hitset " << oid << dendl;
	  break;
	}

	bufferlist bl;
	{
	  int r = osd->store->read(ch, ghobject_t(oid), 0, 0, bl);
	  ceph_assert(r >= 0);
	}
	HitSetRef hs(new HitSet);
	bufferlist::const_iterator pbl = bl.begin();
	decode(*hs, pbl);
	agent_state->add_hit_set(p->begin.sec(), hs);
      }
    }
  }
}

bool PrimaryLogPG::agent_maybe_flush(ObjectContextRef& obc)
{
  if (!obc->obs.oi.is_dirty()) {
    dout(20) << __func__ << " skip (clean) " << obc->obs.oi << dendl;
    osd->logger->inc(l_osd_agent_skip);
    return false;
  }
  if (obc->obs.oi.is_cache_pinned()) {
    dout(20) << __func__ << " skip (cache_pinned) " << obc->obs.oi << dendl;
    osd->logger->inc(l_osd_agent_skip);
    return false;
  }

  utime_t now = ceph_clock_now();
  utime_t ob_local_mtime;
  if (obc->obs.oi.local_mtime != utime_t()) {
    ob_local_mtime = obc->obs.oi.local_mtime;
  } else {
    ob_local_mtime = obc->obs.oi.mtime;
  }
  bool evict_mode_full =
    (agent_state->evict_mode == TierAgentState::EVICT_MODE_FULL);
  if (!evict_mode_full &&
      obc->obs.oi.soid.snap == CEPH_NOSNAP &&  // snaps immutable; don't delay
      (ob_local_mtime + utime_t(pool.info.cache_min_flush_age, 0) > now)) {
    dout(20) << __func__ << " skip (too young) " << obc->obs.oi << dendl;
    osd->logger->inc(l_osd_agent_skip);
    return false;
  }

  if (osd->agent_is_active_oid(obc->obs.oi.soid)) {
    dout(20) << __func__ << " skip (flushing) " << obc->obs.oi << dendl;
    osd->logger->inc(l_osd_agent_skip);
    return false;
  }

  dout(10) << __func__ << " flushing " << obc->obs.oi << dendl;

  // FIXME: flush anything dirty, regardless of what distribution of
  // ages we expect.

  hobject_t oid = obc->obs.oi.soid;
  osd->agent_start_op(oid);
  // no need to capture a pg ref, can't outlive fop or ctx
  std::function<void()> on_flush = [this, oid]() {
    osd->agent_finish_op(oid);
  };

  int result = start_flush(
    OpRequestRef(), obc, false, NULL,
    on_flush);
  if (result != -EINPROGRESS) {
    on_flush();
    dout(10) << __func__ << " start_flush() failed " << obc->obs.oi
      << " with " << result << dendl;
    osd->logger->inc(l_osd_agent_skip);
    return false;
  }

  osd->logger->inc(l_osd_agent_flush);
  return true;
}

bool PrimaryLogPG::agent_maybe_evict(ObjectContextRef& obc, bool after_flush)
{
  const hobject_t& soid = obc->obs.oi.soid;
  if (!after_flush && obc->obs.oi.is_dirty()) {
    dout(20) << __func__ << " skip (dirty) " << obc->obs.oi << dendl;
    return false;
  }
  // This is already checked by agent_work() which passes after_flush = false
  if (after_flush && m_scrubber->range_intersects_scrub(soid, soid.get_head())) {
      dout(20) << __func__ << " skip (scrubbing) " << obc->obs.oi << dendl;
      return false;
  }
  if (!obc->obs.oi.watchers.empty()) {
    dout(20) << __func__ << " skip (watchers) " << obc->obs.oi << dendl;
    return false;
  }
  if (obc->is_blocked()) {
    dout(20) << __func__ << " skip (blocked) " << obc->obs.oi << dendl;
    return false;
  }
  if (obc->obs.oi.is_cache_pinned()) {
    dout(20) << __func__ << " skip (cache_pinned) " << obc->obs.oi << dendl;
    return false;
  }

  if (soid.snap == CEPH_NOSNAP) {
    int result = _verify_no_head_clones(soid, obc->ssc->snapset);
    if (result < 0) {
      dout(20) << __func__ << " skip (clones) " << obc->obs.oi << dendl;
      return false;
    }
  }

  if (agent_state->evict_mode != TierAgentState::EVICT_MODE_FULL) {
    // is this object old than cache_min_evict_age?
    utime_t now = ceph_clock_now();
    utime_t ob_local_mtime;
    if (obc->obs.oi.local_mtime != utime_t()) {
      ob_local_mtime = obc->obs.oi.local_mtime;
    } else {
      ob_local_mtime = obc->obs.oi.mtime;
    }
    if (ob_local_mtime + utime_t(pool.info.cache_min_evict_age, 0) > now) {
      dout(20) << __func__ << " skip (too young) " << obc->obs.oi << dendl;
      osd->logger->inc(l_osd_agent_skip);
      return false;
    }
    // is this object old and/or cold enough?
    int temp = 0;
    uint64_t temp_upper = 0, temp_lower = 0;
    if (hit_set)
      agent_estimate_temp(soid, &temp);
    agent_state->temp_hist.add(temp);
    agent_state->temp_hist.get_position_micro(temp, &temp_lower, &temp_upper);

    dout(20) << __func__
	     << " temp " << temp
	     << " pos " << temp_lower << "-" << temp_upper
	     << ", evict_effort " << agent_state->evict_effort
	     << dendl;
    dout(30) << "agent_state:\n";
    auto f = Formatter::create_unique("");
    f->open_object_section("agent_state");
    agent_state->dump(f.get());
    f->close_section();
    f->flush(*_dout);
    *_dout << dendl;

    if (1000000 - temp_upper >= agent_state->evict_effort)
      return false;
  }

  dout(10) << __func__ << " evicting " << obc->obs.oi << dendl;
  OpContextUPtr ctx = simple_opc_create(obc);

  auto null_op_req = OpRequestRef();
  if (!ctx->lock_manager.get_lock_type(
	RWState::RWWRITE,
	obc->obs.oi.soid,
	obc,
	null_op_req)) {
    close_op_ctx(ctx.release());
    dout(20) << __func__ << " skip (cannot get lock) " << obc->obs.oi << dendl;
    return false;
  }

  osd->agent_start_evict_op();
  ctx->register_on_finish(
    [this]() {
      osd->agent_finish_evict_op();
    });

  ctx->at_version = get_next_version();
  ceph_assert(ctx->new_obs.exists);
  int r = _delete_oid(ctx.get(), true, false);
  if (obc->obs.oi.is_omap())
    ctx->delta_stats.num_objects_omap--;
  ctx->delta_stats.num_evict++;
  ctx->delta_stats.num_evict_kb += shift_round_up(obc->obs.oi.size, 10);
  if (obc->obs.oi.is_dirty())
    --ctx->delta_stats.num_objects_dirty;
  ceph_assert(r == 0);
  finish_ctx(ctx.get(), pg_log_entry_t::DELETE);
  simple_opc_submit(std::move(ctx));
  osd->logger->inc(l_osd_tier_evict);
  osd->logger->inc(l_osd_agent_evict);
  return true;
}

void PrimaryLogPG::agent_stop()
{
  dout(20) << __func__ << dendl;
  if (agent_state && !agent_state->is_idle()) {
    agent_state->evict_mode = TierAgentState::EVICT_MODE_IDLE;
    agent_state->flush_mode = TierAgentState::FLUSH_MODE_IDLE;
    osd->agent_disable_pg(this, agent_state->evict_effort);
  }
}

void PrimaryLogPG::agent_delay()
{
  dout(20) << __func__ << dendl;
  if (agent_state && !agent_state->is_idle()) {
    ceph_assert(agent_state->delaying == false);
    agent_state->delaying = true;
    osd->agent_disable_pg(this, agent_state->evict_effort);
  }
}

void PrimaryLogPG::agent_choose_mode_restart()
{
  dout(20) << __func__ << dendl;
  std::scoped_lock locker{*this};
  if (agent_state && agent_state->delaying) {
    agent_state->delaying = false;
    agent_choose_mode(true);
  }
}

bool PrimaryLogPG::agent_choose_mode(bool restart, OpRequestRef op)
{
  bool requeued = false;
  // Let delay play out
  if (agent_state->delaying) {
    dout(20) << __func__ << " " << this << " delaying, ignored" << dendl;
    return requeued;
  }

  TierAgentState::flush_mode_t flush_mode = TierAgentState::FLUSH_MODE_IDLE;
  TierAgentState::evict_mode_t evict_mode = TierAgentState::EVICT_MODE_IDLE;
  unsigned evict_effort = 0;

  if (info.stats.stats_invalid) {
    // idle; stats can't be trusted until we scrub.
    dout(20) << __func__ << " stats invalid (post-split), idle" << dendl;
    goto skip_calc;
  }

  {
  uint64_t divisor = pool.info.get_pg_num_divisor(info.pgid.pgid);
  ceph_assert(divisor > 0);

  // adjust (effective) user objects down based on the number
  // of HitSet objects, which should not count toward our total since
  // they cannot be flushed.
  uint64_t unflushable = info.stats.stats.sum.num_objects_hit_set_archive;

  // also exclude omap objects if ec backing pool
  const pg_pool_t *base_pool = get_osdmap()->get_pg_pool(pool.info.tier_of);
  ceph_assert(base_pool);
  if (!base_pool->supports_omap())
    unflushable += info.stats.stats.sum.num_objects_omap;

  uint64_t num_user_objects = info.stats.stats.sum.num_objects;
  if (num_user_objects > unflushable)
    num_user_objects -= unflushable;
  else
    num_user_objects = 0;

  uint64_t num_user_bytes = info.stats.stats.sum.num_bytes;
  uint64_t unflushable_bytes = info.stats.stats.sum.num_bytes_hit_set_archive;
  num_user_bytes -= unflushable_bytes;
  uint64_t num_overhead_bytes = osd->store->estimate_objects_overhead(num_user_objects);
  num_user_bytes += num_overhead_bytes;

  // also reduce the num_dirty by num_objects_omap
  int64_t num_dirty = info.stats.stats.sum.num_objects_dirty;
  if (!base_pool->supports_omap()) {
    if (num_dirty > info.stats.stats.sum.num_objects_omap)
      num_dirty -= info.stats.stats.sum.num_objects_omap;
    else
      num_dirty = 0;
  }

  dout(10) << __func__
	   << " flush_mode: "
	   << TierAgentState::get_flush_mode_name(agent_state->flush_mode)
	   << " evict_mode: "
	   << TierAgentState::get_evict_mode_name(agent_state->evict_mode)
	   << " num_objects: " << info.stats.stats.sum.num_objects
	   << " num_bytes: " << info.stats.stats.sum.num_bytes
	   << " num_objects_dirty: " << info.stats.stats.sum.num_objects_dirty
	   << " num_objects_omap: " << info.stats.stats.sum.num_objects_omap
	   << " num_dirty: " << num_dirty
	   << " num_user_objects: " << num_user_objects
	   << " num_user_bytes: " << num_user_bytes
	   << " num_overhead_bytes: " << num_overhead_bytes
	   << " pool.info.target_max_bytes: " << pool.info.target_max_bytes
	   << " pool.info.target_max_objects: " << pool.info.target_max_objects
	   << dendl;

  // get dirty, full ratios
  uint64_t dirty_micro = 0;
  uint64_t full_micro = 0;
  if (pool.info.target_max_bytes && num_user_objects > 0) {
    uint64_t avg_size = num_user_bytes / num_user_objects;
    dirty_micro =
      num_dirty * avg_size * 1000000 /
      std::max<uint64_t>(pool.info.target_max_bytes / divisor, 1);
    full_micro =
      num_user_objects * avg_size * 1000000 /
      std::max<uint64_t>(pool.info.target_max_bytes / divisor, 1);
  }
  if (pool.info.target_max_objects > 0) {
    uint64_t dirty_objects_micro =
      num_dirty * 1000000 /
      std::max<uint64_t>(pool.info.target_max_objects / divisor, 1);
    if (dirty_objects_micro > dirty_micro)
      dirty_micro = dirty_objects_micro;
    uint64_t full_objects_micro =
      num_user_objects * 1000000 /
      std::max<uint64_t>(pool.info.target_max_objects / divisor, 1);
    if (full_objects_micro > full_micro)
      full_micro = full_objects_micro;
  }
  dout(20) << __func__ << " dirty " << ((float)dirty_micro / 1000000.0)
	   << " full " << ((float)full_micro / 1000000.0)
	   << dendl;

  // flush mode
  uint64_t flush_target = pool.info.cache_target_dirty_ratio_micro;
  uint64_t flush_high_target = pool.info.cache_target_dirty_high_ratio_micro;
  uint64_t flush_slop = (float)flush_target * cct->_conf->osd_agent_slop;
  if (restart || agent_state->flush_mode == TierAgentState::FLUSH_MODE_IDLE) {
    flush_target += flush_slop;
    flush_high_target += flush_slop;
  } else {
    flush_target -= std::min(flush_target, flush_slop);
    flush_high_target -= std::min(flush_high_target, flush_slop);
  }

  if (dirty_micro > flush_high_target) {
    flush_mode = TierAgentState::FLUSH_MODE_HIGH;
  } else if (dirty_micro > flush_target || (!flush_target && num_dirty > 0)) {
    flush_mode = TierAgentState::FLUSH_MODE_LOW;
  }

  // evict mode
  uint64_t evict_target = pool.info.cache_target_full_ratio_micro;
  uint64_t evict_slop = (float)evict_target * cct->_conf->osd_agent_slop;
  if (restart || agent_state->evict_mode == TierAgentState::EVICT_MODE_IDLE)
    evict_target += evict_slop;
  else
    evict_target -= std::min(evict_target, evict_slop);

  if (full_micro > 1000000) {
    // evict anything clean
    evict_mode = TierAgentState::EVICT_MODE_FULL;
    evict_effort = 1000000;
  } else if (full_micro > evict_target) {
    // set effort in [0..1] range based on where we are between
    evict_mode = TierAgentState::EVICT_MODE_SOME;
    uint64_t over = full_micro - evict_target;
    uint64_t span  = 1000000 - evict_target;
    evict_effort = std::max(over * 1000000 / span,
			    uint64_t(1000000.0 *
				     cct->_conf->osd_agent_min_evict_effort));

    // quantize effort to avoid too much reordering in the agent_queue.
    uint64_t inc = cct->_conf->osd_agent_quantize_effort * 1000000;
    ceph_assert(inc > 0);
    uint64_t was = evict_effort;
    evict_effort -= evict_effort % inc;
    if (evict_effort < inc)
      evict_effort = inc;
    ceph_assert(evict_effort >= inc && evict_effort <= 1000000);
    dout(30) << __func__ << " evict_effort " << was << " quantized by " << inc << " to " << evict_effort << dendl;
  }
  }

  skip_calc:
  bool old_idle = agent_state->is_idle();
  if (flush_mode != agent_state->flush_mode) {
    dout(5) << __func__ << " flush_mode "
	    << TierAgentState::get_flush_mode_name(agent_state->flush_mode)
	    << " -> "
	    << TierAgentState::get_flush_mode_name(flush_mode)
	    << dendl;
    recovery_state.update_stats(
      [=, this](auto &history, auto &stats) {
	if (flush_mode == TierAgentState::FLUSH_MODE_HIGH) {
	  osd->agent_inc_high_count();
	  stats.stats.sum.num_flush_mode_high = 1;
	} else if (flush_mode == TierAgentState::FLUSH_MODE_LOW) {
	  stats.stats.sum.num_flush_mode_low = 1;
	}
	if (agent_state->flush_mode == TierAgentState::FLUSH_MODE_HIGH) {
	  osd->agent_dec_high_count();
	  stats.stats.sum.num_flush_mode_high = 0;
	} else if (agent_state->flush_mode == TierAgentState::FLUSH_MODE_LOW) {
	  stats.stats.sum.num_flush_mode_low = 0;
	}
	return false;
      });
    agent_state->flush_mode = flush_mode;
  }
  if (evict_mode != agent_state->evict_mode) {
    dout(5) << __func__ << " evict_mode "
	    << TierAgentState::get_evict_mode_name(agent_state->evict_mode)
	    << " -> "
	    << TierAgentState::get_evict_mode_name(evict_mode)
	    << dendl;
    if (agent_state->evict_mode == TierAgentState::EVICT_MODE_FULL &&
	is_active()) {
      if (op)
	requeue_op(op);
      requeue_ops(waiting_for_flush);
      requeue_ops(waiting_for_active);
      requeue_ops(waiting_for_readable);
      requeue_ops(waiting_for_scrub);
      requeue_ops(waiting_for_cache_not_full);
      objects_blocked_on_cache_full.clear();
      requeued = true;
    }
    recovery_state.update_stats(
      [=, this](auto &history, auto &stats) {
	if (evict_mode == TierAgentState::EVICT_MODE_SOME) {
	  stats.stats.sum.num_evict_mode_some = 1;
	} else if (evict_mode == TierAgentState::EVICT_MODE_FULL) {
	  stats.stats.sum.num_evict_mode_full = 1;
	}
	if (agent_state->evict_mode == TierAgentState::EVICT_MODE_SOME) {
	  stats.stats.sum.num_evict_mode_some = 0;
	} else if (agent_state->evict_mode == TierAgentState::EVICT_MODE_FULL) {
	  stats.stats.sum.num_evict_mode_full = 0;
	}
	return false;
      });
    agent_state->evict_mode = evict_mode;
  }
  uint64_t old_effort = agent_state->evict_effort;
  if (evict_effort != agent_state->evict_effort) {
    dout(5) << __func__ << " evict_effort "
	    << ((float)agent_state->evict_effort / 1000000.0)
	    << " -> "
	    << ((float)evict_effort / 1000000.0)
	    << dendl;
    agent_state->evict_effort = evict_effort;
  }

  // NOTE: we are using evict_effort as a proxy for *all* agent effort
  // (including flush).  This is probably fine (they should be
  // correlated) but it is not precisely correct.
  if (agent_state->is_idle()) {
    if (!restart && !old_idle) {
      osd->agent_disable_pg(this, old_effort);
    }
  } else {
    if (restart || old_idle) {
      osd->agent_enable_pg(this, agent_state->evict_effort);
    } else if (old_effort != agent_state->evict_effort) {
      osd->agent_adjust_pg(this, old_effort, agent_state->evict_effort);
    }
  }
  return requeued;
}

void PrimaryLogPG::agent_estimate_temp(const hobject_t& oid, int *temp)
{
  ceph_assert(hit_set);
  ceph_assert(temp);
  *temp = 0;
  if (hit_set->contains(oid))
    *temp = 1000000;
  unsigned i = 0;
  int last_n = pool.info.hit_set_search_last_n;
  for (map<time_t,HitSetRef>::reverse_iterator p =
       agent_state->hit_set_map.rbegin(); last_n > 0 &&
       p != agent_state->hit_set_map.rend(); ++p, ++i) {
    if (p->second->contains(oid)) {
      *temp += pool.info.get_grade(i);
      --last_n;
    }
  }
}

// Dup op detection

bool PrimaryLogPG::already_complete(eversion_t v)
{
  dout(20) << __func__ << ": " << v << dendl;
  for (xlist<RepGather*>::iterator i = repop_queue.begin();
       !i.end();
       ++i) {
    dout(20) << __func__ << ": " << **i << dendl;
    // skip copy from temp object ops
    if ((*i)->v == eversion_t()) {
      dout(20) << __func__ << ": " << **i
	       << " version is empty" << dendl;
      continue;
    }
    if ((*i)->v > v) {
      dout(20) << __func__ << ": " << **i
	       << " (*i)->v past v" << dendl;
      break;
    }
    if (!(*i)->all_committed) {
      dout(20) << __func__ << ": " << **i
	       << " not committed, returning false"
	       << dendl;
      return false;
    }
  }
  dout(20) << __func__ << ": returning true" << dendl;
  return true;
}


// ==========================================================================================
// SCRUB

void PrimaryLogPG::do_replica_scrub_map(OpRequestRef op)
{
  dout(15) << __func__ << " is scrub active? " << is_scrub_active() << dendl;
  op->mark_started();

  if (!is_scrub_active()) {
    dout(10) << __func__ << " scrub isn't active" << dendl;
    return;
  }
  m_scrubber->map_from_replica(op);
}

bool PrimaryLogPG::_range_available_for_scrub(const hobject_t& begin,
					      const hobject_t& end)
{
  pair<hobject_t, ObjectContextRef> next;
  next.second = object_contexts.lookup(begin);
  next.first = begin;
  bool more = true;
  while (more && next.first < end) {
    if (next.second && next.second->is_blocked()) {
      next.second->requeue_scrub_on_unblock = true;
      dout(10) << __func__ << ": scrub delayed, "
	       << next.first << " is blocked"
	       << dendl;
      return false;
    }
    more = object_contexts.get_next(next.first, &next);
  }
  return true;
}


int PrimaryLogPG::rep_repair_primary_object(const hobject_t& soid, OpContext *ctx)
{
  OpRequestRef op = ctx->op;
  // Only supports replicated pools
  ceph_assert(!pool.info.is_erasure());
  ceph_assert(is_primary());

  dout(10) << __func__ << " " << soid
	   << " peers osd.{" << get_acting_recovery_backfill() << "}" << dendl;

  if (!is_clean()) {
    block_for_clean(soid, op);
    return -EAGAIN;
  }

  ceph_assert(!recovery_state.get_pg_log().get_missing().is_missing(soid));
  auto& oi = ctx->new_obs.oi;
  eversion_t v = oi.version;

  if (primary_error(soid, v)) {
    dout(0) << __func__ << " No other replicas available for " << soid << dendl;
    // XXX: If we knew that there is no down osd which could include this
    // object, it would be nice if we could return EIO here.
    // If a "never fail" flag was available, that could be used
    // for rbd to NOT return EIO until object marked lost.

    // Drop through to save this op in case an osd comes up with the object.
  }

  // Restart the op after object becomes readable again
  waiting_for_unreadable_object[soid].push_back(op);
  op->mark_delayed("waiting for missing object");

  ceph_assert(is_clean());
  state_set(PG_STATE_REPAIR);
  state_clear(PG_STATE_CLEAN);
  queue_peering_event(
      PGPeeringEventRef(
	std::make_shared<PGPeeringEvent>(
	get_osdmap_epoch(),
	get_osdmap_epoch(),
	PeeringState::DoRecovery())));

  return -EAGAIN;
}

/*---SnapTrimmer Logging---*/
#undef dout_prefix
#define dout_prefix pg->gen_prefix(*_dout)

void PrimaryLogPG::SnapTrimmer::log_enter(const char *state_name)
{
  ldout(pg->cct, 20) << "enter " << state_name << dendl;
}

void PrimaryLogPG::SnapTrimmer::log_exit(const char *state_name, utime_t enter_time)
{
  ldout(pg->cct, 20) << "exit " << state_name << dendl;
}

bool PrimaryLogPG::SnapTrimmer::permit_trim() {
  return
    pg->is_clean() &&
    !pg->is_scrub_queued_or_active() &&
    !pg->snap_trimq.empty();
}

/*---SnapTrimmer states---*/
#undef dout_prefix
#define dout_prefix (context< SnapTrimmer >().pg->gen_prefix(*_dout) \
		     << "SnapTrimmer state<" << get_state_name() << ">: ")

/* NotTrimming */
PrimaryLogPG::NotTrimming::NotTrimming(my_context ctx)
  : my_base(ctx),
    NamedState(nullptr, "NotTrimming")
{
  context< SnapTrimmer >().log_enter(state_name);
}

void PrimaryLogPG::NotTrimming::exit()
{
  context< SnapTrimmer >().log_exit(state_name, enter_time);
}

boost::statechart::result PrimaryLogPG::NotTrimming::react(const KickTrim&)
{
  PrimaryLogPG *pg = context< SnapTrimmer >().pg;
  ldout(pg->cct, 10) << "NotTrimming react KickTrim" << dendl;

  if (!(pg->is_primary() && pg->is_active())) {
    ldout(pg->cct, 10) << "NotTrimming not primary or active" << dendl;
    return discard_event();
  }
  if (!pg->is_clean() ||
      pg->snap_trimq.empty()) {
    ldout(pg->cct, 10) << "NotTrimming not clean or nothing to trim" << dendl;
    return discard_event();
  }
  if (pg->is_scrub_queued_or_active()) {
    ldout(pg->cct, 10) << " scrubbing, will requeue snap_trimmer after" << dendl;
    return transit< WaitScrub >();
  } else {
    return transit< Trimming >();
  }
}

boost::statechart::result PrimaryLogPG::WaitReservation::react(const SnapTrimReserved&)
{
  PrimaryLogPG *pg = context< SnapTrimmer >().pg;
  ldout(pg->cct, 10) << "WaitReservation react SnapTrimReserved" << dendl;

  pending = nullptr;
  if (!context< SnapTrimmer >().can_trim()) {
    post_event(KickTrim());
    return transit< NotTrimming >();
  }

  context<Trimming>().snap_to_trim = pg->snap_trimq.range_start();
  ldout(pg->cct, 10) << "NotTrimming: trimming "
		     << pg->snap_trimq.range_start()
		     << dendl;
  return transit< AwaitAsyncWork >();
}

/* AwaitAsyncWork */
PrimaryLogPG::AwaitAsyncWork::AwaitAsyncWork(my_context ctx)
  : my_base(ctx),
    NamedState(nullptr, "Trimming/AwaitAsyncWork")
{
  auto *pg = context< SnapTrimmer >().pg;
  // Determine cost in terms of the average object size
  uint64_t cost_per_object = pg->get_average_object_size();
  context< SnapTrimmer >().log_enter(state_name);
  context< SnapTrimmer >().pg->osd->queue_for_snap_trim(pg, cost_per_object);
  pg->state_set(PG_STATE_SNAPTRIM);
  pg->state_clear(PG_STATE_SNAPTRIM_ERROR);
  pg->publish_stats_to_osd();
}

boost::statechart::result PrimaryLogPG::AwaitAsyncWork::react(const DoSnapWork&)
{
  PrimaryLogPGRef pg = context< SnapTrimmer >().pg;
  snapid_t snap_to_trim = context<Trimming>().snap_to_trim;
  auto &in_flight = context<Trimming>().in_flight;
  ceph_assert(in_flight.empty());

  ceph_assert(pg->is_primary() && pg->is_active());
  if (!context< SnapTrimmer >().can_trim()) {
    ldout(pg->cct, 10) << "something changed, reverting to NotTrimming" << dendl;
    post_event(KickTrim());
    return transit< NotTrimming >();
  }

  ldout(pg->cct, 10) << "AwaitAsyncWork: trimming snap " << snap_to_trim << dendl;

  unsigned max = pg->cct->_conf->osd_pg_max_concurrent_snap_trims;
  // we need to look for at least 1 snaptrim, otherwise we'll misinterpret
  // the ENOENT below and erase snap_to_trim.
  ceph_assert(max > 0);

  auto to_trim =
      pg->snap_mapper.get_next_objects_to_trim(snap_to_trim, max);
  if (!to_trim.has_value()) {
    // Done!
    ldout(pg->cct, 10) << "no more entries to trim" << dendl;

    pg->snap_trimq.erase(snap_to_trim);

    if (pg->snap_trimq_repeat.count(snap_to_trim)) {
      ldout(pg->cct, 10) << " removing from snap_trimq_repeat" << dendl;
      pg->snap_trimq_repeat.erase(snap_to_trim);
    } else {
      ldout(pg->cct, 10) << "adding snap " << snap_to_trim
			 << " to purged_snaps"
			 << dendl;
      ObjectStore::Transaction t;
      pg->recovery_state.adjust_purged_snaps(
	[snap_to_trim](auto &purged_snaps) {
	  purged_snaps.insert(snap_to_trim);
	});
      pg->write_if_dirty(t);

      ldout(pg->cct, 10) << "purged_snaps now "
			 << pg->info.purged_snaps << ", snap_trimq now "
			 << pg->snap_trimq << dendl;

      int tr = pg->osd->store->queue_transaction(pg->ch, std::move(t), NULL);
      ceph_assert(tr == 0);

      pg->recovery_state.share_pg_info();
    }
    post_event(KickTrim());
    pg->set_snaptrim_duration();
    return transit< NotTrimming >();
  }

  for (auto &&object: *to_trim) {
    // Get next
    ldout(pg->cct, 10) << "AwaitAsyncWork react trimming " << object << dendl;
    OpContextUPtr ctx;
    int error = pg->trim_object(in_flight.empty(), object, snap_to_trim, &ctx);
    if (error) {
      if (error == -ENOLCK) {
	ldout(pg->cct, 10) << "could not get write lock on obj "
			   << object << dendl;
      } else {
	pg->state_set(PG_STATE_SNAPTRIM_ERROR);
	ldout(pg->cct, 10) << "Snaptrim error=" << error << dendl;
      }
      if (!in_flight.empty()) {
	ldout(pg->cct, 10) << "letting the ones we already started finish" << dendl;
	return transit< WaitRepops >();
      }
      if (error == -ENOLCK) {
	ldout(pg->cct, 10) << "waiting for it to clear"
			   << dendl;
	return transit< WaitRWLock >();
      }
      return transit< NotTrimming >();
    }

    in_flight.insert(object);
    ctx->register_on_success(
      [pg, object, &in_flight]() {
	ceph_assert(in_flight.find(object) != in_flight.end());
	in_flight.erase(object);
	if (in_flight.empty()) {
	  if (pg->state_test(PG_STATE_SNAPTRIM_ERROR)) {
	    pg->snap_trimmer_machine.process_event(Reset());
	  } else {
	    pg->snap_trimmer_machine.process_event(RepopsComplete());
	  }
	}
      });

    pg->simple_opc_submit(std::move(ctx));
  }

  return transit< WaitRepops >();
}

void PrimaryLogPG::setattr_maybe_cache(
  ObjectContextRef obc,
  PGTransaction *t,
  const string &key,
  bufferlist &val)
{
  t->setattr(obc->obs.oi.soid, key, val);
}

void PrimaryLogPG::setattrs_maybe_cache(
  ObjectContextRef obc,
  PGTransaction *t,
  map<string, bufferlist, less<>> &attrs)
{
  t->setattrs(obc->obs.oi.soid, attrs);
}

void PrimaryLogPG::rmattr_maybe_cache(
  ObjectContextRef obc,
  PGTransaction *t,
  const string &key)
{
  t->rmattr(obc->obs.oi.soid, key);
}

int PrimaryLogPG::getattr_maybe_cache(
  ObjectContextRef obc,
  const string &key,
  bufferlist *val)
{
  if (pool.info.is_erasure()) {
    map<string, bufferlist>::iterator i = obc->attr_cache.find(key);
    if (i != obc->attr_cache.end()) {
      if (val)
	*val = i->second;
      return 0;
    } else {
      if (obc->obs.exists) {
        return -ENODATA;
      } else {
        return -ENOENT;
      }
    }
  }
  return pgbackend->objects_get_attr(obc->obs.oi.soid, key, val);
}

int PrimaryLogPG::getattrs_maybe_cache(
  ObjectContextRef obc,
  map<string, bufferlist, less<>> *out)
{
  int r = 0;
  ceph_assert(out);
  if (pool.info.is_erasure()) {
    *out = obc->attr_cache;
  } else {
    r = pgbackend->objects_get_attrs(obc->obs.oi.soid, out);
  }
  map<string, bufferlist, less<>> tmp;
  for (auto& [key, val]: *out) {
    if (key.size() > 1 && key[0] == '_') {
      tmp[key.substr(1, key.size())] = std::move(val);
    }
  }
  tmp.swap(*out);
  return r;
}

bool PrimaryLogPG::check_failsafe_full() {
    return osd->check_failsafe_full(get_dpp());
}

bool PrimaryLogPG::maybe_preempt_replica_scrub(const hobject_t& oid)
{
  return m_scrubber->write_blocked_by_scrub(oid);
}

struct ECListener *PrimaryLogPG::get_eclistener()
{
  return this;
}

void intrusive_ptr_add_ref(PrimaryLogPG *pg) { pg->get("intptr"); }
void intrusive_ptr_release(PrimaryLogPG *pg) { pg->put("intptr"); }

#ifdef PG_DEBUG_REFS
uint64_t get_with_id(PrimaryLogPG *pg) { return pg->get_with_id(); }
void put_with_id(PrimaryLogPG *pg, uint64_t id) { return pg->put_with_id(id); }
#endif

void intrusive_ptr_add_ref(PrimaryLogPG::RepGather *repop) { repop->get(); }
void intrusive_ptr_release(PrimaryLogPG::RepGather *repop) { repop->put(); }
