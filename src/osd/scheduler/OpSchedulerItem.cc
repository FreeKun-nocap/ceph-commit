// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "osd/scheduler/OpSchedulerItem.h"
#include "osd/OSD.h"
#include "osd/osd_tracer.h"


namespace ceph::osd::scheduler {

void PGOpItem::run(
  OSD *osd,
  OSDShard *sdata,
  PGRef& pg,
  ThreadPool::TPHandle &handle)
{
  osd->dequeue_op(pg, op, handle);
  pg->unlock();
}

void PGPeeringItem::run(
  OSD *osd,
  OSDShard *sdata,
  PGRef& pg,
  ThreadPool::TPHandle &handle)
{
  osd->dequeue_peering_evt(sdata, pg.get(), evt, handle);
}

void PGSnapTrim::run(
  OSD *osd,
  OSDShard *sdata,
  PGRef& pg,
  ThreadPool::TPHandle &handle)
{
  pg->snap_trimmer(epoch_queued);
  pg->unlock();
}

void PGScrub::run(OSD* osd, OSDShard* sdata, PGRef& pg, ThreadPool::TPHandle& handle)
{
  pg->scrub(epoch_queued, handle);
  pg->unlock();
}

void PGScrubResched::run(OSD* osd,
			 OSDShard* sdata,
			 PGRef& pg,
			 ThreadPool::TPHandle& handle)
{
  pg->scrub_send_scrub_resched(epoch_queued, handle);
  pg->unlock();
}

void PGScrubPushesUpdate::run(OSD* osd,
			      OSDShard* sdata,
			      PGRef& pg,
			      ThreadPool::TPHandle& handle)
{
  pg->scrub_send_pushes_update(epoch_queued, handle);
  pg->unlock();
}

void PGScrubAppliedUpdate::run(OSD* osd,
			       OSDShard* sdata,
			       PGRef& pg,
			       ThreadPool::TPHandle& handle)
{
  pg->scrub_send_applied_update(epoch_queued, handle);
  pg->unlock();
}

void PGScrubUnblocked::run(OSD* osd,
			   OSDShard* sdata,
			   PGRef& pg,
			   ThreadPool::TPHandle& handle)
{
  pg->scrub_send_unblocking(epoch_queued, handle);
  pg->unlock();
}

void PGScrubDigestUpdate::run(OSD* osd,
			      OSDShard* sdata,
			      PGRef& pg,
			      ThreadPool::TPHandle& handle)
{
  pg->scrub_send_digest_update(epoch_queued, handle);
  pg->unlock();
}

void PGScrubGotReplMaps::run(OSD* osd,
			     OSDShard* sdata,
			     PGRef& pg,
			     ThreadPool::TPHandle& handle)
{
  pg->scrub_send_replmaps_ready(epoch_queued, handle);
  pg->unlock();
}

void PGRepScrub::run(OSD* osd, OSDShard* sdata, PGRef& pg, ThreadPool::TPHandle& handle)
{
  pg->replica_scrub(epoch_queued, activation_index, handle);
  pg->unlock();
}

void PGRepScrubResched::run(OSD* osd,
			    OSDShard* sdata,
			    PGRef& pg,
			    ThreadPool::TPHandle& handle)
{
  pg->replica_scrub_resched(epoch_queued, activation_index, handle);
  pg->unlock();
}

void PGScrubReplicaPushes::run([[maybe_unused]] OSD* osd,
			      OSDShard* sdata,
			      PGRef& pg,
			      ThreadPool::TPHandle& handle)
{
  pg->scrub_send_replica_pushes(epoch_queued, handle);
  pg->unlock();
}

void PGScrubScrubFinished::run([[maybe_unused]] OSD* osd,
			       OSDShard* sdata,
			       PGRef& pg,
			       ThreadPool::TPHandle& handle)
{
  pg->scrub_send_scrub_is_finished(epoch_queued, handle);
  pg->unlock();
}

void PGScrubGetNextChunk::run([[maybe_unused]] OSD* osd,
			       OSDShard* sdata,
			       PGRef& pg,
			       ThreadPool::TPHandle& handle)
{
  pg->scrub_send_get_next_chunk(epoch_queued, handle);
  pg->unlock();
}

void PGScrubChunkIsBusy::run([[maybe_unused]] OSD* osd,
			      OSDShard* sdata,
			      PGRef& pg,
			      ThreadPool::TPHandle& handle)
{
  pg->scrub_send_chunk_busy(epoch_queued, handle);
  pg->unlock();
}

void PGScrubChunkIsFree::run([[maybe_unused]] OSD* osd,
			      OSDShard* sdata,
			      PGRef& pg,
			      ThreadPool::TPHandle& handle)
{
  pg->scrub_send_chunk_free(epoch_queued, handle);
  pg->unlock();
}

/**
 * 执行一个已经从 OSD 调度队列取出的 PGRecovery 任务：记录排队延迟，
 * 调用 OSD::do_recovery() 推进本轮 recovery，并释放 worker 持有的 PG 锁。
 */
void PGRecovery::run(
  OSD *osd,
  OSDShard *sdata,
  PGRef& pg,
  ThreadPool::TPHandle &handle)
{
  // 统计该 recovery 任务从入队到被 worker 取出的等待时间。
  osd->logger->tinc(
    l_osd_recovery_queue_lat,
    ceph_clock_now() - time_queued);
  // 使用排队时的 epoch、预留 push 数和优先级，执行本轮 PG recovery。
  osd->do_recovery(pg.get(), epoch_queued, reserved_pushes, priority, handle);
  // _process() 在调用 run() 前已持有 PG 锁；任务处理完后由此释放。
  pg->unlock();
}

/**
 * 执行已排入 OSD 调度器的 recovery 续接回调。
 *
 * OSDService::queue_recovery_context() 将 GenContext 封装为本调度项；
 * _process() 已在进入本函数前持有 PG 锁。回调完成后由这里释放 PG 锁。
 */
void PGRecoveryContext::run(
  OSD *osd,
  OSDShard *sdata,
  PGRef& pg,
  ThreadPool::TPHandle &handle)
{
  // 统计从 queue_recovery_context() 入队到实际执行的等待时间。
  osd->logger->tinc(
    l_osd_recovery_context_queue_lat,
    ceph_clock_now() - time_queued);
  // 转移 GenContext 所有权并执行；GenContext::complete() 在 finish() 返回后删除回调。
  c.release()->complete(handle);
  // 与 _process() 获取的 PG 锁配对；回调本身执行期间仍受该锁保护。
  pg->unlock();
}

void PGDelete::run(
  OSD *osd,
  OSDShard *sdata,
  PGRef& pg,
  ThreadPool::TPHandle &handle)
{
  osd->dequeue_delete(sdata, pg.get(), epoch_queued, handle);
}

/**
 * 执行一个已从 OSD 调度队列取出的恢复相关消息。
 *
 * 该消息可能是 push、push reply、pull、backfill 或 scan 等恢复协议消息。
 * 函数先记录消息在恢复队列中的等待延迟，再通过 OSD::dequeue_op() 将消息交给目标 PG 及其 backend 处理；
 * 处理完成后释放调度 worker 为该 PG 持有的锁。
 */
void PGRecoveryMsg::run(
  OSD *osd,
  OSDShard *sdata,
  PGRef& pg,
  ThreadPool::TPHandle &handle)
{
  auto latency = ceph_clock_now() - time_queued;
  switch (op->get_req()->get_type()) {
  case MSG_OSD_PG_PUSH:
    osd->logger->tinc(l_osd_recovery_push_queue_lat, latency);
  case MSG_OSD_PG_PUSH_REPLY:
    osd->logger->tinc(l_osd_recovery_push_reply_queue_lat, latency);
  case MSG_OSD_PG_PULL:
    osd->logger->tinc(l_osd_recovery_pull_queue_lat, latency);
  case MSG_OSD_PG_BACKFILL:
    osd->logger->tinc(l_osd_recovery_backfill_queue_lat, latency);
  case MSG_OSD_PG_BACKFILL_REMOVE:
    osd->logger->tinc(l_osd_recovery_backfill_remove_queue_lat, latency);
  case MSG_OSD_PG_SCAN:
    osd->logger->tinc(l_osd_recovery_scan_queue_lat, latency);
  }
  osd->dequeue_op(pg, op, handle);
  pg->unlock();
}

}
