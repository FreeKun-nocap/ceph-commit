// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "./scrub_reservations.h"

#include <span>

#include "common/ceph_time.h"
#include "common/debug.h"
#include "osd/OSD.h"
#include "osd/PG.h"
#include "osd/osd_types_fmt.h"

#include "pg_scrubber.h"

using namespace Scrub;
using namespace std::chrono;
using namespace std::chrono_literals;

#define dout_context (m_osds->cct)
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix_fn(_dout, this, __func__)
template <class T>
static std::ostream& _prefix_fn(std::ostream* _dout, T* t, std::string fn = "")
{
  return t->gen_prefix(*_dout, fn);
}

namespace Scrub {

/**
 * 副本预留管理器构造：收集 actingset 内的副本并向第一个副本发送 MOSDScrubReserve(REQUEST)。
 *
 * 预留是串行的：收到当前副本 grant 才发下一个；高优先级（管理员触发）巡检跳过预留。
 */
ReplicaReservations::ReplicaReservations(
    ScrubMachineListener& scrbr,
    reservation_nonce_t& nonce,
    const ScrubCounterSet& pc)
    : m_scrubber{scrbr}
    , m_pg{m_scrubber.get_pg()}
    , m_pgid{m_scrubber.get_spgid().pgid}
    , m_osds{m_pg->get_pg_osd(ScrubberPasskey())}
    , m_last_request_sent_nonce{nonce}
    , m_perf_indices{pc}
{
  // the acting set is sorted by pg_shard_t. The reservations are to be issued
  // in this order, so that the OSDs will receive the requests in a consistent
  // order. This is done to reduce the chance of having two PGs that share some
  // of their acting-set OSDs, consistently interfering with each other's
  // reservation process.
  // 收集副本列表（按 pg_shard_t 排序、排除自己），固定申请顺序避免互相干扰。
  auto acting = m_pg->get_actingset();
  m_sorted_secondaries.reserve(acting.size());
  std::copy_if(
      acting.cbegin(), acting.cend(), std::back_inserter(m_sorted_secondaries),
      [whoami = m_pg->pg_whoami](const pg_shard_t& shard) {
	return shard != whoami;
      });
  m_osds->logger->set(
      m_perf_indices.rsv_secondaries_num, m_sorted_secondaries.size());

  m_next_to_request = m_sorted_secondaries.cbegin();
  if (m_scrubber.is_reservation_required()) {
    // send out the 1'st request (unless we have no replicas)
    // 重点调用：向第一个副本发送预留申请；无副本时直接回调完成。
    m_process_started_at = ScrubClock::now();
    send_next_reservation_or_complete();
  } else {
    // for high-priority scrubs (i.e. - user-initiated), no reservations are
    // needed. Note: not perf-counted as either success or failure.
    // 高优先级（管理员触发）：不需要副本预留。
    dout(10) << "high-priority scrub - no reservations needed" << dendl;
    m_osds->logger->inc(m_perf_indices.rsv_skipped_cnt);
  }
}

void ReplicaReservations::release_all()
{
  std::span<const pg_shard_t> replicas{
      m_sorted_secondaries.cbegin(), m_next_to_request};
  dout(10) << fmt::format("releasing {}", replicas) << dendl;
  epoch_t epoch = m_pg->get_osdmap_epoch();

  // send 'release' messages to all replicas we have managed to reserve
  for (const auto& peer : replicas) {
    auto m = make_message<MOSDScrubReserve>(
	spg_t{m_pgid, peer.shard}, epoch, MOSDScrubReserve::RELEASE,
	m_pg->pg_whoami, 0);
    m_pg->send_cluster_message(peer.osd, m, epoch, false);
  }

  m_sorted_secondaries.clear();
  m_next_to_request = m_sorted_secondaries.cbegin();
}

void ReplicaReservations::discard_remote_reservations()
{
  dout(10) << "reset w/o issuing messages" << dendl;
  m_sorted_secondaries.clear();
  m_next_to_request = m_sorted_secondaries.cbegin();
}

void ReplicaReservations::log_success_and_duration()
{
  ceph_assert(m_process_started_at.has_value());
  auto logged_duration = ScrubClock::now() - m_process_started_at.value();
  m_osds->logger->tinc(m_perf_indices.rsv_successful_elapsed, logged_duration);
  m_osds->logger->inc(m_perf_indices.rsv_successful_cnt);
  m_osds->logger->hinc(
      l_osd_scrub_reservation_dur_hist, std::ssize(m_sorted_secondaries),
      logged_duration.count());
  m_process_started_at.reset();
}

void ReplicaReservations::log_failure_and_duration(int failure_cause_counter)
{
  if (!m_process_started_at.has_value()) {
    // outcome (success/failure) already logged
    return;
  }
  auto logged_duration = ScrubClock::now() - m_process_started_at.value();
  m_osds->logger->tinc(m_perf_indices.rsv_failed_elapsed, logged_duration);
  m_process_started_at.reset();
  // note: not counted into l_osd_scrub_reservation_dur_hist
  m_osds->logger->inc(failure_cause_counter);
}

ReplicaReservations::~ReplicaReservations()
{
  release_all();
  log_failure_and_duration(m_perf_indices.rsv_aborted_cnt);
}

bool ReplicaReservations::is_reservation_response_relevant(
    reservation_nonce_t msg_nonce) const
{
  return (msg_nonce == 0) || (msg_nonce == m_last_request_sent_nonce);
}

bool ReplicaReservations::is_msg_source_correct(pg_shard_t from) const
{
  const auto exp_source = get_last_sent();
  return exp_source && from == *exp_source;
}

/**
 * 处理一个副本的 GRANT 回复：校验 nonce/来源后推进串行预留。
 *
 * 返回 true 表示全部副本已收齐（见 send_next_reservation_or_complete）。
 */
bool ReplicaReservations::handle_reserve_grant(
    const MOSDScrubReserve& msg,
    pg_shard_t from)
{
  // nonce 不匹配：是旧请求（如超时后）的迟到回复，忽略。
  if (!is_reservation_response_relevant(msg.reservation_nonce)) {
    // this is a stale response to a previous request (e.g. one that
    // timed-out). See m_last_request_sent_nonce for details.
    dout(1) << fmt::format(
		   "stale reservation response from {} with nonce {} vs. "
		   "expected {} (e:{})",
		   from, msg.reservation_nonce, m_last_request_sent_nonce,
		   msg.map_epoch)
	    << dendl;
    return false;
  }

  // verify that the grant is from the peer we expected. If not?
  // for now - abort the OSD. There is no known scenario in which a
  // grant message with a correct nonce can arrive from the wrong peer.
  // (we would not abort for arriving messages with nonce 0, as those
  // are legacy messages, for which the nonce was not verified).
  // 来源校验：nonce 正确却来自非预期副本视为 bug，直接 abort。
  if (!is_msg_source_correct(from)) {
    const auto error_text = fmt::format(
	"unexpected reservation grant from {} vs. the expected {} (e:{} "
	"message nonce:{})",
	from, get_last_sent().value_or(pg_shard_t{}), msg.map_epoch,
	msg.reservation_nonce);
    dout(1) << error_text << dendl;
    if (msg.reservation_nonce != 0) {
      m_osds->clog->error() << error_text;
      ceph_abort_msg(error_text);
    }
    return false;
  }

  auto elapsed = ScrubClock::now() - m_last_request_sent_at;
  dout(10) << fmt::format(
		  "(e:{} nonce:{}) granted by {} ({} of {}) in {}ms",
		  msg.map_epoch, msg.reservation_nonce, from,
		  active_requests_cnt(), m_sorted_secondaries.size(),
		  duration_cast<milliseconds>(elapsed).count())
	   << dendl;
  // 重点调用：推进下一个副本的申请；全部完成则返回 true。
  return send_next_reservation_or_complete();
}

/**
 * 向下一个副本发送 MOSDScrubReserve(REQUEST)，或确认全部副本已预留完成。
 *
 * 返回 true 表示所有副本都已 grant（含无副本）；false 表示还有待回复的申请。
 */
bool ReplicaReservations::send_next_reservation_or_complete()
{
  if (m_next_to_request == m_sorted_secondaries.cend()) {
    // granted by all replicas
    // 全部副本已同意：记录成功耗时，返回 true 触发后续 RemotesReserved。
    dout(10) << "remote reservation complete" << dendl;
    log_success_and_duration();
    return true;  // done
  }

  // send the next reservation request
  // 重点调用：构造并发送预留申请（nonce 防止旧回复串扰）。
  const auto peer = *m_next_to_request;
  const auto epoch = m_pg->get_osdmap_epoch();
  m_last_request_sent_nonce++;

  auto m = make_message<MOSDScrubReserve>(
      spg_t{m_pgid, peer.shard}, epoch, MOSDScrubReserve::REQUEST, m_pg->pg_whoami,
      m_last_request_sent_nonce);
  // 发给副本 OSD（集群消息通道）。
  m_pg->send_cluster_message(peer.osd, m, epoch, false);
  m_last_request_sent_at = ScrubClock::now();
  dout(10) << fmt::format(
		  "reserving {} (the {} of {} replicas) e:{} nonce:{}",
		  *m_next_to_request, active_requests_cnt() + 1,
		  m_sorted_secondaries.size(), epoch, m_last_request_sent_nonce)
	   << dendl;
  // 游标后移：下一个 grant 到来时再申请再下一个。
  m_next_to_request++;
  return false;
}

bool ReplicaReservations::handle_reserve_rejection(
    const MOSDScrubReserve& msg,
    pg_shard_t from)
{
  // a convenient log message for the reservation process conclusion
  // (matches the one in send_next_reservation_or_complete())
  dout(10) << fmt::format(
		  "remote reservation failure. Rejected by {} ({})", from, msg)
	   << dendl;

  if (!is_reservation_response_relevant(msg.reservation_nonce)) {
    // this is a stale response to a previous request (e.g. one that
    // timed-out). See m_last_request_sent_nonce for details.
    dout(10) << fmt::format(
		    "stale reservation response from {} with reservation_nonce "
		    "{} vs. expected {} (e:{})",
		    from, msg.reservation_nonce, m_last_request_sent_nonce,
		    msg.map_epoch)
	     << dendl;
    return false;
  }

  log_failure_and_duration(m_perf_indices.rsv_rejected_cnt);

  // we should never see a rejection carrying a valid
  // reservation nonce - arriving while we have no pending requests
  ceph_assert(get_last_sent().has_value() || msg.reservation_nonce == 0);

  // verify that the denial is from the peer we expected. If not?
  // There is no known scenario in which this can happen, but if it does -
  // we should treat it as though the *correct* peer has rejected the request,
  // but remember to release that peer, too.
  if (is_msg_source_correct(from)) {
    m_next_to_request--;  // no need to release this one
  } else {
    m_osds->clog->warn() << fmt::format(
	"unexpected reservation denial from {} vs the expected {} (e:{} "
	"message reservation_nonce:{})",
	from, get_last_sent().value_or(pg_shard_t{}), msg.map_epoch,
	msg.reservation_nonce);
  }
  return true;
}

std::optional<pg_shard_t> ReplicaReservations::get_last_sent() const
{
  if (m_next_to_request == m_sorted_secondaries.cbegin()) {
    return std::nullopt;
  }
  return *(m_next_to_request - 1);
}

size_t ReplicaReservations::active_requests_cnt() const
{
  return m_next_to_request - m_sorted_secondaries.cbegin();
}

std::ostream& ReplicaReservations::gen_prefix(
    std::ostream& out,
    std::string fn) const
{
  return m_pg->gen_prefix(out)
	 << fmt::format("scrubber::ReplicaReservations:{}: ", fn);
}

} // namespace Scrub
