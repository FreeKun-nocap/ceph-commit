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


#include <memory>
#include <functional>

#include "osd/scheduler/mClockScheduler.h"
#include "common/debug.h"

#ifdef WITH_CRIMSON
#include "crimson/common/perf_counters_collection.h"
#else
#include "common/perf_counters_collection.h"
#endif

namespace dmc = crimson::dmclock;
using namespace std::placeholders;
using namespace std::literals;

#define dout_context cct
#define dout_subsys ceph_subsys_mclock
#undef dout_prefix
#define dout_prefix *_dout << "mClockScheduler: "


namespace ceph::osd::scheduler {

uint32_t mClockScheduler::calc_scaled_cost(int item_cost)
{
  return mclock_conf.calc_scaled_cost(item_cost);
}

void mClockScheduler::dump(ceph::Formatter &f) const
{
  // Display queue sizes
  f.open_object_section("queue_sizes");
  f.dump_int("high_priority_queue", high_priority.size());
  f.dump_int("scheduler", scheduler.request_count());
  f.close_section();

  // client map and queue tops (res, wgt, lim)
  std::ostringstream out;
  f.open_object_section("mClockClients");
  f.dump_int("client_count", scheduler.client_count());
  out << scheduler;
  f.dump_string("clients", out.str());
  f.close_section();

  // Display sorted queues (res, wgt, lim)
  f.open_object_section("mClockQueues");
  f.dump_string("queues", display_queues());
  f.close_section();

  f.open_object_section("HighPriorityQueue");
  for (auto it = high_priority.begin();
       it != high_priority.end(); it++) {
    f.dump_int("priority", it->first);
    f.dump_int("queue_size", it->second.size());
  }
  f.close_section();
}

void mClockScheduler::enqueue(OpSchedulerItem&& item)
{
  // 将任务类型转换为 mClock 客户端标识，其中 class_id 区分 client、
  // background_recovery、background_best_effort 和 immediate 等调度类别。
  auto id = get_scheduler_id(item);
  // 保留消息携带的原始优先级，用于判断是否绕过常规 mClock 队列。
  unsigned priority = item.get_priority();

  // TODO: move this check into OpSchedulerItem, handle backwards compat
  if (SchedulerClass::immediate == id.class_id) {
    // immediate 任务使用最大优先级进入严格高优先级队列，优先于 mClock
    // 队列中的 client、recovery 和 best-effort 请求执行。
    enqueue_high(immediate_class_priority, std::move(item));
  } else if (priority >= cutoff_priority) {
    // 达到 cutoff 的任务同样绕过 mClock QoS，按原始优先级进入高优先级队列。
    enqueue_high(priority, std::move(item));
  } else {
    // 普通任务进入 mClock 队列。将消息的抽象 cost 换算为 mClock 使用的 QoS 成本，
    // 使不同请求的数据量/IO 开销能够参与配额计算。
    auto cost = calc_scaled_cost(item.get_cost());
    // 保存换算后的成本，供出队统计及后续完成请求时使用。
    item.set_qos_cost(cost);
    dout(20) << __func__ << " " << id
             << " item_cost: " << item.get_cost()
             << " scaled_cost: " << cost
             << dendl;

    // 按调度类别 id 和换算后的 cost 加入 dmClock 队列；
    // dmClock 将依据该类别配置的 reservation、weight 和 limit 决定请求何时可以出队。
    scheduler.add_request(
      std::move(item),
      id,
      cost);
    // 确保该调度类别对应的 mClock 性能计数器已经建立。
    mclock_conf.get_mclock_counter(id);
  }

 dout(20) << __func__ << " client_count: " << scheduler.client_count()
          << " queue_sizes: [ "
	  << " high_priority_queue: " << high_priority.size()
          << " sched: " << scheduler.request_count() << " ]"
          << dendl;
 dout(30) << __func__ << " mClockClients: "
          << scheduler
          << dendl;
 dout(30) << __func__ << " mClockQueues: { "
          << display_queues() << " }"
          << dendl;
}

void mClockScheduler::enqueue_front(OpSchedulerItem&& item)
{
  unsigned priority = item.get_priority();
  auto id = get_scheduler_id(item);

  if (SchedulerClass::immediate == id.class_id) {
    enqueue_high(immediate_class_priority, std::move(item), true);
  } else if (priority >= cutoff_priority) {
    enqueue_high(priority, std::move(item), true);
  } else {
    // mClock does not support enqueue at front, so we use
    // the high queue with priority 0
    enqueue_high(0, std::move(item), true);
  }
}

void mClockScheduler::enqueue_high(unsigned priority,
                                   OpSchedulerItem&& item,
				   bool front)
{
  if (front) {
    high_priority[priority].push_back(std::move(item));
  } else {
    high_priority[priority].push_front(std::move(item));
  }

  scheduler_id_t id = scheduler_id_t {
    SchedulerClass::immediate,
    client_profile_id_t()
  };
  mclock_conf.get_mclock_counter(id);
}

WorkItem mClockScheduler::dequeue()
{
  if (!high_priority.empty()) {
    auto iter = high_priority.begin();
    // invariant: high_priority entries are never empty
    ceph_assert(!iter->second.empty());
    WorkItem ret{std::move(iter->second.back())};
    iter->second.pop_back();
    if (iter->second.empty()) {
      // maintain invariant, high priority entries are never empty
      high_priority.erase(iter);
    }
    ceph_assert(std::get_if<OpSchedulerItem>(&ret));

    scheduler_id_t id = scheduler_id_t {
      SchedulerClass::immediate,
      client_profile_id_t()
    };
    mclock_conf.put_mclock_counter(id);
    return ret;
  } else {
    mclock_queue_t::PullReq result = scheduler.pull_request();
    if (result.is_future()) {
      return result.getTime();
    } else if (result.is_none()) {
      ceph_assert(
	0 == "Impossible, must have checked empty() first");
      return {};
    } else {
      ceph_assert(result.is_retn());

      auto &retn = result.get_retn();
      mclock_conf.put_mclock_counter(retn.client);
      return std::move(*retn.request);
    }
  }
}

std::string mClockScheduler::display_queues() const
{
  std::ostringstream out;
  scheduler.display_queues(out);
  return out.str();
}

}
