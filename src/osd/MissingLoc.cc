// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "MissingLoc.h"

#define dout_context cct
#undef dout_prefix
#define dout_prefix (gen_prefix(*_dout))
#define dout_subsys ceph_subsys_osd

using std::set;

bool MissingLoc::readable_with_acting(
  const hobject_t &hoid,
  const set<pg_shard_t> &acting,
  eversion_t* v) const {
  if (!needs_recovery(hoid, v))
    return true;
  if (is_deleted(hoid))
    return false;
  auto missing_loc_entry = missing_loc.find(hoid);
  if (missing_loc_entry == missing_loc.end())
    return false;
  const set<pg_shard_t> &locs = missing_loc_entry->second;
  ldout(cct, 10) << __func__ << ": locs:" << locs << dendl;
  set<pg_shard_t> have_acting;
  for (auto i = locs.begin(); i != locs.end(); ++i) {
    if (acting.count(*i))
      have_acting.insert(*i);
  }
  return (*is_readable)(have_acting);
}

void MissingLoc::add_batch_sources_info(
  const set<pg_shard_t> &sources,
  HBHandle *handle)
{
  ldout(cct, 10) << __func__ << ": adding sources in batch "
		     << sources.size() << dendl;
  unsigned loop = 0;
  bool sources_updated = false;
  for (auto i = needs_recovery_map.begin();
      i != needs_recovery_map.end();
      ++i) {
    if (handle && ++loop >= cct->_conf->osd_loop_before_reset_tphandle) {
      handle->reset_tp_timeout();
      loop = 0;
    }
    if (i->second.is_delete())
      continue;

    auto p = missing_loc.find(i->first);
    if (p == missing_loc.end()) {
      p = missing_loc.emplace(i->first, set<pg_shard_t>()).first;
    } else {
      _dec_count(p->second);
    }
    missing_loc[i->first].insert(sources.begin(), sources.end());
    _inc_count(p->second);

    if (!sources_updated) {
      missing_loc_sources.insert(sources.begin(), sources.end());
      sources_updated = true;
    }
  }
}

/**
 * 判断某个候选来源 shard 上是否持有 needs_recovery_map 中各对象的副本，
 * 并把通过检查的对象位置登记进 missing_loc。
 *
 * activate() 会为每个携带日志/缺失信息的 peer 调用本函数；
 * 与 add_active_missing 只汇总“缺什么”相对，本函数回答“从谁那里恢复”。
 * 对每个待恢复对象依次排除不能作为来源的情形（见下），全部通过才登记 fromosd。
 *
 * 四个排除条件按代价从低到高排列：
 * 1. 删除型缺失不需要数据来源；
 * 2. 对端 last_update 低于目标版本，说明其日志没跟上、必无该版本数据；
 * 3. 对象位于对端 last_backfill 之后的未定义区间，无法确认其是否存在；
 * 4. 对端自己的 missing 集合包含该对象。
 *
 * 注意对象可能已有其他来源：此时先 _dec_count 旧集合再插入，
 * 保证 missing_by_count（按持有 shard 数的索引）与 missing_loc 一致。
 *
 * @param fromosd 候选来源 shard
 * @param oinfo 对端上报的 pg_info_t（last_update/last_backfill 用于排除）
 * @param omissing 对端上报的 pg_missing_t（非空时用于排除对端也缺失的对象）
 * @return 是否至少为一个对象找到了新来源
 */
bool MissingLoc::add_source_info(
  pg_shard_t fromosd,
  const pg_info_t &oinfo,
  const pg_missing_t &omissing,
  HBHandle *handle)
{
  bool found_missing = false;
  unsigned loop = 0;
  bool sources_updated = false;
  // found items?

  auto start = ceph::coarse_mono_clock::now();
  bool suppress_logging = false;
  for (auto p = needs_recovery_map.begin();
       p != needs_recovery_map.end();
       ++p) {
    const hobject_t &soid(p->first);
    eversion_t need = p->second.need;
    // 遍历可能覆盖海量对象，周期性重置线程池超时并按耗时关闭调试日志。
    if (++loop >= cct->_conf->osd_loop_before_reset_tphandle) {
      if (handle) {
	handle->reset_tp_timeout();
      }
      auto now = ceph::coarse_mono_clock::now();
      if (std::chrono::duration<double>(now - start).count() > 0.5) {
	// Suppress logging if this function has be running more than
	// 0.5 seconds
	suppress_logging = true;
      }
      loop = 0;
    }
    // 删除型缺失只需在各副本上执行删除，无需寻找数据来源。
    if (p->second.is_delete()) {
      if (!suppress_logging) {
        ldout(cct, 10) << __func__ << " " << soid
                       << " delete, ignoring source" << dendl;
      }
      continue;
    }
    // 对端日志落后于目标版本：其上不可能存在 need 版本的数据。
    if (oinfo.last_update < need) {
      if (!suppress_logging) {
        ldout(cct, 10) << "search_for_missing " << soid << " " << need
                       << " also missing on osd." << fromosd
                       << " (last_update " << oinfo.last_update
                       << " < needed " << need << ")" << dendl;
      }
      continue;
    }
    // 对象落在对端 backfill 进度之后的区间，其对端是否有该对象未知。
    if (p->first >= oinfo.last_backfill) {
      // FIXME: this is _probably_ true, although it could conceivably
      // be in the undefined region!  Hmm!
      if (!suppress_logging) {
        ldout(cct, 10) << "search_for_missing " << soid << " " << need
                       << " also missing on osd." << fromosd
                       << " (past last_backfill " << oinfo.last_backfill
                       << ")" << dendl;
      }
      continue;
    }
    // 对端自身也缺失该对象，不能作为来源。
    if (omissing.is_missing(soid)) {
      if (!suppress_logging) {
        ldout(cct, 10) << "search_for_missing " << soid << " " << need
                       << " also missing on osd." << fromosd << dendl;
      }
      continue;
    }

    if (!suppress_logging) {
      ldout(cct, 10) << "search_for_missing " << soid << " " << need
                     << " is on osd." << fromosd << dendl;
    }

    {
      // 登记来源。对象已有来源集合时先减计数，替换后统一重新计数，
      // 以维护 missing_by_count 与 missing_loc 的一致性。
      auto p = missing_loc.find(soid);
      if (p == missing_loc.end()) {
        // 情况 A:该对象第一次找到来源 → 建空条目
	p = missing_loc.emplace(soid, set<pg_shard_t>()).first;
      } else {
        // 情况 B:已有来源集合 → 先把旧集合从计数索引中减掉
	_dec_count(p->second);
      }
      p->second.insert(fromosd);  // 两种情况统一:把 fromosd 加入来源集合
      _inc_count(p->second);  // 重新计入 missing_by_count
    }

    // missing_loc_sources 记录曾提供过来源的全部 shard，
    // 供 check_recovery_sources() 在其 down 掉时清除相关位置。
    if (!sources_updated) {
      missing_loc_sources.insert(fromosd);
      sources_updated = true;
    }
    found_missing = true;
  }

  ldout(cct, 20) << "needs_recovery_map missing " << needs_recovery_map
                 << dendl;
  return found_missing;
}

void MissingLoc::check_recovery_sources(const OSDMapRef& osdmap)
{
  set<pg_shard_t> now_down;
  for (auto p = missing_loc_sources.begin();
       p != missing_loc_sources.end();
       ) {
    if (osdmap->is_up(p->osd)) {
      ++p;
      continue;
    }
    ldout(cct, 10) << __func__ << " source osd." << *p << " now down" << dendl;
    now_down.insert(*p);
    missing_loc_sources.erase(p++);
  }

  if (now_down.empty()) {
    ldout(cct, 10) << __func__ << " no source osds (" << missing_loc_sources << ") went down" << dendl;
  } else {
    ldout(cct, 10) << __func__ << " sources osds " << now_down << " now down, remaining sources are "
		       << missing_loc_sources << dendl;

    // filter missing_loc
    auto p = missing_loc.begin();
    while (p != missing_loc.end()) {
      auto q = p->second.begin();
      bool changed = false;
      while (q != p->second.end()) {
	if (now_down.count(*q)) {
	  if (!changed) {
	    changed = true;
	    _dec_count(p->second);
	  }
	  p->second.erase(q++);
	} else {
	  ++q;
	}
      }
      if (p->second.empty()) {
	missing_loc.erase(p++);
      } else {
	if (changed) {
	  _inc_count(p->second);
	}
	++p;
      }
    }
  }
}

void MissingLoc::remove_stray_recovery_sources(pg_shard_t stray)
{
  ldout(cct, 10) << __func__ << " remove osd " << stray << " from missing_loc" << dendl;
  // filter missing_loc
  auto p = missing_loc.begin();
  while (p != missing_loc.end()) {
    auto q = p->second.begin();
    bool changed = false;
    while (q != p->second.end()) {
      if (*q == stray) {
        if (!changed) {
          changed = true;
          _dec_count(p->second);
        }
        p->second.erase(q++);
      } else {
        ++q;
      }
    }
    if (p->second.empty()) {
      missing_loc.erase(p++);
    } else {
      if (changed) {
        _inc_count(p->second);
      }
      ++p;
    }
  }
  // filter missing_loc_sources
  for (auto p = missing_loc_sources.begin(); p != missing_loc_sources.end();) {
    if (*p != stray) {
      ++p;
      continue;
    }
    ldout(cct, 10) << __func__ << " remove osd" << stray << " from missing_loc_sources" << dendl;
    missing_loc_sources.erase(p++);
  }
}
