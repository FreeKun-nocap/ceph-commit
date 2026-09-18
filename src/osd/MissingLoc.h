// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <map>
#include <set>

#include "OSDMap.h"
#include "common/HBHandle.h"
#include "common/ceph_context.h"
#include "common/dout.h"
#include "osd_types.h"

class MissingLoc {
 public:

  class MappingInfo {
  public:
    virtual const std::set<pg_shard_t> &get_upset() const = 0;
    virtual bool is_ec_pg() const = 0;
    virtual int get_pg_size() const = 0;
    virtual ~MappingInfo() {}
  };

  // a loc_count indicates how many locations we know in each of
  // these distinct sets
  struct loc_count_t {
    int up = 0;        //< up
    int other = 0;    //< other

    friend bool operator<(const loc_count_t& l,
			  const loc_count_t& r) {
      return (l.up < r.up ||
	      (l.up == r.up &&
	       (l.other < r.other)));
    }
    friend std::ostream& operator<<(std::ostream& out, const loc_count_t& l) {
      ceph_assert(l.up >= 0);
      ceph_assert(l.other >= 0);
      return out << "(" << l.up << "+" << l.other << ")";
    }
  };


  using missing_by_count_t = std::map<shard_id_t, std::map<loc_count_t,int>>;
 private:
  loc_count_t _get_count(const std::set<pg_shard_t> &shards) {
    loc_count_t r;
    for (auto s : shards) {
      if (mapping_info->get_upset().count(s)) {
	r.up++;
      } else {
	r.other++;
      }
    }
    return r;
  }

  std::map<hobject_t, pg_missing_item> needs_recovery_map;
  std::map<hobject_t, std::set<pg_shard_t> > missing_loc;
  std::set<pg_shard_t> missing_loc_sources;

  // for every entry in missing_loc, we count how many of each type of shard we have,
  // and maintain totals here.  The sum of the values for this std::map will always equal
  // missing_loc.size().
  missing_by_count_t missing_by_count;

  void pgs_by_shard_id(
    const std::set<pg_shard_t>& s,
    std::map<shard_id_t, std::set<pg_shard_t> >& pgsbs) {
    if (mapping_info->is_ec_pg()) {
      int num_shards = mapping_info->get_pg_size();
      // For completely missing shards initialize with empty std::set<pg_shard_t>
      for (int i = 0 ; i < num_shards ; ++i) {
	shard_id_t shard(i);
	pgsbs[shard];
      }
      for (auto pgs: s)
	pgsbs[pgs.shard].insert(pgs);
    } else {
      pgsbs[shard_id_t::NO_SHARD] = s;
    }
  }

  void _inc_count(const std::set<pg_shard_t>& s) {
    std::map< shard_id_t, std::set<pg_shard_t> > pgsbs;
    pgs_by_shard_id(s, pgsbs);
    for (auto shard: pgsbs)
      ++missing_by_count[shard.first][_get_count(shard.second)];
  }
  void _dec_count(const std::set<pg_shard_t>& s) {
    std::map< shard_id_t, std::set<pg_shard_t> > pgsbs;
    pgs_by_shard_id(s, pgsbs);
    for (auto shard: pgsbs) {
      auto p = missing_by_count[shard.first].find(_get_count(shard.second));
      ceph_assert(p != missing_by_count[shard.first].end());
      if (--p->second == 0) {
	missing_by_count[shard.first].erase(p);
      }
    }
  }

  spg_t pgid;
  MappingInfo *mapping_info;
  DoutPrefixProvider *dpp;
  CephContext *cct;
  std::set<pg_shard_t> empty_set;
 public:
  boost::scoped_ptr<IsPGReadablePredicate> is_readable;
  boost::scoped_ptr<IsPGRecoverablePredicate> is_recoverable;
  explicit MissingLoc(
    spg_t pgid,
    MappingInfo *mapping_info,
    DoutPrefixProvider *dpp,
    CephContext *cct)
    : pgid(pgid), mapping_info(mapping_info), dpp(dpp), cct(cct) { }
  void set_backend_predicates(
    IsPGReadablePredicate *_is_readable,
    IsPGRecoverablePredicate *_is_recoverable) {
    is_readable.reset(_is_readable);
    is_recoverable.reset(_is_recoverable);
  }
  const IsPGRecoverablePredicate &get_recoverable_predicate() const {
    return *is_recoverable;
  }
  std::ostream& gen_prefix(std::ostream& out) const {
    return dpp->gen_prefix(out);
  }
  bool needs_recovery(
    const hobject_t &hoid,
    eversion_t *v = 0) const {
    std::map<hobject_t, pg_missing_item>::const_iterator i =
      needs_recovery_map.find(hoid);
    if (i == needs_recovery_map.end())
      return false;
    if (v)
      *v = i->second.need;
    return true;
  }
  bool is_deleted(const hobject_t &hoid) const {
    auto i = needs_recovery_map.find(hoid);
    if (i == needs_recovery_map.end())
      return false;
    return i->second.is_delete();
  }
  bool is_unfound(const hobject_t &hoid) const {
    auto it = needs_recovery_map.find(hoid);
    if (it == needs_recovery_map.end()) {
      return false;
    }
    if (it->second.is_delete()) {
      return false;
    }
    auto mit = missing_loc.find(hoid);
    return mit == missing_loc.end() || !(*is_recoverable)(mit->second);
  }
  bool readable_with_acting(
    const hobject_t &hoid,
    const std::set<pg_shard_t> &acting,
    eversion_t* v = 0) const;
  uint64_t num_unfound() const {
    uint64_t ret = 0;
    for (std::map<hobject_t, pg_missing_item>::const_iterator i =
	   needs_recovery_map.begin();
	 i != needs_recovery_map.end();
	 ++i) {
      if (i->second.is_delete())
	continue;
      auto mi = missing_loc.find(i->first);
      if (mi == missing_loc.end() || !(*is_recoverable)(mi->second))
	++ret;
    }
    return ret;
  }

  bool have_unfound() const {
    for (std::map<hobject_t, pg_missing_item>::const_iterator i =
	   needs_recovery_map.begin();
	 i != needs_recovery_map.end();
	 ++i) {
      if (i->second.is_delete())
	continue;
      auto mi = missing_loc.find(i->first);
      if (mi == missing_loc.end() || !(*is_recoverable)(mi->second))
	return true;
    }
    return false;
  }
  void clear() {
    needs_recovery_map.clear();
    missing_loc.clear();
    missing_loc_sources.clear();
    missing_by_count.clear();
  }

  void add_location(const hobject_t &hoid, pg_shard_t location) {
    auto p = missing_loc.find(hoid);
    if (p == missing_loc.end()) {
      p = missing_loc.emplace(hoid, std::set<pg_shard_t>()).first;
    } else {
      _dec_count(p->second);
    }
    p->second.insert(location);
    _inc_count(p->second);
  }
  void remove_location(const hobject_t &hoid, pg_shard_t location) {
    auto p = missing_loc.find(hoid);
    if (p != missing_loc.end()) {
      _dec_count(p->second);
      p->second.erase(location);
      if (p->second.empty()) {
	missing_loc.erase(p);
      } else {
	_inc_count(p->second);
      }
    }
  }

  void clear_location(const hobject_t &hoid) {
    auto p = missing_loc.find(hoid);
    if (p != missing_loc.end()) {
      _dec_count(p->second);
      missing_loc.erase(p);
    }
  }

  /**
   * 将一个参与本轮激活的 shard 的 missing 集合并入 needs_recovery_map。
   *
   * activate() 会对 primary 和所有 acting/recovery/backfill 副本调用本函数。
   * needs_recovery_map 因此表示当前 PG 中至少有一个参与 shard 缺失的对象，
   * 是后续建立对象恢复来源（missing_loc）的输入。
   * 这里仅汇总“哪些对象需要恢复”及其目标版本，不判断哪个 shard 可以提供对象。
   *
   * 同一对象可能在多个 shard 的 missing 集合中出现，但它们必须请求相同的 need 版本；
   * 否则表示已归一化的 PG 日志状态彼此矛盾，不能继续恢复。
   *
   * @param missing 一个参与 shard 的 pg_missing_t
   */
  void add_active_missing(const pg_missing_t &missing) {
    for (std::map<hobject_t, pg_missing_item>::const_iterator i =
	   missing.get_items().begin();
	 i != missing.get_items().end();
	 ++i) {
      std::map<hobject_t, pg_missing_item>::const_iterator j =
	needs_recovery_map.find(i->first);
      if (j == needs_recovery_map.end()) {
	// 首次看到该对象，登记其恢复目标版本及已有版本。
	needs_recovery_map.insert(*i);
      } else {
	if (i->second.need != j->second.need) {
	  // 同一 PG 内的缺失对象必须收敛到相同的目标版本；
	  // 否则无法确定应从哪个版本恢复，说明前面的日志归一化结果不一致。
	  lgeneric_dout(cct, 0) << this << " " << pgid << " unexpected need for "
				<< i->first << " have " << j->second
				<< " tried to add " << i->second << dendl;
	  ceph_assert(0 == "unexpected need for missing item");
	}
      }
    }
  }

  void add_missing(const hobject_t &hoid, eversion_t need, eversion_t have, bool is_delete=false) {
    needs_recovery_map[hoid] = pg_missing_item(need, have, is_delete);
  }
  void revise_need(const hobject_t &hoid, eversion_t need) {
    auto it = needs_recovery_map.find(hoid);
    ceph_assert(it != needs_recovery_map.end());
    it->second.need = need;
  }

  /// Adds info about a possible recovery source
  bool add_source_info(
    pg_shard_t source,           ///< [in] source
    const pg_info_t &oinfo,      ///< [in] info
    const pg_missing_t &omissing, ///< [in] (optional) missing
    HBHandle *handle             ///< [in] ThreadPool handle
    ); ///< @return whether a new object location was discovered

  /// Adds recovery sources in batch
  void add_batch_sources_info(
    const std::set<pg_shard_t> &sources,  ///< [in] a std::set of resources which can be used for all objects
    HBHandle *handle  ///< [in] ThreadPool handle
    );

  /// Uses osdmap to update structures for now down sources
  void check_recovery_sources(const OSDMapRef& osdmap);

  /// Remove stray from recovery sources
  void remove_stray_recovery_sources(pg_shard_t stray);

  /// Call when hoid is no longer missing in acting std::set
  void recovered(const hobject_t &hoid) {
    needs_recovery_map.erase(hoid);
    auto p = missing_loc.find(hoid);
    if (p != missing_loc.end()) {
      _dec_count(p->second);
      missing_loc.erase(p);
    }
  }

  /// Call to update structures for hoid after a change
  void rebuild(
    const hobject_t &hoid,
    pg_shard_t self,
    const std::set<pg_shard_t> &to_recover,
    const pg_info_t &info,
    const pg_missing_t &missing,
    const std::map<pg_shard_t, pg_missing_t> &pmissing,
    const std::map<pg_shard_t, pg_info_t> &pinfo) {
    recovered(hoid);
    std::optional<pg_missing_item> item;
    auto miter = missing.get_items().find(hoid);
    if (miter != missing.get_items().end()) {
      item = miter->second;
    } else {
      for (auto &&i: to_recover) {
	if (i == self)
	  continue;
	auto pmiter = pmissing.find(i);
	ceph_assert(pmiter != pmissing.end());
	miter = pmiter->second.get_items().find(hoid);
	if (miter != pmiter->second.get_items().end()) {
	  item = miter->second;
	  break;
	}
      }
    }
    if (!item)
      return; // recovered!

    needs_recovery_map[hoid] = *item;
    if (item->is_delete())
      return;
    auto mliter =
      missing_loc.emplace(hoid, std::set<pg_shard_t>()).first;
    ceph_assert(info.last_backfill.is_max());
    ceph_assert(info.last_update >= item->need);
    if (!missing.is_missing(hoid))
      mliter->second.insert(self);
    for (auto &&i: pmissing) {
      if (i.first == self)
	continue;
      auto pinfoiter = pinfo.find(i.first);
      ceph_assert(pinfoiter != pinfo.end());
      if (item->need <= pinfoiter->second.last_update &&
	  hoid <= pinfoiter->second.last_backfill &&
	  !i.second.is_missing(hoid))
	mliter->second.insert(i.first);
    }
    _inc_count(mliter->second);
  }

  const std::set<pg_shard_t> &get_locations(const hobject_t &hoid) const {
    auto it = missing_loc.find(hoid);
    return it == missing_loc.end() ? empty_set : it->second;
  }
  const std::map<hobject_t, std::set<pg_shard_t>> &get_missing_locs() const {
    return missing_loc;
  }
  const std::map<hobject_t, pg_missing_item> &get_needs_recovery() const {
    return needs_recovery_map;
  }

  const missing_by_count_t &get_missing_by_count() const {
    return missing_by_count;
  }
};
