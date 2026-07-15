// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_CEPHCONTEXT_H
#define CEPH_CEPHCONTEXT_H

#include <atomic>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <string_view>
#include <typeinfo>
#include <typeindex>
#include <vector>

#include <boost/intrusive_ptr.hpp>

#include "include/any.h"
#include "include/common_fwd.h"
#include "include/compat.h"

#include "common/cmdparse.h"
#include "common/code_environment.h"
#include "msg/msg_types.h"
#ifdef WITH_CRIMSON
#include "crimson/common/config_proxy.h"
#else
#include "common/config_proxy.h"
#include "include/spinlock.h"
#endif


#include "crush/CrushLocation.h"

#ifdef HAVE_BREAKPAD
namespace google_breakpad {
  class ExceptionHandler;
}
#endif

class AdminSocket;
class AdminSocketHook;
class CryptoHandler;
class CryptoRandom;
class MonMap;

namespace ceph::common {
  class CephContextServiceThread;
  class CephContextObs;
  class CephContextHook;
}

namespace ceph {
  class PluginRegistry;
  class HeartbeatMap;
  namespace logging {
    class Log;
    class SubsystemMap;
  }
}

#ifdef WITH_CRIMSON
namespace crimson::common {
class CephContext {
public:
  CephContext();
  CephContext(uint32_t,
	      code_environment_t=CODE_ENVIRONMENT_UTILITY,
	      int = 0)
    : CephContext{}
  {}
  CephContext(CephContext&&) = default;
  ~CephContext();

  uint32_t get_module_type() const;
  bool check_experimental_feature_enabled(const std::string& feature) {
    // everything crimson is experimental...
    return true;
  }
  ceph::PluginRegistry* get_plugin_registry() {
    return _plugin_registry;
  }
  CryptoRandom* random() const;
  PerfCountersCollectionImpl* get_perfcounters_collection();
  crimson::common::ConfigProxy& _conf;
  crimson::common::PerfCountersCollection& _perf_counters_collection;
  CephContext* get();
  void put();
private:
  std::unique_ptr<CryptoRandom> _crypto_random;
  unsigned nref = 1;
  ceph::PluginRegistry* _plugin_registry;
};
}
#else
#ifdef __cplusplus
namespace ceph::common {
#endif
/* A CephContext represents the context held by a single library user.
 * There can be multiple CephContexts in the same process.
 *
 * For daemons and utility programs, there will be only one CephContext.  The
 * CephContext contains the configuration, the dout object, and anything else
 * that you might want to pass to libcommon with every function call.
 */
/**
 * CephContext —— Ceph 进程的核心上下文对象（通常缩写为 cct）
 *
 * 每个 Ceph 进程启动时创建一个 CephContext 实例，它承载全部基础设施：
 *   - _conf      : 全局配置（ConfigProxy）
 *   - _log       : 日志系统（Log）
 *   - _module_type: 当前模块类型（OSD / MON / MDS / CLIENT 等）
 *   - 加密、性能计数器、Admin Socket、插件注册表、心跳、崩溃处理等
 *
 * 生命周期：引用计数管理（get() / put()），当引用计数归零时自动析构。
 */
class CephContext {
public:
  CephContext(uint32_t module_type_,
              enum code_environment_t code_env=CODE_ENVIRONMENT_UTILITY,
              int init_flags_ = 0);
  struct create_options {
    enum code_environment_t code_env=CODE_ENVIRONMENT_UTILITY;
    int init_flags = 0;
    // 自定义日志创建函数（若不指定则使用默认的 Log 实现）
    std::function<ceph::logging::Log* (const ceph::logging::SubsystemMap *)> create_log;
  };
  CephContext(uint32_t module_type_,
	      const create_options& options);
  // 禁止拷贝 / 移动 —— CephContext 是全局唯一实例，不应被复制
  CephContext(const CephContext&) = delete;
  CephContext& operator =(const CephContext&) = delete;
  CephContext(CephContext&&) = delete;
  CephContext& operator =(CephContext&&) = delete;

  bool _finished = false;       // 标记 CephContext 是否已完成析构（供断言使用）
  ~CephContext();

private:
  std::atomic<unsigned> nref;   // 引用计数（原子操作，线程安全）
public:
  CephContext *get() {          // 增加引用计数，返回 this
    ++nref;
    return this;
  }
  void put();                   // 减少引用计数，归零时调用 delete this

  ConfigProxy _conf;            // 全局配置代理（读写运行时配置项）
  ceph::logging::Log *_log;     // 日志后端（输出到文件 / syslog / stderr 等）
#ifdef HAVE_BREAKPAD
  std::unique_ptr<google_breakpad::ExceptionHandler> _ex_handler;
  static_assert(sizeof(std::unique_ptr<google_breakpad::ExceptionHandler>) == sizeof(std::unique_ptr<char>));
#else
  // Reserve the space for the case when part of ceph is compiled with and other without HAVE_BREAKPAD
  std::unique_ptr<char> _ex_handler;
#endif

  /* 初始化加密子系统（内部引用计数，可多次调用） */
  void init_crypto();

  /// 关闭加密子系统（与 init_crypto 配对调用）
  void shutdown_crypto();

  /* 启动 CephContext 的后台服务线程（处理 SIGHUP 日志重开等） */
  void start_service_thread();

  /* 重新打开日志文件（SIGHUP 信号触发，用于日志轮转） */
  void reopen_logs();

  /* 获取当前模块类型（返回 CEPH_ENTITY_TYPE_OSD / MON / MDS / CLIENT 等） */
  uint32_t get_module_type() const;

  // 仅供测试使用！修改模块类型
  void _set_module_type(uint32_t t) {
    _module_type = t;
  }

  void set_init_flags(int flags);
  int get_init_flags() const;

  /* 获取性能计数器集合（用于采集运行时指标） */
  PerfCountersCollection *get_perfcounters_collection();

  ceph::HeartbeatMap *get_heartbeat_map() {
    return _heartbeat_map;
  }

  /**
   * 获取 Admin Socket（管理接口）。
   * 每个 CephContext 都有一个 Admin Socket 实例，不会返回 NULL。
   */
  AdminSocket *get_admin_socket();

  /**
   * 处理来自 admin socket 的命令（daemon/admin_socket 机制）
   */
  int do_command(std::string_view command, const cmdmap_t& cmdmap,
		 Formatter *f,
		 std::ostream& errss,
		 ceph::bufferlist *out);
  int _do_command(std::string_view command, const cmdmap_t& cmdmap,
		  Formatter *f,
		  std::ostream& errss,
		  ceph::bufferlist *out);

  static constexpr std::size_t largest_singleton = 8 * 72;

  /**
   * 获取或创建单例对象（按名称 + 类型索引）。
   * 供各子系统存储全局唯一实例，避免使用裸全局变量。
   * @param name         单例名称
   * @param drop_on_fork fork 时是否销毁重建（处理 fork 后的线程安全问题）
   */
  template<typename T, typename... Args>
  T& lookup_or_create_singleton_object(std::string_view name,
				       bool drop_on_fork,
				       Args&&... args) {
    static_assert(sizeof(T) <= largest_singleton,
		  "Please increase largest_singleton.");
    std::lock_guard lg(associated_objs_lock);
    std::type_index type = typeid(T);

    auto i = associated_objs.find(std::make_pair(name, type));
    if (i == associated_objs.cend()) {
      if (drop_on_fork) {
	associated_objs_drop_on_fork.insert(std::string(name));
      }
      i = associated_objs.emplace_hint(
	i,
	std::piecewise_construct,
	std::forward_as_tuple(name, type),
	std::forward_as_tuple(std::in_place_type<T>,
			      std::forward<Args>(args)...));
    }
    return ceph::any_cast<T&>(i->second);
  }

  /**
   * 获取加密处理器（按类型索引，如 AES）
   */
  CryptoHandler *get_crypto_handler(int type);

  CryptoRandom* random() const { return _crypto_random.get(); }  // 安全随机数生成器

  /// 检查实验性功能是否已启用，若未启用在日志中发出警告
  bool check_experimental_feature_enabled(const std::string& feature);
  bool check_experimental_feature_enabled(const std::string& feature,
					  std::ostream *message);

  ceph::PluginRegistry *get_plugin_registry() {  // 插件注册表（动态加载 .so）
    return _plugin_registry;
  }

  // 设置/获取进程的 uid/gid（用于权限降级）
  void set_uid_gid(uid_t u, gid_t g) {
    _set_uid = u;
    _set_gid = g;
  }
  uid_t get_set_uid() const {
    return _set_uid;
  }
  gid_t get_set_gid() const {
    return _set_gid;
  }

  void set_uid_gid_strings(const std::string &u, const std::string &g) {
    _set_uid_string = u;
    _set_gid_string = g;
  }
  std::string get_set_uid_string() const {
    return _set_uid_string;
  }
  std::string get_set_gid_string() const {
    return _set_gid_string;
  }

  class ForkWatcher {
   public:
    virtual ~ForkWatcher() {}
    virtual void handle_pre_fork() = 0;   // fork() 之前回调
    virtual void handle_post_fork() = 0;  // fork() 之后回调（在子进程中）
  };

  // 注册 fork 观察者（多线程 fork 时用于释放/重建锁等资源）
  void register_fork_watcher(ForkWatcher *w) {
    std::lock_guard lg(_fork_watchers_lock);
    _fork_watchers.push_back(w);
  }

  void drop_temp_messenger_obj();
  void notify_pre_fork();   // 通知所有 ForkWatcher：即将 fork
  void notify_post_fork();  // 通知所有 ForkWatcher：fork 完成（子进程）

  /**
   * update CephContext with a copy of the passed in MonMap mon addrs
   *
   * @param mm MonMap to extract and update mon addrs
   */
  void set_mon_addrs(const MonMap& mm);
  void set_mon_addrs(const std::vector<entity_addrvec_t>& in) {
    auto ptr = std::make_shared<std::vector<entity_addrvec_t>>(in);
#ifdef __cpp_lib_atomic_shared_ptr
    _mon_addrs.store(std::move(ptr), std::memory_order_relaxed);
#else
    atomic_store_explicit(&_mon_addrs, std::move(ptr), std::memory_order_relaxed);
#endif
  }
  std::shared_ptr<std::vector<entity_addrvec_t>> get_mon_addrs() const {
#ifdef __cpp_lib_atomic_shared_ptr
    auto ptr = _mon_addrs.load(std::memory_order_relaxed);
#else
    auto ptr = atomic_load_explicit(&_mon_addrs, std::memory_order_relaxed);
#endif
    return ptr;
  }

private:

  /* 停止并 join 后台服务线程 */
  void join_service_thread();

  uint32_t _module_type;    // 模块类型（OSD / MON / MDS / CLIENT 等）

  int _init_flags;          // 初始化标志位

  uid_t _set_uid;           // 权限降级目标 uid
  gid_t _set_gid;           // 权限降级目标 gid
  std::string _set_uid_string;
  std::string _set_gid_string;

  int _crypto_inited;       // 加密初始化引用计数

#ifdef __cpp_lib_atomic_shared_ptr
  std::atomic<std::shared_ptr<std::vector<entity_addrvec_t>>> _mon_addrs;
#else
  std::shared_ptr<std::vector<entity_addrvec_t>> _mon_addrs;
#endif

  /* 后台服务线程。
   * SIGHUP 信号会唤醒此线程，然后重新打开日志文件 */
  friend class CephContextServiceThread;
  CephContextServiceThread *_service_thread;

  using md_config_obs_t = ceph::md_config_obs_impl<ConfigProxy>;

  md_config_obs_t *_log_obs;  // 日志相关的配置变更观察者

  /* 此 CephContext 关联的 Admin Socket（管理接口） */
  AdminSocket *_admin_socket;

  /* 保护服务线程创建/销毁的锁 */
  ceph::spinlock _service_thread_lock;

  /* 性能计数器集合（采集运行时性能指标） */
  PerfCountersCollection *_perf_counters_collection;

  md_config_obs_t *_perf_counters_conf_obs;

  CephContextHook *_admin_hook;

  ceph::HeartbeatMap *_heartbeat_map;  // 心跳映射表（检测工作线程卡死）

  ceph::spinlock associated_objs_lock; // 保护单例对象 map 的锁

  // 单例对象 map：按 (名称, 类型) 索引，存储 immobile_any 对象
  struct associated_objs_cmp {
    using is_transparent = std::true_type;
    template<typename T, typename U>
    bool operator ()(const std::pair<T, std::type_index>& l,
		     const std::pair<U, std::type_index>& r) const noexcept {
      return ((l.first < r.first)  ||
	      (l.first == r.first && l.second < r.second));
    }
  };

  std::map<std::pair<std::string, std::type_index>,
	   ceph::immobile_any<largest_singleton>,
	   associated_objs_cmp> associated_objs;
  std::set<std::string> associated_objs_drop_on_fork;

  // fork 观察者列表锁
  ceph::spinlock _fork_watchers_lock;
  std::vector<ForkWatcher*> _fork_watchers;

  // ===== 加密子系统 =====
  CryptoHandler *_crypto_none;  // 无加密处理器
  CryptoHandler *_crypto_aes;   // AES 加密处理器
  std::unique_ptr<CryptoRandom> _crypto_random;  // 安全随机数生成器

  // ===== 实验性功能 =====
  CephContextObs *_cct_obs;
  ceph::spinlock _feature_lock;
  std::set<std::string> _experimental_features;  // 已启用的实验性功能集合

  ceph::PluginRegistry* _plugin_registry;  // 插件注册表（动态加载共享库）
#ifdef CEPH_DEBUG_MUTEX
  md_config_obs_t *_lockdep_obs;           // 锁依赖检测观察者
#endif

  std::unique_ptr<AdminSocketHook> _msgr_hook;
  ceph::mutex _msgr_hook_lock = ceph::make_mutex("CephContext::msgr_hook");
public:
  TOPNSPC::crush::CrushLocation crush_location;
  void modify_msgr_hook(std::function<AdminSocketHook*(void)> create,
			std::function<void(AdminSocketHook*)> add);
private:

  enum {
    l_cct_first,
    l_cct_total_workers,
    l_cct_unhealthy_workers,
    l_cct_last
  };
  enum {
    l_mempool_first = 873222,
    l_mempool_bytes,
    l_mempool_items,
    l_mempool_last
  };
  // This is just how PerfCounters indices work, we have a bunch of
  // bare enums all over.
  enum {
    // Picked by grepping for the current highest value and adding 1000
    l_service_first = 1001000,
    l_service_unique_id,
    l_service_last
  };
  PerfCounters *_cct_perf = nullptr;
  PerfCounters* _mempool_perf = nullptr;
  std::vector<std::string> _mempool_perf_names, _mempool_perf_descriptions;
  std::string service_unique_id;
  PerfCounters* _service_perf = nullptr;

  /**
   * Enable the performance counters.
   */
  void _enable_perf_counter();

  /**
   * Disable the performance counter.
   */
  void _disable_perf_counter();

  /**
   * Refresh perf counter values.
   */
  void _refresh_perf_values();

  friend class CephContextObs;
};
#ifdef __cplusplus
}
#endif
#endif	// WITH_CRIMSON

#if !defined(WITH_CRIMSON) && defined(__cplusplus)
namespace ceph::common {
inline void intrusive_ptr_add_ref(CephContext* cct)
{
  cct->get();
}

inline void intrusive_ptr_release(CephContext* cct)
{
  cct->put();
}
}
#endif // !defined(WITH_CRIMSON) && defined(__cplusplus)
#endif
