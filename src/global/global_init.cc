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

#include <filesystem>
#include <memory>
#include <sstream>
#include "acconfig.h"
#ifdef HAVE_BREAKPAD
#include <breakpad/client/linux/handler/exception_handler.h>
#include <breakpad/client/linux/handler/minidump_descriptor.h>
#include <breakpad/google_breakpad/common/minidump_format.h>
#endif
#include "common/async/context_pool.h"
#include "common/ceph_argparse.h"
#include "common/code_environment.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/signal.h"
#include "common/version.h"
#include "erasure-code/ErasureCodePlugin.h"
#include "extblkdev/ExtBlkDevPlugin.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "global/pidfile.h"
#include "global/signal_handler.h"
#include "include/compat.h"
#include "include/str_list.h"
#include "log/Log.h"
#include "mon/MonClient.h"

#ifndef _WIN32
#include <pwd.h>
#include <grp.h>
#endif
#include <errno.h>

#ifdef HAVE_SYS_PRCTL_H
#include <sys/prctl.h>
#endif

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_

namespace fs = std::filesystem;

using std::cerr;
using std::string;

// 将 CephContext 设置到全局变量，方便各处代码无需传参即可访问上下文
static void global_init_set_globals(CephContext *cct)
{
  g_ceph_context = cct;  // 全局 CephContext 指针（全局单例）
  get_process_name(g_process_name, sizeof(g_process_name));  // 获取并缓存进程名（如 "ceph-osd"）
}

static void output_ceph_version()
{
  char buf[1024];
  snprintf(buf, sizeof(buf), "%s, process %s, pid %d",
	   pretty_version_to_str().c_str(),
	   get_process_name_cpp().c_str(), getpid());
  generic_dout(0) << buf << dendl;
}

static const char* c_str_or_null(const std::string &str)
{
  if (str.empty())
    return NULL;
  return str.c_str();
}

static int chown_path(const std::string &pathname, const uid_t owner, const gid_t group,
		      const std::string &uid_str, const std::string &gid_str)
{
  #ifdef _WIN32
  return 0;
  #else

  const char *pathname_cstr = c_str_or_null(pathname);

  if (!pathname_cstr) {
    return 0;
  }

  int r = ::chown(pathname_cstr, owner, group);

  if (r < 0) {
    r = -errno;
    cerr << "warning: unable to chown() " << pathname << " as "
	 << uid_str << ":" << gid_str << ": " << cpp_strerror(r) << std::endl;
  }

  return r;
  #endif
}

/**
 * global_pre_init —— Ceph 全局预初始化（加载配置、启动日志、进入可运行状态前的准备）。
 *
 * @param defaults    调用方传入的额外默认配置（key-value 映射，可为空）
 * @param args        命令行参数（会被修改，已识别的参数会被移除）
 * @param module_type 模块类型（OSD / MON / MDS / CLIENT 等）
 * @param code_env    代码运行环境（守护进程 / 工具 / 库）
 * @param flags       初始化标志位（CINIT_FLAG_*）
 *
 * 执行流程：
 *   1. 把环境变量（CEPH_ARGS 等）合并进 args
 *   2. 第一轮参数解析，提取早期参数（--cluster、--conf、-i、--name 等）
 *   3. common_preinit：创建 CephContext + 基础默认值
 *   4. 设置集群名、全局变量、MON 配置开关
 *   5. 应用调用方传入的 defaults 默认值
 *   6. 解析配置文件（ceph.conf）
 *   7. 环境变量覆盖
 *   8. 命令行参数覆盖（最高优先级）
 *   9. 启动日志系统
 *  10. 处理 --show-config 等展示型参数
 *  11. 输出配置文件解析警告
 */
void global_pre_init(
  const std::map<std::string,std::string> *defaults,
  std::vector < const char* >& args,
  uint32_t module_type, code_environment_t code_env,
  int flags)
{
  std::string conf_file_list;
  std::string cluster = "";

  // 把环境变量中的参数（CEPH_ARGS、CEPH_KEYRING 等）合并到 args 中，
  // 确保环境变量也参与第一轮早期参数解析
  env_to_vec(args);

  // 第一轮参数解析：从命令行中提取 --cluster、--conf/-c、-i、--name、--version 等早期参数
  CephInitParameters iparams = ceph_argparse_early_args(
    args, module_type,
    &cluster, &conf_file_list);

  // 预初始化 CephContext：创建上下文对象 + 设置基础默认值（keyring、admin_socket、日志等）
  CephContext *cct = common_preinit(iparams, code_env, flags); 
  cct->_conf->cluster = cluster;      // 设置集群名（来自 --cluster 参数或默认 "ceph"）
  // 将 cct 和进程名存入全局变量（g_ceph_context、g_process_name），方便全代码访问
  global_init_set_globals(cct);
  auto& conf = cct->_conf;

  // 如果指定了不读默认配置文件 / 不从 MON 拉配置，则关闭 mon config 功能
  if (flags & (CINIT_FLAG_NO_DEFAULT_CONFIG_FILE|
	       CINIT_FLAG_NO_MON_CONFIG)) {
    conf->no_mon_config = true;
  }

  // 应用调用方传入的额外默认值（优先级仍然是最低的 default 级别）
  if (defaults) {
    for (auto& i : *defaults) {
      conf.set_val_default(i.first, i.second);
    }
  }

  // 如果命令行传了 --no-config_file，追加 NO_DEFAULT_CONFIG_FILE 标志
  if (conf.get_val<bool>("no_config_file")) {
    flags |= CINIT_FLAG_NO_DEFAULT_CONFIG_FILE;
  }

  // 解析配置文件（ceph.conf，按搜索路径查找）
  int ret = conf.parse_config_files(c_str_or_null(conf_file_list),
				    &cerr, flags);
  if (ret == -EDOM) {
    // -EDOM = 配置文件内容有语法错误 → 直接退出
    cct->_log->flush();
    cerr << "global_init: error parsing config file." << std::endl;
    _exit(1);
  }
  else if (ret == -ENOENT) {
    // -ENOENT = 没找到配置文件
    if (!(flags & CINIT_FLAG_NO_DEFAULT_CONFIG_FILE)) {
      // 如果用户显式指定了配置文件路径但找不到 → 报错退出
      if (conf_file_list.length()) {
	cct->_log->flush();
	cerr << "global_init: unable to open config file from search list "
	     << conf_file_list << std::endl;
        _exit(1);
      } else {
        // 没指定路径也没找到默认配置 → 打印提示，用默认值继续
	cerr << "did not load config file, using default settings."
	     << std::endl;
      }
    }
  }
  else if (ret) {
    // 其他读取错误 → 报错退出
    cct->_log->flush();
    cerr << "global_init: error reading config file. "
         << conf.get_parse_error() << std::endl;
    _exit(1);
  }

  // 环境变量覆盖（CEPH_ARGS、CEPH_KEYRING 等），优先级高于配置文件
  conf.parse_env(cct->get_module_type());

  // 命令行参数覆盖（最高优先级，最后应用）
  conf.parse_argv(args);

  // 启动日志系统（如果还没启动的话）
  if (!cct->_log->is_started()) {
    cct->_log->start();
  }

  // 处理 --show-config / --show-config-val 等展示型命令行参数（展示后直接退出）
  conf.do_argv_commands();

  // 现在日志系统已就绪，输出配置文件解析过程中收集的警告信息
  g_conf().complain_about_parse_error(g_ceph_context);
}

#ifdef HAVE_BREAKPAD
static bool dumpCallback(
    const google_breakpad::MinidumpDescriptor& descriptor, void* context,
    bool succeeded) {
  char buf[1024];
  snprintf(buf, sizeof(buf), "minidump created in path %s", descriptor.path());
  dout_emergency(buf);
  return succeeded;
}
#endif

/**
 * global_init —— Ceph 全局初始化主函数（Linux 平台完整流程）。
 *
 * @param defaults       调用方传入的额外默认配置
 * @param args           命令行参数（会被修改，已识别的参数被移除）
 * @param module_type    模块类型（OSD/MON/MDS/CLIENT 等）
 * @param code_env       运行环境（守护进程/工具/库）
 * @param flags          初始化标志位
 * @param run_pre_init   是否在本函数内调用 global_pre_init（false 表示调用方已手动调用）
 * @return               CephContext 的 intrusive_ptr（引用计数管理生命周期）
 *
 * Linux 平台执行流程：
 *   1. 调用 global_pre_init（加载配置、启动日志）
 *   2. 屏蔽 SIGPIPE 信号
 *   3. 安装致命信号处理器（fatal_signal_handlers）
 *   4. 注册断言上下文
 *   5. 权限降级（setuid/setgid，root 启动时切换到普通用户）
 *   6. 设置 dumpable 标志（允许生成 core dump）
 *   7. 禁用透明大页（THP）
 *   8. 从 Monitor 拉取配置（如果启用 mon_config）
 *   9. 应用配置变更、通知所有观察者
 *  10. 创建 run_dir 并设置权限
 *  11. 输出版本号、初始化 CRUSH location
 */
boost::intrusive_ptr<CephContext>
global_init(const std::map<std::string,std::string> *defaults,
	    std::vector < const char* >& args,
	    uint32_t module_type, code_environment_t code_env,
	    int flags, bool run_pre_init)
{
  // 防止重复调用 global_init（整个进程只能初始化一次）
  static bool first_run = true;
  if (run_pre_init) {
    // 默认路径：在本函数内执行预初始化
    ceph_assert(!g_ceph_context && first_run);
    global_pre_init(defaults, args, module_type, code_env, flags);
  } else {
    // 调用方已手动执行了预初始化，检查确保已执行
    ceph_assert(g_ceph_context && first_run);
  }
  first_run = false;

  // 如果 flags 与预初始化时不同，更新一下（主要影响 mon_config 开关）
  if (g_ceph_context->get_init_flags() != flags) {
    g_ceph_context->set_init_flags(flags);
    if (flags & (CINIT_FLAG_NO_DEFAULT_CONFIG_FILE|
		 CINIT_FLAG_NO_MON_CONFIG)) {
      g_conf()->no_mon_config = true;
    }
  }

  // ===== Linux：信号处理 =====
  // 屏蔽 SIGPIPE：网络连接断开时默认会触发 SIGPIPE 导致进程退出，
  // Ceph 自己处理 EPIPE 错误码，所以屏蔽这个信号
  int siglist[] = { SIGPIPE, 0 };
  block_signals(siglist, NULL);

  // 安装致命信号处理器（段错误、总线错误等 → 打印调用栈）
  if (g_conf()->fatal_signal_handlers) {
    install_standard_sighandlers();
  }

  // 注册断言上下文（断言失败时能打印更多上下文信息）
  ceph::register_assert_context(g_ceph_context);

  // 退出时刷新日志（如果配置为 true）
  if (g_conf()->log_flush_on_exit)
    g_ceph_context->_log->set_flush_on_exit();

  // ===== Linux：权限降级 =====
  // 如果以 root 启动，且配置了 --setuser/--setgroup，则切换到普通用户/组
  // 非 root 时忽略 --setuser（因为没权限切换）
  std::ostringstream priv_ss;
  if (getuid() != 0) {
    // 非 root 用户：打印忽略提示
    if (g_conf()->setuser.length()) {
      cerr << "ignoring --setuser " << g_conf()->setuser << " since I am not root"
	   << std::endl;
    }
    if (g_conf()->setgroup.length()) {
      cerr << "ignoring --setgroup " << g_conf()->setgroup
	   << " since I am not root" << std::endl;
    }
  } else if (g_conf()->setgroup.length() ||
             g_conf()->setuser.length()) {
    uid_t uid = 0;  // 0 表示不切换
    gid_t gid = 0;
    std::string uid_string;
    std::string gid_string;
    std::string home_directory;

    if (g_conf()->setuser.length()) {
      char buf[4096];
      struct passwd pa;
      struct passwd *p = 0;

      // 先尝试按数字 UID 解析
      uid = atoi(g_conf()->setuser.c_str());
      if (uid) {
        getpwuid_r(uid, &pa, buf, sizeof(buf), &p);
      } else {
        // 按用户名查找
	getpwnam_r(g_conf()->setuser.c_str(), &pa, buf, sizeof(buf), &p);
        if (!p) {
	  cerr << "unable to look up user '" << g_conf()->setuser << "'"
	       << std::endl;
	  exit(1);
        }
        uid = p->pw_uid;     // 记录 UID
        gid = p->pw_gid;     // 同时记录默认 GID
        uid_string = g_conf()->setuser;
      }
      // 记录 home 目录（切换后设置 HOME 环境变量）
      if (p && p->pw_dir != nullptr) {
        home_directory = std::string(p->pw_dir);
      }
    }

    if (g_conf()->setgroup.length() > 0) {
      // 先尝试按数字 GID 解析
      gid = atoi(g_conf()->setgroup.c_str());
      if (!gid) {
        // 按组名查找
	static constexpr std::size_t size = 64 * 1024;
	auto buf = std::make_unique_for_overwrite<char[]>(size);
	struct group gr;
	struct group *g = 0;
	getgrnam_r(g_conf()->setgroup.c_str(), &gr, buf.get(), size, &g);
	if (!g) {
	  cerr << "unable to look up group '" << g_conf()->setgroup << "'"
	       << ": " << cpp_strerror(errno) << std::endl;
	  exit(1);
	}
	gid = g->gr_gid;
	gid_string = g_conf()->setgroup;
      }
    }

    // setuser_match_path：切换前检查指定路径的所有者是否匹配
    // 如果不匹配则拒绝切换（安全机制，防止数据目录权限错乱）
    if ((uid || gid) &&
	g_conf()->setuser_match_path.length()) {
      // 先展开路径中的元变量（如 $data_dir 等）
      string match_path = g_conf()->setuser_match_path;
      g_conf().early_expand_meta(match_path, &cerr);
      struct stat st;
      int r = ::stat(match_path.c_str(), &st);
      if (r < 0) {
	cerr << "unable to stat setuser_match_path "
	     << g_conf()->setuser_match_path
	     << ": " << cpp_strerror(errno) << std::endl;
	exit(1);
      }
      // UID/GID 不匹配 → 不切换，保持 root
      if ((uid && uid != st.st_uid) ||
	  (gid && gid != st.st_gid)) {
	cerr << "WARNING: will not setuid/gid: " << match_path
	     << " owned by " << st.st_uid << ":" << st.st_gid
	     << " and not requested " << uid << ":" << gid
	     << std::endl;
	uid = 0;
	gid = 0;
	uid_string.erase();
	gid_string.erase();
      } else {
	priv_ss << "setuser_match_path "
		<< match_path << " owned by "
		<< st.st_uid << ":" << st.st_gid << ". ";
      }
    }
    // 把要切换的 uid/gid 存到 CephContext（延迟切换时会用到）
    g_ceph_context->set_uid_gid(uid, gid);
    g_ceph_context->set_uid_gid_strings(uid_string, gid_string);

    if ((flags & CINIT_FLAG_DEFER_DROP_PRIVILEGES) == 0) {
      // 立即切换权限：先 setgid 再 setuid（顺序不能反）
      if (setgid(gid) != 0) {
	cerr << "unable to setgid " << gid << ": " << cpp_strerror(errno)
	     << std::endl;
	exit(1);
      }
      // set_keepcaps：setuid 后保留能力位（CAP_*），RDMA 等场景需要
      if (g_conf().get_val<bool>("set_keepcaps")) {
	if (prctl(PR_SET_KEEPCAPS, 1) == -1) {
	  cerr << "warning: unable to set keepcaps flag: " << cpp_strerror(errno) << std::endl;
	}
      }
      if (setuid(uid) != 0) {
	cerr << "unable to setuid " << uid << ": " << cpp_strerror(errno)
	     << std::endl;
	exit(1);
      }
      // 更新 HOME 环境变量
      if (setenv("HOME", home_directory.c_str(), 1) != 0) {
	cerr << "warning: unable to set HOME to " << home_directory << ": "
             << cpp_strerror(errno) << std::endl;
      }
      priv_ss << "set uid:gid to " << uid << ":" << gid << " (" << uid_string << ":" << gid_string << ")";
    } else {
      // 延迟切换（稍后再切），先记录到日志
      priv_ss << "deferred set uid:gid to " << uid << ":" << gid << " (" << uid_string << ":" << gid_string << ")";
    }
  }

  // ===== Linux：prctl 系统调优 =====
  // 设置 dumpable=1：允许生成 core dump（setuid 后默认禁止 core dump）
  if (prctl(PR_SET_DUMPABLE, 1) == -1) {
    cerr << "warning: unable to set dumpable flag: " << cpp_strerror(errno) << std::endl;
  }
  // 禁用透明大页（THP）：THP 会导致 Ceph OSD 内存分配 latency 抖动
  if (!g_conf().get_val<bool>("thp") && prctl(PR_SET_THP_DISABLE, 1, 0, 0, 0) == -1) {
    cerr << "warning: unable to disable THP: " << cpp_strerror(errno) << std::endl;
  }

  // ===== 从 Monitor 拉取配置 =====
  // 注意：必须在 setuid 之后建立网络连接！
  // RDMA 传输层在权限变更后会报 EACCES，所以网络初始化必须在权限切换完成后
  if (!g_conf()->no_mon_config) {
    // 确保 mini-session 也有最新的遗留字段值
    g_conf().apply_changes(nullptr);

    ceph::async::io_context_pool cp(1);
    MonClient mc_bootstrap(g_ceph_context, cp);
    // 从 MON 拉 monmap 和配置项
    if (mc_bootstrap.get_monmap_and_config() < 0) {
      cp.stop();
      g_ceph_context->_log->flush();
      cerr << "failed to fetch mon config (--no-mon-config to skip)"
	   << std::endl;
      _exit(1);
    }
    cp.stop();
  }

  // 展开所有元变量、调用配置观察者、打开日志文件
  g_conf().apply_changes(nullptr);

  // 创建 run_dir（存放 admin socket、pid 文件等运行时文件）
  if (g_conf()->run_dir.length() &&
      code_env == CODE_ENVIRONMENT_DAEMON &&
      !(flags & CINIT_FLAG_NO_DAEMON_ACTIONS)) {

    if (!fs::exists(g_conf()->run_dir.c_str())) {
      std::error_code ec;
      if (!fs::create_directory(g_conf()->run_dir, ec)) {
       cerr << "warning: unable to create " << g_conf()->run_dir
            << ec.message() << std::endl;
      }
      // 设置目录权限：owner 全权限，group 和 others 只读 + 执行
      fs::permissions(
        g_conf()->run_dir.c_str(),
        fs::perms::owner_all |
        fs::perms::group_read | fs::perms::group_exec |
        fs::perms::others_read | fs::perms::others_exec);
    }
  }

  // 通知所有配置观察者（副作用：日志文件被立即配置并打开）
  g_conf().call_all_observers();

  // 输出权限变更信息到日志
  if (priv_ss.str().length()) {
    dout(0) << priv_ss.str() << dendl;
  }

  // 如果延迟切换权限，此时调整日志文件和 run_dir 的所有者
  if ((flags & CINIT_FLAG_DEFER_DROP_PRIVILEGES) &&
      (g_ceph_context->get_set_uid() || g_ceph_context->get_set_gid())) {
    // 修正 run_dir 和日志文件的所有者
    chown_path(g_conf()->run_dir,
	       g_ceph_context->get_set_uid(),
	       g_ceph_context->get_set_gid(),
	       g_ceph_context->get_set_uid_string(),
	       g_ceph_context->get_set_gid_string());
    g_ceph_context->_log->chown_log_file(
      g_ceph_context->get_set_uid(),
      g_ceph_context->get_set_gid());
  }

  // 日志就绪后，输出配置文件解析过程中收集的警告
  g_conf().complain_about_parse_error(g_ceph_context);

  // 调试用：故意泄漏内存（测试泄漏检测）
  if (g_conf()->debug_deliberately_leak_memory) {
    derr << "deliberately leaking some memory" << dendl;
    char *s = new char[1234567];
    (void)s;
  }

  // 守护进程模式：输出版本号
  if (code_env == CODE_ENVIRONMENT_DAEMON && !(flags & CINIT_FLAG_NO_DAEMON_ACTIONS))
    output_ceph_version();

  // 初始化 CRUSH 位置信息（如 rack/row/room 等）
  if (g_ceph_context->crush_location.init_on_startup()) {
    cerr << " failed to init_on_startup : " << cpp_strerror(errno) << std::endl;
    exit(1);
  }

  // 返回 CephContext 的 intrusive_ptr（引用计数 +1，false 表示不增加引用）
  return boost::intrusive_ptr<CephContext>{g_ceph_context, false};
}

void global_print_banner(void)
{
  output_ceph_version();
}

// fork 之前的准备工作
// 返回值：
//   -1 — 不需要 fork（非守护进程 / daemonize=false），调用者跳过 prefork 流程
//    0 — 需要 fork，调用者应执行 fork() 进入守护进程模式
int global_init_prefork(CephContext *cct)
{
  // 非守护进程（工具/库模式）：不需要 fork
  if (g_code_env != CODE_ENVIRONMENT_DAEMON)
    return -1;

  const auto& conf = cct->_conf;
  // 配置了 daemonize=false（-f 前台运行）：不 fork，只写 pid 文件
  if (!conf->daemonize) {

    if (pidfile_write(conf->pid_file) < 0)            // 写 PID 文件
      exit(1);

    // 延迟降权模式下修正 pid 文件的所有者
    if ((cct->get_init_flags() & CINIT_FLAG_DEFER_DROP_PRIVILEGES) &&
	(cct->get_set_uid() || cct->get_set_gid())) {
      chown_path(conf->pid_file, cct->get_set_uid(), cct->get_set_gid(),
		 cct->get_set_uid_string(), cct->get_set_gid_string());
    }
    // global_init 过程中，为了从 Monitor 拉取配置，Ceph 临时创建了一个 Messenger。这个 Messenger 依赖 NetworkStack 单例。
    cct->drop_temp_messenger_obj();                    // 释放临时 Messenger（fork 后才需要）
    return -1;
  }

  // daemonize=true：准备 fork
  cct->notify_pre_fork();                              // 通知所有子系统即将 fork
  cct->_log->flush();                                  // 刷写日志
  cct->_log->stop();                                   // 停止日志线程（fork 时不能有后台线程）
  return 0;
}

void global_init_daemonize(CephContext *cct)
{
  if (global_init_prefork(cct) < 0)
    return;

#if !defined(_AIX) && !defined(_WIN32)
  int ret = daemon(1, 1);
  if (ret) {
    ret = errno;
    derr << "global_init_daemonize: BUG: daemon error: "
	 << cpp_strerror(ret) << dendl;
    exit(1);
  }
 
  global_init_postfork_start(cct);
  global_init_postfork_finish(cct);
#else
# warning daemon not supported on aix
#endif
}

/* Make file descriptors 0, 1, and possibly 2 point to /dev/null.
 *
 * Instead of just closing fd, we redirect it to /dev/null with dup2().
 * We have to do this because otherwise some arbitrary call to open() later
 * in the program might get back one of these file descriptors. It's hard to
 * guarantee that nobody ever writes to stdout, even though they're not
 * supposed to.
 */
int reopen_as_null(CephContext *cct, int fd)
{
  int newfd = open(DEV_NULL, O_RDWR | O_CLOEXEC);
  if (newfd < 0) {
    int err = errno;
    lderr(cct) << __func__ << " failed to open /dev/null: " << cpp_strerror(err)
	       << dendl;
    return -1;
  }
  // atomically dup newfd to target fd.  target fd is implicitly closed if
  // open and atomically replaced; see man dup2
  int r = dup2(newfd, fd);
  if (r < 0) {
    int err = errno;
    lderr(cct) << __func__ << " failed to dup2 " << fd << ": "
	       << cpp_strerror(err) << dendl;
    return -1;
  }
  // close newfd (we cloned it to target fd)
  VOID_TEMP_FAILURE_RETRY(close(newfd));
  // N.B. FD_CLOEXEC is cleared on fd (see dup2(2))
  return 0;
}

void global_init_postfork_start(CephContext *cct)
{
  // reexpand the meta in child process
  cct->_conf.finalize_reexpand_meta();

  // restart log thread
  cct->_log->start();
  cct->notify_post_fork();

  reopen_as_null(cct, STDIN_FILENO);

  const auto& conf = cct->_conf;
  if (pidfile_write(conf->pid_file) < 0)
    exit(1);

  if ((cct->get_init_flags() & CINIT_FLAG_DEFER_DROP_PRIVILEGES) &&
      (cct->get_set_uid() || cct->get_set_gid())) {
    chown_path(conf->pid_file, cct->get_set_uid(), cct->get_set_gid(),
	       cct->get_set_uid_string(), cct->get_set_gid_string());
  }
}

void global_init_postfork_finish(CephContext *cct)
{
  /* We only close stdout+stderr once the caller decides the daemonization
   * process is finished.  This way we can allow error or other messages to be
   * propagated in a manner that the user is able to see.
   */
  if (!(cct->get_init_flags() & CINIT_FLAG_NO_CLOSE_STDERR)) {
    int ret = global_init_shutdown_stderr(cct);
    if (ret) {
      derr << "global_init_daemonize: global_init_shutdown_stderr failed with "
	   << "error code " << ret << dendl;
      exit(1);
    }
  }

  reopen_as_null(cct, STDOUT_FILENO);

  ldout(cct, 1) << "finished global_init_daemonize" << dendl;
}


void global_init_chdir(const CephContext *cct)
{
  const auto& conf = cct->_conf;
  if (conf->chdir.empty())
    return;
  if (::chdir(conf->chdir.c_str())) {
    int err = errno;
    derr << "global_init_chdir: failed to chdir to directory: '"
	 << conf->chdir << "': " << cpp_strerror(err) << dendl;
  }
}

int global_init_shutdown_stderr(CephContext *cct)
{
  reopen_as_null(cct, STDERR_FILENO);
  cct->_log->set_stderr_level(-2, -2);
  return 0;
}

int global_init_preload_erasure_code(const CephContext *cct)
{
  const auto& conf = cct->_conf;
  string plugins = conf->osd_erasure_code_plugins;

  // validate that this is a not a legacy plugin
  std::list<string> plugins_list;
  get_str_list(plugins, plugins_list);
  for (auto i = plugins_list.begin(); i != plugins_list.end(); ++i) {
	string plugin_name = *i;
	string replacement = "";

	if (plugin_name == "jerasure_generic" || 
	    plugin_name == "jerasure_sse3" ||
	    plugin_name == "jerasure_sse4" ||
	    plugin_name == "jerasure_neon") {
	  replacement = "jerasure";
	}
	else if (plugin_name == "shec_generic" ||
		 plugin_name == "shec_sse3" ||
		 plugin_name == "shec_sse4" ||
		 plugin_name == "shec_neon") {
	  replacement = "shec";
	}

	if (replacement != "") {
	  dout(0) << "WARNING: osd_erasure_code_plugins contains plugin "
		  << plugin_name << " that is now deprecated. Please modify the value "
		  << "for osd_erasure_code_plugins to use "  << replacement << " instead." << dendl;
	}
  }

  std::stringstream ss;
  int r = ceph::ErasureCodePluginRegistry::instance().preload(
    plugins,
    conf.get_val<std::string>("erasure_code_dir"),
    &ss);
  if (r)
    derr << ss.str() << dendl;
  else
    dout(0) << ss.str() << dendl;
  return r;
}
