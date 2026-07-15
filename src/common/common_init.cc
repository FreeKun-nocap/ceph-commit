// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2010-2011 Dreamhost
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/common_init.h"
#include "common/admin_socket.h"
#include "common/ceph_argparse.h"
#include "common/ceph_context.h"
#include "common/config.h"
#include "common/dout.h"
#include "common/hostname.h"
#include "common/strtol.h"
#include "common/valgrind.h"
#include "common/zipkin_trace.h"
#include "include/compat.h"
#include "log/Log.h"

#define dout_subsys ceph_subsys_

#ifndef WITH_CRIMSON
/**
 * common_preinit —— Ceph 公共预初始化函数，创建并配置 CephContext 的早期阶段。
 *
 * @param iparams  初始化参数（模块类型、实体名、是否跳过配置文件等）
 * @param code_env 代码运行环境（守护进程/工具/库等）
 * @param flags    初始化标志位（如 CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS）
 * @return         已初步配置好的 CephContext 对象（调用者负责 release）
 *
 * 执行流程：
 *   1. 设置全局代码环境标识 g_code_env
 *   2. 创建 CephContext 对象（此时配置系统已初始化）
 *   3. 设置实体名（如 osd.0）
 *   4. 根据模块类型设置 keyring 默认路径（向后兼容）
 *   5. 根据 flags 和 code_env 设置各类默认值（admin_socket、日志等）
 *   6. 设置 no_config_file / host 等基础配置项
 */
CephContext *common_preinit(const CephInitParameters &iparams,
			    enum code_environment_t code_env, int flags)
{
  // 设置全局代码环境标识（用注解标记为良性竞争，初始化只发生一次）
  ANNOTATE_BENIGN_RACE_SIZED(&g_code_env, sizeof(g_code_env), "g_code_env");
  g_code_env = code_env;

  // 创建 CephContext 对象：内部会初始化配置系统（md_config_t + schema）
  CephContext *cct = new CephContext(iparams.module_type, code_env, flags);

  auto& conf = cct->_conf;
  // 在此处注册配置观察者（目前为空，后续可扩展）

  // 设置本进程的实体名称（如 osd.0, mon.a, client.admin）
  conf->name = iparams.name;

  // OSD 和 MDS 使用不同的默认 keyring 路径（历史兼容原因）。
  // 长远来看所有进程应统一位置；MON 已强制使用 $mon_data/keyring。
  if (conf->name.is_mds()) {
    conf.set_val_default("keyring", "$mds_data/keyring");
  } else if (conf->name.is_osd()) {
    conf.set_val_default("keyring", "$osd_data/keyring");
  }

  // 非特权守护进程模式：admin_socket 路径带上 pid 和 cctid，
  // 保证同名多个实例之间不冲突
  if ((flags & CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS)) {
    conf.set_val_default("admin_socket",
			  "$run_dir/$cluster-$name.$pid.$cctid.asok");
  }

  // 库模式或无输出工具模式：默认关闭 stderr 日志
  if (code_env == CODE_ENVIRONMENT_LIBRARY ||
      code_env == CODE_ENVIRONMENT_UTILITY_NODOUT) {
    conf.set_val_default("log_to_stderr", "false");
    conf.set_val_default("err_to_stderr", "false");
    conf.set_val_default("log_flush_on_exit", "false");
  }

  // 设置是否跳过配置文件（来自命令行 --no-config-file）
  conf.set_val("no_config_file", iparams.no_config_file ? "true" : "false");

  // 如果 host 配置为空，用本机短主机名填充
  if (conf->host.empty()) {
    conf.set_val("host", ceph_get_short_hostname());
  }
  return cct;
}
#endif	// #ifndef WITH_CRIMSON

void complain_about_parse_error(CephContext *cct,
				const std::string& parse_error)
{
  if (parse_error.empty())
    return;
  lderr(cct) << "Errors while parsing config file!" << dendl;
  lderr(cct) << parse_error << dendl;
}

#ifndef WITH_CRIMSON

/* Please be sure that this can safely be called multiple times by the
 * same application. */
void common_init_finish(CephContext *cct)
{
  // only do this once per cct
  if (cct->_finished) {
    return;
  }
  cct->_finished = true;
  cct->init_crypto();
  ZTracer::ztrace_init();

  if (!cct->_log->is_started()) {
    cct->_log->start();
  }

  int flags = cct->get_init_flags();
  if (!(flags & CINIT_FLAG_NO_DAEMON_ACTIONS))
    cct->start_service_thread();

  if ((flags & CINIT_FLAG_DEFER_DROP_PRIVILEGES) &&
      (cct->get_set_uid() || cct->get_set_gid())) {
    cct->get_admin_socket()->chown(cct->get_set_uid(), cct->get_set_gid());
  }

  const auto& conf = cct->_conf;

  if (!conf->admin_socket.empty() && !conf->admin_socket_mode.empty()) {
    int ret = 0;
    std::string err;

    ret = strict_strtol(conf->admin_socket_mode.c_str(), 8, &err);
    if (err.empty()) {
      if (!(ret & (~ACCESSPERMS))) {
        cct->get_admin_socket()->chmod(static_cast<mode_t>(ret));
      } else {
        lderr(cct) << "Invalid octal permissions string: "
            << conf->admin_socket_mode << dendl;
      }
    } else {
      lderr(cct) << "Invalid octal string: " << err << dendl;
    }
  }
}

#endif	// #ifndef WITH_CRIMSON
