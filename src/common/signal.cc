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

#include <cstdlib>
#include <sstream>

#include <sys/stat.h>
#include <sys/types.h>

#include <signal.h>

#include "common/BackTrace.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/signal.h"
#include "common/perf_counters.h"

#include "global/pidfile.h"

using namespace std::literals;

#ifndef _WIN32
std::string signal_mask_to_str()
{
  sigset_t old_sigset;
  if (pthread_sigmask(SIG_SETMASK, NULL, &old_sigset)) {
    return "(pthread_signmask failed)";
  }

  std::ostringstream oss;
  oss << "show_signal_mask: { ";
  auto sep = ""s;
  for (int signum = 0; signum < NSIG; ++signum) {
    if (sigismember(&old_sigset, signum) == 1) {
      oss << sep << signum;
      sep = ", ";
    }
  }
  oss << " }";
  return oss.str();
}

/**
 * block_signals —— 阻塞指定的信号列表（或阻塞全部信号）。
 *
 * @param siglist      要阻塞的信号数组，以 0 结尾（如 { SIGPIPE, 0 }）。
 *                     传 NULL 表示阻塞所有信号。
 * @param old_sigset   输出参数：保存阻塞前的信号掩码（用于后续恢复）。
 *                     传 NULL 表示不保存旧状态。
 *
 * 底层调用 pthread_sigmask(SIG_BLOCK, ...)，是线程级别的信号屏蔽。
 */
void block_signals(const int *siglist, sigset_t *old_sigset)
{
  sigset_t sigset;
  if (!siglist) {
    sigfillset(&sigset);        // siglist 为空 → 阻塞全部信号
  }
  else {
    int i = 0;
    sigemptyset(&sigset);        // 清空信号集
    while (siglist[i]) {
      sigaddset(&sigset, siglist[i]);  // 逐个加入要阻塞的信号
      ++i;
    }
  }
  // 设置线程信号掩码：SIG_BLOCK = 将指定信号加入阻塞集合
  int ret = pthread_sigmask(SIG_BLOCK, &sigset, old_sigset);
  ceph_assert(ret == 0);        // 失败就断言（理论上不会失败）
}

void restore_sigset(const sigset_t *old_sigset)
{
  int ret = pthread_sigmask(SIG_SETMASK, old_sigset, NULL);
  ceph_assert(ret == 0);
}

void unblock_all_signals(sigset_t *old_sigset)
{
  sigset_t sigset;
  sigfillset(&sigset);
  sigdelset(&sigset, SIGKILL);
  int ret = pthread_sigmask(SIG_UNBLOCK, &sigset, old_sigset);
  ceph_assert(ret == 0);
}
#else
std::string signal_mask_to_str()
{
  return "(unsupported signal)";
}

// Windows provides limited signal functionality.
void block_signals(const int *siglist, sigset_t *old_sigset) {}
void restore_sigset(const sigset_t *old_sigset) {}
void unblock_all_signals(sigset_t *old_sigset) {}
#endif /* _WIN32 */
