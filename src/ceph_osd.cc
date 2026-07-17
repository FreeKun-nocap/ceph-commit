// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <boost/scoped_ptr.hpp>

#include <iostream>
#include <sstream>
#include <string>

#include "auth/KeyRing.h"
#include "osd/OSD.h"
#include "os/ObjectStore.h"
#include "mon/MonClient.h"
#include "include/ceph_features.h"
#include "common/config.h"
#include "extblkdev/ExtBlkDevPlugin.h"

#include "mon/MonMap.h"

#include "msg/Messenger.h"

#include "common/Throttle.h"
#include "common/Timer.h"
#include "common/TracepointProvider.h"
#include "common/ceph_argparse.h"
#include "common/numa.h"

#include "global/global_init.h"
#include "global/signal_handler.h"

#include "include/color.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/pick_address.h"

#include "log/Log.h"
#include "perfglue/heap_profiler.h"

#include "include/ceph_assert.h"

#include "common/Preforker.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_osd

using std::cerr;
using std::cout;
using std::map;
using std::ostringstream;
using std::string;
using std::vector;

using ceph::bufferlist;

//这是一个匿名命名空间（anonymous namespace），这意味着这些定义只在当前编译单元（.cpp文件）中可见，不会与其他文件产生符号冲突。
namespace {

/**
作用：初始化 OSD 主模块的跟踪点
库文件：libosd_tp.so - 包含 OSD 核心操作的跟踪点定义
跟踪域：osd_tracing - 标识这些跟踪属于 OSD 主功能域
用途：跟踪 OSD 的主要操作，如请求处理、Peering 过程等
*/
TracepointProvider::Traits osd_tracepoint_traits("libosd_tp.so",
                                                 "osd_tracing");
/**
作用：初始化对象存储层的跟踪点
库文件：libos_tp.so - 对象存储相关的跟踪点
跟踪域：osd_objectstore_tracing - 对象存储操作域
用途：跟踪对象存储接口的调用，如读、写、事务操作等
*/
TracepointProvider::Traits os_tracepoint_traits("libos_tp.so",
                                                "osd_objectstore_tracing");
/**
作用：初始化 BlueStore 后端存储的跟踪点
库文件：libbluestore_tp.so - BlueStore 特有的跟踪点
跟踪域：bluestore_tracing - BlueStore 操作域
用途：跟踪 BlueStore 的底层操作，如 RocksDB 调用、磁盘 I/O 等
*/
TracepointProvider::Traits bluestore_tracepoint_traits("libbluestore_tp.so",
						       "bluestore_tracing");
/**
条件：仅在定义了 WITH_OSD_INSTRUMENT_FUNCTIONS 宏时编译
库文件：libcyg_profile_tp.so - 函数级跟踪库
跟踪域：osd_function_tracing - 函数调用跟踪域
用途：实现细粒度的函数级性能分析（类似 gprof 的 instrumentation）
*/
#ifdef WITH_OSD_INSTRUMENT_FUNCTIONS
TracepointProvider::Traits cyg_profile_traits("libcyg_profile_tp.so",
                                                 "osd_function_tracing");
#endif

} // anonymous namespace

OSD *osdptr = nullptr;

void handle_osd_signal(int signum)
{
  if (osdptr)
    osdptr->handle_signal(signum);
}

static void usage()
{
  cout << "usage: ceph-osd -i <ID> [flags]\n"
       << "  --osd-data PATH data directory\n"
       << "  --osd-journal PATH\n"
       << "                    journal file or block device\n"
       << "  --mkfs            create a [new] data directory\n"
       << "  --mkkey           generate a new secret key. This is normally used in combination with --mkfs\n"
       << "  --monmap          specify the path to the monitor map. This is normally used in combination with --mkfs\n"
       << "  --osd-uuid        specify the OSD's fsid. This is normally used in combination with --mkfs\n"
       << "  --keyring         specify a path to the osd keyring. This is normally used in combination with --mkfs\n"
       << "  --convert-filestore\n"
       << "                    run any pending upgrade operations\n"
       << "  --flush-journal   flush all data out of journal\n"
       << "  --osdspec-affinity\n"
       << "                    set affinity to an osdspec\n"
       << "  --dump-journal    dump all data of journal\n"
       << "  --mkjournal       initialize a new journal\n"
       << "  --check-wants-journal\n"
       << "                    check whether a journal is desired\n"
       << "  --check-allows-journal\n"
       << "                    check whether a journal is allowed\n"
       << "  --check-needs-journal\n"
       << "                    check whether a journal is required\n"
       << "  --debug_osd <N>   set debug level (e.g. 10)\n"
       << "  --get-device-fsid PATH\n"
       << "                    get OSD fsid for the given block device\n"
       << "  --run-benchmark   run a throughput benchmark test against the OSD and dump the result\n"
       << std::endl;
  generic_server_usage();
}

/**
 * argc: 命令行参数数量, 包括程序名本身
 * argv: 命令行参数数组的指针
 * 例：./ceph-osd -i 0 --debug-osd 20
 * 则：
 * argc = 5
 * argv = ["./ceph-osd", "-i", "0", "--debug-osd", "20"]
 *
 * 启动流程：
 *   1. 解析命令行参数（-i <ID> 指定 OSD ID）
 *   2. global_init() 初始化配置系统、日志、CRUSH 位置等基础设施
 *   3. Preforker 机制：先 fork 子进程初始化 OSD，成功后才让父进程退出
 *      （这样 systemd 等到初始化完成才认为服务已启动）
 *   4. 处理一次性模式（mkfs/flush-journal/convert-filestore 等）
 *   5. 进入 OSD 主循环：初始化 ObjectStore → 启动心跳、scrub 等线程 → 循环处理 IO
 */
int main(int argc, const char **argv)
{
  auto args = argv_to_vec(argc, argv);  // 将命令行参数转换为vector，便于后续处理，例如：["-i", "0", "--debug-osd", "20"]
  if (args.empty()) {
    cerr << argv[0] << ": -h or --help for usage" << std::endl;
    exit(1);
  }

  // 如果参数中包含 -h 或 --help，打印帮助信息并退出程序
  if (ceph_argparse_need_usage(args)) {
    usage();
    exit(0);
  }

  // 全局初始化：创建 CephContext + 解析配置 + 启动日志
  auto cct = global_init(
    nullptr,
    args, CEPH_ENTITY_TYPE_OSD,
    CODE_ENVIRONMENT_DAEMON, 0);
  // 堆内存剖析器初始化（开发调试用，生产环境通常禁用）
  ceph_heap_profiler_init();

  // Preforker：父进程先 fork 子进程初始化 OSD
  // 初始化成功后子进程继续运行，父进程退出返回 0 给 systemd
  Preforker forker;

  // ==================== OSD 专用命令行参数解析 ====================
  bool mkfs = false;
  bool mkjournal = false;
  bool check_wants_journal = false;
  bool check_allows_journal = false;
  bool check_needs_journal = false;
  bool mkkey = false;
  bool flushjournal = false;
  bool dump_journal = false;
  bool convertfilestore = false;
  bool get_osd_fsid = false;
  bool get_cluster_fsid = false;
  bool get_journal_fsid = false;
  bool get_device_fsid = false;
  bool run_benchmark = false;
  string device_path;
  std::string dump_pg_log;
  std::string osdspec_affinity;

  std::string val;
  // 第二轮参数解析：只解析 OSD 专有的命令行参数
  // 注：--cluster/-i/--conf 等通用参数已在 global_init 中处理完毕
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {          // 遇到 "--" 停止解析
      break;
    } else if (ceph_argparse_flag(args, i, "--mkfs", (char*)NULL)) {
      mkfs = true;                                     // 格式化 OSD 磁盘（创建元数据）
    } else if (ceph_argparse_witharg(args, i, &val, "--osdspec-affinity", (char*)NULL)) {
     osdspec_affinity = val;                           // OSD spec 亲和性（用于自动化部署）
    } else if (ceph_argparse_flag(args, i, "--mkjournal", (char*)NULL)) {
      mkjournal = true;                                // 单独创建 journal 分区
    } else if (ceph_argparse_flag(args, i, "--check-allows-journal", (char*)NULL)) {
      check_allows_journal = true;                     // 检查设备是否支持 journal
    } else if (ceph_argparse_flag(args, i, "--check-wants-journal", (char*)NULL)) {
      check_wants_journal = true;                      // 检查设备是否想要 journal
    } else if (ceph_argparse_flag(args, i, "--check-needs-journal", (char*)NULL)) {
      check_needs_journal = true;                      // 检查设备是否必须 journal
    } else if (ceph_argparse_flag(args, i, "--mkkey", (char*)NULL)) {
      mkkey = true;                                    // 生成 OSD 认证密钥
    } else if (ceph_argparse_flag(args, i, "--flush-journal", (char*)NULL)) {
      flushjournal = true;                             // 刷写 journal（回放后清空）
    } else if (ceph_argparse_flag(args, i, "--convert-filestore", (char*)NULL)) {
      convertfilestore = true;                         // 从 FileStore 迁移到 BlueStore
    } else if (ceph_argparse_witharg(args, i, &val, "--dump-pg-log", (char*)NULL)) {
      dump_pg_log = val;                               // 导出指定 PG 的日志（调试用）
    } else if (ceph_argparse_flag(args, i, "--dump-journal", (char*)NULL)) {
      dump_journal = true;                             // 导出 journal 内容（调试用）
    } else if (ceph_argparse_flag(args, i, "--get-cluster-fsid", (char*)NULL)) {
      get_cluster_fsid = true;                         // 打印集群 FSID
    } else if (ceph_argparse_flag(args, i, "--get-osd-fsid", "--get-osd-uuid", (char*)NULL)) {
      get_osd_fsid = true;                             // 打印 OSD FSID/UUID
    } else if (ceph_argparse_flag(args, i, "--get-journal-fsid", "--get-journal-uuid", (char*)NULL)) {
      get_journal_fsid = true;                         // 打印 journal UUID
    } else if (ceph_argparse_witharg(args, i, &device_path,
				     "--get-device-fsid", (char*)NULL)) {
      get_device_fsid = true;                          // 打印指定块设备的 FSID
    } else if (ceph_argparse_flag(args, i, "--run-benchmark", (char*)NULL)) {
      run_benchmark = true;                            // 运行 SimpleBench（测试磁盘性能）
    } else {
      ++i;                                             // 不认识的参数跳过，下面统一检查报错
    }
  }
  if (!args.empty()) {                                 // 还有未识别的参数
    cerr << "unrecognized arg " << args[0] << std::endl;
    exit(1);
  }

  // ==================== Preforker：父进程 fork 子进程 ====================
  // global_init_prefork 判断是否应该进入 prefork 模式
  // prefork 之后，is_parent() 为 true 则父进程等待子进程初始化成功再退出
  // is_parent() 为 false 则子进程继续执行后续初始化
  if (global_init_prefork(g_ceph_context) >= 0) {
    std::string err;
    int r = forker.prefork(err);                         // fork 子进程
    if (r < 0) {
      cerr << err << std::endl;
      return r;
    }
    if (forker.is_parent()) {                            // 父进程：等待子进程初始化结果
      g_ceph_context->_log->start();
      if (forker.parent_wait(err) != 0) {                // 子进程初始化失败
        return -ENXIO;
      }
      return 0;                                          // 子进程成功，父进程 exit(0)
    }
    setsid();                                            // 子进程：脱离终端，成为守护进程
    global_init_postfork_start(g_ceph_context);
  }
  common_init_finish(g_ceph_context);  // common_init_finish 是 初始化收尾函数 ——把 global_init 中跳过的最后几步补上。
  global_init_chdir(g_ceph_context);                     // 切换到 osd_data 目录

  // ==================== 一次性模式：获取 journal UUID ====================
  if (get_journal_fsid) {
    device_path = g_conf().get_val<std::string>("osd_journal");
    get_device_fsid = true;
  }
  // ==================== 一次性模式：获取块设备 UUID ====================
  if (get_device_fsid) {
    uuid_d uuid;
    int r = ObjectStore::probe_block_device_fsid(g_ceph_context, device_path,
						 &uuid);
    if (r < 0) {
      cerr << "failed to get device fsid for " << device_path
	   << ": " << cpp_strerror(r) << std::endl;
      forker.exit(1);
    }
    cout << uuid << std::endl;
    forker.exit(0);
  }

  // ==================== 一次性模式：导出 PG 日志 ====================
  if (!dump_pg_log.empty()) {
    common_init_finish(g_ceph_context);
    bufferlist bl;
    std::string error;

    if (bl.read_file(dump_pg_log.c_str(), &error) >= 0) {
      pg_log_entry_t e;
      auto p = bl.cbegin();
      while (!p.end()) {
	uint64_t pos = p.get_off();
	try {
	  decode(e, p);
	}
	catch (const ceph::buffer::error &e) {
	  derr << "failed to decode LogEntry at offset " << pos << dendl;
	  forker.exit(1);
	}
	derr << pos << ":\t" << e << dendl;
      }
    } else {
      derr << "unable to open " << dump_pg_log << ": " << error << dendl;
    }
    forker.exit(0);
  }

  // ==================== 验证 OSD ID ====================
  // whoami
  char *end;
  const char *id = g_conf()->name.get_id().c_str();
  int whoami = strtol(id, &end, 10);                     // 从名称解析 OSD ID（如 "0" → 0）
  std::string data_path = g_conf().get_val<std::string>("osd_data");
  if (*end || end == id || whoami < 0) {
    derr << "must specify '-i #' where # is the osd number" << dendl;
    forker.exit(1);
  }

  if (data_path.empty()) {
    derr << "must specify '--osd-data=foo' data path" << dendl;
    forker.exit(1);
  }

  // ==================== 检测 ObjectStore 类型 ====================
  // 三种方式确定存储后端类型：
  //   1. 读 data_path/type 文件（最直接）
  //   2. mkfs 模式：从配置 osd_objectstore 读取（默认 bluestore）
  //   3. 推断：data_path/current/ 目录存在 → filestore
  //             data_path/block 是符号链接 → bluestore
  // the store
  std::string store_type;
  {
    char fn[PATH_MAX];
    snprintf(fn, sizeof(fn), "%s/type", data_path.c_str());
    int fd = ::open(fn, O_RDONLY|O_CLOEXEC);
    if (fd >= 0) {
      bufferlist bl;
      bl.read_fd(fd, 64);
      if (bl.length()) {
	store_type = string(bl.c_str(), bl.length() - 1);  // drop \n
	dout(5) << "object store type is " << store_type << dendl;
      }
      ::close(fd);
    } else if (mkfs) {
      store_type = g_conf().get_val<std::string>("osd_objectstore");
    } else {
      // hrm, infer the type
      snprintf(fn, sizeof(fn), "%s/current", data_path.c_str());
      struct stat st;
      if (::stat(fn, &st) == 0 &&
	  S_ISDIR(st.st_mode)) {
	derr << "missing 'type' file, inferring filestore from current/ dir"
	     << dendl;
	store_type = "filestore";                        // 推断为 FileStore
      } else {
	snprintf(fn, sizeof(fn), "%s/block", data_path.c_str());
	if (::stat(fn, &st) == 0 &&
	    S_ISLNK(st.st_mode)) {
	  derr << "missing 'type' file, inferring bluestore from block symlink"
	       << dendl;
	  store_type = "bluestore";                      // 推断为 BlueStore
	} else {
	  derr << "missing 'type' file and unable to infer osd type" << dendl;
	  forker.exit(1);
	}
      }
    }
  }

  // ==================== 创建 ObjectStore 实例 ====================
  // ObjectStore 是 Ceph 存储后端的抽象接口，BlueStore 是默认实现
  std::string journal_path = g_conf().get_val<std::string>("osd_journal");
  uint32_t flags = g_conf().get_val<uint64_t>("osd_os_flags");
  std::unique_ptr<ObjectStore> store = ObjectStore::create(g_ceph_context,
							   store_type,
							   data_path,
							   journal_path,
							   flags);
  if (!store) {
    derr << "unable to create object store" << dendl;
    forker.exit(-ENODEV);
  }

  // ==================== 生成 OSD 密钥（通常与 --mkfs 搭配，不是一次性模式） ====================
  if (mkkey) {
    common_init_finish(g_ceph_context);
    KeyRing keyring;

    EntityName ename{g_conf()->name};
    EntityAuth eauth;

    std::string keyring_path = g_conf().get_val<std::string>("keyring");
    int ret = keyring.load(g_ceph_context, keyring_path);
    if (ret == 0 &&
	keyring.get_auth(ename, eauth)) {
      derr << "already have key in keyring " << keyring_path << dendl;  // key 已存在，跳过
    } else {
      eauth.key.create(g_ceph_context, CEPH_CRYPTO_AES);         // 生成 AES 密钥
      keyring.add(ename, eauth);
      bufferlist bl;
      keyring.encode_plaintext(bl);
      int r = bl.write_file(keyring_path.c_str(), 0600);         // 写入 keyring 文件（权限 0600）
      if (r)
	derr << TEXT_RED << " ** ERROR: writing new keyring to "
             << keyring_path << ": " << cpp_strerror(r) << TEXT_NORMAL
             << dendl;
      else
	derr << "created new key in keyring " << keyring_path << dendl;
    }
  }

  // ==================== 一次性模式：格式化 OSD ====================
  if (mkfs) {
    common_init_finish(g_ceph_context);

    if (g_conf().get_val<uuid_d>("fsid").is_zero()) {            // 必须有集群 FSID
      derr << "must specify cluster fsid" << dendl;
      forker.exit(-EINVAL);
    }

    int err = OSD::mkfs(g_ceph_context, std::move(store), g_conf().get_val<uuid_d>("fsid"),
                        whoami, osdspec_affinity);
    if (err < 0) {
      derr << TEXT_RED << " ** ERROR: error creating empty object store in "
	   << data_path << ": " << cpp_strerror(-err) << TEXT_NORMAL << dendl;
      forker.exit(1);
    }
    dout(0) << "created object store " << data_path
	    << " for osd." << whoami
	    << " fsid " << g_conf().get_val<uuid_d>("fsid")
	    << dendl;
    forker.exit(0);
  }
  if (mkkey) {
    forker.exit(0);
  }
  // ==================== 一次性模式：磁盘基准测试 ====================
  if (run_benchmark) {
    store->mount();                                        // 挂载存储后端
    tl::expected<std::string, int> res =
      OSD::run_osd_bench(g_ceph_context, store.get());     // 运行 SimpleBench
    if (!res.has_value()) {
      int ret = res.error();
      derr << TEXT_RED << " ** ERROR: error running benchmark: "
           << cpp_strerror(ret) << TEXT_NORMAL << dendl;
      cerr << " ** ERROR: error running benchmark: "
           << cpp_strerror(ret) << std::endl;
      forker.exit(ret);
    }
    cout << res.value() << std::endl;
    store->umount();                                       // 卸载存储后端
    forker.exit(0);
  }
  // ==================== 一次性模式：创建 journal ====================
  if (mkjournal) {
    common_init_finish(g_ceph_context);
    int err = store->mkjournal();
    if (err < 0) {
      derr << TEXT_RED << " ** ERROR: error creating fresh journal "
           << journal_path << " for object store " << data_path << ": "
           << cpp_strerror(-err) << TEXT_NORMAL << dendl;
      forker.exit(1);
    }
    derr << "created new journal " << journal_path
	 << " for object store " << data_path << dendl;
    forker.exit(0);
  }
  // ==================== 一次性模式：检查 journal 支持情况 ====================
  if (check_wants_journal) {
    if (store->wants_journal()) {
      cout << "wants journal: yes" << std::endl;
      forker.exit(0);
    } else {
      cout << "wants journal: no" << std::endl;
      forker.exit(1);
    }
  }
  if (check_allows_journal) {
    if (store->allows_journal()) {
      cout << "allows journal: yes" << std::endl;
      forker.exit(0);
    } else {
      cout << "allows journal: no" << std::endl;
      forker.exit(1);
    }
  }
  if (check_needs_journal) {
    if (store->needs_journal()) {
      cout << "needs journal: yes" << std::endl;
      forker.exit(0);
    } else {
      cout << "needs journal: no" << std::endl;
      forker.exit(1);
    }
  }
  // ==================== 一次性模式：刷写 journal ====================
  if (flushjournal) {
    common_init_finish(g_ceph_context);
    int err = store->mount();
    if (err < 0) {
      derr << TEXT_RED << " ** ERROR: error flushing journal " << journal_path
	   << " for object store " << data_path
	   << ": " << cpp_strerror(-err) << TEXT_NORMAL << dendl;
      goto flushjournal_out;
    }
    store->umount();
    derr << "flushed journal " << journal_path
	 << " for object store " << data_path
	 << dendl;
flushjournal_out:
    store.reset();
    forker.exit(err < 0 ? 1 : 0);
  }
  // ==================== 一次性模式：导出 journal ====================
  if (dump_journal) {
    common_init_finish(g_ceph_context);
    int err = store->dump_journal(cout);
    if (err < 0) {
      derr << TEXT_RED << " ** ERROR: error dumping journal " << journal_path
	   << " for object store " << data_path
	   << ": " << cpp_strerror(-err) << TEXT_NORMAL << dendl;
      forker.exit(1);
    }
    derr << "dumped journal " << journal_path
	 << " for object store " << data_path
	 << dendl;
    forker.exit(0);
  }

  // ==================== 一次性模式：FileStore → BlueStore 转换 ====================
  if (convertfilestore) {
    int err = store->mount();                              // 挂载旧存储
    if (err < 0) {
      derr << TEXT_RED << " ** ERROR: error mounting store " << data_path
	   << ": " << cpp_strerror(-err) << TEXT_NORMAL << dendl;
      forker.exit(1);
    }
    err = store->upgrade();                                // 执行升级转换
    store->umount();
    if (err < 0) {
      derr << TEXT_RED << " ** ERROR: error converting store " << data_path
	   << ": " << cpp_strerror(-err) << TEXT_NORMAL << dendl;
      forker.exit(1);
    }
    forker.exit(0);
  }
  
  // ==================== 加载外部块设备插件 ====================
  {
    int r = extblkdev::preload(g_ceph_context);
    if (r < 0) {
      derr << "Failed preloading extblkdev plugins, error code: " << r << dendl;
      forker.exit(1);
    }
  }

  // ==================== 读取 OSD Superblock（on-disk 元数据） ====================
  // Superblock 包含：magic 魔数、集群 FSID、OSD FSID、OSD ID 等关键信息
  string magic;
  uuid_d cluster_fsid, osd_fsid;
  ceph_release_t require_osd_release = ceph_release_t::unknown;
  int w;
  int r = OSD::peek_meta(store.get(), &magic, &cluster_fsid, &osd_fsid, &w,
			 &require_osd_release);
  if (r < 0) {
    derr << TEXT_RED << " ** ERROR: unable to open OSD superblock on "
	 << data_path << ": " << cpp_strerror(-r)
	 << TEXT_NORMAL << dendl;
    if (r == -ENOTSUP) {
      derr << TEXT_RED << " **        please verify that underlying storage "
	   << "supports xattrs" << TEXT_NORMAL << dendl;
    }
    forker.exit(1);
  }
  if (w != whoami) {                                       // 磁盘上的 OSD ID 必须和命令行 -i 一致
    derr << "OSD id " << w << " != my id " << whoami << dendl;
    forker.exit(1);
  }
  if (strcmp(magic.c_str(), CEPH_OSD_ONDISK_MAGIC)) {     // 魔数校验
    derr << "OSD magic " << magic << " != my " << CEPH_OSD_ONDISK_MAGIC
	 << dendl;
    forker.exit(1);
  }

  // ==================== 一次性模式：获取集群/OSD FSID ====================
  if (get_cluster_fsid) {
    cout << cluster_fsid << std::endl;
    forker.exit(0);
  }
  if (get_osd_fsid) {
    cout << osd_fsid << std::endl;
    forker.exit(0);
  }

  // ==================== 检查 OSD 版本升级兼容性 ====================
  {
    ostringstream err;
    if (!can_upgrade_from(require_osd_release, "require_osd_release", err)) {
      derr << err.str() << dendl;
      forker.exit(1);
    }
  }

  // ==================== NUMA 亲和性优化 ====================
  // 如果 osd_numa_prefer_iface 开启，优先选择与存储设备同 NUMA 节点的网络接口
  int os_numa_node = -1;
  r = store->get_numa_node(&os_numa_node, nullptr, nullptr);
  if (r >= 0 && os_numa_node >= 0) {
    dout(1) << " objectstore numa_node " << os_numa_node << dendl;
  }
  int iface_preferred_numa_node = -1;
  if (g_conf().get_val<bool>("osd_numa_prefer_iface")) {
    iface_preferred_numa_node = os_numa_node;
  }

  // messengers
  std::string msg_type = g_conf().get_val<std::string>("ms_type");
  std::string public_msg_type =
    g_conf().get_val<std::string>("ms_public_type");
  std::string cluster_msg_type =
    g_conf().get_val<std::string>("ms_cluster_type");

  public_msg_type = public_msg_type.empty() ? msg_type : public_msg_type;
  cluster_msg_type = cluster_msg_type.empty() ? msg_type : cluster_msg_type;
  uint64_t nonce = Messenger::get_random_nonce();
  Messenger *ms_public = Messenger::create(g_ceph_context, public_msg_type,
					   entity_name_t::OSD(whoami), "client", nonce);
  Messenger *ms_cluster = Messenger::create(g_ceph_context, cluster_msg_type,
					    entity_name_t::OSD(whoami), "cluster", nonce);
  Messenger *ms_hb_back_client = Messenger::create(g_ceph_context, cluster_msg_type,
					     entity_name_t::OSD(whoami), "hb_back_client", nonce);
  Messenger *ms_hb_front_client = Messenger::create(g_ceph_context, public_msg_type,
					     entity_name_t::OSD(whoami), "hb_front_client", nonce);
  Messenger *ms_hb_back_server = Messenger::create(g_ceph_context, cluster_msg_type,
						   entity_name_t::OSD(whoami), "hb_back_server", nonce);
  Messenger *ms_hb_front_server = Messenger::create(g_ceph_context, public_msg_type,
						    entity_name_t::OSD(whoami), "hb_front_server", nonce);
  Messenger *ms_objecter = Messenger::create(g_ceph_context, public_msg_type,
					     entity_name_t::OSD(whoami), "ms_objecter", nonce);
  if (!ms_public || !ms_cluster || !ms_hb_front_client || !ms_hb_back_client || !ms_hb_back_server || !ms_hb_front_server || !ms_objecter)
    forker.exit(1);
  ms_cluster->set_cluster_protocol(CEPH_OSD_PROTOCOL);
  ms_hb_front_client->set_cluster_protocol(CEPH_OSD_PROTOCOL);
  ms_hb_back_client->set_cluster_protocol(CEPH_OSD_PROTOCOL);
  ms_hb_back_server->set_cluster_protocol(CEPH_OSD_PROTOCOL);
  ms_hb_front_server->set_cluster_protocol(CEPH_OSD_PROTOCOL);

  dout(0) << "starting osd." << whoami
          << " osd_data " << data_path
          << " " << ((journal_path.empty()) ?
		    "(no journal)" : journal_path)
          << dendl;

  uint64_t message_size =
    g_conf().get_val<Option::size_t>("osd_client_message_size_cap");
  boost::scoped_ptr<Throttle> client_byte_throttler(
    new Throttle(g_ceph_context, "osd_client_bytes", message_size));
  uint64_t message_cap = g_conf().get_val<uint64_t>("osd_client_message_cap");
  boost::scoped_ptr<Throttle> client_msg_throttler(
    new Throttle(g_ceph_context, "osd_client_messages", message_cap));

  // All feature bits 0 - 34 should be present from dumpling v0.67 forward
  uint64_t osd_required =
    CEPH_FEATURE_UID |
    CEPH_FEATURE_PGID64 |
    CEPH_FEATURE_OSDENC;

  ms_public->set_default_policy(Messenger::Policy::stateless_registered_server(0));
  ms_public->set_policy_throttlers(entity_name_t::TYPE_CLIENT,
				   client_byte_throttler.get(),
				   client_msg_throttler.get());
  ms_public->set_policy(entity_name_t::TYPE_MON,
                        Messenger::Policy::lossy_client(osd_required));
  ms_public->set_policy(entity_name_t::TYPE_MGR,
                        Messenger::Policy::lossy_client(osd_required));

  ms_cluster->set_default_policy(Messenger::Policy::stateless_server(0));
  ms_cluster->set_policy(entity_name_t::TYPE_MON, Messenger::Policy::lossy_client(0));
  ms_cluster->set_policy(entity_name_t::TYPE_OSD,
			 Messenger::Policy::lossless_peer(osd_required));
  ms_cluster->set_policy(entity_name_t::TYPE_CLIENT,
			 Messenger::Policy::stateless_server(0));

  ms_hb_front_client->set_policy(entity_name_t::TYPE_OSD,
			  Messenger::Policy::lossy_client(0));
  ms_hb_back_client->set_policy(entity_name_t::TYPE_OSD,
			  Messenger::Policy::lossy_client(0));
  ms_hb_back_server->set_policy(entity_name_t::TYPE_OSD,
				Messenger::Policy::stateless_server(0));
  ms_hb_front_server->set_policy(entity_name_t::TYPE_OSD,
				 Messenger::Policy::stateless_server(0));

  ms_objecter->set_default_policy(Messenger::Policy::lossy_client(CEPH_FEATURE_OSDREPLYMUX));

  entity_addrvec_t public_addrs, public_bind_addrs, cluster_addrs;
  r = pick_addresses(g_ceph_context, CEPH_PICK_ADDRESS_PUBLIC, &public_addrs,
		     iface_preferred_numa_node);
  if (r < 0) {
    derr << "Failed to pick public address." << dendl;
    forker.exit(1);
  } else {
    dout(10) << "picked public_addrs " << public_addrs << dendl;
  }
  r = pick_addresses(g_ceph_context, CEPH_PICK_ADDRESS_PUBLIC_BIND,
		     &public_bind_addrs, iface_preferred_numa_node);
  if (r == -ENOENT) {
    dout(10) << "there is no public_bind_addrs, defaulting to public_addrs"
	     << dendl;
    public_bind_addrs = public_addrs;
  } else if (r < 0) {
    derr << "Failed to pick public bind address." << dendl;
    forker.exit(1);
  } else {
    dout(10) << "picked public_bind_addrs " << public_bind_addrs << dendl;
  }
  r = pick_addresses(g_ceph_context, CEPH_PICK_ADDRESS_CLUSTER, &cluster_addrs,
		     iface_preferred_numa_node);
  if (r < 0) {
    derr << "Failed to pick cluster address." << dendl;
    forker.exit(1);
  }

  if (ms_public->bindv(public_bind_addrs, public_addrs) < 0) {
    derr << "Failed to bind to " << public_bind_addrs << dendl;
    forker.exit(1);
  }

  if (ms_cluster->bindv(cluster_addrs) < 0)
    forker.exit(1);

  bool is_delay = g_conf().get_val<bool>("osd_heartbeat_use_min_delay_socket");
  if (is_delay) {
    ms_hb_front_client->set_socket_priority(SOCKET_PRIORITY_MIN_DELAY);
    ms_hb_back_client->set_socket_priority(SOCKET_PRIORITY_MIN_DELAY);
    ms_hb_back_server->set_socket_priority(SOCKET_PRIORITY_MIN_DELAY);
    ms_hb_front_server->set_socket_priority(SOCKET_PRIORITY_MIN_DELAY);
  }

  entity_addrvec_t hb_front_addrs = public_bind_addrs;
  for (auto& a : hb_front_addrs.v) {
    a.set_port(0);
  }
  if (ms_hb_front_server->bindv(hb_front_addrs) < 0)
    forker.exit(1);
  if (ms_hb_front_client->client_bind(hb_front_addrs.front()) < 0)
    forker.exit(1);

  entity_addrvec_t hb_back_addrs = cluster_addrs;
  for (auto& a : hb_back_addrs.v) {
    a.set_port(0);
  }
  if (ms_hb_back_server->bindv(hb_back_addrs) < 0)
    forker.exit(1);
  if (ms_hb_back_client->client_bind(hb_back_addrs.front()) < 0)
    forker.exit(1);

  // install signal handlers
  init_async_signal_handler();
  register_async_signal_handler(SIGHUP, sighup_handler);

  TracepointProvider::initialize<osd_tracepoint_traits>(g_ceph_context);
  TracepointProvider::initialize<os_tracepoint_traits>(g_ceph_context);
  TracepointProvider::initialize<bluestore_tracepoint_traits>(g_ceph_context);
#ifdef WITH_OSD_INSTRUMENT_FUNCTIONS
  TracepointProvider::initialize<cyg_profile_traits>(g_ceph_context);
#endif

  srand(time(NULL) + getpid());

  ceph::async::io_context_pool poolctx(
    cct->_conf.get_val<std::uint64_t>("osd_asio_thread_count"));

  MonClient mc(g_ceph_context, poolctx);
  if (mc.build_initial_monmap() < 0)
    return -1;
  global_init_chdir(g_ceph_context);

  if (global_init_preload_erasure_code(g_ceph_context) < 0) {
    forker.exit(1);
  }

  osdptr = new OSD(g_ceph_context,
		   std::move(store),
		   whoami,
		   ms_cluster,
		   ms_public,
		   ms_hb_front_client,
		   ms_hb_back_client,
		   ms_hb_front_server,
		   ms_hb_back_server,
		   ms_objecter,
		   &mc,
		   data_path,
		   journal_path,
		   poolctx);

  int err = osdptr->pre_init();
  if (err < 0) {
    derr << TEXT_RED << " ** ERROR: osd pre_init failed: " << cpp_strerror(-err)
	 << TEXT_NORMAL << dendl;
    forker.exit(1);
  }

  ms_public->start();
  ms_hb_front_client->start();
  ms_hb_back_client->start();
  ms_hb_front_server->start();
  ms_hb_back_server->start();
  ms_cluster->start();
  ms_objecter->start();

  // start osd
  err = osdptr->init();
  if (err < 0) {
    derr << TEXT_RED << " ** ERROR: osd init failed: " << cpp_strerror(-err)
         << TEXT_NORMAL << dendl;
    forker.exit(1);
  }

  // -- daemonize --

  if (g_conf()->daemonize) {
    global_init_postfork_finish(g_ceph_context);
    forker.daemonize();
  }


  register_async_signal_handler_oneshot(SIGINT, handle_osd_signal);
  register_async_signal_handler_oneshot(SIGTERM, handle_osd_signal);

  osdptr->final_init();

  if (g_conf().get_val<bool>("inject_early_sigterm"))
    kill(getpid(), SIGTERM);

  ms_public->wait();
  ms_hb_front_client->wait();
  ms_hb_back_client->wait();
  ms_hb_front_server->wait();
  ms_hb_back_server->wait();
  ms_cluster->wait();
  ms_objecter->wait();

  unregister_async_signal_handler(SIGHUP, sighup_handler);
  unregister_async_signal_handler(SIGINT, handle_osd_signal);
  unregister_async_signal_handler(SIGTERM, handle_osd_signal);
  shutdown_async_signal_handler();

  // done
  poolctx.stop();
  delete osdptr;
  delete ms_public;
  delete ms_hb_front_client;
  delete ms_hb_back_client;
  delete ms_hb_front_server;
  delete ms_hb_back_server;
  delete ms_cluster;
  delete ms_objecter;

  client_byte_throttler.reset();
  client_msg_throttler.reset();

  // cd on exit, so that gmon.out (if any) goes into a separate directory for each node.
  char s[20];
  snprintf(s, sizeof(s), "gmon/%d", getpid());
  if ((mkdir(s, 0755) == 0) && (chdir(s) == 0)) {
    dout(0) << "ceph-osd: gmon.out should be in " << s << dendl;
  }

  return 0;
}
