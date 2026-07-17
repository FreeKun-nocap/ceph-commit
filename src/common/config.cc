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

#include <filesystem>
#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "common/config.h"
#include "common/config_obs.h"
#include "include/str_list.h"
#include "include/stringify.h"
#include "osd/osd_types.h"
#include "common/errno.h"
#include "common/hostname.h"
#include "common/dout.h"

#include <fmt/core.h>

#include <sstream>

/* Don't use standard Ceph logging in this file.
 * We can't use logging until it's initialized, and a lot of the necessary
 * initialization happens here.
 */
#undef dout
#undef pdout
#undef derr
#undef generic_dout

// set set_mon_vals()
#define dout_subsys ceph_subsys_monc

namespace fs = std::filesystem;

using std::cerr;
using std::cout;
using std::map;
using std::less;
using std::list;
using std::ostream;
using std::ostringstream;
using std::pair;
using std::string;
using std::string_view;
using std::vector;

using ceph::bufferlist;
using ceph::decode;
using ceph::encode;
using ceph::Formatter;

const char *CEPH_CONF_FILE_DEFAULT = "$data_dir/config,/etc/ceph/$cluster.conf,$home/.ceph/$cluster.conf,$cluster.conf"
#if defined(__FreeBSD__)
    ",/usr/local/etc/ceph/$cluster.conf"
#elif defined(_WIN32)
    ",$programdata/ceph/$cluster.conf"
#endif
    ;

#define _STR(x) #x
#define STRINGIFY(x) _STR(x)

// Populate list of legacy_values according to the OPTION() definitions
// Note that this is just setting up our map of name->member ptr.  The
// default values etc will get loaded in along with new-style data,
// as all loads write to both the values map, and the legacy
// members if present.
const std::map<std::string_view, md_config_t::member_ptr_t> md_config_t::legacy_values = {
#define OPTION(name, type) \
  {STRINGIFY(name), &ConfigValues::name},
#define SAFE_OPTION(name, type) OPTION(name, type)
#include "options/legacy_config_opts.h"
#undef OPTION
#undef SAFE_OPTION
};

const char *ceph_conf_level_name(int level)
{
  switch (level) {
  case CONF_DEFAULT: return "default";   // built-in default
  case CONF_MON: return "mon";           // monitor config database
  case CONF_ENV: return "env";           // process environment (CEPH_ARGS)
  case CONF_FILE: return "file";         // ceph.conf file
  case CONF_CMDLINE: return "cmdline";   // process command line args
  case CONF_OVERRIDE: return "override"; // injectargs or 'config set' at runtime
  case CONF_FINAL: return "final";
  default: return "???";
  }
}

int ceph_resolve_file_search(const std::string& filename_list,
			     std::string& result)
{
  list<string> ls;
  get_str_list(filename_list, ";,", ls);

  int ret = -ENOENT;
  list<string>::iterator iter;
  for (iter = ls.begin(); iter != ls.end(); ++iter) {
    int fd = ::open(iter->c_str(), O_RDONLY|O_CLOEXEC);
    if (fd < 0) {
      ret = -errno;
      continue;
    }
    close(fd);
    result = *iter;
    return 0;
  }

  return ret;
}

static int conf_stringify(const Option::value_t& v, string *out)
{
  if (v == Option::value_t{}) {
    return -ENOENT;
  }
  *out = Option::to_str(v);
  return 0;
}

/**
 * md_config_t 构造函数 —— 初始化 Ceph 配置系统的核心对象。
 *
 * @param values   配置值存储（ConfigValues），实际持有所有配置项的当前值
 * @param tracker  配置变更追踪器（用于通知观察者配置变更）
 * @param is_daemon 是否为守护进程模式（影响日志输出目标等行为）
 *
 * 构造时执行：
 *   1. 将编译时注册的全部 Option 加载到 schema map 中，实现 O(1) 查找
 *   2. 为每个调试子系统自动生成 debug_<name> 选项
 */
md_config_t::md_config_t(ConfigValues& values,
			 const ConfigTracker& tracker,
			 bool is_daemon)
  : is_daemon(is_daemon)
{
  // 遍历编译时注册的全部 Option（来自各模块 YAML 定义生成的 ceph_options[] 数组）
  for (const auto &i : ceph_options) {
    if (schema.count(i.name)) {
      // 检测重复注册的配置项（可能是代码 bug），直接终止进程
      std::cerr << "Duplicate config key in schema: '" << i.name << "'"
                << std::endl;
      ceph_abort();
    }
    // 插入 schema map，key 为配置项名，value 为 Option 元数据引用
    schema.emplace(i.name, i);
  }

  // ===== 动态生成 debug_* 选项 =====
  // 为每个调试子系统（如 osd, mon, mds, bluestore 等）自动注册对应的
  // debug_<subsys> 配置项，格式为 "N" 或 "N/M"
  subsys_options.reserve(values.subsys.get_num());
  for (unsigned i = 0; i < values.subsys.get_num(); ++i) {
    subsys_options.emplace_back(
      fmt::format("debug_{}", values.subsys.get_name(i)), Option::TYPE_STR, Option::LEVEL_ADVANCED);
    Option& opt = subsys_options.back();
    // 默认值：日志级别/收集级别，如 "4/5"
    opt.set_default(fmt::format("{}/{}", values.subsys.get_log_level(i), values.subsys.get_gather_level(i)));
    opt.set_description(fmt::format("Debug level for {}", values.subsys.get_name(i)).c_str());
    opt.set_flag(Option::FLAG_RUNTIME);     // 标记为运行时可变（无需重启）
    opt.set_long_description("The value takes the form 'N' or 'N/M' where N and M are values between 0 and 99.  N is the debug level to log (all values below this are included), and M is the level to gather and buffer in memory.  In the event of a crash, the most recent items <= M are dumped to the log file.");
    opt.set_subsys(i);
    // 值校验器：检查 N/M 格式是否合法（两个 0-99 的整数）
    opt.set_validator([](std::string *value, std::string *error_message) {
	int m, n;
	int r = sscanf(value->c_str(), "%d/%d", &m, &n);
	if (r >= 1) {
	  if (m < 0 || m > 99) {
	    *error_message = "value must be in range [0, 99]";
	    return -ERANGE;
	  }
	  if (r == 2) {
	    if (n < 0 || n > 99) {
	      *error_message = "value must be in range [0, 99]";
	      return -ERANGE;
	    }
	  } else {
	    // 如果只写了 N（无 /M），归一化为 N/N
	    n = m;
	    *value = fmt::format("{}/{}", m, n);
	  }
	} else {
	  *error_message = "value must take the form N or N/M, where N and M are integers";
	  return -EINVAL;
	}
	return 0;
      });
  }
  // 将动态生成的 debug_* 选项也注册进 schema
  for (auto& opt : subsys_options) {
    schema.emplace(opt.name, opt);
  }

  // 校验 schema 完整性（检查 see-also 引用等）
  validate_schema();

  // ===== 校验所有配置项的默认值 =====
  // 对每个 STR 类型配置项，用其 pre_validate() 函数检查默认值是否合法。
  // 如果校验器对字符串做了规范化（如 trim、大小写转换），则用规范化后的值覆盖默认值。
  for (const auto &i : schema) {
    const Option &opt = i.second;
    if (opt.type == Option::TYPE_STR) {
      bool has_daemon_default = (opt.daemon_value != Option::value_t{});
      Option::value_t default_val;
      if (is_daemon && has_daemon_default) {
        default_val = opt.daemon_value;     // 守护进程使用 daemon_value
      } else {
        default_val = opt.value;            // 否则使用通用默认值
      }
      auto* def_str = std::get_if<std::string>(&default_val);
      std::string val = *def_str;
      std::string err;
      if (opt.pre_validate(&val, &err) != 0) {
        // 编译内置的默认值居然通不过自己的校验 → 严重 bug，终止进程
        std::cerr << "Default value " << opt.name << "=" << *def_str << " is "
                     "invalid: " << err << std::endl;
        ceph_abort();
      }
      if (val != *def_str) {
        // 校验器对默认值做了规范化（如归一化字符串形式），用规范化后的值替换
        set_val_default(values, tracker, opt.name, val);
      }
    }
  }

  // 将默认值同步到遗留 C 结构体字段（向后兼容旧代码使用的全局变量）
  update_legacy_vals(values);
}

md_config_t::~md_config_t()
{
}

/**
 * Sanity check schema.  Assert out on failures, to ensure any bad changes
 * cannot possibly pass any testing and make it into a release.
 */
void md_config_t::validate_schema()
{
  for (const auto &i : schema) {
    const auto &opt = i.second;
    for (const auto &see_also_key : opt.see_also) {
      if (schema.count(see_also_key) == 0) {
        std::cerr << "Non-existent see-also key '" << see_also_key
                  << "' on option '" << opt.name << "'" << std::endl;
        ceph_abort();
      }
    }
  }

  for (const auto &i : legacy_values) {
    if (schema.count(i.first) == 0) {
      std::cerr << "Schema is missing legacy field '" << i.first << "'"
                << std::endl;
      ceph_abort();
    }
  }
}

/**
 * 根据配置项名称查找对应的 Option 定义元数据。
 *
 * 在 schema map 中查找配置项，该 map 在程序启动时由所有模块注册的
 * Option 定义填充。如果配置项未注册，返回 nullptr。
 *
 * @param name 配置项名称（如 "osd_data", "log_file" 等）
 * @return 找到的 Option 指针，不存在则返回 nullptr
 */
const Option *md_config_t::find_option(const std::string_view name) const
{
  auto p = schema.find(name);           // 在全局 schema 表中查找
  if (p != schema.end()) {
    return &p->second;                  // 返回 Option 的指针
  }
  return nullptr;                       // 未注册的配置项返回空指针
}

void md_config_t::set_val_default(ConfigValues& values,
				  const ConfigTracker& tracker,
				  const string_view name, const std::string& val)
{
  const Option *o = find_option(name);
  ceph_assert(o);
  string err;
  int r = _set_val(values, tracker, val, *o, CONF_DEFAULT, &err);
  ceph_assert(r >= 0);
}

int md_config_t::set_mon_vals(CephContext *cct,
    ConfigValues& values,
    const ConfigTracker& tracker,
    const map<string,string,less<>>& kv,
    config_callback config_cb)
{
  ignored_mon_values.clear();

  if (!config_cb) {
    ldout(cct, 4) << __func__ << " no callback set" << dendl;
  }

  for (auto& i : kv) {
    if (config_cb) {
      if (config_cb(i.first, i.second)) {
	ldout(cct, 4) << __func__ << " callback consumed " << i.first << dendl;
	continue;
      }
      ldout(cct, 4) << __func__ << " callback ignored " << i.first << dendl;
    }
    const Option *o = find_option(i.first);
    if (!o) {
      ldout(cct,10) << __func__ << " " << i.first << " = " << i.second
		    << " (unrecognized option)" << dendl;
      continue;
    }
    if (o->has_flag(Option::FLAG_NO_MON_UPDATE)) {
      ignored_mon_values.emplace(i);
      continue;
    }
    std::string err;
    int r = _set_val(values, tracker, i.second, *o, CONF_MON, &err);
    if (r < 0) {
      ldout(cct, 4) << __func__ << " failed to set " << i.first << " = "
		    << i.second << ": " << err << dendl;
      ignored_mon_values.emplace(i);
    } else if (r == ConfigValues::SET_NO_CHANGE ||
	       r == ConfigValues::SET_NO_EFFECT) {
      ldout(cct,20) << __func__ << " " << i.first << " = " << i.second
		    << " (no change)" << dendl;
    } else if (r == ConfigValues::SET_HAVE_EFFECT) {
      ldout(cct,10) << __func__ << " " << i.first << " = " << i.second << dendl;
    } else {
      ceph_abort();
    }
  }
  values.for_each([&] (auto name, auto configs) {
    auto config = configs.find(CONF_MON);
    if (config == configs.end()) {
      return;
    }
    if (kv.find(name) != kv.end()) {
      return;
    }
    ldout(cct,10) << __func__ << " " << name
		  << " cleared (was " << Option::to_str(config->second) << ")"
		  << dendl;
    values.rm_val(name, CONF_MON);
    // if this is a debug option, it needs to propagate to teh subsys;
    // this isn't covered by update_legacy_vals() below.  similarly,
    // we want to trigger a config notification for these items.
    const Option *o = find_option(name);
    _refresh(values, *o);
  });
  values_bl.clear();
  update_legacy_vals(values);
  return 0;
}

/**
 * parse_config_files —— 解析 Ceph 配置文件（ceph.conf）。
 *
 * @param values          配置值存储（解析结果写入这里）
 * @param tracker         配置变更追踪器
 * @param conf_files_str  用户指定的配置文件路径列表（冒号分隔，为空则用默认搜索路径）
 * @param warnings        警告信息输出流（解析过程中的警告写到这里）
 * @param flags           标志位（CINIT_FLAG_NO_DEFAULT_CONFIG_FILE 等）
 * @return                0 成功，-ENOENT 没找到文件，-EINVAL/其他 解析错误
 *
 * 执行流程：
 *   1. 安全检查：线程已启动则不允许再读配置文件
 *   2. 生成配置文件搜索路径列表（用户指定路径 + /etc/ceph/ + ~/.ceph/ 等）
 *   3. 按顺序逐个尝试读取并解析，第一个成功的即为使用的配置文件
 *   4. 全部失败返回 -ENOENT；解析过程中的语法错误返回对应错误码
 */
int md_config_t::parse_config_files(ConfigValues& values,
				    const ConfigTracker& tracker,
				    const char *conf_files_str,
				    std::ostream *warnings,
				    int flags)
{
  // 运行时保护：线程已启动后不允许重新解析配置文件
  // （运行时改配置走 admin socket / monitor 下发等路径）
  if (safe_to_start_threads)
    return -ENOSYS;

  // 如果还没有集群名且用户未指定配置文件，从环境变量或默认值推断
  if (values.cluster.empty() && !conf_files_str) {
    values.cluster = get_cluster_name(nullptr);
  }

  // 逐个尝试配置文件搜索路径（默认路径：/etc/ceph/$cluster.conf 等）
  for (auto& fn : get_conffile_paths(values, conf_files_str, warnings, flags)) {
    bufferlist bl;
    std::string error;
    if (bl.read_file(fn.c_str(), &error)) {   // 读取文件内容到 buffer
      parse_error = error;                      // 记录读取错误
      continue;                                  // 读失败，试下一个路径
    }
    ostringstream oss;
    int ret = parse_buffer(values, tracker, bl.c_str(), bl.length(), &oss);
    if (ret == 0) {                              // 解析成功
      parse_error.clear();                        // 清除之前的错误
      conf_path = fn;                             // 记录实际生效的配置文件路径
      break;                                      // 第一个成功的就用它
    }
    parse_error = oss.str();                     // 记录解析错误信息
    if (ret != -ENOENT) {
      return ret;                                 // 不是"文件不存在"的错误 → 直接返回
    }
  }
  // 走到这里说明所有路径都试过了，且都是 ENOENT（文件不存在）
  if (conf_path.empty()) {
    return -ENOENT;                              // 一个配置文件都没找到
  }

  // 如果还没有集群名，从配置文件路径推断（如 ceph.conf → 集群名 ceph）
  if (values.cluster.empty()) {
    values.cluster = get_cluster_name(conf_path.c_str());
  }
  update_legacy_vals(values);                     // 同步到遗留的 C 结构体字段
  return 0;
}

/**
 * parse_buffer —— 解析内存中的配置文件内容（INI 格式），把结果写入 values。
 *
 * @param values    配置值存储（解析结果写入这里，以 CONF_FILE 优先级）
 * @param tracker   配置变更追踪器
 * @param buf       配置文件内容的内存指针
 * @param len       内容长度
 * @param warnings  警告输出流（解析错误时写入）
 * @return          0 成功，-EINVAL 解析失败
 *
 * 执行流程：
 *   1. 用 ConfFile 解析 INI 格式的文本（[global]、[osd]、[osd.0] 等 section）
 *   2. 确定本进程需要读取哪些 section（如 [global] + [osd] + [osd.$id]）
 *   3. 遍历 schema 中所有已知配置项，从配置文件对应 section 取值
 *   4. 以 CONF_FILE 优先级写入 values（解析失败的项记录警告，不中断）
 */
int
md_config_t::parse_buffer(ConfigValues& values,
			  const ConfigTracker& tracker,
			  const char* buf, size_t len,
			  std::ostream* warnings)
{
  // 第一步：用 ConfFile 解析 INI 格式文本（处理 section、key=value、注释等）
  if (!cf.parse_buffer(string_view{buf, len}, warnings)) {
    return -EINVAL;                           // 语法错误，直接返回
  }
  // 第二步：确定本进程应读取的 section 列表
  // 例如 OSD 进程会读：[global] → [osd] → [osd.0]（越后面优先级越高）
  const auto my_sections = get_my_sections(values);

  // 第三步：遍历所有已知配置项，从配置文件中取值并写入
  for (const auto &i : schema) {
    const auto &opt = i.second;
    std::string val;
    // 从配置文件的 section 中查找该配置项的值（找不到则跳过，保留默认值）
    if (_get_val_from_conf_file(my_sections, opt.name, val)) {
      continue;
    }
    // 以 CONF_FILE 优先级写入 values
    std::string error_message;
    if (_set_val(values, tracker, val, opt, CONF_FILE, &error_message) < 0) {
      // 写入失败（类型不匹配、校验失败等）→ 记录警告，继续处理其他项
      if (warnings != nullptr) {
        *warnings << "parse error setting " << std::quoted(opt.name)
                  << " to " << std::quoted(val);
        if (!error_message.empty()) {
          *warnings << " (" << error_message << ")";
        }
        *warnings << '\n';
      }
    }
  }
  // 检查并警告旧版本的 section 命名（如 [mds] → 应使用 [mds.$id]）
  cf.check_old_style_section_names({"mds", "mon", "osd"}, cerr);
  return 0;
}

/**
 * get_conffile_paths —— 生成配置文件的搜索路径列表。
 *
 * @param values          当前配置值（用于变量展开，如 $cluster、$data_dir）
 * @param conf_files_str  用户指定的配置文件路径（冒号/分号分隔，可为空）
 * @param warnings        警告输出流（路径展开失败时写入警告）
 * @param flags           初始化标志位
 * @return                按优先级排序的配置文件路径列表
 *
 * 优先级从高到低：
 *   1. 命令行 -c/--conf 指定的路径（conf_files_str）
 *   2. 环境变量 CEPH_CONF 指定的路径
 *   3. 编译时默认路径 CEPH_CONF_FILE_DEFAULT（如 /etc/ceph/$cluster.conf）
 *   4. 如果 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE 则跳过默认路径
 */
std::list<std::string>
md_config_t::get_conffile_paths(const ConfigValues& values,
				const char *conf_files_str,
				std::ostream *warnings,
				int flags) const
{
  // 如果用户未指定配置文件路径
  if (!conf_files_str) {
    // 先查环境变量 CEPH_CONF
    const char *c = getenv("CEPH_CONF");
    if (c) {
      conf_files_str = c;
    } else {
      // 没有环境变量 → 用编译时默认路径
      // 但如果设置了 NO_DEFAULT_CONFIG_FILE 标志，则一个都不找
      if (flags & CINIT_FLAG_NO_DEFAULT_CONFIG_FILE)
	return {};
      conf_files_str = CEPH_CONF_FILE_DEFAULT;  // 如 "/etc/ceph/$cluster.conf"
    }
  }

  // 将路径字符串按分号或逗号分隔，拆成列表（支持多个候选路径）
  std::list<std::string> paths;
  get_str_list(conf_files_str, ";,", paths);

  // 遍历路径列表，做变量展开和无效路径过滤
  for (auto i = paths.begin(); i != paths.end(); ) {
    string& path = *i;
    // 如果路径里有 $data_dir 但还不知道 data_dir 是什么 → 这条路径没用，删掉
    if (path.find("$data_dir") != path.npos &&
	data_dir_option.empty()) {
      // $data_dir 还没确定，跳过这条路径
      i = paths.erase(i);
    } else {
      early_expand_meta(values, path, warnings);  // 展开 $cluster、$name 等变量
      ++i;
    }
  }
  return paths;
}

std::string md_config_t::get_cluster_name(const char* conffile)
{
  if (conffile) {
    // If cluster name is not set yet, use the prefix of the
    // basename of configuration file as cluster name.
    if (fs::path path{conffile}; path.extension() == ".conf") {
      return path.stem().string();
    } else {
      // If the configuration file does not follow $cluster.conf
      // convention, we do the last try and assign the cluster to
      // 'ceph'.
      return "ceph";
    }
  } else {
    // set the cluster name to 'ceph' when configuration file is not specified.
    return "ceph";
  }
}

/**
 * parse_env —— 从环境变量中读取配置，以 CONF_ENV 优先级写入 values。
 *
 * @param entity_type  模块类型（OSD/MON/MDS 等，用于针对性处理内存等配置）
 * @param values       配置值存储（写入这里）
 * @param tracker      配置变更追踪器
 * @param args_var     参数环境变量名（默认 "CEPH_ARGS"）
 *
 * 处理的环境变量：
 *   - CEPH_KEYRING    → keyring 路径
 *   - CEPH_LIB        → erasure_code_dir / plugin_dir / osd_class_dir
 *   - TMPDIR          → tmp_dir
 *   - POD_MEMORY_LIMIT → K8s Pod 内存上限（推算 osd_memory_target 默认值）
 *   - POD_MEMORY_REQUEST → K8s Pod 内存申请（设置 osd_memory_target）
 *   - CEPH_ARGS       → 额外命令行参数（再经 parse_argv 解析）
 */
void md_config_t::parse_env(unsigned entity_type,
			    ConfigValues& values,
			    const ConfigTracker& tracker,
			    const char *args_var)
{
  // 运行时保护：线程启动后不允许再从环境变量加载配置
  if (safe_to_start_threads)
    return;
  if (!args_var) {
    args_var = "CEPH_ARGS";
  }

  // CEPH_KEYRING → keyring 配置项
  if (auto s = getenv("CEPH_KEYRING"); s) {
    string err;
    _set_val(values, tracker, s, *find_option("keyring"), CONF_ENV, &err);
  }

  // CEPH_LIB → 三个插件目录（纠删码、通用插件、OSD 类）
  if (auto dir = getenv("CEPH_LIB"); dir) {
    for (auto name : { "erasure_code_dir", "plugin_dir", "osd_class_dir" }) {
    std::string err;
      const Option *o = find_option(name);
      ceph_assert(o);
      _set_val(values, tracker, dir, *o, CONF_ENV, &err);
    }
  }

  // TMPDIR → tmp_dir（临时文件目录）
  if (auto s = getenv("TMPDIR"); s) {
    string err;
    _set_val(values, tracker, s, *find_option("tmp_dir"), CONF_ENV, &err);
  }

  // ===== Kubernetes Pod 内存限制处理 =====
  //
  // K8s 有两种资源规格：
  //   - requests（申请值）：调度器用，决定 Pod 放哪台节点，保守值
  //     对应 POD_MEMORY_REQUEST（Rook 设置）→ 作为内存目标值
  //   - limits（上限值）：运行时限制，超了会 OOM kill，通常 > requests
  //     对应 cgroup 内存限制 → 作为 osd_memory_target 的默认值（可被覆盖）
  //
  // 优先级：POD_MEMORY_REQUEST > 配置文件/其他 > POD_MEMORY_LIMIT(默认值)
  //
  uint64_t pod_limit = 0, pod_request = 0;

  // POD_MEMORY_LIMIT：按比例推算 osd_memory_target 默认值
  if (auto pod_lim = getenv("POD_MEMORY_LIMIT"); pod_lim) {
    string err;
    uint64_t v = atoll(pod_lim);
    if (v) {
      switch (entity_type) {
      case CEPH_ENTITY_TYPE_OSD:
        {
	  double cgroup_ratio = get_val<double>(
	    values, "osd_memory_target_cgroup_limit_ratio");
	  if (cgroup_ratio > 0.0) {
	    pod_limit = v * cgroup_ratio;
	    // 设为默认值（最低优先级），这样显式配置可以覆盖
	    set_val_default(values, tracker,
			    "osd_memory_target", stringify(pod_limit));
	  }
	}
      }
    }
  }

  // POD_MEMORY_REQUEST 赋值到 pod_request
  if (auto pod_req = getenv("POD_MEMORY_REQUEST"); pod_req) {
    if (uint64_t v = atoll(pod_req); v) {
      pod_request = v;
    }
  }
  // 如果同时设置了 LIMIT 和 REQUEST，取较小值（k8s 可能把 LIMIT 和 REQUEST 设相等）
  if (pod_request && pod_limit) {
    pod_request = std::min<uint64_t>(pod_request, pod_limit);
  }
  if (pod_request) {
    string err;
    switch (entity_type) {
    case CEPH_ENTITY_TYPE_OSD:
      _set_val(values, tracker, stringify(pod_request),
	       *find_option("osd_memory_target"),
	       CONF_ENV, &err);
      break;
    }
  }

  // CEPH_ARGS：把环境变量里的额外参数当作命令行参数再解析一次
  if (getenv(args_var)) {
    vector<const char *> env_args;
    env_to_vec(env_args, args_var);       // 环境变量字符串 → 参数数组
    parse_argv(values, tracker, env_args, CONF_ENV);
  }
}

void md_config_t::show_config(const ConfigValues& values,
			      std::ostream& out) const
{
  _show_config(values, &out, nullptr);
}

void md_config_t::show_config(const ConfigValues& values,
			      Formatter *f) const
{
  _show_config(values, nullptr, f);
}

void md_config_t::config_options(Formatter *f) const
{
  f->open_array_section("options");
  for (const auto& i: schema) {
    f->dump_object("option", i.second);
  }
  f->close_section();
}

void md_config_t::_show_config(const ConfigValues& values,
			       std::ostream *out, Formatter *f) const
{
  if (out) {
    *out << "name = " << values.name << std::endl;
    *out << "cluster = " << values.cluster << std::endl;
  }
  if (f) {
    f->dump_string("name", stringify(values.name));
    f->dump_string("cluster", values.cluster);
  }
  for (const auto& i: schema) {
    const Option &opt = i.second;
    string val;
    conf_stringify(_get_val(values, opt), &val);
    if (out) {
      *out << opt.name << " = " << val << std::endl;
    }
    if (f) {
      f->dump_string(opt.name.c_str(), val);
    }
  }
}

/**
 * parse_argv —— 解析命令行参数，以指定优先级写入配置。
 *
 * @param values  配置值存储（写入结果）
 * @param tracker 配置变更追踪器
 * @param args    命令行参数数组（会被修改，已识别的参数被移除）
 * @param level   优先级级别（CONF_CMDLINE / CONF_ENV 等）
 * @return        0 成功，负值错误码
 *
 * 处理三类参数：
 *   1. 特殊展示型参数（--show_conf / --show-config / --show-config-value）
 *   2. 有短选项的常用参数（-f / -d / -m / -k / -K / -M / -r 等）
 *   3. 通用 --key=value 形式（走 parse_option 统一处理）
 */
int md_config_t::parse_argv(ConfigValues& values,
			    const ConfigTracker& tracker,
			    std::vector<const char*>& args, int level)
{
  // 运行时保护：线程启动后不允许再解析命令行参数
  if (safe_to_start_threads) {
    return -ENOSYS;
  }

  // 注意：此函数不直接修改 values 中的成员变量，而是通过 set_val 系列函数写入，
  // 这样可以确保正确触发观察者通知和一致性更新。

  std::string val;
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    // 遇到 -- 则停止解析（后续参数留给业务层处理）
    if (strcmp(*i, "--") == 0) {
      // 这里不移除 --，因为后续的参数解析仍需要看到它
      break;
    }
    // --show_conf：直接打印已加载的配置文件内容并退出
    else if (ceph_argparse_flag(args, i, "--show_conf", (char*)NULL)) {
      cerr << cf << std::endl;
      _exit(0);
    }
    // --show-config：标记为展示配置，待后面 do_argv_commands 统一输出
    else if (ceph_argparse_flag(args, i, "--show_config", (char*)NULL)) {
      do_show_config = true;
    }
    // --show-config-value <key>：展示单个配置项的值
    else if (ceph_argparse_witharg(args, i, &val, "--show_config_value", (char*)NULL)) {
      do_show_config_value = val;
    }
    // --no-mon-config / --mon-config：控制是否从 Monitor 拉配置
    else if (ceph_argparse_flag(args, i, "--no-mon-config", (char*)NULL)) {
      values.no_mon_config = true;
    }
    else if (ceph_argparse_flag(args, i, "--mon-config", (char*)NULL)) {
      values.no_mon_config = false;
    }
    // --foreground / -f：前台运行（不 daemonize）
    else if (ceph_argparse_flag(args, i, "--foreground", "-f", (char*)NULL)) {
      set_val_or_die(values, tracker, "daemonize", "false");
    }
    // -d：调试模式（前台运行 + 输出日志到 stderr + 关闭 syslog）
    else if (ceph_argparse_flag(args, i, "-d", (char*)NULL)) {
      set_val_or_die(values, tracker, "fuse_debug", "true");
      set_val_or_die(values, tracker, "daemonize", "false");
      set_val_or_die(values, tracker, "log_file", "");
      set_val_or_die(values, tracker, "log_to_stderr", "true");
      set_val_or_die(values, tracker, "err_to_stderr", "true");
      set_val_or_die(values, tracker, "log_to_syslog", "false");
    }
    // 以下是带短选项的常用参数（注意：短字母资源有限，谨慎添加）
    else if (ceph_argparse_witharg(args, i, &val, "--monmap", "-M", (char*)NULL)) {
      set_val_or_die(values, tracker, "monmap", val.c_str());
    }
    else if (ceph_argparse_witharg(args, i, &val, "--mon_host", "-m", (char*)NULL)) {
      set_val_or_die(values, tracker, "mon_host", val.c_str());
    }
    else if (ceph_argparse_witharg(args, i, &val, "--bind", (char*)NULL)) {
      set_val_or_die(values, tracker, "public_addr", val.c_str());
    }
    // --keyfile / -K：从文件或 stdin 读取密钥
    else if (ceph_argparse_witharg(args, i, &val, "--keyfile", "-K", (char*)NULL)) {
      bufferlist bl;
      string err;
      int r;
      if (val == "-") {
	r = bl.read_fd(STDIN_FILENO, 1024);        // 从标准输入读
      } else {
	r = bl.read_file(val.c_str(), &err);      // 从文件读
      }
      if (r >= 0) {
	string k(bl.c_str(), bl.length());
	set_val_or_die(values, tracker, "key", k.c_str());
      }
    }
    else if (ceph_argparse_witharg(args, i, &val, "--keyring", "-k", (char*)NULL)) {
      set_val_or_die(values, tracker, "keyring", val.c_str());
    }
    else if (ceph_argparse_witharg(args, i, &val, "--client_mountpoint", "-r", (char*)NULL)) {
      set_val_or_die(values, tracker, "client_mountpoint", val.c_str());
    }
    else if (ceph_argparse_witharg(args, i, &val, "--service_unique_id", (char*)NULL)) {
      set_val_or_die(values, tracker, "service_unique_id", val.c_str());
    }
    // 其他参数：走通用 --key=value 解析逻辑
    else {
      int r = parse_option(values, tracker, args, i, NULL, level);
      if (r < 0) {
        return r;
      }
    }
  }
  // 元变量展开可能修改了任何配置 → 重新同步到遗留 C 结构体字段
  update_legacy_vals(values);
  return 0;
}

/**
 * do_argv_commands —— 执行展示型命令行参数（--show-config 等）。
 *
 * @param values  当前配置值
 *
 * 在 parse_argv 阶段只打标记不输出，确保配置全部加载完毕后
 * 再展示最终生效值（包含配置文件、环境变量、命令行的合并结果）。
 */
void md_config_t::do_argv_commands(const ConfigValues& values) const
{
  // --show-config：打印全部配置项及当前值
  if (do_show_config) {
    _show_config(values, &cout, NULL);
    _exit(0);
  }

  // --show-config-value <key>：打印单个配置项的值
  if (do_show_config_value.size()) {
    string val;
    int r = conf_stringify(_get_val(values, do_show_config_value, 0, &cerr),
			   &val);
    if (r < 0) {
      if (r == -ENOENT)
	std::cerr << "failed to get config option '"
		  << do_show_config_value << "': option not found" << std::endl;
      else
	std::cerr << "failed to get config option '"
		  << do_show_config_value << "': " << cpp_strerror(r)
		  << std::endl;
      _exit(1);
    }
    std::cout << val << std::endl;
    _exit(0);
  }
}

int md_config_t::parse_option(ConfigValues& values,
			      const ConfigTracker& tracker,
			      std::vector<const char*>& args,
			      std::vector<const char*>::iterator& i,
			      ostream *oss,
			      int level)
{
  int ret = 0;
  size_t o = 0;
  std::string val;

  std::string option_name;
  std::string error_message;
  o = 0;
  for (const auto& opt_iter: schema) {
    const Option &opt = opt_iter.second;
    ostringstream err;
    std::string as_option("--");
    as_option += opt.name;
    option_name = opt.name;
    if (ceph_argparse_witharg(
	  args, i, &val, err,
	  fmt::format("--default-{}", opt.name).c_str(), (char*)NULL)) {
      if (!err.str().empty()) {
        error_message = err.str();
	ret = -EINVAL;
	break;
      }
      ret = _set_val(values, tracker,  val, opt, CONF_DEFAULT, &error_message);
      break;
    } else if (opt.type == Option::TYPE_BOOL) {
      int res;
      if (ceph_argparse_binary_flag(args, i, &res, oss, as_option.c_str(),
				    (char*)NULL)) {
	if (res == 0)
	  ret = _set_val(values, tracker, "false", opt, level, &error_message);
	else if (res == 1)
	  ret = _set_val(values, tracker, "true", opt, level, &error_message);
	else
	  ret = res;
	break;
      } else {
	std::string no("--no-");
	no += opt.name;
	if (ceph_argparse_flag(args, i, no.c_str(), (char*)NULL)) {
	  ret = _set_val(values, tracker, "false", opt, level, &error_message);
	  break;
	}
      }
    } else if (ceph_argparse_witharg(args, i, &val, err,
                                     as_option.c_str(), (char*)NULL)) {
      if (!err.str().empty()) {
        error_message = err.str();
	ret = -EINVAL;
	break;
      }
      ret = _set_val(values, tracker,  val, opt, level, &error_message);
      break;
    }
    ++o;
  }

  if (ret < 0 || !error_message.empty()) {
    ceph_assert(!option_name.empty());
    if (oss) {
      *oss << "Parse error setting " << option_name << " to '"
           << val << "' using injectargs";
      if (!error_message.empty()) {
        *oss << " (" << error_message << ")";
      }
      *oss << ".\n";
    } else {
      cerr << "parse error setting '" << option_name << "' to '"
	   << val << "'";
      if (!error_message.empty()) {
        cerr << " (" << error_message << ")";
      }
      cerr << "\n" << std::endl;
    }
  }

  if (o == schema.size()) {
    // ignore
    ++i;
  }
  return ret >= 0 ? 0 : ret;
}

int md_config_t::parse_injectargs(ConfigValues& values,
				  const ConfigTracker& tracker,
				  std::vector<const char*>& args,
				  std::ostream *oss)
{
  int ret = 0;
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    int r = parse_option(values, tracker, args, i, oss, CONF_OVERRIDE);
    if (r < 0)
      ret = r;
  }
  return ret;
}

void md_config_t::set_safe_to_start_threads()
{
  safe_to_start_threads = true;
}

void md_config_t::_clear_safe_to_start_threads()
{
  safe_to_start_threads = false;
}

int md_config_t::injectargs(ConfigValues& values,
			    const ConfigTracker& tracker,
			    const std::string& s, std::ostream *oss)
{
  int ret;
  char b[s.length()+1];
  strcpy(b, s.c_str());
  std::vector<const char*> nargs;
  char *p = b;
  while (*p) {
    nargs.push_back(p);
    while (*p && *p != ' ') p++;
    if (!*p)
      break;
    *p++ = 0;
    while (*p && *p == ' ') p++;
  }
  ret = parse_injectargs(values, tracker, nargs, oss);
  if (!nargs.empty()) {
    *oss << " failed to parse arguments: ";
    std::string prefix;
    for (std::vector<const char*>::const_iterator i = nargs.begin();
	 i != nargs.end(); ++i) {
      *oss << prefix << *i;
      prefix = ",";
    }
    *oss << "\n";
    ret = -EINVAL;
  }
  update_legacy_vals(values);
  return ret;
}

void md_config_t::set_val_or_die(ConfigValues& values,
				 const ConfigTracker& tracker,
				 const std::string_view key,
				 const std::string &val)
{
  std::stringstream err;
  int ret = set_val(values, tracker, key, val, &err);
  if (ret != 0) {
    std::cerr << "set_val_or_die(" << key << "): " << err.str();
  }
  ceph_assert(ret == 0);
}

int md_config_t::set_val(ConfigValues& values,
			 const ConfigTracker& tracker,
			 const std::string_view key, const char *val,
			 std::stringstream *err_ss)
{
  if (key.empty()) {
    if (err_ss) *err_ss << "No key specified";
    return -EINVAL;
  }
  if (!val) {
    return -EINVAL;
  }

  std::string v(val);

  string k(ConfFile::normalize_key_name(key));

  const auto &opt_iter = schema.find(k);
  if (opt_iter != schema.end()) {
    const Option &opt = opt_iter->second;
    std::string error_message;
    int r = _set_val(values, tracker, v, opt, CONF_OVERRIDE, &error_message);
    if (r >= 0) {
      if (err_ss) *err_ss << "Set " << opt.name << " to " << v;
      r = 0;
    } else {
      if (err_ss) *err_ss << error_message;
    }
    return r;
  }

  if (err_ss) *err_ss << "Configuration option not found: '" << key << "'";
  return -ENOENT;
}

int md_config_t::rm_val(ConfigValues& values, const std::string_view key)
{
  return _rm_val(values, key, CONF_OVERRIDE);
}

void md_config_t::get_defaults_bl(const ConfigValues& values,
					 bufferlist *bl)
{
  if (defaults_bl.length() == 0) {
    uint32_t n = 0;
    bufferlist bl;
    for (const auto &i : schema) {
      ++n;
      encode(i.second.name, bl);
      auto [value, found] = values.get_value(i.second.name, CONF_DEFAULT);
      if (found) {
	encode(Option::to_str(value), bl);
      } else {
	string val;
	conf_stringify(_get_val_default(i.second), &val);
	encode(val, bl);
      }
    }
    encode(n, defaults_bl);
    defaults_bl.claim_append(bl);
  }
  *bl = defaults_bl;
}

void md_config_t::get_config_bl(
  const ConfigValues& values,
  uint64_t have_version,
  bufferlist *bl,
  uint64_t *got_version)
{
  if (values_bl.length() == 0) {
    uint32_t n = 0;
    bufferlist bl;
    values.for_each([&](auto& name, auto& configs) {
      if (name == "fsid" ||
	  name == "host") {
	return;
      }
      ++n;
      encode(name, bl);
      encode((uint32_t)configs.size(), bl);
      for (auto& j : configs) {
	encode(j.first, bl);
	encode(Option::to_str(j.second), bl);
      }
    });
    // make sure overridden items appear, and include the default value
    for (auto& i : ignored_mon_values) {
      if (values.contains(i.first)) {
	continue;
      }
      if (i.first == "fsid" ||
	  i.first == "host") {
	continue;
      }
      const Option *opt = find_option(i.first);
      if (!opt) {
	continue;
      }
      ++n;
      encode(i.first, bl);
      encode((uint32_t)1, bl);
      encode((int32_t)CONF_DEFAULT, bl);
      string val;
      conf_stringify(_get_val_default(*opt), &val);
      encode(val, bl);
    }
    encode(n, values_bl);
    values_bl.claim_append(bl);
    encode(ignored_mon_values, values_bl);
    ++values_bl_version;
  }
  if (have_version != values_bl_version) {
    *bl = values_bl;
    *got_version = values_bl_version;
  }
}

std::optional<std::string> md_config_t::get_val_default(std::string_view key)
{
  std::string val;
  const Option *opt = find_option(key);
  if (opt && (conf_stringify(_get_val_default(*opt), &val) == 0)) {
    return std::make_optional(std::move(val));
  }
  return std::nullopt;
}

int md_config_t::get_val(const ConfigValues& values,
			 const std::string_view key, char **buf, int len) const
{
  string k(ConfFile::normalize_key_name(key));
  return _get_val_cstr(values, k, buf, len);
}

int md_config_t::get_val(
  const ConfigValues& values,
  const std::string_view key,
  std::string *val) const
{
  return conf_stringify(get_val_generic(values, key), val);
}

Option::value_t md_config_t::get_val_generic(
  const ConfigValues& values,
  const std::string_view key) const
{
  return _get_val(values, key);
}

Option::value_t md_config_t::_get_val(
  const ConfigValues& values,
  const std::string_view key,
  expand_stack_t *stack,
  std::ostream *err) const
{
  if (key.empty()) {
    return {};
  }

  // In key names, leading and trailing whitespace are not significant.
  string k(ConfFile::normalize_key_name(key));

  const Option *o = find_option(k);
  if (!o) {
    // not a valid config option
    return {};
  }

  return _get_val(values, *o, stack, err);
}

Option::value_t md_config_t::_get_val(
  const ConfigValues& values,
  const Option& o,
  expand_stack_t *stack,
  std::ostream *err) const
{
  expand_stack_t a_stack;
  if (!stack) {
    stack = &a_stack;
  }
  return _expand_meta(values,
		      _get_val_nometa(values, o),
		      &o, stack, err);
}

Option::value_t md_config_t::_get_val_nometa(const ConfigValues& values,
					     const Option& o) const
{
  if (auto [value, found] = values.get_value(o.name, -1); found) {
    return value;
  } else {
    return _get_val_default(o);
  }
}

const Option::value_t& md_config_t::_get_val_default(const Option& o) const
{
  bool has_daemon_default = (o.daemon_value != Option::value_t{});
  if (is_daemon && has_daemon_default) {
    return o.daemon_value;
  } else {
    return o.value;
  }
}

void md_config_t::early_expand_meta(
  const ConfigValues& values,
  std::string &val,
  std::ostream *err) const
{
  expand_stack_t stack;
  Option::value_t v = _expand_meta(values,
				   Option::value_t(val),
				   nullptr, &stack, err);
  conf_stringify(v, &val);
}

bool md_config_t::finalize_reexpand_meta(ConfigValues& values,
					 const ConfigTracker& tracker)
{
  std::vector<std::string> reexpands;
  reexpands.swap(may_reexpand_meta);
  for (auto& name : reexpands) {
    // always refresh the options if they are in the may_reexpand_meta
    // map, because the options may have already been expanded with old
    // meta.
    const auto &opt_iter = schema.find(name);
    ceph_assert(opt_iter != schema.end());
    const Option &opt = opt_iter->second;
    _refresh(values, opt);
  }

  return !may_reexpand_meta.empty();
}

Option::value_t md_config_t::_expand_meta(
  const ConfigValues& values,
  const Option::value_t& in,
  const Option *o,
  expand_stack_t *stack,
  std::ostream *err) const
{
  //cout << __func__ << " in '" << in << "' stack " << stack << std::endl;
  if (!stack) {
    return in;
  }
  const auto str = std::get_if<std::string>(&in);
  if (!str) {
    // strings only!
    return in;
  }

  auto pos = str->find('$');
  if (pos == std::string::npos) {
    // no substitutions!
    return in;
  }

  if (o) {
    stack->push_back(make_pair(o, &in));
  }
  string out;
  decltype(pos) last_pos = 0;
  while (pos != std::string::npos) {
    ceph_assert((*str)[pos] == '$');
    if (pos > last_pos) {
      out += str->substr(last_pos, pos - last_pos);
    }

    // try to parse the variable name into var, either \$\{(.+)\} or
    // \$[a-z\_]+
    const char *valid_chars = "abcdefghijklmnopqrstuvwxyz_";
    string var;
    size_t endpos = 0;
    if ((*str)[pos+1] == '{') {
      // ...${foo_bar}...
      endpos = str->find_first_not_of(valid_chars, pos + 2);
      if (endpos != std::string::npos &&
	  (*str)[endpos] == '}') {
	var = str->substr(pos + 2, endpos - pos - 2);
	endpos++;
      }
    } else {
      // ...$foo...
      endpos = str->find_first_not_of(valid_chars, pos + 1);
      if (endpos != std::string::npos)
	var = str->substr(pos + 1, endpos - pos - 1);
      else
	var = str->substr(pos + 1);
    }
    last_pos = endpos;

    if (!var.size()) {
      out += '$';
    } else {
      //cout << " found var " << var << std::endl;
      // special metavariable?
      if (var == "type") {
	out += values.name.get_type_name();
      } else if (var == "cluster") {
	out += values.cluster;
      } else if (var == "name") {
	out += values.name.to_cstr();
      } else if (var == "host") {
	if (values.host == "") {
	  out += ceph_get_short_hostname();
	} else {
	  out += values.host;
	}
      } else if (var == "num") {
	out += values.name.get_id().c_str();
      } else if (var == "id") {
	out += values.name.get_id();
      } else if (var == "pid") {
        char *_pid = getenv("PID");
        if (_pid) {
          out += _pid;
        } else {
          out += stringify(getpid());
        }
        if (o) {
          may_reexpand_meta.push_back(o->name);
        }
      } else if (var == "cctid") {
	out += stringify((unsigned long long)this);
      } else if (var == "home") {
	const char *home = getenv("HOME");
	out = home ? std::string(home) : std::string();
      } else if (var == "programdata") {
        const char *home = getenv("ProgramData");
        out = home ? std::string(home) : std::string();
      }else {
	if (var == "data_dir") {
	  var = data_dir_option;
	}
	const Option *o = find_option(var);
	if (!o) {
	  out += str->substr(pos, endpos - pos);
	} else {
	  auto match = std::find_if(
	    stack->begin(), stack->end(),
	    [o](pair<const Option *,const Option::value_t*>& item) {
	      return item.first == o;
	    });
	  if (match != stack->end()) {
	    // substitution loop; break the cycle
	    if (err) {
	      *err << "variable expansion loop at " << var << "="
		   << Option::to_str(*match->second) << "\n"
		   << "expansion stack:\n";
	      for (auto i = stack->rbegin(); i != stack->rend(); ++i) {
		*err << i->first->name << "="
		     << Option::to_str(*i->second) << "\n";
	      }
	    }
	    return Option::value_t(fmt::format("${}", o->name));
	  } else {
	    // recursively evaluate!
	    string n;
	    conf_stringify(_get_val(values, *o, stack, err), &n);
	    out += n;
	  }
	}
      }
    }
    pos = str->find('$', last_pos);
  }
  if (last_pos != std::string::npos) {
    out += str->substr(last_pos);
  }
  if (o) {
    stack->pop_back();
  }

  return Option::value_t(out);
}

int md_config_t::_get_val_cstr(
  const ConfigValues& values,
  const std::string& key, char **buf, int len) const
{
  if (key.empty())
    return -EINVAL;

  string val;
  // 拒绝修改非 RUNTIME 配置 ← 防止运行时改基础配置
  if (conf_stringify(_get_val(values, key), &val) == 0) {
    int l = val.length() + 1;
    if (len == -1) {
      *buf = (char*)malloc(l);
      if (!*buf)
        return -ENOMEM;
      strncpy(*buf, val.c_str(), l);
      return 0;
    }
    snprintf(*buf, len, "%s", val.c_str());
    return (l > len) ? -ENAMETOOLONG : 0;
  }

  // couldn't find a configuration option with key 'k'
  return -ENOENT;
}

void md_config_t::get_all_keys(std::vector<std::string> *keys) const {
  const std::string negative_flag_prefix("no_");

  keys->clear();
  keys->reserve(schema.size());
  for (const auto &i: schema) {
    const Option &opt = i.second;
    keys->push_back(opt.name);
    if (opt.type == Option::TYPE_BOOL) {
      keys->push_back(negative_flag_prefix + opt.name);
    }
  }
}

/* The order of the sections here is important.  The first section in the
 * vector is the "highest priority" section; if we find it there, we'll stop
 * looking. The lowest priority section is the one we look in only if all
 * others had nothing.  This should always be the global section.
 */
std::vector <std::string>
md_config_t::get_my_sections(const ConfigValues& values) const
{
  return {values.name.to_str(),
	  values.name.get_type_name().data(),
	  "global"};
}

// Return a list of all sections
int md_config_t::get_all_sections(std::vector <std::string> &sections) const
{
  for (auto [section_name, section] : cf) {
    sections.push_back(section_name);
    std::ignore = section;
  }
  return 0;
}

int md_config_t::get_val_from_conf_file(
  const ConfigValues& values,
  const std::vector <std::string> &sections,
  const std::string_view key,
  std::string &out,
  bool emeta) const
{
  int r = _get_val_from_conf_file(sections, key, out);
  if (r < 0) {
    return r;
  }
  if (emeta) {
    expand_stack_t stack;
    auto v = _expand_meta(values, Option::value_t(out), nullptr, &stack, nullptr);
    conf_stringify(v, &out);
  }
  return 0;
}

int md_config_t::_get_val_from_conf_file(
  const std::vector <std::string> &sections,
  const std::string_view key,
  std::string &out) const
{
  for (auto &s : sections) {
    int ret = cf.read(s, key, out);
    if (ret == 0) {
      return 0;
    } else if (ret != -ENOENT) {
      return ret;
    }
  }
  return -ENOENT;
}

/**
 * _set_val —— 配置项赋值的核心实现（内部函数，外部通过 set_val / set_val_default 等调用）。
 *
 * @param values         配置值存储（ConfigValues），实际持有所有配置项的值
 * @param observers      配置变更观察者（用于判断是否已被追踪）
 * @param raw_val        字符串形式的原始值（如 "1024"、"/path/to/data"）
 * @param opt            配置项的 Option 元数据（类型、校验规则等）
 * @param level          优先级级别（CONF_DEFAULT / CONF_FILE / CONF_CMDLINE / CONF_MON 等）
 * @param error_message  输出参数：失败时写入错误描述
 * @return               0 成功，负值错误码
 *
 * 执行流程：
 *   1. 将字符串 raw_val 解析为对应类型的值（int/bool/str/...）
 *   2. 运行时安全检查：如果配置项标记为 startup-only，且线程已启动且无人追踪，拒绝修改
 *   3. 写入 values 存储，并根据结果决定是否触发刷新
 */
int md_config_t::_set_val(
  ConfigValues& values,
  const ConfigTracker& observers,
  const std::string &raw_val,
  const Option &opt,
  int level,
  std::string *error_message)
{
  Option::value_t new_value;                        // 解析后的值（variant 类型）
  ceph_assert(error_message);                       // error_message 指针不能为空
  int r = opt.parse_value(raw_val, &new_value, error_message);  // 字符串 → 类型化的值
  if (r < 0) {
    return r;                                       // 解析失败，直接返回错误
  }

  // 运行时修改安全检查：
  // 如果配置项只能在启动时修改（FLAG_RUNTIME 未设置），且当前线程已经启动，
  // 且没有任何人追踪这个配置项的变更 → 拒绝修改
  if (!opt.can_update_at_runtime() &&
      safe_to_start_threads &&
      !observers.is_tracking(opt.name)) {
    // 例外：如果新旧值相同（等于没改），仍然允许通过
    if (new_value != _get_val_nometa(values, opt)) {
      *error_message = string("Configuration option '") + opt.name +
	"' may not be modified at runtime";
      return -EPERM;
    }
  }

  // 将解析后的值写入 values map（按优先级 level 存储）
  auto result = values.set_value(opt.name, std::move(new_value), level);
  switch (result) {
  case ConfigValues::SET_NO_CHANGE:
    // 值没有变化 → 什么都不做
    break;
  case ConfigValues::SET_NO_EFFECT:
    // 值存进去了，但当前生效值没变（被更高优先级覆盖了） → 只清缓存
    values_bl.clear();
    break;
  case ConfigValues::SET_HAVE_EFFECT:
    // 当前生效值发生了变化 → 清缓存 + 刷新依赖项（如扩展变量 $name 等）
    values_bl.clear();
    _refresh(values, opt);
    break;
  }
  return result;
}

void md_config_t::_refresh(ConfigValues& values, const Option& opt)
{
  // Apply the value to its legacy field, if it has one
  auto legacy_ptr_iter = legacy_values.find(std::string(opt.name));
  if (legacy_ptr_iter != legacy_values.end()) {
    update_legacy_val(values, opt, legacy_ptr_iter->second);
  }
  // Was this a debug_* option update?
  if (opt.subsys >= 0) {
    string actual_val;
    conf_stringify(_get_val(values, opt), &actual_val);
    values.set_logging(opt.subsys, actual_val.c_str());
  } else {
    // normal option, advertise the change.
    values.changed.insert(opt.name);
  }
}

int md_config_t::_rm_val(ConfigValues& values,
			 const std::string_view key,
			 int level)
{
  if (schema.count(key) == 0) {
    return -EINVAL;
  }
  auto ret = values.rm_val(std::string{key}, level);
  if (ret < 0) {
    return ret;
  }
  if (ret == ConfigValues::SET_HAVE_EFFECT) {
    _refresh(values, *find_option(key));
  }
  values_bl.clear();
  return 0;
}

namespace {
template<typename Size>
struct get_size_visitor
{
  get_size_visitor() {}

  template<typename T>
  Size operator()(const T&) const {
    return -1;
  }
  Size operator()(const Option::size_t& sz) const {
    return static_cast<Size>(sz.value);
  }
  Size operator()(const Size& v) const {
    return v;
  }
};

/**
 * Handles assigning from a variant-of-types to a variant-of-pointers-to-types
 */
class assign_visitor
{
  ConfigValues *conf;
  Option::value_t val;
  public:

  assign_visitor(ConfigValues *conf_, Option::value_t val_)
    : conf(conf_), val(val_)
  {}

  template <typename T>
  void operator()(T ConfigValues::* ptr) const
  {
    T *member = const_cast<T *>(&(conf->*(ptr)));

    *member = std::get<T>(val);
  }
  void operator()(uint64_t ConfigValues::* ptr) const
  {
    using T = uint64_t;
    auto member = const_cast<T*>(&(conf->*(ptr)));
    *member = std::visit(get_size_visitor<T>{}, val);
  }
  void operator()(int64_t ConfigValues::* ptr) const
  {
    using T = int64_t;
    auto member = const_cast<T*>(&(conf->*(ptr)));
    *member = std::visit(get_size_visitor<T>{}, val);
  }
};
} // anonymous namespace

/**
 * update_legacy_vals —— 将所有配置项的当前值同步到遗留 C 结构体成员变量。
 *
 * @param values  当前配置值存储
 *
 * 背景：Ceph 早期代码用 C 结构体（md_config_t）的成员变量存配置值，
 *       后来重构为 ConfigValues（map 存储）+ schema 表。为了兼容旧代码
 *       仍然直接访问结构体成员的写法，每次配置变更后都要把新值同步回去。
 */
void md_config_t::update_legacy_vals(ConfigValues& values)
{
  // 遍历所有需要同步的遗留字段映射
  for (const auto &i : legacy_values) {
    const auto &name = i.first;                    // 配置项名称
    const auto &option = schema.at(name);           // 对应的 Option 元数据
    auto ptr = i.second;                            // 遗留结构体成员的指针（偏移量）
    update_legacy_val(values, option, ptr);          // 执行单条同步
  }
}

void md_config_t::update_legacy_val(ConfigValues& values,
				    const Option &opt,
                                    md_config_t::member_ptr_t member_ptr)
{
  Option::value_t v = _get_val(values, opt);
  std::visit(assign_visitor(&values, v), member_ptr);
}

static void dump(Formatter *f, int level, Option::value_t in)
{
  if (const auto v = std::get_if<bool>(&in)) {
    f->dump_bool(ceph_conf_level_name(level), *v);
  } else if (const auto v = std::get_if<int64_t>(&in)) {
    f->dump_int(ceph_conf_level_name(level), *v);
  } else if (const auto v = std::get_if<uint64_t>(&in)) {
    f->dump_unsigned(ceph_conf_level_name(level), *v);
  } else if (const auto v = std::get_if<double>(&in)) {
    f->dump_float(ceph_conf_level_name(level), *v);
  } else {
    f->dump_stream(ceph_conf_level_name(level)) << Option::to_str(in);
  }
}

void md_config_t::diff(
  const ConfigValues& values,
  Formatter *f,
  string name) const
{
  values.for_each([this, f, &values] (auto& name, auto& configs) {
    if (configs.empty()) {
      return;
    }
    f->open_object_section(std::string{name}.c_str());
    const Option *o = find_option(name);
    if (configs.size() &&
	configs.begin()->first != CONF_DEFAULT) {
      // show compiled-in default only if an override default wasn't provided
      dump(f, CONF_DEFAULT, _get_val_default(*o));
    }
    for (auto& j : configs) {
      dump(f, j.first, j.second);
    }
    dump(f, CONF_FINAL, _get_val(values, *o));
    f->close_section();
  });
}

void md_config_t::complain_about_parse_error(CephContext *cct)
{
  ::complain_about_parse_error(cct, parse_error);
}
