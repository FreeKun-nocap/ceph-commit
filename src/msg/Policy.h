// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include "include/ceph_features.h"
#include <map>

namespace ceph::net {

using peer_type_t = int;

/**
 * A Policy describes the rules of a Connection. Is there a limit on how
 * much data this Connection can have locally? When the underlying connection
 * experiences an error, does the Connection disappear? Can this Messenger
 * re-establish the underlying connection?
 */
template<class ThrottleType>
struct Policy {
  /// If true, the Connection is tossed out on errors.
  // 一旦连接出现错误，是否丢弃该连接
  bool lossy;
  /// If true, the underlying connection can't be re-established from this end.
  // 当底层连接断开时，是否不允许重新建立连接；当为 true 时，本端为服务端，被动等待 对端（客户端）来连接，自己不发起重连；当为 false 时，本端为客户端，可以主动发起重连
  bool server;
  /// If true, we will standby when idle
  // 当连接空闲（idle）时，不直接销毁连接，而是进入 standby（待命）状态，等待后续需要时恢复。
  bool standby;
  /// If true, we will try to detect session resets
  // 当对端重连时，本端会检测是否是同一个 session（会话）的恢复，还是全新的连接。如果检测到 session 被重置（比如对端重启导致连接标识变了），会执行清理动作。
  bool resetcheck;

  /// Server: register lossy client connections.
  // 服务端是否将客户端的有损连接注册到连接跟踪表中。如果为 true ，服务端会跟踪每个客户端的连接，确保同一客户端（相同 peer addr）最多只有一个活跃连接。
  bool register_lossy_clients = true;
  // The net result of this is that a given client can only have one
  // open connection with the server.  If a new connection is made,
  // the old (registered) one is closed by the messenger during the accept
  // process.
  
  /**
   *  The throttler is used to limit how much data is held by Messages from
   *  the associated Connection(s). When reading in a new Message, the Messenger
   *  will call throttler->throttle() for the size of the new Message.
   */
  ThrottleType* throttler_bytes;
  ThrottleType* throttler_messages;
  
  /// Specify features supported locally by the endpoint.
#ifdef MSG_POLICY_UNIT_TESTING
  uint64_t features_supported{CEPH_FEATURES_SUPPORTED_DEFAULT};
#else
  static constexpr uint64_t features_supported{CEPH_FEATURES_SUPPORTED_DEFAULT};
#endif

  /// Specify features any remotes must have to talk to this endpoint.
  uint64_t features_required;
  
  Policy()
    : lossy(false), server(false), standby(false), resetcheck(true),
      throttler_bytes(NULL),
      throttler_messages(NULL),
      features_required(0) {}
private:
  Policy(bool l, bool s, bool st, bool r, bool rlc, uint64_t req)
    : lossy(l), server(s), standby(st), resetcheck(r),
      register_lossy_clients(rlc),
      throttler_bytes(NULL),
      throttler_messages(NULL),
      features_required(req) {}
  
public:
  /**
   * 以下提供一些常用的 Policy 配置，用于不同的场景
   * Policy 构造器入参说明
   * @param lossy 一旦连接出现错误，是否丢弃该连接
   * @param server 当底层连接断开时，是否不允许重新建立连接；当为 true 时，本端为服务端，被动等待 对端（客户端）来连接，自己不发起重连；当为 false 时，本端为客户端，可以主动发起重连
   * @param standby 当连接空闲（idle）时，不直接销毁连接，而是进入 standby（待命）状态，等待后续需要时恢复。
   * @param resetcheck 当对端重连时，本端会检测是否是同一个 session（会话）的恢复，还是全新的连接。如果检测到 session 被重置（比如对端重启导致连接标识变了），会执行清理动作。
   * @param register_lossy_clients 服务端是否将客户端的有损连接注册到连接跟踪表中。如果为 true ，服务端会跟踪每个客户端的连接，确保同一客户端（相同 peer addr）最多只有一个活跃连接。
   * @param req 需要的特征
   */

  // 有状态服务器: 持久 + server + standby + resetcheck + 唯一连接
  // 用于 MON、MDS 等有状态服务端，连接持久，空闲时 standby
  static Policy stateful_server(uint64_t req) {
    return Policy(false, true, true, true, true, req);
  }
  // 无状态注册服务器: 瞬断 + server + 无 standby + 无 resetcheck + 唯一连接
  // 用于公网 Messenger（ms_public），接受外部客户端连接，同一客户端只保留一个连接
  static Policy stateless_registered_server(uint64_t req) {
    return Policy(true, true, false, false, true, req);
  }
  // 无状态服务器: 瞬断 + server + 无 standby + 无 resetcheck + 不唯一
  // 用于心跳 server 等轻量无状态服务，不跟踪客户端连接
  static Policy stateless_server(uint64_t req) {
    return Policy(true, true, false, false, false, req);
  }
  // 持久对等体: 持久 + 非 server + standby + 无 resetcheck + 唯一连接
  // 用于 OSD 间集群内部通信，连接持久，断连自动重连
  static Policy lossless_peer(uint64_t req) {
    return Policy(false, false, true, false, true, req);
  }
  // 持久对等体(可重用): 持久 + 非 server + standby + resetcheck + 唯一连接
  // 与 lossless_peer 类似，额外检测会话重置以重用连接
  static Policy lossless_peer_reuse(uint64_t req) {
    return Policy(false, false, true, true, true, req);
  }
  // 瞬断客户端: 瞬断 + 非 server + 无 standby + 无 resetcheck + 唯一连接
  // 用于 librados 等外部客户端，连接出错即丢弃
  static Policy lossy_client(uint64_t req) {
    return Policy(true, false, false, false, true, req);
  }
  // 持久客户端: 持久 + 非 server + 无 standby + resetcheck + 唯一连接
  // 用于需要持久连接但非服务端的场景
  static Policy lossless_client(uint64_t req) {
    return Policy(false, false, false, true, true, req);
  }
};

template<class ThrottleType>
class PolicySet {
  using policy_t = Policy<ThrottleType> ;
  /// the default Policy we use for Pipes
  policy_t default_policy;
  /// map specifying different Policies for specific peer types
  std::map<int, policy_t> policy_map; // entity_name_t::type -> Policy

public:
  const policy_t& get(peer_type_t peer_type) const {
    if (auto found = policy_map.find(peer_type); found != policy_map.end()) {
      return found->second;
    } else {
      return default_policy;
    }
  }
  policy_t& get(peer_type_t peer_type) {
    if (auto found = policy_map.find(peer_type); found != policy_map.end()) {
      return found->second;
    } else {
      return default_policy;
    }
  }
  void set(peer_type_t peer_type, const policy_t& p) {
    policy_map[peer_type] = p;
  }
  const policy_t& get_default() const {
    return default_policy;
  }
  void set_default(const policy_t& p) {
    default_policy = p;
  }
  void set_throttlers(peer_type_t peer_type,
                      ThrottleType* byte_throttle,
                      ThrottleType* msg_throttle) {
    auto& policy = get(peer_type);
    policy.throttler_bytes = byte_throttle;
    policy.throttler_messages = msg_throttle;
  }
};

}
