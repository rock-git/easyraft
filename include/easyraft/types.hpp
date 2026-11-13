// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright 2026 easyraft contributors

#pragma once

#include <algorithm>
#include <cstdint>
#include <optional>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

namespace easyraft {

using NodeId = std::uint64_t;
using Term = std::uint64_t;
using LogIndex = std::uint64_t;

// OpenRaft 0.10 default leader id: totally ordered by (term, node_id).
// A higher node id can supersede another candidate in the same term.
struct LeaderId {
  Term term{0};
  NodeId node{0};

  friend auto operator<=>(const LeaderId&, const LeaderId&) = default;
  friend bool operator==(const LeaderId&, const LeaderId&) = default;
};

// Log identity in advanced mode. Order is (term, node, index), matching
// OpenRaft's default LogId so election "up-to-date" checks agree with votes.
struct LogId {
  Term term{0};
  NodeId node{0};
  LogIndex index{0};

  friend auto operator<=>(const LogId&, const LogId&) = default;
  friend bool operator==(const LogId&, const LogId&) = default;
};

// A single partially ordered value replacing Raft's (currentTerm, votedFor).
// committed=true means this vote belongs to an established leader.
// Comparison: greater leader id wins; for the same leader id, committed wins.
struct Vote {
  LeaderId leader{};
  bool committed{false};

  Vote non_committed() const {
    Vote copy = *this;
    copy.committed = false;
    return copy;
  }

  friend bool operator==(const Vote& lhs, const Vote& rhs) {
    return lhs.leader == rhs.leader && lhs.committed == rhs.committed;
  }

  friend bool operator<(const Vote& lhs, const Vote& rhs) {
    if (lhs.leader != rhs.leader) {
      return lhs.leader < rhs.leader;
    }
    return !lhs.committed && rhs.committed;
  }

  friend bool operator>(const Vote& lhs, const Vote& rhs) { return rhs < lhs; }
  friend bool operator<=(const Vote& lhs, const Vote& rhs) { return !(rhs < lhs); }
  friend bool operator>=(const Vote& lhs, const Vote& rhs) { return !(lhs < rhs); }
};

enum class Role {
  Follower,
  Candidate,
  Leader,
};

enum class EntryKind {
  Blank,
  Application,
};

struct LogEntry {
  LogId id{};
  EntryKind kind{EntryKind::Application};
  std::string data;

  friend bool operator==(const LogEntry& lhs, const LogEntry& rhs) {
    return lhs.id == rhs.id && lhs.kind == rhs.kind && lhs.data == rhs.data;
  }
};

// Fixed voter set chosen at initialize(). Majority quorum, no learners.
class Membership {
 public:
  Membership() = default;

  explicit Membership(std::vector<NodeId> voters) {
    std::sort(voters.begin(), voters.end());
    voters.erase(std::unique(voters.begin(), voters.end()), voters.end());
    voters_ = std::move(voters);
  }

  const std::vector<NodeId>& voters() const { return voters_; }
  bool empty() const { return voters_.empty(); }
  bool contains(NodeId id) const {
    return std::binary_search(voters_.begin(), voters_.end(), id);
  }
  std::size_t size() const { return voters_.size(); }
  std::size_t quorum() const { return voters_.size() / 2 + 1; }

  friend bool operator==(const Membership&, const Membership&) = default;

 private:
  std::vector<NodeId> voters_;
};

inline const char* to_string(Role role) {
  switch (role) {
    case Role::Follower:
      return "follower";
    case Role::Candidate:
      return "candidate";
    case Role::Leader:
      return "leader";
  }
  return "unknown";
}

inline std::ostream& operator<<(std::ostream& out, const LeaderId& id) {
  return out << id.term << "/" << id.node;
}

inline std::ostream& operator<<(std::ostream& out, const LogId& id) {
  return out << id.term << "/" << id.node << "@" << id.index;
}

inline std::ostream& operator<<(std::ostream& out, const Vote& vote) {
  return out << vote.leader << (vote.committed ? " committed" : " granted");
}

inline std::ostream& operator<<(std::ostream& out, Role role) { return out << to_string(role); }

inline bool log_is_up_to_date(const std::optional<LogId>& candidate,
                              const std::optional<LogId>& local) {
  if (!local) {
    return true;
  }
  if (!candidate) {
    return false;
  }
  return *candidate >= *local;
}

}  // namespace easyraft
