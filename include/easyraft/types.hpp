// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright 2026 easyraft contributors

#pragma once

#include <algorithm>
#include <cstdint>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
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
  // `data` is Membership::encode() of the config that becomes effective once
  // this entry is appended.
  Membership,
};

struct LogEntry {
  LogId id{};
  EntryKind kind{EntryKind::Application};
  std::string data;

  friend bool operator==(const LogEntry& lhs, const LogEntry& rhs) {
    return lhs.id == rhs.id && lhs.kind == rhs.kind && lhs.data == rhs.data;
  }
};

// One uniform voter config, or a joint pair of configs (old and new).
// A node is a voter if it belongs to any config. A vote or a commit needs a
// majority of every config. There are no learners.
//
// `initialize()` stores the first uniform config. A later joint config is
// written as a membership log entry.
class Membership {
 public:
  Membership() = default;

  // Uniform membership. Voter ids are sorted and de-duplicated.
  explicit Membership(std::vector<NodeId> voters) {
    configs_.push_back(normalize(std::move(voters)));
    rebuild_voters();
  }

  // Joint membership. `older` is the current uniform set and `newer` is the
  // target uniform set. Quorum is a majority of each.
  static Membership joint_of(std::vector<NodeId> older, std::vector<NodeId> newer) {
    Membership membership;
    membership.configs_.push_back(normalize(std::move(older)));
    membership.configs_.push_back(normalize(std::move(newer)));
    membership.rebuild_voters();
    return membership;
  }

  const std::vector<std::vector<NodeId>>& configs() const { return configs_; }
  // Union of every config, sorted and unique. For a uniform config this is
  // that config.
  const std::vector<NodeId>& voters() const { return voters_; }
  bool is_joint() const { return configs_.size() > 1; }
  bool empty() const { return voters_.empty(); }
  bool contains(NodeId id) const {
    return std::binary_search(voters_.begin(), voters_.end(), id);
  }
  std::size_t size() const { return voters_.size(); }

  // Majority of a uniform config (`floor(n / 2) + 1`). A joint config has no
  // single quorum; use `is_quorum`.
  std::size_t quorum() const {
    if (is_joint()) {
      return 0;
    }
    return voters_.size() / 2 + 1;
  }

  // True when `granted` reports a majority of every config.
  template <typename Pred>
  bool is_quorum(Pred granted) const {
    if (configs_.empty()) {
      return false;
    }
    for (const std::vector<NodeId>& config : configs_) {
      if (config.empty()) {
        return false;
      }
      std::size_t votes = 0;
      for (NodeId id : config) {
        if (granted(id)) {
          ++votes;
        }
      }
      if (votes < config.size() / 2 + 1) {
        return false;
      }
    }
    return true;
  }

  // Lossless encoding stored in a membership log entry. Not the on-disk
  // metadata layout; FileLogStorage has its own versioned encoding.
  std::string encode() const;
  static std::optional<Membership> decode(std::string_view bytes);

  friend bool operator==(const Membership&, const Membership&) = default;

 private:
  static std::vector<NodeId> normalize(std::vector<NodeId> voters) {
    std::sort(voters.begin(), voters.end());
    voters.erase(std::unique(voters.begin(), voters.end()), voters.end());
    return voters;
  }

  void rebuild_voters() {
    voters_.clear();
    for (const std::vector<NodeId>& config : configs_) {
      voters_.insert(voters_.end(), config.begin(), config.end());
    }
    std::sort(voters_.begin(), voters_.end());
    voters_.erase(std::unique(voters_.begin(), voters_.end()), voters_.end());
  }

  std::vector<std::vector<NodeId>> configs_;
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

inline std::string Membership::encode() const {
  std::string out;
  out.push_back(static_cast<char>(1));
  out.push_back(static_cast<char>(configs_.size()));
  for (const std::vector<NodeId>& config : configs_) {
    const auto count = static_cast<std::uint32_t>(config.size());
    for (int shift = 0; shift < 32; shift += 8) {
      out.push_back(static_cast<char>((count >> shift) & 0xffu));
    }
    for (NodeId id : config) {
      for (int shift = 0; shift < 64; shift += 8) {
        out.push_back(static_cast<char>((id >> shift) & 0xffu));
      }
    }
  }
  return out;
}

inline std::optional<Membership> Membership::decode(std::string_view bytes) {
  if (bytes.size() < 2) {
    return std::nullopt;
  }
  const auto* data = reinterpret_cast<const unsigned char*>(bytes.data());
  if (data[0] != 1) {
    return std::nullopt;
  }
  const unsigned nconfigs = data[1];
  if (nconfigs < 1 || nconfigs > 2) {
    return std::nullopt;
  }
  std::size_t offset = 2;
  std::vector<std::vector<NodeId>> configs;
  configs.reserve(nconfigs);
  for (unsigned config_index = 0; config_index < nconfigs; ++config_index) {
    if (bytes.size() - offset < 4) {
      return std::nullopt;
    }
    std::uint32_t count = 0;
    for (int byte = 0; byte < 4; ++byte) {
      count |= static_cast<std::uint32_t>(data[offset + static_cast<std::size_t>(byte)])
               << (8 * byte);
    }
    offset += 4;
    if (count > 100000u) {
      return std::nullopt;
    }
    if (bytes.size() - offset < static_cast<std::size_t>(count) * 8) {
      return std::nullopt;
    }
    std::vector<NodeId> voters;
    voters.reserve(count);
    for (std::uint32_t i = 0; i < count; ++i) {
      std::uint64_t id = 0;
      for (int byte = 0; byte < 8; ++byte) {
        id |= static_cast<std::uint64_t>(data[offset + static_cast<std::size_t>(byte)])
              << (8 * byte);
      }
      offset += 8;
      voters.push_back(id);
    }
    configs.push_back(std::move(voters));
  }
  if (offset != bytes.size()) {
    return std::nullopt;
  }
  if (configs.size() == 1) {
    return Membership{std::move(configs[0])};
  }
  return joint_of(std::move(configs[0]), std::move(configs[1]));
}

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
