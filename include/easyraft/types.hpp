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

// Election and commit thresholds for one voter config.
//
// Zero means the majority of that config, `floor(n / 2) + 1`. A stored pair is
// either both zero or both concrete, so a default config matches a config that
// was given the majority counts explicitly. Learners are not part of `n`.
struct Quorum {
  std::size_t election{0};
  std::size_t commit{0};

  bool is_default() const { return election == 0 && commit == 0; }

  friend bool operator==(const Quorum&, const Quorum&) = default;
};

inline std::size_t majority_count(std::size_t voters) { return voters / 2 + 1; }

// Concrete thresholds that can form a quorum and intersect, or nullopt.
// `1 <= election, commit <= n` and `election + commit > n`. A zero field is
// the majority of `voter_count`. Both majorities canonicalize back to zeros.
inline std::optional<Quorum> normalize_quorum(std::size_t voter_count, Quorum quorum) {
  if (voter_count == 0 || voter_count > 100000u) {
    return std::nullopt;
  }
  const std::size_t election = quorum.election == 0 ? majority_count(voter_count) : quorum.election;
  const std::size_t commit = quorum.commit == 0 ? majority_count(voter_count) : quorum.commit;
  if (election < 1 || commit < 1 || election > voter_count || commit > voter_count) {
    return std::nullopt;
  }
  if (election + commit <= voter_count) {
    return std::nullopt;
  }
  if (election == majority_count(voter_count) && commit == majority_count(voter_count)) {
    return Quorum{};
  }
  return Quorum{election, commit};
}

// Every `election` quorum of `election_voters` intersects every `commit` quorum
// of `commit_voters`. For threshold quorums this is `election + commit > |union|`.
inline bool election_intersects_commit(const std::vector<NodeId>& election_voters,
                                       Quorum election_quorum,
                                       const std::vector<NodeId>& commit_voters,
                                       Quorum commit_quorum) {
  const std::optional<Quorum> election = normalize_quorum(election_voters.size(), election_quorum);
  const std::optional<Quorum> commit = normalize_quorum(commit_voters.size(), commit_quorum);
  if (!election || !commit) {
    return false;
  }
  const std::size_t election_need =
      election->is_default() ? majority_count(election_voters.size()) : election->election;
  const std::size_t commit_need =
      commit->is_default() ? majority_count(commit_voters.size()) : commit->commit;
  std::vector<NodeId> both = election_voters;
  both.insert(both.end(), commit_voters.begin(), commit_voters.end());
  std::sort(both.begin(), both.end());
  both.erase(std::unique(both.begin(), both.end()), both.end());
  return election_need + commit_need > both.size();
}

// One uniform entry is safe when the voter set is unchanged and each side's
// election quorums intersect the other side's commit quorums. Otherwise a
// quorum change has to go through joint consensus.
inline bool uniform_quorum_change_is_direct(const std::vector<NodeId>& voters,
                                            Quorum from,
                                            Quorum to) {
  return election_intersects_commit(voters, from, voters, to) &&
         election_intersects_commit(voters, to, voters, from);
}

// One uniform voter config, or a joint pair of configs (old and new), plus a
// learner set. A node is a voter if it belongs to any config. Learners receive
// the log and snapshots and are not voters: they are absent from every quorum.
// A node id belongs to at most one of the two sets. Id 0 is not a valid voter
// or learner; callers reject it.
//
// Each voter config carries its own election quorum and commit quorum. The
// default is a majority of that config. In a joint config an election still
// needs the election quorum of every config, and a commit needs the commit
// quorum of every config. Every election quorum of a config intersects every
// commit quorum of that same config, which is also what makes a joint election
// quorum intersect a joint commit quorum.
//
// `initialize()` stores the first uniform config. A later voter-set change is
// a joint membership log entry. A learner-set change, and a quorum change that
// `uniform_quorum_change_is_direct` accepts, is one uniform entry.
//
// Equality is the effective config, including each config's quorum. Prior
// learners and the prior quorum are log-recovery data carried by a membership
// entry and are not part of that comparison.
class Membership {
 public:
  Membership() = default;

  // Uniform membership. Voter and learner ids are sorted and de-duplicated.
  // Overlap is preserved so the caller can reject it. `quorum` is the
  // election and commit definition of this one voter config.
  explicit Membership(std::vector<NodeId> voters) : Membership(std::move(voters), {}) {}

  Membership(std::vector<NodeId> voters, std::vector<NodeId> learners)
      : Membership(std::move(voters), std::move(learners), {}) {}

  Membership(std::vector<NodeId> voters, std::vector<NodeId> learners, Quorum quorum) {
    configs_.push_back(normalize(std::move(voters)));
    learners_ = normalize(std::move(learners));
    quorums_.push_back(store_quorum(configs_.back().size(), quorum));
    rebuild_voters();
  }

  // Joint membership. `older` is the current uniform set and `newer` is the
  // target uniform set. Each side keeps its own quorum; the default is a
  // majority of that side. Learners that are also voters are dropped so the
  // joint config stays disjoint.
  static Membership joint_of(std::vector<NodeId> older, std::vector<NodeId> newer) {
    return joint_of(std::move(older), std::move(newer), {}, {});
  }

  static Membership joint_of(std::vector<NodeId> older,
                             std::vector<NodeId> newer,
                             std::vector<NodeId> learners) {
    std::vector<NodeId> after = learners;
    return joint_of(std::move(older), std::move(newer), std::move(learners), std::move(after));
  }

  // `learners_after_joint` is the learner set installed with the uniform
  // target. It differs when a voter is demoted: during the joint config that
  // node is still a voter, and it becomes a learner only in the uniform config.
  // `older_quorum` and `newer_quorum` are the definitions of those two configs.
  static Membership joint_of(std::vector<NodeId> older,
                             std::vector<NodeId> newer,
                             std::vector<NodeId> learners,
                             std::vector<NodeId> learners_after_joint,
                             Quorum older_quorum = {},
                             Quorum newer_quorum = {}) {
    Membership membership;
    membership.configs_.push_back(normalize(std::move(older)));
    membership.configs_.push_back(normalize(std::move(newer)));
    membership.quorums_.push_back(store_quorum(membership.configs_[0].size(), older_quorum));
    membership.quorums_.push_back(store_quorum(membership.configs_[1].size(), newer_quorum));
    membership.rebuild_voters();
    membership.learners_ = subtract(normalize(std::move(learners)), membership.voters_);
    std::vector<NodeId> after =
        subtract(normalize(std::move(learners_after_joint)), membership.configs_.back());
    if (after != membership.learners_) {
      membership.uniform_learners_ = std::move(after);
    }
    return membership;
  }

  const std::vector<std::vector<NodeId>>& configs() const { return configs_; }
  // Union of every config, sorted and unique. For a uniform config this is
  // that config. Learners are not included.
  const std::vector<NodeId>& voters() const { return voters_; }
  const std::vector<NodeId>& learners() const { return learners_; }
  // Voters and learners, sorted and unique. The leader replicates to this set.
  std::vector<NodeId> replication_targets() const {
    std::vector<NodeId> ids = voters_;
    ids.insert(ids.end(), learners_.begin(), learners_.end());
    return normalize(std::move(ids));
  }
  bool is_joint() const { return configs_.size() > 1; }
  bool empty() const { return voters_.empty(); }
  // True when `id` votes in any config. Learners return false.
  bool contains(NodeId id) const {
    return std::binary_search(voters_.begin(), voters_.end(), id);
  }
  bool is_learner(NodeId id) const {
    return std::binary_search(learners_.begin(), learners_.end(), id);
  }
  bool learners_overlap_voters() const {
    for (NodeId id : learners_) {
      if (contains(id)) {
        return true;
      }
    }
    return false;
  }
  std::size_t size() const { return voters_.size(); }

  // Learners of the uniform config that follows a joint entry. Equal to
  // `learners()` when nobody is waiting to be demoted.
  const std::vector<NodeId>& learners_after_joint() const {
    return uniform_learners_ ? *uniform_learners_ : learners_;
  }
  bool has_uniform_learner_override() const { return uniform_learners_.has_value(); }

  // Set when this object was decoded from a log entry that recorded the
  // learner set from before the entry. Empty and false for a constructed
  // config, a snapshot, and a version-1 entry.
  bool has_prior_learners() const { return has_prior_; }
  const std::vector<NodeId>& prior_learners() const { return prior_learners_; }

  // Set when a log entry recorded the quorum from before the entry. Absent
  // means the quorum did not change. Not set on a constructed config.
  bool has_prior_quorum() const { return has_prior_quorum_; }
  Quorum prior_quorum() const { return prior_quorum_; }

  // Quorum definition of one voter config. Out of range is the default.
  Quorum config_quorum(std::size_t index) const {
    if (index >= quorums_.size()) {
      return {};
    }
    return quorums_[index];
  }
  bool has_custom_quorum() const {
    for (const Quorum& quorum : quorums_) {
      if (!quorum.is_default()) {
        return true;
      }
    }
    return false;
  }

  // True when every config can form both quorums and every election quorum of
  // that config intersects every commit quorum of that config. A joint config
  // passes only when both sides do: a joint election quorum contains an
  // election quorum of each side, and a joint commit quorum contains a commit
  // quorum of each side, so the two joint families intersect as well.
  bool quorums_intersect() const {
    if (configs_.empty() || configs_.size() != quorums_.size()) {
      return false;
    }
    for (std::size_t index = 0; index < configs_.size(); ++index) {
      if (!normalize_quorum(configs_[index].size(), quorums_[index])) {
        return false;
      }
    }
    return true;
  }

  // Commit threshold of a uniform config. Learners are not counted. A joint
  // config has no single number; use `is_commit_quorum`. The default is
  // `floor(n / 2) + 1`.
  std::size_t quorum() const { return uniform_threshold(false); }
  // Election threshold of a uniform config. A joint config returns 0.
  std::size_t election_quorum() const { return uniform_threshold(true); }
  // Threshold of one config. Out of range is 0.
  std::size_t commit_threshold(std::size_t index) const {
    if (index >= configs_.size()) {
      return 0;
    }
    return threshold_at(index, false);
  }
  std::size_t election_threshold(std::size_t index) const {
    if (index >= configs_.size()) {
      return 0;
    }
    return threshold_at(index, true);
  }

  // True when `granted` reports a commit quorum of every voter config.
  template <typename Pred>
  bool is_quorum(Pred granted) const {
    return is_commit_quorum(granted);
  }

  template <typename Pred>
  bool is_commit_quorum(Pred granted) const {
    return is_threshold_quorum(false, granted);
  }

  template <typename Pred>
  bool is_election_quorum(Pred granted) const {
    return is_threshold_quorum(true, granted);
  }

  // Lossless encoding of the effective config. Version 1 is the historical
  // voter-only form. Version 2 adds learners. Version 3 adds a non-default
  // quorum. Not the on-disk metadata layout; FileLogStorage has its own
  // versioned encoding.
  std::string encode() const;
  // Same as `encode`, and records `prior_learners` so truncating this entry
  // can restore the previous learner set. A version-1 entry is written when
  // both sets are empty and the quorum is the default.
  std::string encode_for_log(const std::vector<NodeId>& prior_learners) const;
  // Also records `prior_quorum` when the quorum itself changed.
  std::string encode_for_log(const std::vector<NodeId>& prior_learners,
                             std::optional<Quorum> prior_quorum) const;
  static std::optional<Membership> decode(std::string_view bytes);

  friend bool operator==(const Membership& lhs, const Membership& rhs) {
    return lhs.configs_ == rhs.configs_ && lhs.quorums_ == rhs.quorums_ &&
           lhs.learners_ == rhs.learners_ && lhs.uniform_learners_ == rhs.uniform_learners_;
  }

 private:
  static std::vector<NodeId> normalize(std::vector<NodeId> ids) {
    std::sort(ids.begin(), ids.end());
    ids.erase(std::unique(ids.begin(), ids.end()), ids.end());
    return ids;
  }

  static std::vector<NodeId> subtract(const std::vector<NodeId>& ids,
                                      const std::vector<NodeId>& drop) {
    std::vector<NodeId> kept;
    kept.reserve(ids.size());
    for (NodeId id : ids) {
      if (!std::binary_search(drop.begin(), drop.end(), id)) {
        kept.push_back(id);
      }
    }
    return kept;
  }

  void rebuild_voters() {
    voters_.clear();
    for (const std::vector<NodeId>& config : configs_) {
      voters_.insert(voters_.end(), config.begin(), config.end());
    }
    std::sort(voters_.begin(), voters_.end());
    voters_.erase(std::unique(voters_.begin(), voters_.end()), voters_.end());
  }

  static Quorum store_quorum(std::size_t voters, Quorum requested) {
    if (voters == 0) {
      return requested;
    }
    if (const std::optional<Quorum> normalized = normalize_quorum(voters, requested)) {
      return *normalized;
    }
    const std::size_t election = requested.election == 0 ? majority_count(voters) : requested.election;
    const std::size_t commit = requested.commit == 0 ? majority_count(voters) : requested.commit;
    return Quorum{election, commit};
  }

  std::size_t threshold_at(std::size_t index, bool election) const {
    const std::size_t voters = configs_[index].size();
    const Quorum quorum = index < quorums_.size() ? quorums_[index] : Quorum{};
    const std::size_t raw = election ? quorum.election : quorum.commit;
    if (raw == 0) {
      return voters == 0 ? 0 : majority_count(voters);
    }
    return raw;
  }

  std::size_t uniform_threshold(bool election) const {
    if (is_joint() || configs_.empty()) {
      return 0;
    }
    return threshold_at(0, election);
  }

  template <typename Pred>
  bool is_threshold_quorum(bool election, Pred granted) const {
    if (configs_.empty()) {
      return false;
    }
    for (std::size_t index = 0; index < configs_.size(); ++index) {
      const std::vector<NodeId>& config = configs_[index];
      const std::size_t need = threshold_at(index, election);
      if (config.empty() || need == 0 || need > config.size()) {
        return false;
      }
      std::size_t votes = 0;
      for (NodeId id : config) {
        if (granted(id)) {
          ++votes;
        }
      }
      if (votes < need) {
        return false;
      }
    }
    return true;
  }

  std::string encode_bytes(const std::vector<NodeId>* prior, const Quorum* prior_quorum) const;

  std::vector<std::vector<NodeId>> configs_;
  std::vector<Quorum> quorums_;
  std::vector<NodeId> voters_;
  std::vector<NodeId> learners_;
  std::optional<std::vector<NodeId>> uniform_learners_;
  std::vector<NodeId> prior_learners_;
  bool has_prior_{false};
  Quorum prior_quorum_{};
  bool has_prior_quorum_{false};
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

inline void append_u32_le(std::string& out, std::uint32_t value) {
  for (int shift = 0; shift < 32; shift += 8) {
    out.push_back(static_cast<char>((value >> shift) & 0xffu));
  }
}

inline void append_u64_le(std::string& out, std::uint64_t value) {
  for (int shift = 0; shift < 64; shift += 8) {
    out.push_back(static_cast<char>((value >> shift) & 0xffu));
  }
}

inline void append_ids(std::string& out, const std::vector<NodeId>& ids) {
  append_u32_le(out, static_cast<std::uint32_t>(ids.size()));
  for (NodeId id : ids) {
    append_u64_le(out, id);
  }
}

inline std::string Membership::encode() const { return encode_bytes(nullptr, nullptr); }

inline std::string Membership::encode_for_log(const std::vector<NodeId>& prior_learners) const {
  return encode_for_log(prior_learners, std::nullopt);
}

inline std::string Membership::encode_for_log(const std::vector<NodeId>& prior_learners,
                                             std::optional<Quorum> prior_quorum) const {
  const std::vector<NodeId> prior = normalize(prior_learners);
  const bool learner_section = !learners_.empty() || uniform_learners_.has_value() || !prior.empty();
  if (!has_custom_quorum() && !prior_quorum && !learner_section) {
    return encode_bytes(nullptr, nullptr);
  }
  const Quorum* recorded = prior_quorum ? &*prior_quorum : nullptr;
  return encode_bytes(learner_section ? &prior : nullptr, recorded);
}

inline std::string Membership::encode_bytes(const std::vector<NodeId>* prior,
                                           const Quorum* prior_quorum) const {
  const bool version3 = has_custom_quorum() || prior_quorum != nullptr;
  const bool extended = version3 || !learners_.empty() || uniform_learners_.has_value() || prior != nullptr;
  std::string out;
  out.push_back(static_cast<char>(version3 ? 3 : extended ? 2 : 1));
  out.push_back(static_cast<char>(configs_.size()));
  for (const std::vector<NodeId>& config : configs_) {
    append_ids(out, config);
  }
  if (!extended) {
    return out;
  }
  append_ids(out, learners_);
  unsigned char flags = 0;
  if (uniform_learners_) {
    flags |= 1u;
  }
  if (prior != nullptr) {
    flags |= 2u;
  }
  if (version3 && has_custom_quorum()) {
    flags |= 4u;
  }
  if (prior_quorum != nullptr) {
    flags |= 8u;
  }
  out.push_back(static_cast<char>(flags));
  if (uniform_learners_) {
    append_ids(out, *uniform_learners_);
  }
  if (prior != nullptr) {
    append_ids(out, *prior);
  }
  if ((flags & 4u) != 0) {
    for (const Quorum& quorum : quorums_) {
      append_u32_le(out, static_cast<std::uint32_t>(quorum.election));
      append_u32_le(out, static_cast<std::uint32_t>(quorum.commit));
    }
  }
  if (prior_quorum != nullptr) {
    append_u32_le(out, static_cast<std::uint32_t>(prior_quorum->election));
    append_u32_le(out, static_cast<std::uint32_t>(prior_quorum->commit));
  }
  return out;
}

inline std::optional<Membership> Membership::decode(std::string_view bytes) {
  if (bytes.size() < 2) {
    return std::nullopt;
  }
  const auto* data = reinterpret_cast<const unsigned char*>(bytes.data());
  const unsigned version = data[0];
  if (version != 1 && version != 2 && version != 3) {
    return std::nullopt;
  }
  const unsigned nconfigs = data[1];
  if (nconfigs < 1 || nconfigs > 2) {
    return std::nullopt;
  }
  std::size_t offset = 2;
  auto read_u32 = [&](std::uint32_t& value) -> bool {
    if (offset > bytes.size() || bytes.size() - offset < 4) {
      return false;
    }
    value = 0;
    for (int byte = 0; byte < 4; ++byte) {
      value |= static_cast<std::uint32_t>(data[offset + static_cast<std::size_t>(byte)])
               << (8 * byte);
    }
    offset += 4;
    return true;
  };
  auto read_ids = [&](std::vector<NodeId>& ids) -> bool {
    std::uint32_t count = 0;
    if (!read_u32(count) || count > 100000u) {
      return false;
    }
    if (bytes.size() - offset < static_cast<std::size_t>(count) * 8) {
      return false;
    }
    ids.clear();
    ids.reserve(count);
    for (std::uint32_t i = 0; i < count; ++i) {
      std::uint64_t id = 0;
      for (int byte = 0; byte < 8; ++byte) {
        id |= static_cast<std::uint64_t>(data[offset + static_cast<std::size_t>(byte)])
              << (8 * byte);
      }
      offset += 8;
      ids.push_back(id);
    }
    return true;
  };

  std::vector<std::vector<NodeId>> configs;
  configs.reserve(nconfigs);
  for (unsigned config_index = 0; config_index < nconfigs; ++config_index) {
    std::vector<NodeId> voters;
    if (!read_ids(voters)) {
      return std::nullopt;
    }
    // Version 1 builds a Membership, which sorts and de-duplicates. Version 2
    // assigns the configs directly, so do the same here. A repeated id would
    // otherwise be counted twice by is_quorum and quorum_match_index.
    configs.push_back(normalize(std::move(voters)));
  }
  if (version == 1) {
    if (offset != bytes.size()) {
      return std::nullopt;
    }
    if (configs.size() == 1) {
      return Membership{std::move(configs[0])};
    }
    return joint_of(std::move(configs[0]), std::move(configs[1]));
  }

  std::vector<NodeId> learners;
  if (!read_ids(learners)) {
    return std::nullopt;
  }
  if (offset >= bytes.size()) {
    return std::nullopt;
  }
  const unsigned char flags = data[offset];
  ++offset;
  const unsigned char allowed = version == 3 ? 0x0fu : 0x03u;
  if ((flags & static_cast<unsigned char>(~allowed)) != 0) {
    return std::nullopt;
  }
  if (version != 3 && (flags & 0x0cu) != 0) {
    return std::nullopt;
  }
  std::vector<NodeId> uniform_learners;
  const bool has_uniform = (flags & 1u) != 0;
  if (has_uniform && !read_ids(uniform_learners)) {
    return std::nullopt;
  }
  std::vector<NodeId> prior;
  const bool has_prior = (flags & 2u) != 0;
  if (has_prior && !read_ids(prior)) {
    return std::nullopt;
  }
  std::vector<Quorum> decoded_quorums;
  const bool has_quorums = (flags & 4u) != 0;
  if (has_quorums) {
    decoded_quorums.reserve(configs.size());
    for (std::size_t index = 0; index < configs.size(); ++index) {
      std::uint32_t election = 0;
      std::uint32_t commit = 0;
      if (!read_u32(election) || !read_u32(commit)) {
        return std::nullopt;
      }
      decoded_quorums.push_back(Quorum{election, commit});
    }
  }
  Quorum prior_quorum;
  const bool has_prior_quorum = (flags & 8u) != 0;
  if (has_prior_quorum) {
    std::uint32_t election = 0;
    std::uint32_t commit = 0;
    if (!read_u32(election) || !read_u32(commit)) {
      return std::nullopt;
    }
    prior_quorum = Quorum{election, commit};
  }
  if (offset != bytes.size()) {
    return std::nullopt;
  }

  Membership membership;
  membership.configs_ = std::move(configs);
  membership.rebuild_voters();
  membership.learners_ = normalize(std::move(learners));
  membership.quorums_.assign(membership.configs_.size(), Quorum{});
  if (has_quorums) {
    for (std::size_t index = 0; index < membership.configs_.size(); ++index) {
      membership.quorums_[index] =
          store_quorum(membership.configs_[index].size(), decoded_quorums[index]);
    }
  }
  if (has_uniform) {
    std::vector<NodeId> after = normalize(std::move(uniform_learners));
    if (after != membership.learners_) {
      membership.uniform_learners_ = std::move(after);
    }
  }
  if (has_prior) {
    membership.has_prior_ = true;
    membership.prior_learners_ = normalize(std::move(prior));
  }
  if (has_prior_quorum) {
    membership.has_prior_quorum_ = true;
    const std::size_t prior_voters =
        membership.configs_.empty() ? 0 : membership.configs_.front().size();
    membership.prior_quorum_ = store_quorum(prior_voters, prior_quorum);
  }
  return membership;
}

// Identifies one durable image of applied state. `last_included` is the last
// log entry the image covers. `membership` is the effective config at that
// index, uniform or joint. `snapshot_id` changes when the image does.
struct SnapshotMeta {
  LogId last_included{};
  Membership membership{};
  std::string snapshot_id;

  friend bool operator==(const SnapshotMeta&, const SnapshotMeta&) = default;
};

// Opaque application bytes plus the meta that places them in the log.
struct Snapshot {
  SnapshotMeta meta{};
  std::string data;
};

inline std::string snapshot_id_from(const LogId& id) {
  return std::to_string(id.index) + "-" + std::to_string(id.term) + "-" + std::to_string(id.node);
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
