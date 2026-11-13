// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright 2026 easyraft contributors

#pragma once

#include "easyraft/config.hpp"
#include "easyraft/messages.hpp"
#include "easyraft/types.hpp"

#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <vector>

namespace easyraft {

// I/O the runtime must perform to mirror an engine transition.
// The engine has already applied the same change to its in-memory shadow.
enum class CommandKind {
  SaveVote,
  SaveMembership,
  Truncate,
  Append,
  Apply,
};

struct Command {
  CommandKind kind{CommandKind::SaveVote};
  Vote vote{};
  Membership membership{};
  LogIndex index{0};
  std::vector<LogEntry> entries;
  LogEntry entry{};
};

struct Outbound {
  NodeId to{0};
  Payload payload{PreVoteRequest{}};
};

struct StepResult {
  std::vector<Command> commands;
  std::vector<Outbound> outbound;
};

struct ProposeResult {
  StepResult step;
  bool accepted{false};
  LogIndex index{0};
};

struct VoteStep {
  StepResult step;
  VoteResponse response;
};

struct AppendStep {
  StepResult step;
  AppendEntriesResponse response;
};

// Pure Raft state machine. Time advances only through on_tick(); no wall clock.
//
// The runtime (RaftNode) executes `commands` against storage and the state
// machine, then sends `outbound`. Feeding the replies back in is the whole
// protocol loop, which is what makes the tests deterministic.
class Engine {
 public:
  Engine(NodeId id, Config config);

  // `already_applied` is the state machine's cursor. Entries at or below it are
  // not emitted again when a leader re-commits the prefix.
  StepResult recover(Membership membership,
                     Vote vote,
                     std::vector<LogEntry> log,
                     LogIndex already_applied = 0);
  StepResult on_tick();
  ProposeResult propose(std::string data);
  // Appends a joint config of the current voters and `voters`, then the
  // uniform target once that joint entry commits. `accepted` is false when
  // this node cannot start a change. The returned index is the joint entry.
  ProposeResult change_membership(std::vector<NodeId> voters);

  VoteStep on_pre_vote(const VoteRequest& request);
  StepResult on_pre_vote_response(NodeId from, const PreVoteResponse& response);
  VoteStep on_vote(const VoteRequest& request);
  StepResult on_vote_response(NodeId from, const VoteResponse& response);
  AppendStep on_append(const AppendEntriesRequest& request);
  StepResult on_append_response(NodeId from, const AppendEntriesResponse& response);

  bool initialized() const { return initialized_; }
  const Membership& membership() const { return membership_; }
  // Joint config still in effect, or a membership entry not yet committed.
  bool membership_changing() const;
  Role role() const { return role_; }
  const Vote& vote() const { return vote_; }
  Term term() const { return vote_.leader.term; }
  std::optional<NodeId> current_leader() const;
  LogIndex commit_index() const { return commit_index_; }
  LogIndex last_index() const { return last_index_; }
  LogIndex last_applied() const { return last_applied_; }
  const std::vector<LogEntry>& log() const { return log_; }
  std::string debug() const;

 private:
  struct Progress {
    LogIndex next_index{1};
    LogIndex match_index{0};
    std::uint64_t last_ack_tick{0};
  };

  struct Campaign {
    Vote vote{};
    std::set<NodeId> grants;
    std::uint64_t probe{0};
  };

  const LogEntry* find(LogIndex index) const;
  std::optional<LogId> last_log_id() const;
  bool has_quorum(const std::set<NodeId>& grants) const;
  std::optional<LogIndex> quorum_match_index() const;
  std::uint64_t sample_timeout();
  bool follower_lease_valid() const;
  bool leader_quorum_lease_valid() const;
  void note_leader_contact();
  bool update_vote(StepResult& out, const Vote& incoming);
  void become_leader(StepResult& out);
  void step_down();
  void append_local(StepResult& out, EntryKind kind, std::string data);
  void truncate_from(StepResult& out, LogIndex index);
  void advance_commit(StepResult& out);
  void adopt_membership(StepResult& out, Membership next);
  Membership infer_base(const Membership& stored) const;
  Membership membership_from_log() const;
  void sync_progress();
  void continue_membership(StepResult& out);
  void replicate_all(StepResult& out);
  void send_append(StepResult& out, NodeId to);
  void send_vote_requests(StepResult& out, bool pre_vote, const Vote& vote);
  void pre_elect(StepResult& out);
  void elect(StepResult& out);
  void establish_leader(StepResult& out);
  void enqueue_applies(StepResult& out);

  NodeId id_;
  Config config_;
  bool initialized_{false};
  Membership membership_{};
  // Membership in effect when the log has no membership entry. Inferred on
  // recover from the first joint entry's old config, or from the stored one.
  Membership base_membership_{};
  bool continuing_{false};
  Vote vote_{};
  Role role_{Role::Follower};
  bool leading_{false};
  std::vector<LogEntry> log_;
  LogIndex last_index_{0};
  LogIndex commit_index_{0};
  LogIndex last_applied_{0};
  std::uint64_t now_{0};
  std::uint64_t election_timeout_ticks_{1};
  std::uint64_t next_election_at_{1};
  std::uint64_t ticks_since_heartbeat_{0};
  std::uint64_t last_leader_contact_{0};
  bool ever_contacted_{false};
  std::uint32_t rng_{1};
  std::optional<Campaign> campaign_;
  std::optional<Campaign> pre_campaign_;
  std::uint64_t next_probe_{0};
  std::map<NodeId, Progress> progress_;
};

}  // namespace easyraft
