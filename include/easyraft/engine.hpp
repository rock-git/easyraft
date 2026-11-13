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
  Truncate,
  Append,
  Apply,
};

struct Command {
  CommandKind kind{CommandKind::SaveVote};
  Vote vote{};
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

  StepResult recover(Membership membership, Vote vote, std::vector<LogEntry> log);
  StepResult on_tick();
  ProposeResult propose(std::string data);

  VoteStep on_pre_vote(const VoteRequest& request);
  StepResult on_pre_vote_response(NodeId from, const VoteResponse& response);
  VoteStep on_vote(const VoteRequest& request);
  StepResult on_vote_response(NodeId from, const VoteResponse& response);
  AppendStep on_append(const AppendEntriesRequest& request);
  StepResult on_append_response(NodeId from, const AppendEntriesResponse& response);

  bool initialized() const { return initialized_; }
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
  };

  const LogEntry* find(LogIndex index) const;
  std::optional<LogId> last_log_id() const;
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
  std::size_t replica_count(LogIndex index) const;
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
  std::map<NodeId, Progress> progress_;
};

}  // namespace easyraft
