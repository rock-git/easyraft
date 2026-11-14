// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright 2026 easyraft contributors

#pragma once

#include "easyraft/engine.hpp"
#include "easyraft/network.hpp"
#include "easyraft/state_machine.hpp"
#include "easyraft/storage.hpp"

#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace easyraft {

// Runtime around Engine: persists commands, applies committed entries, and
// exchanges RPCs on a MessageBus. Not thread-safe. Drive it with tick() and
// the on_* RPC entry points (the in-process network calls those).
class RaftNode {
 public:
  RaftNode(NodeId id,
           Config config,
           std::shared_ptr<RaftLogStorage> storage,
           std::shared_ptr<RaftStateMachine> state_machine,
           MessageBus* bus);

  Result<void> initialize(Membership membership);
  Result<LogIndex> write(std::string data);
  // Only the leader starts a voter-set change. The joint entry is appended
  // locally; the uniform target is appended once the joint entry commits.
  // Learners stay in the learner set, except a learner listed in `voters`,
  // who is promoted and leaves that set. Returns the joint entry's index.
  // An unchanged voter set returns the last index.
  Result<LogIndex> change_membership(std::vector<NodeId> voters);
  // Only the leader changes the learner set. One uniform membership entry is
  // appended; joint consensus is not used. Returns that entry's index.
  Result<LogIndex> add_learner(NodeId learner);
  Result<LogIndex> remove_learner(NodeId learner);
  // Only the leader demotes a voter. Joint consensus removes it from the
  // voter set, and the uniform target adds it as a learner. Returns the
  // joint entry's index.
  Result<LogIndex> demote_voter(NodeId voter);
  // Compact through the applied index. Followers may compact locally; only a
  // leader replicates the image. `Rejected` means nothing new has been applied
  // since the current snapshot.
  Result<void> trigger_snapshot();
  void tick();

  void on_message(const Envelope& message);

  bool initialized() const { return initialized_; }
  // A failed storage or state-machine command. The node stays inert until it
  // is destroyed and rebuilt from storage.
  bool failed() const { return failed_; }
  NodeId id() const { return id_; }
  const Membership& membership() const { return engine_.membership(); }
  Role role() const { return engine_.role(); }
  Term term() const { return engine_.term(); }
  const Vote& vote() const { return engine_.vote(); }
  std::optional<NodeId> current_leader() const { return engine_.current_leader(); }
  LogIndex commit_index() const { return engine_.commit_index(); }
  LogIndex last_log_index() const { return engine_.last_index(); }
  LogIndex first_available_index() const { return engine_.first_available_index(); }
  LogIndex compacted_index() const { return engine_.compacted_index(); }
  std::optional<SnapshotMeta> snapshot_meta() const { return engine_.snapshot_meta(); }
  const std::vector<LogEntry>& log() const { return engine_.log(); }
  std::string debug() const { return engine_.debug(); }

 private:
  void restore_if_present();
  void apply(const StepResult& step);
  Result<LogIndex> finish_membership(ProposeResult proposed);
  void send(NodeId to, Payload payload);
  void dispatch(const Envelope& message);

  NodeId id_;
  std::shared_ptr<RaftLogStorage> storage_;
  std::shared_ptr<RaftStateMachine> state_machine_;
  MessageBus* bus_;
  Engine engine_;
  bool initialized_{false};
  bool failed_{false};
};

}  // namespace easyraft
