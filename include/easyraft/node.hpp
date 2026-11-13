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
  void tick();

  void on_message(const Envelope& message);

  bool initialized() const { return initialized_; }
  // A failed storage or state-machine command. The node stays inert until it
  // is destroyed and rebuilt from storage.
  bool failed() const { return failed_; }
  NodeId id() const { return id_; }
  Role role() const { return engine_.role(); }
  Term term() const { return engine_.term(); }
  const Vote& vote() const { return engine_.vote(); }
  std::optional<NodeId> current_leader() const { return engine_.current_leader(); }
  LogIndex commit_index() const { return engine_.commit_index(); }
  LogIndex last_log_index() const { return engine_.last_index(); }
  const std::vector<LogEntry>& log() const { return engine_.log(); }
  std::string debug() const { return engine_.debug(); }

 private:
  void restore_if_present();
  void apply(const StepResult& step);
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
