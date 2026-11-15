// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright 2026 easyraft contributors

#pragma once

#include "easyraft/error.hpp"
#include "easyraft/node.hpp"

#include <map>
#include <memory>
#include <stdexcept>
#include <utility>
#include <vector>

namespace easyraft {

// Several independent Raft groups in one process, each an ordinary RaftNode.
//
// `id` is this process's node id in every group. A group has its own
// membership, log, vote, snapshot, state machine, and leader. `tick` advances
// each group on that group's own timer. Heartbeat, election, and lease
// intervals are whatever `Config` the caller passed to `add`; this type does
// not change them. Not thread-safe.
//
// Outbound envelopes already carry the sending node's group. `on_message`
// delivers an envelope to that group when `to` is this node, and drops every
// other destination or a group this host does not run.
class RaftGroups {
 public:
  RaftGroups(NodeId id, MessageBus* bus) : id_(id), bus_(bus) {}

  RaftGroups(const RaftGroups&) = delete;
  RaftGroups& operator=(const RaftGroups&) = delete;
  RaftGroups(RaftGroups&&) = default;
  RaftGroups& operator=(RaftGroups&&) = default;

  // Inserts a group. The storage and the state machine must be non-null and
  // not already used by another group on this host. The node restores from
  // storage the way `RaftNode` does; a new group still needs `initialize`.
  // `AlreadyInitialized` means this group id is already present. `Rejected`
  // means the arguments are missing or shared with another group. `Storage`
  // means opening the node failed while reading its log.
  Result<void> add(GroupId group,
                   Config config,
                   std::shared_ptr<RaftLogStorage> storage,
                   std::shared_ptr<RaftStateMachine> state_machine);

  RaftNode* find(GroupId group);
  const RaftNode* find(GroupId group) const;

  // Ascending group id. Each call is one tick of that group only.
  void tick();
  void on_message(const Envelope& message);

  NodeId id() const { return id_; }
  std::vector<GroupId> groups() const;

 private:
  struct Slot {
    std::unique_ptr<RaftNode> node;
    std::shared_ptr<RaftLogStorage> storage;
    std::shared_ptr<RaftStateMachine> state_machine;
  };

  NodeId id_;
  MessageBus* bus_{nullptr};
  std::map<GroupId, Slot> slots_;
};

inline Result<void> RaftGroups::add(GroupId group,
                                    Config config,
                                    std::shared_ptr<RaftLogStorage> storage,
                                    std::shared_ptr<RaftStateMachine> state_machine) {
  if (!storage || !state_machine) {
    return Result<void>::failure(Error::Rejected);
  }
  if (slots_.find(group) != slots_.end()) {
    return Result<void>::failure(Error::AlreadyInitialized);
  }
  for (const auto& [_, slot] : slots_) {
    if (slot.storage.get() == storage.get() || slot.state_machine.get() == state_machine.get()) {
      return Result<void>::failure(Error::Rejected);
    }
  }
  try {
    Slot slot;
    slot.storage = storage;
    slot.state_machine = state_machine;
    slot.node = std::make_unique<RaftNode>(id_, std::move(config), std::move(storage),
                                           std::move(state_machine), bus_, group);
    slots_.emplace(group, std::move(slot));
  } catch (const std::runtime_error&) {
    return Result<void>::failure(Error::Storage);
  }
  return Result<void>::success();
}

inline RaftNode* RaftGroups::find(GroupId group) {
  const auto it = slots_.find(group);
  return it == slots_.end() ? nullptr : it->second.node.get();
}

inline const RaftNode* RaftGroups::find(GroupId group) const {
  const auto it = slots_.find(group);
  return it == slots_.end() ? nullptr : it->second.node.get();
}

inline void RaftGroups::tick() {
  // Copy first so a state-machine callback that adds a group cannot invalidate
  // the map walk. The new group waits until the next tick.
  std::vector<RaftNode*> nodes;
  nodes.reserve(slots_.size());
  for (auto& [_, slot] : slots_) {
    nodes.push_back(slot.node.get());
  }
  for (RaftNode* node : nodes) {
    node->tick();
  }
}

inline void RaftGroups::on_message(const Envelope& message) {
  if (message.to != id_) {
    return;
  }
  const auto it = slots_.find(message.group);
  if (it == slots_.end()) {
    return;
  }
  it->second.node->on_message(message);
}

inline std::vector<GroupId> RaftGroups::groups() const {
  std::vector<GroupId> ids;
  ids.reserve(slots_.size());
  for (const auto& [id, _] : slots_) {
    ids.push_back(id);
  }
  return ids;
}

}  // namespace easyraft
