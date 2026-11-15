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
  // not already used by another group on this host. The id and those two
  // pointers are reserved before the node is built. Recovery runs inside the
  // constructor and can call back into `add` from the state machine, and the
  // reservation is visible to that callback. `find` stays empty for this id
  // until `add` returns. A failed open removes the reservation.
  // `AlreadyInitialized` means this group id is already present, including a
  // group whose node is still opening. `Rejected` means the arguments are
  // missing or shared with another group. `Storage` means opening the node
  // failed while reading its log.
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
  // Reserve before RaftNode::RaftNode. restore_if_present can invoke the state
  // machine, and that callback can call add again. Inserting first makes the
  // same id and the same resources visible, and a failed emplace is not reported
  // as success.
  auto [it, inserted] = slots_.emplace(group, Slot{nullptr, storage, state_machine});
  if (!inserted) {
    return Result<void>::failure(Error::AlreadyInitialized);
  }
  try {
    it->second.node = std::make_unique<RaftNode>(id_, std::move(config), it->second.storage,
                                                 it->second.state_machine, bus_, group);
  } catch (const std::runtime_error&) {
    slots_.erase(it);
    return Result<void>::failure(Error::Storage);
  } catch (...) {
    slots_.erase(it);
    throw;
  }
  return Result<void>::success();
}

inline RaftNode* RaftGroups::find(GroupId group) {
  const auto it = slots_.find(group);
  if (it == slots_.end() || !it->second.node) {
    return nullptr;
  }
  return it->second.node.get();
}

inline const RaftNode* RaftGroups::find(GroupId group) const {
  const auto it = slots_.find(group);
  if (it == slots_.end() || !it->second.node) {
    return nullptr;
  }
  return it->second.node.get();
}

inline void RaftGroups::tick() {
  // Copy first so a state-machine callback that adds a group cannot invalidate
  // the map walk. The new group waits until the next tick.
  std::vector<RaftNode*> nodes;
  nodes.reserve(slots_.size());
  for (auto& [_, slot] : slots_) {
    // A group reserved by an add that has not returned has no node yet.
    if (slot.node) {
      nodes.push_back(slot.node.get());
    }
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
  if (it == slots_.end() || !it->second.node) {
    return;
  }
  it->second.node->on_message(message);
}

inline std::vector<GroupId> RaftGroups::groups() const {
  std::vector<GroupId> ids;
  ids.reserve(slots_.size());
  for (const auto& [id, slot] : slots_) {
    if (slot.node) {
      ids.push_back(id);
    }
  }
  return ids;
}

}  // namespace easyraft
