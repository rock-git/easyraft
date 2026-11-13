// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright 2026 easyraft contributors

#pragma once

#include "easyraft/easyraft.hpp"

#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <stdexcept>
#include <utility>
#include <vector>

namespace easyraft {
namespace test {

// Deterministic in-process network. send() only queues; Cluster::tick() drains
// in FIFO order after every node has observed the same tick.
class Cluster {
 public:
  Cluster(std::vector<NodeId> ids, Config config, bool stagger_timeouts)
      : ids_(std::move(ids)) {
    if (ids_.empty()) {
      throw std::invalid_argument("cluster needs at least one node");
    }
    for (std::size_t i = 0; i < ids_.size(); ++i) {
      const NodeId id = ids_[i];
      Config node_config = config.normalized();
      if (stagger_timeouts) {
        const int timeout = node_config.election_timeout_min + static_cast<int>(i) * 4;
        node_config.election_timeout_min = timeout;
        node_config.election_timeout_max = timeout;
      }
      configs_.emplace(id, node_config);
      stores_.emplace(id, std::make_shared<MemoryLogStorage>());
      machines_.emplace(id, std::make_shared<MemoryStateMachine>());
      running_.emplace(id, true);
      nodes_.emplace(id, std::make_unique<RaftNode>(id, node_config, stores_.at(id), machines_.at(id), &bus_));
      bus_.bind(id, [this, id](const Envelope& message) {
        if (!running_.at(id)) {
          return;
        }
        nodes_.at(id)->on_message(message);
      });
    }
    const Membership membership{ids_};
    for (NodeId id : ids_) {
      const Result<void> started = nodes_.at(id)->initialize(membership);
      if (!started.ok()) {
        throw std::runtime_error("initialize failed");
      }
    }
  }

  void tick() {
    for (NodeId id : ids_) {
      if (running_.at(id)) {
        nodes_.at(id)->tick();
      }
    }
    bus_.drain([this](const Envelope& message) { return allow(message); });
  }

  void tick_n(int ticks) {
    for (int i = 0; i < ticks; ++i) {
      tick();
    }
  }

  template <typename Pred>
  bool until(Pred pred, int max_ticks) {
    if (pred()) {
      return true;
    }
    for (int i = 0; i < max_ticks; ++i) {
      tick();
      if (pred()) {
        return true;
      }
    }
    return false;
  }

  void isolate(NodeId id) {
    std::set<NodeId> alone{id};
    std::set<NodeId> rest;
    for (NodeId other : ids_) {
      if (other != id) {
        rest.insert(other);
      }
    }
    groups_ = {std::move(alone), std::move(rest)};
  }

  void partition(std::vector<std::set<NodeId>> groups) { groups_ = std::move(groups); }

  void heal() { groups_.clear(); }

  void drop_append_responses(bool drop) { drop_append_responses_ = drop; }

  void stop(NodeId id) { running_.at(id) = false; }

  void restart(NodeId id) {
    bus_.discard(id);
    nodes_.at(id).reset();
    machines_.at(id) = std::make_shared<MemoryStateMachine>();
    nodes_.at(id) =
        std::make_unique<RaftNode>(id, configs_.at(id), stores_.at(id), machines_.at(id), &bus_);
    running_.at(id) = true;
  }

  RaftNode& node(NodeId id) { return *nodes_.at(id); }
  const RaftNode& node(NodeId id) const { return *nodes_.at(id); }
  MemoryLogStorage& storage(NodeId id) { return *stores_.at(id); }
  MemoryStateMachine& machine(NodeId id) { return *machines_.at(id); }

  std::vector<NodeId> leaders() const {
    std::vector<NodeId> found;
    for (NodeId id : ids_) {
      if (running_.at(id) && nodes_.at(id)->role() == Role::Leader) {
        found.push_back(id);
      }
    }
    return found;
  }

  bool agreed_on(NodeId leader) const {
    if (!running_.at(leader) || nodes_.at(leader)->role() != Role::Leader) {
      return false;
    }
    const Term term = nodes_.at(leader)->term();
    for (NodeId id : ids_) {
      if (!running_.at(id)) {
        continue;
      }
      const RaftNode& current = *nodes_.at(id);
      if (current.term() != term) {
        return false;
      }
      if (id == leader) {
        continue;
      }
      if (current.role() == Role::Leader || current.current_leader() != leader) {
        return false;
      }
    }
    return true;
  }

  bool log_contains(NodeId id, const std::string& data) const {
    for (const LogEntry& entry : nodes_.at(id)->log()) {
      if (entry.kind == EntryKind::Application && entry.data == data) {
        return true;
      }
    }
    return false;
  }

  std::string debug() const {
    std::ostringstream out;
    out << "leaders:";
    for (NodeId id : leaders()) {
      out << " " << id;
    }
    out << "\n";
    for (NodeId id : ids_) {
      out << nodes_.at(id)->debug();
      out << " applied=[";
      const auto& rows = machines_.at(id)->data();
      for (std::size_t i = 0; i < rows.size(); ++i) {
        if (i != 0) {
          out << ", ";
        }
        out << rows[i];
      }
      out << "] running=" << running_.at(id) << "\n";
    }
    return out.str();
  }

 private:
  class Bus final : public MessageBus {
   public:
    void bind(NodeId id, std::function<void(const Envelope&)> handler) { handlers_[id] = std::move(handler); }

    void send(const Envelope& message) override { queue_.push_back(message); }

    void discard(NodeId id) {
      std::deque<Envelope> kept;
      for (auto& message : queue_) {
        if (message.from != id && message.to != id) {
          kept.push_back(std::move(message));
        }
      }
      queue_ = std::move(kept);
    }

    template <typename Allow>
    void drain(Allow allow) {
      int steps = 0;
      while (!queue_.empty()) {
        if (++steps > 100000) {
          throw std::runtime_error("in-process network did not quiesce");
        }
        Envelope message = std::move(queue_.front());
        queue_.pop_front();
        if (!allow(message)) {
          continue;
        }
        const auto handler = handlers_.find(message.to);
        if (handler == handlers_.end()) {
          continue;
        }
        handler->second(message);
      }
    }

   private:
    std::deque<Envelope> queue_;
    std::map<NodeId, std::function<void(const Envelope&)>> handlers_;
  };

  bool allow(const Envelope& message) const {
    if (drop_append_responses_ && std::holds_alternative<AppendEntriesResponse>(message.payload)) {
      return false;
    }
    if (groups_.empty()) {
      return true;
    }
    for (const std::set<NodeId>& group : groups_) {
      if (group.count(message.from) != 0 && group.count(message.to) != 0) {
        return true;
      }
    }
    return false;
  }

  Bus bus_;
  std::vector<NodeId> ids_;
  std::map<NodeId, Config> configs_;
  std::map<NodeId, std::shared_ptr<MemoryLogStorage>> stores_;
  std::map<NodeId, std::shared_ptr<MemoryStateMachine>> machines_;
  std::map<NodeId, bool> running_;
  std::map<NodeId, std::unique_ptr<RaftNode>> nodes_;
  std::vector<std::set<NodeId>> groups_;
  bool drop_append_responses_{false};
};

inline Config fast_config(int timeout) {
  Config config;
  config.heartbeat_interval = 1;
  config.election_timeout_min = timeout;
  config.election_timeout_max = timeout;
  config.enable_pre_vote = true;
  return config;
}

}  // namespace test
}  // namespace easyraft
