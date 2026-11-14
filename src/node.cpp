// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright 2026 easyraft contributors

#include "easyraft/node.hpp"

#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>

namespace easyraft {
namespace {

template <typename T>
T check(Result<T> result) {
  if (!result.ok()) {
    throw std::runtime_error(std::string("easyraft storage error: ") + to_string(result.error()));
  }
  return std::move(result).value();
}

void check(Result<void> result) {
  if (!result.ok()) {
    throw std::runtime_error(std::string("easyraft storage error: ") + to_string(result.error()));
  }
}

}  // namespace

RaftNode::RaftNode(NodeId id,
                   Config config,
                   std::shared_ptr<RaftLogStorage> storage,
                   std::shared_ptr<RaftStateMachine> state_machine,
                   MessageBus* bus)
    : id_(id),
      storage_(std::move(storage)),
      state_machine_(std::move(state_machine)),
      bus_(bus),
      engine_(id, std::move(config)) {
  restore_if_present();
}

namespace {

bool membership_ok(const Membership& membership, NodeId self) {
  // The first config is uniform. This process may be absent from it, or be a
  // learner, so it can catch up before a membership entry adds it as a voter.
  // Id 0 stays reserved for voters and learners. A node is never both.
  if (self == 0 || membership.empty() || membership.is_joint() ||
      membership.learners_overlap_voters() || !membership.quorums_intersect()) {
    return false;
  }
  for (NodeId voter : membership.voters()) {
    if (voter == 0) {
      return false;
    }
  }
  for (NodeId learner : membership.learners()) {
    if (learner == 0) {
      return false;
    }
  }
  return true;
}

bool voter_ids_ok(const std::vector<NodeId>& voters) {
  if (voters.empty()) {
    return false;
  }
  for (NodeId id : voters) {
    if (id == 0) {
      return false;
    }
  }
  return true;
}

}  // namespace

Result<void> RaftNode::initialize(Membership membership) {
  if (failed_) {
    return Result<void>::failure(Error::Storage);
  }
  if (initialized_) {
    return Result<void>::failure(Error::AlreadyInitialized);
  }
  if (!membership_ok(membership, id_)) {
    return Result<void>::failure(Error::InvalidMembership);
  }
  try {
    check(storage_->save_membership(membership));
    check(storage_->save_vote(Vote{}));
    apply(engine_.recover(std::move(membership), Vote{}, {}, state_machine_->last_applied()));
  } catch (const std::runtime_error&) {
    failed_ = true;
    return Result<void>::failure(Error::Storage);
  }
  initialized_ = true;
  return Result<void>::success();
}

Result<LogIndex> RaftNode::write(std::string data) {
  if (failed_) {
    return Result<LogIndex>::failure(Error::Storage);
  }
  if (!initialized_) {
    return Result<LogIndex>::failure(Error::NotInitialized);
  }
  if (role() != Role::Leader) {
    return Result<LogIndex>::failure(Error::NotLeader);
  }
  ProposeResult proposed = engine_.propose(std::move(data));
  if (!proposed.accepted) {
    return Result<LogIndex>::failure(Error::NotLeader);
  }
  try {
    apply(proposed.step);
  } catch (const std::runtime_error&) {
    return Result<LogIndex>::failure(Error::Storage);
  }
  if (failed_) {
    return Result<LogIndex>::failure(Error::Storage);
  }
  return Result<LogIndex>::success(proposed.index);
}

Result<LogIndex> RaftNode::change_membership(std::vector<NodeId> voters, Quorum quorum) {
  if (failed_) {
    return Result<LogIndex>::failure(Error::Storage);
  }
  if (!initialized_) {
    return Result<LogIndex>::failure(Error::NotInitialized);
  }
  if (role() != Role::Leader) {
    return Result<LogIndex>::failure(Error::NotLeader);
  }
  std::vector<NodeId> unique = voters;
  std::sort(unique.begin(), unique.end());
  unique.erase(std::unique(unique.begin(), unique.end()), unique.end());
  if (!voter_ids_ok(unique) || !normalize_quorum(unique.size(), quorum)) {
    return Result<LogIndex>::failure(Error::InvalidMembership);
  }
  if (engine_.membership_changing()) {
    return Result<LogIndex>::failure(Error::Rejected);
  }
  return finish_membership(engine_.change_membership(std::move(voters), quorum));
}

Result<LogIndex> RaftNode::change_quorum(Quorum quorum) {
  if (failed_) {
    return Result<LogIndex>::failure(Error::Storage);
  }
  if (!initialized_) {
    return Result<LogIndex>::failure(Error::NotInitialized);
  }
  if (role() != Role::Leader) {
    return Result<LogIndex>::failure(Error::NotLeader);
  }
  // An in-flight change is retryable. Report it before rejecting the
  // definition, including while the current config is still joint.
  if (engine_.membership_changing()) {
    return Result<LogIndex>::failure(Error::Rejected);
  }
  if (membership().empty() || !normalize_quorum(membership().voters().size(), quorum)) {
    return Result<LogIndex>::failure(Error::InvalidMembership);
  }
  return finish_membership(engine_.change_quorum(quorum));
}

Result<LogIndex> RaftNode::finish_membership(ProposeResult proposed) {
  if (!proposed.accepted) {
    return Result<LogIndex>::failure(Error::Rejected);
  }
  try {
    apply(proposed.step);
  } catch (const std::runtime_error&) {
    return Result<LogIndex>::failure(Error::Storage);
  }
  if (failed_) {
    return Result<LogIndex>::failure(Error::Storage);
  }
  return Result<LogIndex>::success(proposed.index);
}

Result<LogIndex> RaftNode::add_learner(NodeId learner) {
  if (failed_) {
    return Result<LogIndex>::failure(Error::Storage);
  }
  if (!initialized_) {
    return Result<LogIndex>::failure(Error::NotInitialized);
  }
  if (role() != Role::Leader) {
    return Result<LogIndex>::failure(Error::NotLeader);
  }
  if (learner == 0 || membership().contains(learner) || membership().is_learner(learner)) {
    return Result<LogIndex>::failure(Error::InvalidMembership);
  }
  if (engine_.membership_changing()) {
    return Result<LogIndex>::failure(Error::Rejected);
  }
  return finish_membership(engine_.add_learner(learner));
}

Result<LogIndex> RaftNode::remove_learner(NodeId learner) {
  if (failed_) {
    return Result<LogIndex>::failure(Error::Storage);
  }
  if (!initialized_) {
    return Result<LogIndex>::failure(Error::NotInitialized);
  }
  if (role() != Role::Leader) {
    return Result<LogIndex>::failure(Error::NotLeader);
  }
  if (!membership().is_learner(learner)) {
    return Result<LogIndex>::failure(Error::InvalidMembership);
  }
  if (engine_.membership_changing()) {
    return Result<LogIndex>::failure(Error::Rejected);
  }
  return finish_membership(engine_.remove_learner(learner));
}

Result<LogIndex> RaftNode::demote_voter(NodeId voter) {
  if (failed_) {
    return Result<LogIndex>::failure(Error::Storage);
  }
  if (!initialized_) {
    return Result<LogIndex>::failure(Error::NotInitialized);
  }
  if (role() != Role::Leader) {
    return Result<LogIndex>::failure(Error::NotLeader);
  }
  if (voter == 0 || !membership().contains(voter) || membership().voters().size() < 2) {
    return Result<LogIndex>::failure(Error::InvalidMembership);
  }
  if (engine_.membership_changing()) {
    return Result<LogIndex>::failure(Error::Rejected);
  }
  const Quorum current = membership().config_quorum(0);
  if (!current.is_default() &&
      !normalize_quorum(membership().voters().size() - 1, current)) {
    return Result<LogIndex>::failure(Error::InvalidMembership);
  }
  return finish_membership(engine_.demote_voter(voter));
}

Result<std::uint64_t> RaftNode::linearizable_read() {
  if (failed_) {
    return Result<std::uint64_t>::failure(Error::Storage);
  }
  if (!initialized_) {
    return Result<std::uint64_t>::failure(Error::NotInitialized);
  }
  if (role() != Role::Leader) {
    return Result<std::uint64_t>::failure(Error::NotLeader);
  }
  const ReadStart started = engine_.begin_linearizable_read();
  if (!started.accepted) {
    return Result<std::uint64_t>::failure(Error::NotLeader);
  }
  outstanding_reads_.insert(started.id);
  try {
    apply(started.step);
  } catch (const std::runtime_error&) {
    return Result<std::uint64_t>::failure(Error::Storage);
  }
  if (failed_) {
    return Result<std::uint64_t>::failure(Error::Storage);
  }
  return Result<std::uint64_t>::success(started.id);
}

Result<LogIndex> RaftNode::poll_linearizable_read(std::uint64_t id) {
  if (failed_) {
    return Result<LogIndex>::failure(Error::Storage);
  }
  if (!initialized_) {
    return Result<LogIndex>::failure(Error::NotInitialized);
  }
  const auto ready = ready_reads_.find(id);
  if (ready != ready_reads_.end()) {
    return Result<LogIndex>::success(ready->second);
  }
  if (aborted_reads_.find(id) != aborted_reads_.end()) {
    return Result<LogIndex>::failure(Error::NotLeader);
  }
  if (outstanding_reads_.find(id) != outstanding_reads_.end()) {
    return Result<LogIndex>::failure(Error::Pending);
  }
  if (role() != Role::Leader) {
    return Result<LogIndex>::failure(Error::NotLeader);
  }
  return Result<LogIndex>::failure(Error::Rejected);
}

Result<void> RaftNode::trigger_snapshot() {
  if (failed_) {
    return Result<void>::failure(Error::Storage);
  }
  if (!initialized_) {
    return Result<void>::failure(Error::NotInitialized);
  }
  const SnapshotBuild built = engine_.begin_snapshot();
  if (!built.started) {
    return Result<void>::failure(Error::Rejected);
  }
  try {
    apply(built.step);
    if (!failed_ && role() == Role::Leader) {
      apply(engine_.replicate_now());
    }
  } catch (const std::runtime_error&) {
    return Result<void>::failure(Error::Storage);
  }
  if (failed_) {
    return Result<void>::failure(Error::Storage);
  }
  return Result<void>::success();
}

void RaftNode::tick() {
  if (failed_ || !initialized_) {
    return;
  }
  apply(engine_.on_tick());
}

void RaftNode::on_message(const Envelope& message) {
  if (failed_ || !initialized_) {
    return;
  }
  dispatch(message);
}

void RaftNode::restore_if_present() {
  auto membership = check(storage_->load_membership());
  if (!membership) {
    return;
  }
  auto vote = check(storage_->load_vote());
  Vote restored = vote.value_or(Vote{});
  auto snapshot = check(storage_->load_snapshot());
  if (snapshot && state_machine_ &&
      state_machine_->last_applied() < snapshot->meta.last_included.index) {
    check(state_machine_->install_snapshot(snapshot->meta, snapshot->data));
  }
  auto stored = check(storage_->read(1, UINT64_MAX));
  const LogIndex already_applied = state_machine_ ? state_machine_->last_applied() : 0;
  apply(engine_.recover(std::move(*membership), restored, std::move(stored), already_applied,
                        std::move(snapshot)));
  initialized_ = true;
}

void RaftNode::apply(const StepResult& step) {
  if (failed_) {
    return;
  }
  // Published after Apply commands in this step, so a read is not visible
  // while the state machine is still catching up to its index.
  std::vector<Command> reads;
  try {
    for (const Command& command : step.commands) {
      switch (command.kind) {
        case CommandKind::SaveVote:
          check(storage_->save_vote(command.vote));
          break;
        case CommandKind::SaveMembership:
          check(storage_->save_membership(command.membership));
          break;
        case CommandKind::Truncate:
          check(storage_->truncate_from(command.index));
          break;
        case CommandKind::Append:
          check(storage_->append(command.entries));
          break;
        case CommandKind::Apply:
          check(state_machine_->apply(command.entry));
          break;
        case CommandKind::SaveSnapshot: {
          Snapshot image = command.snapshot;
          if (command.build_snapshot) {
            image.data = check(state_machine_->build_snapshot());
            engine_.store_snapshot_bytes(image.data);
          }
          check(storage_->save_snapshot(image));
          check(state_machine_->install_snapshot(image.meta, image.data));
          break;
        }
        case CommandKind::Purge:
          check(storage_->purge_through(command.index));
          break;
        case CommandKind::ReadReady:
        case CommandKind::ReadAborted:
          reads.push_back(command);
          break;
      }
    }
  } catch (const std::runtime_error&) {
    // The engine shadow already moved. Refuse every later RPC and proposal so
    // a retry cannot acknowledge state that never reached storage.
    failed_ = true;
    throw;
  }
  publish_reads(reads);
  for (const Outbound& message : step.outbound) {
    send(message.to, message.payload);
  }
}

void RaftNode::publish_reads(const std::vector<Command>& reads) {
  for (const Command& command : reads) {
    if (command.kind == CommandKind::ReadReady) {
      aborted_reads_.erase(command.read_id);
      ready_reads_[command.read_id] = command.index;
      outstanding_reads_.erase(command.read_id);
    } else if (command.kind == CommandKind::ReadAborted) {
      ready_reads_.erase(command.read_id);
      outstanding_reads_.erase(command.read_id);
      aborted_reads_.insert(command.read_id);
    }
  }
}

void RaftNode::send(NodeId to, Payload payload) {
  if (bus_ == nullptr || to == id_) {
    return;
  }
  Envelope envelope;
  envelope.from = id_;
  envelope.to = to;
  envelope.payload = std::move(payload);
  bus_->send(envelope);
}

void RaftNode::dispatch(const Envelope& message) {
  std::visit(
      [&](const auto& payload) {
        using T = std::decay_t<decltype(payload)>;
        if constexpr (std::is_same_v<T, PreVoteRequest>) {
          VoteStep step = engine_.on_pre_vote(payload.body);
          apply(step.step);
          PreVoteResponse response;
          response.body = step.response;
          response.probe = payload.probe;
          send(message.from, response);
        } else if constexpr (std::is_same_v<T, PreVoteResponse>) {
          apply(engine_.on_pre_vote_response(message.from, payload));
        } else if constexpr (std::is_same_v<T, RequestVote>) {
          VoteStep step = engine_.on_vote(payload.body);
          apply(step.step);
          send(message.from, RequestVoteResponse{step.response});
        } else if constexpr (std::is_same_v<T, RequestVoteResponse>) {
          apply(engine_.on_vote_response(message.from, payload.body));
        } else if constexpr (std::is_same_v<T, AppendEntriesRequest>) {
          AppendStep step = engine_.on_append(payload);
          apply(step.step);
          send(message.from, step.response);
        } else if constexpr (std::is_same_v<T, AppendEntriesResponse>) {
          apply(engine_.on_append_response(message.from, payload));
        } else if constexpr (std::is_same_v<T, InstallSnapshotRequest>) {
          InstallStep step = engine_.on_install(payload);
          apply(step.step);
          send(message.from, step.response);
        } else if constexpr (std::is_same_v<T, InstallSnapshotResponse>) {
          apply(engine_.on_install_response(message.from, payload));
        }
      },
      message.payload);
}

}  // namespace easyraft
