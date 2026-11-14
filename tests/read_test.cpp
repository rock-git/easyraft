// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright 2026 easyraft contributors

#include "cluster.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <deque>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace easyraft {
namespace {

Config pinned(int timeout) {
  Config config;
  config.heartbeat_interval = 1;
  config.election_timeout_min = timeout;
  config.election_timeout_max = timeout;
  config.enable_pre_vote = true;
  return config;
}

std::uint64_t prevote_probe(const StepResult& step) {
  for (const Outbound& message : step.outbound) {
    if (const auto* request = std::get_if<PreVoteRequest>(&message.payload)) {
      return request->probe;
    }
  }
  return 0;
}

void become_leader(Engine& engine) {
  std::uint64_t probe = 0;
  for (int tick = 0; tick < 8 && engine.role() != Role::Leader; ++tick) {
    probe = std::max(probe, prevote_probe(engine.on_tick()));
    if (probe != 0 && engine.role() != Role::Leader && engine.term() == 0) {
      PreVoteResponse grant;
      grant.probe = probe;
      grant.body.granted = true;
      engine.on_pre_vote_response(2, grant);
    }
    if (engine.role() == Role::Candidate) {
      VoteResponse vote;
      vote.granted = true;
      vote.vote = engine.vote();
      engine.on_vote_response(2, vote);
    }
  }
}

std::uint64_t sent_seq(const StepResult& step) {
  for (const Outbound& message : step.outbound) {
    if (const auto* request = std::get_if<AppendEntriesRequest>(&message.payload)) {
      return request->heartbeat_seq;
    }
  }
  return 0;
}

StepResult ack_seq(Engine& engine, NodeId from, std::uint64_t seq) {
  AppendEntriesResponse response;
  response.success = true;
  response.request_vote = engine.vote();
  response.responder_vote = engine.vote().non_committed();
  response.match_index = engine.last_index();
  response.heartbeat_seq = seq;
  return engine.on_append_response(from, response);
}

bool command_ready(const StepResult& step, std::uint64_t id, LogIndex& index, int& apply_at,
                   int& ready_at) {
  apply_at = -1;
  ready_at = -1;
  index = 0;
  for (int i = 0; i < static_cast<int>(step.commands.size()); ++i) {
    const Command& command = step.commands[static_cast<std::size_t>(i)];
    if (command.kind == CommandKind::Apply && apply_at < 0) {
      apply_at = i;
    }
    if (command.kind == CommandKind::ReadReady && command.read_id == id) {
      ready_at = i;
      index = command.index;
    }
  }
  return ready_at >= 0;
}

Result<LogIndex> await_read(test::Cluster& cluster, NodeId id, std::uint64_t read_id, int max_ticks) {
  Result<LogIndex> result = Result<LogIndex>::failure(Error::Pending);
  cluster.until(
      [&] {
        result = cluster.node(id).poll_linearizable_read(read_id);
        return result.error() != Error::Pending;
      },
      max_ticks);
  return result;
}

bool applied_row(test::Cluster& cluster, NodeId id, const std::string& data) {
  const std::vector<std::string>& rows = cluster.machine(id).data();
  return std::find(rows.begin(), rows.end(), data) != rows.end();
}

TEST(EngineRead, ConfirmingAckAppliesBeforeTheReadIsReady) {
  Engine engine(1, pinned(4));
  engine.recover(Membership{{1, 2, 3}}, Vote{}, {});
  become_leader(engine);
  ASSERT_EQ(engine.role(), Role::Leader) << engine.debug();
  EXPECT_EQ(engine.commit_index(), 0u);
  // The lease clock started at election, but this term has not committed yet.
  EXPECT_EQ(engine.lease_read().error(), Error::Rejected);

  const ReadStart started = engine.begin_linearizable_read();
  ASSERT_TRUE(started.accepted);
  EXPECT_TRUE(engine.linearizable_read_pending(started.id));
  const std::uint64_t seq = sent_seq(started.step);
  EXPECT_GT(seq, 0u);

  const StepResult confirmed = ack_seq(engine, 2, seq);
  LogIndex index = 0;
  int apply_at = -1;
  int ready_at = -1;
  ASSERT_TRUE(command_ready(confirmed, started.id, index, apply_at, ready_at)) << engine.debug();
  EXPECT_GT(ready_at, apply_at);
  EXPECT_GE(engine.last_applied(), index);
  EXPECT_GE(index, 1u);
  EXPECT_FALSE(engine.linearizable_read_pending(started.id));
  const Result<LogIndex> leased = engine.lease_read();
  ASSERT_TRUE(leased.ok()) << leased.error();
  EXPECT_GE(leased.value(), index);
}

TEST(EngineRead, ShortOfTheCommitQuorumDoesNotConfirm) {
  Engine engine(1, pinned(4));
  engine.recover(Membership{{1, 2, 3}, {}, Quorum{2, 3}}, Vote{}, {});
  become_leader(engine);
  ASSERT_EQ(engine.role(), Role::Leader) << engine.debug();

  const ReadStart started = engine.begin_linearizable_read();
  ASSERT_TRUE(started.accepted);
  const std::uint64_t seq = sent_seq(started.step);
  EXPECT_GT(seq, 0u);

  const StepResult one = ack_seq(engine, 2, seq);
  LogIndex ignored = 0;
  int apply_at = -1;
  int ready_at = -1;
  EXPECT_FALSE(command_ready(one, started.id, ignored, apply_at, ready_at));
  EXPECT_TRUE(engine.linearizable_read_pending(started.id));
  EXPECT_EQ(engine.commit_index(), 0u);
  EXPECT_EQ(engine.lease_read().error(), Error::Rejected);

  const StepResult both = ack_seq(engine, 3, seq);
  LogIndex index = 0;
  ASSERT_TRUE(command_ready(both, started.id, index, apply_at, ready_at)) << engine.debug();
  EXPECT_GT(ready_at, apply_at);
  EXPECT_GE(engine.last_applied(), index);
  EXPECT_FALSE(engine.linearizable_read_pending(started.id));
  ASSERT_TRUE(engine.lease_read().ok());
}

TEST(LinearizableRead, SingleNodeServesAsSoonAsTheEntryIsApplied) {
  test::Cluster cluster({1}, test::fast_config(2), false);
  ASSERT_TRUE(cluster.until([&] { return cluster.node(1).role() == Role::Leader; }, 5))
      << cluster.debug();
  const Result<LogIndex> written = cluster.node(1).write("solo");
  ASSERT_TRUE(written.ok()) << written.error();
  EXPECT_GE(cluster.machine(1).last_applied(), written.value());

  const Result<std::uint64_t> started = cluster.node(1).linearizable_read();
  ASSERT_TRUE(started.ok()) << started.error();
  const Result<LogIndex> ready = cluster.node(1).poll_linearizable_read(started.value());
  ASSERT_TRUE(ready.ok()) << ready.error();
  EXPECT_GE(ready.value(), written.value());
  EXPECT_GE(cluster.node(1).last_applied(), ready.value());
  EXPECT_TRUE(applied_row(cluster, 1, "solo"));

  const Result<LogIndex> leased = cluster.node(1).lease_read();
  ASSERT_TRUE(leased.ok()) << leased.error();
  EXPECT_GE(leased.value(), written.value());
}

TEST(LinearizableRead, SeesACommittedEntryOnlyAfterItIsApplied) {
  test::Cluster cluster({1, 2, 3}, test::fast_config(4), true);
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 30)) << cluster.debug();
  const Result<LogIndex> written = cluster.node(1).write("seen");
  ASSERT_TRUE(written.ok()) << written.error();
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.node(1).commit_index() >= written.value() &&
               cluster.machine(1).last_applied() >= written.value() &&
               applied_row(cluster, 1, "seen");
      },
      20))
      << cluster.debug();

  cluster.hold_append_responses(true);
  const Result<std::uint64_t> started = cluster.node(1).linearizable_read();
  ASSERT_TRUE(started.ok()) << started.error();
  cluster.tick();
  const Result<LogIndex> waiting = cluster.node(1).poll_linearizable_read(started.value());
  EXPECT_EQ(waiting.error(), Error::Pending) << cluster.debug();
  EXPECT_GE(cluster.machine(1).last_applied(), written.value());
  EXPECT_TRUE(applied_row(cluster, 1, "seen"));

  cluster.hold_append_responses(false);
  cluster.release_held_append_responses_reversed();
  const Result<LogIndex> ready = await_read(cluster, 1, started.value(), 20);
  ASSERT_TRUE(ready.ok()) << ready.error() << "\n" << cluster.debug();
  EXPECT_GE(ready.value(), written.value());
  EXPECT_GE(cluster.node(1).last_applied(), ready.value());
  EXPECT_GE(cluster.machine(1).last_applied(), ready.value());
  EXPECT_TRUE(applied_row(cluster, 1, "seen"));
  EXPECT_EQ(cluster.node(2).linearizable_read().error(), Error::NotLeader);
  EXPECT_EQ(cluster.node(2).lease_read().error(), Error::NotLeader);
}

class WatchMachine final : public RaftStateMachine {
 public:
  RaftNode* node{nullptr};
  std::uint64_t watch{0};
  int polls_during_apply{0};
  bool served_before_apply{false};

  Result<void> apply(const LogEntry& entry) override {
    if (entry.id.index > last_applied_) {
      if (entry.id.index != last_applied_ + 1) {
        return Result<void>::failure(Error::Storage);
      }
      last_applied_ = entry.id.index;
      if (entry.kind == EntryKind::Application) {
        data_.push_back(entry.data);
      }
    }
    if (watch != 0 && node != nullptr) {
      ++polls_during_apply;
      const Result<LogIndex> polled = node->poll_linearizable_read(watch);
      if (polled.ok() && last_applied_ < polled.value()) {
        served_before_apply = true;
      }
    }
    return Result<void>::success();
  }

  LogIndex last_applied() const override { return last_applied_; }

  Result<std::string> build_snapshot() override { return Result<std::string>::success({}); }

  Result<void> install_snapshot(const SnapshotMeta& meta, std::string_view) override {
    if (meta.last_included.index < last_applied_) {
      return Result<void>::failure(Error::Rejected);
    }
    last_applied_ = meta.last_included.index;
    return Result<void>::success();
  }

  const std::vector<std::string>& data() const { return data_; }

 private:
  LogIndex last_applied_{0};
  std::vector<std::string> data_;
};

class LoopBus final : public MessageBus {
 public:
  void send(const Envelope& message) override { queue.push_back(message); }

  void drain() {
    int steps = 0;
    while (!queue.empty()) {
      if (++steps > 100000) {
        throw std::runtime_error("in-process network did not quiesce");
      }
      Envelope message = std::move(queue.front());
      queue.pop_front();
      if (hold_responses && std::holds_alternative<AppendEntriesResponse>(message.payload)) {
        held.push_back(std::move(message));
        continue;
      }
      const auto found = nodes.find(message.to);
      if (found == nodes.end() || found->second == nullptr) {
        continue;
      }
      found->second->on_message(message);
    }
  }

  void release() {
    while (!held.empty()) {
      queue.push_front(std::move(held.back()));
      held.pop_back();
    }
  }

  std::map<NodeId, RaftNode*> nodes;
  bool hold_responses{false};
  std::deque<Envelope> queue;
  std::deque<Envelope> held;
};

TEST(LinearizableRead, DoesNotPublishBeforeApply) {
  LoopBus bus;
  bus.hold_responses = true;
  std::map<NodeId, std::shared_ptr<MemoryLogStorage>> stores;
  std::map<NodeId, std::shared_ptr<WatchMachine>> machines;
  std::map<NodeId, std::unique_ptr<RaftNode>> nodes;
  for (NodeId id : {1u, 2u, 3u}) {
    Config config = pinned(4 + static_cast<int>(id - 1) * 4);
    stores.emplace(id, std::make_shared<MemoryLogStorage>());
    machines.emplace(id, std::make_shared<WatchMachine>());
    nodes.emplace(id, std::make_unique<RaftNode>(id, config, stores.at(id), machines.at(id), &bus));
    bus.nodes.emplace(id, nodes.at(id).get());
    machines.at(id)->node = nodes.at(id).get();
    ASSERT_TRUE(nodes.at(id)->initialize(Membership{{1, 2, 3}}).ok());
  }

  bool elected = false;
  for (int tick = 0; tick < 20 && !elected; ++tick) {
    for (NodeId id : {1u, 2u, 3u}) {
      nodes.at(id)->tick();
    }
    bus.drain();
    elected = nodes.at(1)->role() == Role::Leader;
  }
  ASSERT_TRUE(elected);
  EXPECT_EQ(nodes.at(1)->commit_index(), 0u);
  EXPECT_EQ(nodes.at(1)->last_applied(), 0u);

  const Result<std::uint64_t> started = nodes.at(1)->linearizable_read();
  ASSERT_TRUE(started.ok()) << started.error();
  const Result<LogIndex> written = nodes.at(1)->write("seen");
  ASSERT_TRUE(written.ok()) << written.error();
  EXPECT_EQ(nodes.at(1)->poll_linearizable_read(started.value()).error(), Error::Pending);
  EXPECT_EQ(nodes.at(1)->last_applied(), 0u);

  machines.at(1)->watch = started.value();
  bus.hold_responses = false;
  bus.release();
  bus.drain();
  for (int tick = 0; tick < 8 &&
                     nodes.at(1)->poll_linearizable_read(started.value()).error() == Error::Pending;
       ++tick) {
    for (NodeId id : {1u, 2u, 3u}) {
      nodes.at(id)->tick();
    }
    bus.drain();
  }

  const Result<LogIndex> ready = nodes.at(1)->poll_linearizable_read(started.value());
  ASSERT_TRUE(ready.ok()) << ready.error();
  EXPECT_GE(nodes.at(1)->last_applied(), ready.value());
  EXPECT_GE(machines.at(1)->last_applied(), ready.value());
  const std::vector<std::string>& rows = machines.at(1)->data();
  EXPECT_NE(std::find(rows.begin(), rows.end(), "seen"), rows.end());
  EXPECT_GT(machines.at(1)->polls_during_apply, 0);
  EXPECT_FALSE(machines.at(1)->served_before_apply);
}

TEST(LinearizableRead, SmallerCommitQuorumConfirmsWithOneFollower) {
  test::Cluster cluster({1, 2, 3, 4, 5}, test::fast_config(4), true, test::StorageMode::Memory, {},
                        Quorum{4, 2});
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 50)) << cluster.debug();
  const Result<LogIndex> written = cluster.node(1).write("small");
  ASSERT_TRUE(written.ok()) << written.error();
  ASSERT_TRUE(cluster.until(
      [&] { return cluster.node(1).commit_index() >= written.value() && applied_row(cluster, 1, "small"); },
      20))
      << cluster.debug();

  cluster.isolate(1);
  const Result<std::uint64_t> started = cluster.node(1).linearizable_read();
  ASSERT_TRUE(started.ok()) << started.error();
  cluster.tick_n(6);
  EXPECT_EQ(cluster.node(1).poll_linearizable_read(started.value()).error(), Error::Pending)
      << cluster.debug();

  cluster.partition({{1, 2}, {3}, {4}, {5}});
  const Result<LogIndex> ready = await_read(cluster, 1, started.value(), 20);
  ASSERT_TRUE(ready.ok()) << ready.error() << "\n" << cluster.debug();
  EXPECT_GE(ready.value(), written.value());
  EXPECT_GE(cluster.machine(1).last_applied(), ready.value());
  EXPECT_TRUE(applied_row(cluster, 1, "small"));
}

TEST(LinearizableRead, MissingCommitQuorumDoesNotSucceed) {
  test::Cluster cluster({1, 2, 3}, test::fast_config(4), true, test::StorageMode::Memory, {},
                        Quorum{2, 3});
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 40)) << cluster.debug();
  const Result<LogIndex> written = cluster.node(1).write("all-three");
  ASSERT_TRUE(written.ok()) << written.error();
  ASSERT_TRUE(cluster.until(
      [&] {
        return applied_row(cluster, 1, "all-three") && applied_row(cluster, 2, "all-three") &&
               applied_row(cluster, 3, "all-three");
      },
      30))
      << cluster.debug();

  cluster.partition({{1, 2}, {3}});
  const Result<std::uint64_t> started = cluster.node(1).linearizable_read();
  ASSERT_TRUE(started.ok()) << started.error();
  cluster.tick_n(8);
  EXPECT_EQ(cluster.node(1).poll_linearizable_read(started.value()).error(), Error::Pending)
      << cluster.debug();

  cluster.heal();
  const Result<LogIndex> ready = await_read(cluster, 1, started.value(), 20);
  ASSERT_TRUE(ready.ok()) << ready.error() << "\n" << cluster.debug();
  EXPECT_GE(ready.value(), written.value());
  EXPECT_TRUE(applied_row(cluster, 1, "all-three"));
}

TEST(LeaseRead, SucceedsWhileValidAndRefusesWhenTheLeaseExpires) {
  // Node 1's timeout stays 8. The other nodes wait longer, so they do not
  // elect during the eight ticks that expire node 1's lease.
  test::Cluster cluster({1, 2, 3}, test::fast_config(8), true);
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 30)) << cluster.debug();
  const Result<LogIndex> written = cluster.node(1).write("leased");
  ASSERT_TRUE(written.ok()) << written.error();
  ASSERT_TRUE(cluster.until([&] { return applied_row(cluster, 1, "leased"); }, 20)) << cluster.debug();

  const Result<LogIndex> fresh = cluster.node(1).lease_read();
  ASSERT_TRUE(fresh.ok()) << fresh.error() << "\n" << cluster.debug();
  EXPECT_GE(fresh.value(), written.value());
  EXPECT_GE(cluster.machine(1).last_applied(), fresh.value());
  EXPECT_EQ(cluster.node(2).lease_read().error(), Error::NotLeader);

  cluster.isolate(1);
  EXPECT_TRUE(cluster.node(1).lease_read().ok()) << cluster.debug();
  cluster.tick_n(7);
  const Result<LogIndex> still = cluster.node(1).lease_read();
  ASSERT_TRUE(still.ok()) << still.error() << "\n" << cluster.debug();
  EXPECT_GE(still.value(), written.value());
  cluster.tick();
  EXPECT_EQ(cluster.node(1).role(), Role::Leader) << cluster.debug();
  EXPECT_EQ(cluster.node(1).lease_read().error(), Error::LeaseExpired) << cluster.debug();
  EXPECT_EQ(cluster.node(1).poll_linearizable_read(99).error(), Error::Rejected);
}

TEST(ReadAfterPartition, OldLeaderDoesNotServeStaleLeadership) {
  test::Cluster cluster({1, 2, 3}, test::fast_config(6), true);
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 30)) << cluster.debug();
  const Result<LogIndex> written = cluster.node(1).write("kept");
  ASSERT_TRUE(written.ok()) << written.error();
  ASSERT_TRUE(cluster.until(
      [&] { return applied_row(cluster, 2, "kept") && applied_row(cluster, 3, "kept"); }, 20))
      << cluster.debug();

  cluster.partition({{1}, {2, 3}});
  ASSERT_TRUE(cluster.until(
      [&] {
        const bool node2 = cluster.node(2).role() == Role::Leader && cluster.node(3).current_leader() == 2;
        const bool node3 = cluster.node(3).role() == Role::Leader && cluster.node(2).current_leader() == 3;
        return node2 || node3;
      },
      40))
      << cluster.debug();
  const NodeId leader = cluster.node(2).role() == Role::Leader ? 2 : 3;
  ASSERT_TRUE(cluster.until([&] { return applied_row(cluster, leader, "kept"); }, 30)) << cluster.debug();

  const Result<std::uint64_t> fresh = cluster.node(leader).linearizable_read();
  ASSERT_TRUE(fresh.ok()) << fresh.error() << "\n" << cluster.debug();
  const Result<LogIndex> seen = await_read(cluster, leader, fresh.value(), 20);
  ASSERT_TRUE(seen.ok()) << seen.error() << "\n" << cluster.debug();
  EXPECT_GE(cluster.machine(leader).last_applied(), seen.value());
  EXPECT_TRUE(applied_row(cluster, leader, "kept"));

  EXPECT_EQ(cluster.node(1).role(), Role::Leader) << cluster.debug();
  EXPECT_EQ(cluster.node(1).lease_read().error(), Error::LeaseExpired) << cluster.debug();
  const Result<std::uint64_t> stale = cluster.node(1).linearizable_read();
  ASSERT_TRUE(stale.ok()) << stale.error();
  cluster.tick_n(4);
  EXPECT_EQ(cluster.node(1).poll_linearizable_read(stale.value()).error(), Error::Pending)
      << cluster.debug();

  bool served = false;
  cluster.heal();
  ASSERT_TRUE(cluster.until(
      [&] {
        const Result<LogIndex> polled = cluster.node(1).poll_linearizable_read(stale.value());
        if (polled.ok()) {
          served = true;
        }
        return polled.error() == Error::NotLeader;
      },
      30))
      << cluster.debug();
  EXPECT_FALSE(served);
  EXPECT_NE(cluster.node(1).role(), Role::Leader);
  EXPECT_EQ(cluster.node(1).lease_read().error(), Error::NotLeader);
}

TEST(ReadApi, FollowersAndLearnersRejectReads) {
  test::Cluster cluster({1, 2, 3}, test::fast_config(4), true);
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 30)) << cluster.debug();
  cluster.add_node(4);
  ASSERT_TRUE(cluster.node(1).add_learner(4).ok()) << cluster.debug();
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.node(4).membership().is_learner(4) && cluster.node(4).current_leader() == 1 &&
               cluster.node(2).role() == Role::Follower;
      },
      40))
      << cluster.debug();

  EXPECT_EQ(cluster.node(2).linearizable_read().error(), Error::NotLeader);
  EXPECT_EQ(cluster.node(2).lease_read().error(), Error::NotLeader);
  EXPECT_EQ(cluster.node(2).poll_linearizable_read(1).error(), Error::NotLeader);
  EXPECT_EQ(cluster.node(4).role(), Role::Follower);
  EXPECT_EQ(cluster.node(4).linearizable_read().error(), Error::NotLeader);
  EXPECT_EQ(cluster.node(4).lease_read().error(), Error::NotLeader);
  EXPECT_EQ(cluster.node(4).poll_linearizable_read(1).error(), Error::NotLeader);
  EXPECT_EQ(cluster.node(1).poll_linearizable_read(1).error(), Error::Rejected);
}

TEST(ReadApi, LearnerAcksDoNotConfirmALinearizableRead) {
  test::Cluster cluster({1, 2, 3}, test::fast_config(4), true);
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 30)) << cluster.debug();
  cluster.add_node(4);
  ASSERT_TRUE(cluster.node(1).add_learner(4).ok()) << cluster.debug();
  ASSERT_TRUE(cluster.until(
      [&] { return cluster.node(4).membership().is_learner(4) && cluster.node(4).current_leader() == 1; },
      40))
      << cluster.debug();
  const Result<LogIndex> written = cluster.node(1).write("voter");
  ASSERT_TRUE(written.ok()) << written.error();
  ASSERT_TRUE(cluster.until([&] { return applied_row(cluster, 1, "voter"); }, 20)) << cluster.debug();

  cluster.partition({{1, 4}, {2}, {3}});
  const Result<std::uint64_t> started = cluster.node(1).linearizable_read();
  ASSERT_TRUE(started.ok()) << started.error();
  cluster.tick_n(8);
  EXPECT_EQ(cluster.node(1).poll_linearizable_read(started.value()).error(), Error::Pending)
      << cluster.debug();

  cluster.partition({{1, 2, 4}, {3}});
  const Result<LogIndex> ready = await_read(cluster, 1, started.value(), 20);
  ASSERT_TRUE(ready.ok()) << ready.error() << "\n" << cluster.debug();
  EXPECT_GE(ready.value(), written.value());
  EXPECT_TRUE(applied_row(cluster, 1, "voter"));
}

TEST(ReadAfterSnapshot, RestartStillServesTheCompactedPrefix) {
  test::Cluster cluster({1, 2, 3}, test::fast_config(4), true, test::StorageMode::File);
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 30)) << cluster.debug();
  const Result<LogIndex> written = cluster.node(1).write("kept");
  ASSERT_TRUE(written.ok()) << written.error();
  ASSERT_TRUE(cluster.until(
      [&] {
        return applied_row(cluster, 1, "kept") && applied_row(cluster, 2, "kept") &&
               applied_row(cluster, 3, "kept");
      },
      20))
      << cluster.debug();
  ASSERT_TRUE(cluster.node(1).trigger_snapshot().ok()) << cluster.debug();
  EXPECT_GE(cluster.node(1).compacted_index(), written.value());
  EXPECT_TRUE(applied_row(cluster, 1, "kept"));

  const Result<std::uint64_t> before = cluster.node(1).linearizable_read();
  ASSERT_TRUE(before.ok()) << before.error();
  const Result<LogIndex> compacted = await_read(cluster, 1, before.value(), 20);
  ASSERT_TRUE(compacted.ok()) << compacted.error() << "\n" << cluster.debug();
  EXPECT_GE(compacted.value(), written.value());
  EXPECT_GE(cluster.machine(1).last_applied(), compacted.value());
  const Result<LogIndex> leased = cluster.node(1).lease_read();
  ASSERT_TRUE(leased.ok()) << leased.error();
  EXPECT_GE(leased.value(), written.value());

  cluster.restart(1);
  ASSERT_TRUE(cluster.until(
      [&] {
        if (cluster.leaders().size() != 1) {
          return false;
        }
        const NodeId leader = cluster.leaders().front();
        return applied_row(cluster, leader, "kept") && cluster.node(leader).commit_index() >= written.value();
      },
      40))
      << cluster.debug();
  const NodeId leader = cluster.leaders().front();
  const Result<std::uint64_t> after = cluster.node(leader).linearizable_read();
  ASSERT_TRUE(after.ok()) << after.error() << "\n" << cluster.debug();
  const Result<LogIndex> restored = await_read(cluster, leader, after.value(), 30);
  ASSERT_TRUE(restored.ok()) << restored.error() << "\n" << cluster.debug();
  EXPECT_GE(cluster.machine(leader).last_applied(), restored.value());
  EXPECT_TRUE(applied_row(cluster, leader, "kept"));
  const Result<LogIndex> leased_again = cluster.node(leader).lease_read();
  ASSERT_TRUE(leased_again.ok()) << leased_again.error() << "\n" << cluster.debug();
  EXPECT_GE(cluster.machine(leader).last_applied(), leased_again.value());
}

}  // namespace
}  // namespace easyraft
