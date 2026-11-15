// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright 2026 easyraft contributors

#include "easyraft/easyraft.hpp"

#include <gtest/gtest.h>

#include <memory>
#include <stdexcept>
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

LogEntry entry(Term term, NodeId node, LogIndex index, EntryKind kind, std::string data = {}) {
  LogEntry value;
  value.id = LogId{term, node, index};
  value.kind = kind;
  value.data = std::move(data);
  return value;
}

class FailAfterAppends final : public RaftLogStorage {
 public:
  explicit FailAfterAppends(int succeed) : succeed_(succeed) {}

  Result<void> save_vote(const Vote& vote) override { return inner_.save_vote(vote); }
  Result<std::optional<Vote>> load_vote() override { return inner_.load_vote(); }
  Result<void> save_membership(const Membership& membership) override {
    return inner_.save_membership(membership);
  }
  Result<std::optional<Membership>> load_membership() override { return inner_.load_membership(); }

  Result<void> append(const std::vector<LogEntry>& entries) override {
    if (appends_ >= succeed_) {
      return Result<void>::failure(Error::Storage);
    }
    ++appends_;
    return inner_.append(entries);
  }

  Result<void> save_snapshot(const Snapshot& snapshot) override { return inner_.save_snapshot(snapshot); }
  Result<std::optional<Snapshot>> load_snapshot() override { return inner_.load_snapshot(); }
  Result<void> purge_through(LogIndex index) override { return inner_.purge_through(index); }

  Result<void> truncate_from(LogIndex index) override { return inner_.truncate_from(index); }
  Result<std::vector<LogEntry>> read(LogIndex from, LogIndex to) override { return inner_.read(from, to); }

  const std::vector<LogEntry>& persisted_log() const { return inner_.persisted_log(); }

 private:
  MemoryLogStorage inner_;
  int succeed_;
  int appends_{0};
};

class RecordingBus final : public MessageBus {
 public:
  void send(const Envelope& message) override { sent.push_back(message); }

  std::vector<Envelope> sent;
};

class CursorStateMachine final : public RaftStateMachine {
 public:
  explicit CursorStateMachine(LogIndex applied) : last_(applied) {}

  Result<void> apply(const LogEntry& applied) override {
    seen.push_back(applied.id.index);
    if (applied.id.index > last_) {
      last_ = applied.id.index;
    }
    return Result<void>::success();
  }

  LogIndex last_applied() const override { return last_; }

  Result<std::string> build_snapshot() override {
    return Result<std::string>::success(std::string{});
  }

  Result<void> install_snapshot(const SnapshotMeta& meta, std::string_view) override {
    if (meta.last_included.index < last_) {
      return Result<void>::failure(Error::Rejected);
    }
    last_ = meta.last_included.index;
    return Result<void>::success();
  }

  std::vector<LogIndex> seen;

 private:
  LogIndex last_;
};

TEST(Node, RejectsReservedVoterIdZero) {
  auto storage = std::make_shared<MemoryLogStorage>();
  auto machine = std::make_shared<MemoryStateMachine>();
  RaftNode node(1, pinned(4), storage, machine, nullptr);
  const Result<void> started = node.initialize(Membership{{0, 1}});
  EXPECT_FALSE(started.ok());
  EXPECT_EQ(started.error(), Error::InvalidMembership);
  EXPECT_FALSE(node.initialized());
}

TEST(Node, StorageFailureLatchesTheNode) {
  auto storage = std::make_shared<FailAfterAppends>(1);
  auto machine = std::make_shared<MemoryStateMachine>();
  RaftNode node(1, pinned(2), storage, machine, nullptr);
  ASSERT_TRUE(node.initialize(Membership{{1}}).ok());
  node.tick();
  node.tick();
  ASSERT_EQ(node.role(), Role::Leader);

  const Result<LogIndex> failed = node.write("x");
  EXPECT_FALSE(failed.ok());
  EXPECT_EQ(failed.error(), Error::Storage);
  EXPECT_TRUE(node.failed());
  EXPECT_EQ(storage->persisted_log().size(), 1u);

  const Result<LogIndex> retry = node.write("y");
  EXPECT_FALSE(retry.ok());
  EXPECT_EQ(retry.error(), Error::Storage);
  EXPECT_EQ(storage->persisted_log().size(), 1u);
  node.tick();
  EXPECT_EQ(node.role(), Role::Leader);
  EXPECT_EQ(storage->persisted_log().size(), 1u);
}

TEST(Node, RecoverySkipsEntriesTheStateMachineAlreadyApplied) {
  auto storage = std::make_shared<MemoryLogStorage>();
  ASSERT_TRUE(storage->save_membership(Membership{{1, 2}}).ok());
  Vote leader;
  leader.leader = LeaderId{3, 1};
  leader.committed = true;
  ASSERT_TRUE(storage->save_vote(leader).ok());
  ASSERT_TRUE(storage
                  ->append({entry(3, 1, 1, EntryKind::Blank),
                            entry(3, 1, 2, EntryKind::Application, "a"),
                            entry(3, 1, 3, EntryKind::Application, "b")})
                  .ok());

  auto machine = std::make_shared<CursorStateMachine>(2);
  RaftNode node(2, pinned(8), storage, machine, nullptr);
  ASSERT_TRUE(node.initialized());

  AppendEntriesRequest heartbeat;
  heartbeat.vote = leader;
  heartbeat.prev_log_id = LogId{3, 1, 3};
  heartbeat.leader_commit = 3;
  Envelope message;
  message.from = 1;
  message.to = 2;
  message.payload = heartbeat;
  node.on_message(message);

  ASSERT_EQ(machine->seen.size(), 1u);
  EXPECT_EQ(machine->seen.front(), 3u);
  EXPECT_EQ(machine->last_applied(), 3u);
}

TEST(Node, DefaultsToGroupZero) {
  auto storage = std::make_shared<MemoryLogStorage>();
  auto machine = std::make_shared<MemoryStateMachine>();
  RaftNode node(1, pinned(4), storage, machine, nullptr);
  EXPECT_EQ(node.group(), 0u);
}

TEST(Node, IgnoresAnotherGroupsRpcAndStampsItsOwn) {
  auto storage = std::make_shared<MemoryLogStorage>();
  auto machine = std::make_shared<MemoryStateMachine>();
  RecordingBus bus;
  constexpr GroupId group = 7;
  RaftNode node(1, pinned(4), storage, machine, &bus, group);
  ASSERT_TRUE(node.initialize(Membership{{1, 2}}).ok());
  EXPECT_EQ(node.group(), group);

  RequestVote vote;
  vote.body.vote.leader = LeaderId{3, 2};
  Envelope granted;
  granted.from = 2;
  granted.to = 1;
  granted.group = group;
  granted.payload = vote;
  node.on_message(granted);
  EXPECT_EQ(node.term(), 3u);
  EXPECT_EQ(node.vote().leader.node, 2u);
  EXPECT_EQ(node.role(), Role::Follower);
  ASSERT_EQ(bus.sent.size(), 1u);
  EXPECT_EQ(bus.sent[0].group, group);
  EXPECT_EQ(bus.sent[0].from, 1u);
  EXPECT_EQ(bus.sent[0].to, 2u);
  EXPECT_TRUE(std::holds_alternative<RequestVoteResponse>(bus.sent[0].payload));

  const Vote stored = node.vote();
  const std::size_t log_size = node.log().size();
  const std::size_t sent_before = bus.sent.size();

  auto deliver = [&](GroupId foreign, Payload payload) {
    Envelope message;
    message.from = 2;
    message.to = 1;
    message.group = foreign;
    message.payload = std::move(payload);
    node.on_message(message);
  };

  RequestVote higher;
  higher.body.vote.leader = LeaderId{9, 2};
  deliver(0, higher);
  deliver(8, higher);

  PreVoteRequest prevote;
  prevote.body.vote.leader = LeaderId{10, 2};
  prevote.probe = 4;
  deliver(8, prevote);

  AppendEntriesRequest append;
  append.vote.leader = LeaderId{11, 2};
  append.vote.committed = true;
  append.leader_commit = 1;
  LogEntry foreign_entry;
  foreign_entry.id = LogId{11, 2, 1};
  foreign_entry.kind = EntryKind::Application;
  foreign_entry.data = "other-group";
  append.entries.push_back(foreign_entry);
  deliver(8, append);

  InstallSnapshotRequest snapshot;
  snapshot.vote.leader = LeaderId{12, 2};
  snapshot.vote.committed = true;
  snapshot.meta.last_included = LogId{12, 2, 4};
  snapshot.meta.membership = Membership{{1, 2}};
  snapshot.meta.snapshot_id = "4-12-2";
  snapshot.data = "other-snapshot";
  snapshot.done = true;
  deliver(8, snapshot);

  TimeoutNow timeout;
  timeout.leader.leader = LeaderId{13, 2};
  timeout.leader.committed = true;
  deliver(8, timeout);

  EXPECT_EQ(node.vote(), stored);
  EXPECT_EQ(node.term(), stored.leader.term);
  EXPECT_EQ(node.role(), Role::Follower);
  EXPECT_EQ(node.log().size(), log_size);
  EXPECT_FALSE(node.snapshot_meta().has_value());
  EXPECT_FALSE(node.failed());
  EXPECT_EQ(storage->persisted_vote(), stored);
  EXPECT_EQ(bus.sent.size(), sent_before);
  for (const LogEntry& item : node.log()) {
    EXPECT_NE(item.data, "other-group");
  }

  for (int tick = 0; tick < 4; ++tick) {
    node.tick();
  }
  ASSERT_GT(bus.sent.size(), sent_before);
  for (const Envelope& message : bus.sent) {
    EXPECT_EQ(message.group, group);
    EXPECT_EQ(message.from, 1u);
  }
}

}  // namespace
}  // namespace easyraft
