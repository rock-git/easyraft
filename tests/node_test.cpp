// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright 2026 easyraft contributors

#include "easyraft/easyraft.hpp"

#include <gtest/gtest.h>

#include <memory>
#include <stdexcept>
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

  Result<void> truncate_from(LogIndex index) override { return inner_.truncate_from(index); }
  Result<std::vector<LogEntry>> read(LogIndex from, LogIndex to) override { return inner_.read(from, to); }

  const std::vector<LogEntry>& persisted_log() const { return inner_.persisted_log(); }

 private:
  MemoryLogStorage inner_;
  int succeed_;
  int appends_{0};
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

}  // namespace
}  // namespace easyraft
