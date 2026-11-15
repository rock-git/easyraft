// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright 2026 easyraft contributors

#include "easyraft/easyraft.hpp"

#include <gtest/gtest.h>

#include <memory>
#include <variant>
#include <vector>

namespace easyraft {
namespace {

LogEntry app(LogIndex index, std::string data) {
  LogEntry entry;
  entry.id = LogId{1, 1, index};
  entry.kind = EntryKind::Application;
  entry.data = std::move(data);
  return entry;
}

TEST(MemoryGroupStore, LogsVotesAndSnapshotsStayApart) {
  MemoryGroupStore store;
  const std::shared_ptr<MemoryLogStorage> first = store.open(1);
  const std::shared_ptr<MemoryLogStorage> second = store.open(2);
  EXPECT_NE(first.get(), second.get());
  EXPECT_EQ(store.open(1).get(), first.get());
  EXPECT_EQ(store.find(1), first.get());
  EXPECT_EQ(store.find(3), nullptr);
  EXPECT_TRUE(store.contains(1));
  EXPECT_FALSE(store.contains(3));
  EXPECT_EQ(store.groups(), (std::vector<GroupId>{1, 2}));

  Vote vote1;
  vote1.leader = LeaderId{4, 1};
  vote1.committed = true;
  Vote vote2;
  vote2.leader = LeaderId{7, 3};
  ASSERT_TRUE(first->save_vote(vote1).ok());
  ASSERT_TRUE(second->save_vote(vote2).ok());
  ASSERT_TRUE(first->save_membership(Membership{{1, 2}}).ok());
  ASSERT_TRUE(second->save_membership(Membership{{3}}).ok());
  ASSERT_TRUE(first->append({app(1, "group-one-payload")}).ok());
  ASSERT_TRUE(second->append({app(1, "group-two-payload")}).ok());

  Snapshot snap1;
  snap1.meta.last_included = LogId{1, 1, 1};
  snap1.meta.membership = Membership{{1, 2}};
  snap1.meta.snapshot_id = snapshot_id_from(snap1.meta.last_included);
  snap1.data = "snap-group-1";
  Snapshot snap2 = snap1;
  snap2.meta.last_included = LogId{2, 3, 1};
  snap2.meta.membership = Membership{{3}};
  snap2.meta.snapshot_id = snapshot_id_from(snap2.meta.last_included);
  snap2.data = "snap-group-2";
  ASSERT_TRUE(first->save_snapshot(snap1).ok());
  ASSERT_TRUE(second->save_snapshot(snap2).ok());

  EXPECT_EQ(first->persisted_vote(), vote1);
  EXPECT_EQ(second->persisted_vote(), vote2);
  EXPECT_EQ(first->persisted_membership().voters(), (std::vector<NodeId>{1, 2}));
  EXPECT_EQ(second->persisted_membership().voters(), (std::vector<NodeId>{3}));
  ASSERT_EQ(first->persisted_log().size(), 1u);
  ASSERT_EQ(second->persisted_log().size(), 1u);
  EXPECT_EQ(first->persisted_log().front().data, "group-one-payload");
  EXPECT_EQ(second->persisted_log().front().data, "group-two-payload");
  ASSERT_TRUE(first->load_snapshot().value().has_value());
  ASSERT_TRUE(second->load_snapshot().value().has_value());
  EXPECT_EQ(first->load_snapshot().value()->data, "snap-group-1");
  EXPECT_EQ(second->load_snapshot().value()->data, "snap-group-2");
}

Config pinned(int timeout) {
  Config config;
  config.heartbeat_interval = 1;
  config.election_timeout_min = timeout;
  config.election_timeout_max = timeout;
  config.enable_pre_vote = true;
  return config;
}

class RecordingBus final : public MessageBus {
 public:
  void send(const Envelope& message) override { sent.push_back(message); }

  std::vector<Envelope> sent;
};

bool log_has(const RaftNode& node, const std::string& data) {
  for (const LogEntry& entry : node.log()) {
    if (entry.kind == EntryKind::Application && entry.data == data) {
      return true;
    }
  }
  return false;
}

TEST(RaftGroups, TwoGroupsOnOneNodeCommitApart) {
  MemoryGroupStore store;
  const std::shared_ptr<MemoryLogStorage> first = store.open(10);
  const std::shared_ptr<MemoryLogStorage> second = store.open(20);
  auto alpha = std::make_shared<MemoryStateMachine>();
  auto beta = std::make_shared<MemoryStateMachine>();
  auto extra_machine = std::make_shared<MemoryStateMachine>();
  RaftGroups host(1, nullptr);

  ASSERT_TRUE(host.add(10, pinned(2), first, alpha).ok());
  ASSERT_TRUE(host.add(20, pinned(2), second, beta).ok());
  EXPECT_EQ(host.add(10, pinned(2), store.open(30), extra_machine).error(), Error::AlreadyInitialized);
  EXPECT_EQ(host.add(30, pinned(2), first, extra_machine).error(), Error::Rejected);
  EXPECT_EQ(host.add(30, pinned(2), store.open(30), alpha).error(), Error::Rejected);
  EXPECT_EQ(host.add(30, pinned(2), nullptr, extra_machine).error(), Error::Rejected);
  EXPECT_EQ(host.add(30, pinned(2), store.open(30), nullptr).error(), Error::Rejected);
  EXPECT_EQ(host.id(), 1u);
  EXPECT_EQ(host.groups(), (std::vector<GroupId>{10, 20}));
  EXPECT_EQ(host.find(30), nullptr);

  ASSERT_TRUE(host.find(10)->initialize(Membership{{1}}).ok());
  ASSERT_TRUE(host.find(20)->initialize(Membership{{1}}).ok());
  EXPECT_EQ(host.find(10)->group(), 10u);
  EXPECT_EQ(host.find(20)->group(), 20u);

  for (int tick = 0; tick < 4; ++tick) {
    host.tick();
  }
  ASSERT_EQ(host.find(10)->role(), Role::Leader);
  ASSERT_EQ(host.find(20)->role(), Role::Leader);

  const Result<LogIndex> wrote_alpha = host.find(10)->write("alpha");
  const Result<LogIndex> wrote_beta = host.find(20)->write("beta");
  ASSERT_TRUE(wrote_alpha.ok()) << wrote_alpha.error();
  ASSERT_TRUE(wrote_beta.ok()) << wrote_beta.error();
  EXPECT_GE(host.find(10)->commit_index(), wrote_alpha.value());
  EXPECT_GE(host.find(20)->commit_index(), wrote_beta.value());
  ASSERT_EQ(alpha->data().size(), 1u);
  ASSERT_EQ(beta->data().size(), 1u);
  EXPECT_EQ(alpha->data().front(), "alpha");
  EXPECT_EQ(beta->data().front(), "beta");
  EXPECT_TRUE(log_has(*host.find(10), "alpha"));
  EXPECT_FALSE(log_has(*host.find(10), "beta"));
  EXPECT_TRUE(log_has(*host.find(20), "beta"));
  EXPECT_FALSE(log_has(*host.find(20), "alpha"));
  EXPECT_EQ(first->persisted_vote().leader.node, 1u);
  EXPECT_EQ(second->persisted_vote().leader.node, 1u);
  ASSERT_FALSE(first->persisted_log().empty());
  ASSERT_FALSE(second->persisted_log().empty());
  EXPECT_EQ(first->persisted_log().back().data, "alpha");
  EXPECT_EQ(second->persisted_log().back().data, "beta");
}

TEST(RaftGroups, RoutesAMessageToItsGroupOnly) {
  RecordingBus bus;
  MemoryGroupStore store;
  RaftGroups host(1, &bus);
  ASSERT_TRUE(host.add(10, pinned(8), store.open(10), std::make_shared<MemoryStateMachine>()).ok());
  ASSERT_TRUE(host.add(20, pinned(8), store.open(20), std::make_shared<MemoryStateMachine>()).ok());
  ASSERT_TRUE(host.find(10)->initialize(Membership{{1, 2}}).ok());
  ASSERT_TRUE(host.find(20)->initialize(Membership{{1, 2}}).ok());

  RequestVote vote;
  vote.body.vote.leader = LeaderId{3, 2};
  Envelope message;
  message.from = 2;
  message.to = 1;
  message.group = 10;
  message.payload = vote;
  host.on_message(message);
  EXPECT_EQ(host.find(10)->term(), 3u);
  EXPECT_EQ(host.find(20)->term(), 0u);
  ASSERT_EQ(bus.sent.size(), 1u);
  EXPECT_EQ(bus.sent[0].group, 10u);
  EXPECT_EQ(bus.sent[0].to, 2u);
  EXPECT_TRUE(std::holds_alternative<RequestVoteResponse>(bus.sent[0].payload));

  vote.body.vote.leader = LeaderId{5, 2};
  message.group = 20;
  message.payload = vote;
  host.on_message(message);
  EXPECT_EQ(host.find(10)->term(), 3u);
  EXPECT_EQ(host.find(20)->term(), 5u);
  ASSERT_EQ(bus.sent.size(), 2u);
  EXPECT_EQ(bus.sent[1].group, 20u);

  const Vote left = host.find(10)->vote();
  const Vote right = host.find(20)->vote();
  vote.body.vote.leader = LeaderId{9, 2};
  message.group = 99;
  message.payload = vote;
  host.on_message(message);
  message.to = 4;
  message.group = 10;
  host.on_message(message);

  AppendEntriesRequest append;
  append.vote.leader = LeaderId{11, 2};
  append.vote.committed = true;
  append.leader_commit = 1;
  LogEntry foreign;
  foreign.id = LogId{11, 2, 1};
  foreign.kind = EntryKind::Application;
  foreign.data = "other-group";
  append.entries.push_back(foreign);
  message.to = 1;
  message.group = 20;
  message.payload = append;
  host.find(10)->on_message(message);

  EXPECT_EQ(host.find(10)->vote(), left);
  EXPECT_EQ(host.find(20)->vote(), right);
  EXPECT_FALSE(log_has(*host.find(10), "other-group"));
  EXPECT_FALSE(log_has(*host.find(20), "other-group"));
  EXPECT_EQ(bus.sent.size(), 2u);
}

}  // namespace
}  // namespace easyraft
