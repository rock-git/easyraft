// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright 2026 easyraft contributors

#include "easyraft/easyraft.hpp"

#include <gtest/gtest.h>

#include <memory>
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

}  // namespace
}  // namespace easyraft
