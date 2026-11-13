// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright 2026 easyraft contributors

#include "cluster.hpp"

#include <gtest/gtest.h>

namespace easyraft {
namespace {

TEST(ClusterPersistence, RestartRecoversVoteAndLogAndDropsUncommitted) {
  test::Cluster cluster({1, 2, 3}, test::fast_config(4), true);
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 20)) << cluster.debug();

  const Result<LogIndex> committed = cluster.node(1).write("kept");
  ASSERT_TRUE(committed.ok());
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.machine(1).data().size() == 1 && cluster.machine(2).data().size() == 1 &&
               cluster.machine(3).data().size() == 1;
      },
      20))
      << cluster.debug();

  cluster.partition({{1}, {2, 3}});
  const Result<LogIndex> uncommitted = cluster.node(1).write("transient");
  ASSERT_TRUE(uncommitted.ok()) << uncommitted.error();
  const Term term_before_restart = cluster.node(1).term();
  const std::size_t log_size = cluster.storage(1).persisted_log().size();
  ASSERT_TRUE(cluster.log_contains(1, "transient"));
  EXPECT_EQ(cluster.storage(1).persisted_vote().leader.node, 1u);
  EXPECT_TRUE(cluster.storage(1).persisted_vote().committed);

  cluster.restart(1);

  EXPECT_EQ(cluster.node(1).role(), Role::Follower) << cluster.debug();
  EXPECT_EQ(cluster.node(1).term(), term_before_restart) << cluster.debug();
  EXPECT_FALSE(cluster.node(1).vote().committed);
  EXPECT_EQ(cluster.storage(1).persisted_vote().leader.term, term_before_restart);
  EXPECT_EQ(cluster.storage(1).persisted_log().size(), log_size);
  EXPECT_TRUE(cluster.log_contains(1, "kept"));
  EXPECT_TRUE(cluster.log_contains(1, "transient"));
  EXPECT_TRUE(cluster.machine(1).data().empty());

  ASSERT_TRUE(cluster.until(
      [&] {
        const std::vector<NodeId> leaders = cluster.leaders();
        if (leaders.size() != 1 || leaders.front() == 1) {
          return false;
        }
        return cluster.node(2).term() == cluster.node(3).term() &&
               cluster.node(2).current_leader() == leaders.front() &&
               cluster.node(3).current_leader() == leaders.front();
      },
      40))
      << cluster.debug();

  const NodeId leader = cluster.leaders().front();
  const Result<LogIndex> replacement = cluster.node(leader).write("replacement");
  ASSERT_TRUE(replacement.ok()) << cluster.debug();
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.machine(2).data().size() == 2 && cluster.machine(3).data().size() == 2;
      },
      20))
      << cluster.debug();
  EXPECT_FALSE(cluster.log_contains(2, "transient"));
  EXPECT_FALSE(cluster.log_contains(3, "transient"));

  cluster.heal();
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.agreed_on(leader) && !cluster.log_contains(1, "transient") &&
               cluster.log_contains(1, "kept") && cluster.log_contains(1, "replacement") &&
               cluster.machine(1).data().size() == 2;
      },
      40))
      << cluster.debug();

  for (NodeId id : {2u, 3u}) {
    ASSERT_EQ(cluster.machine(id).data().size(), 2u) << cluster.debug();
    EXPECT_EQ(cluster.machine(id).data()[0], "kept");
    EXPECT_EQ(cluster.machine(id).data()[1], "replacement");
    EXPECT_FALSE(cluster.log_contains(id, "transient"));
  }
  EXPECT_EQ(cluster.machine(1).data()[0], "kept");
  EXPECT_EQ(cluster.machine(1).data()[1], "replacement");
}

}  // namespace
}  // namespace easyraft
