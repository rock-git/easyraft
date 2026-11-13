// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright 2026 easyraft contributors

#include "cluster.hpp"

#include <gtest/gtest.h>

namespace easyraft {
namespace {

TEST(ClusterElection, SingleNodeBecomesLeaderAppendsAndCommits) {
  test::Cluster cluster({1}, test::fast_config(2), false);

  ASSERT_TRUE(cluster.until([&] { return cluster.leaders().size() == 1; }, 5)) << cluster.debug();
  EXPECT_EQ(cluster.leaders().front(), 1u);

  const Result<LogIndex> written = cluster.node(1).write("hello");
  ASSERT_TRUE(written.ok()) << written.error();
  EXPECT_GE(cluster.node(1).commit_index(), written.value());
  ASSERT_EQ(cluster.machine(1).data().size(), 1u);
  EXPECT_EQ(cluster.machine(1).data().front(), "hello");
  EXPECT_TRUE(cluster.storage(1).persisted_vote().committed);
  EXPECT_EQ(cluster.storage(1).persisted_log().size(), cluster.node(1).log().size());
}

TEST(ClusterElection, ThreeNodesElectExactlyOneLeader) {
  test::Cluster cluster({1, 2, 3}, test::fast_config(4), true);

  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 20)) << cluster.debug();
  EXPECT_EQ(cluster.leaders().size(), 1u);
  EXPECT_GT(cluster.node(1).term(), 0u);

  for (int i = 0; i < 10; ++i) {
    cluster.tick();
    ASSERT_TRUE(cluster.agreed_on(1)) << cluster.debug();
  }
}

TEST(ClusterElection, SimultaneousCampaignsConvergeToOneLeader) {
  // Identical timeouts: every node campaigns together. Advanced leader ids
  // break the tie, and the term still moves off the initial zero.
  test::Cluster cluster({1, 2, 3}, test::fast_config(4), false);

  const bool converged = cluster.until(
      [&] {
        const std::vector<NodeId> leaders = cluster.leaders();
        return leaders.size() == 1 && cluster.agreed_on(leaders.front());
      },
      80);
  ASSERT_TRUE(converged) << cluster.debug();

  const NodeId leader = cluster.leaders().front();
  EXPECT_GT(cluster.node(leader).term(), 0u);
  for (int i = 0; i < 15; ++i) {
    cluster.tick();
    ASSERT_EQ(cluster.leaders().size(), 1u) << cluster.debug();
    ASSERT_TRUE(cluster.agreed_on(leader)) << cluster.debug();
  }
}

TEST(ClusterElection, PreVoteDoesNotDisruptAHealthyLeader) {
  test::Cluster cluster({1, 2, 3}, test::fast_config(4), true);
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 20)) << cluster.debug();

  const Term term = cluster.node(1).term();
  cluster.isolate(3);
  cluster.tick_n(40);

  EXPECT_EQ(cluster.node(3).term(), term) << cluster.debug();
  EXPECT_EQ(cluster.node(1).term(), term) << cluster.debug();
  EXPECT_TRUE(cluster.agreed_on(1)) << cluster.debug();
  EXPECT_FALSE(cluster.storage(3).persisted_vote().leader.term > term);

  cluster.heal();
  cluster.tick_n(15);
  EXPECT_TRUE(cluster.agreed_on(1)) << cluster.debug();
  EXPECT_EQ(cluster.node(1).term(), term) << cluster.debug();
  EXPECT_EQ(cluster.node(3).current_leader(), 1u) << cluster.debug();
}

}  // namespace
}  // namespace easyraft
