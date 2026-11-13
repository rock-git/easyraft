// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright 2026 easyraft contributors

#include "cluster.hpp"

#include <gtest/gtest.h>

namespace easyraft {
namespace {

TEST(ClusterReplication, CommitsOnlyAfterMajorityThenCatchesUp) {
  test::Cluster cluster({1, 2, 3}, test::fast_config(4), true);
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 20)) << cluster.debug();

  const LogIndex committed = cluster.node(1).commit_index();
  cluster.drop_append_responses(true);
  const Result<LogIndex> hidden = cluster.node(1).write("hidden");
  ASSERT_TRUE(hidden.ok()) << hidden.error();
  cluster.tick_n(8);

  EXPECT_EQ(cluster.node(1).commit_index(), committed) << cluster.debug();
  for (NodeId id : {1u, 2u, 3u}) {
    EXPECT_TRUE(cluster.machine(id).data().empty()) << cluster.debug();
  }

  cluster.drop_append_responses(false);
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.machine(1).data().size() == 1 && cluster.machine(2).data().size() == 1 &&
               cluster.machine(3).data().size() == 1;
      },
      20))
      << cluster.debug();
  EXPECT_EQ(cluster.machine(1).data().front(), "hidden");

  cluster.isolate(3);
  const Result<LogIndex> majority = cluster.node(1).write("majority");
  ASSERT_TRUE(majority.ok()) << majority.error();
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.node(1).commit_index() >= majority.value() &&
               cluster.machine(1).data().size() == 2 && cluster.machine(2).data().size() == 2;
      },
      20))
      << cluster.debug();
  EXPECT_EQ(cluster.machine(3).data().size(), 1u) << cluster.debug();

  cluster.heal();
  ASSERT_TRUE(cluster.until([&] { return cluster.machine(3).data().size() == 2; }, 20))
      << cluster.debug();
  EXPECT_EQ(cluster.machine(3).data().back(), "majority");
}

TEST(ClusterReplication, FollowerConflictIsTruncatedAndOverwritten) {
  test::Cluster cluster({1, 2, 3}, test::fast_config(4), true);
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 20)) << cluster.debug();

  const Result<LogIndex> first = cluster.node(1).write("a");
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.machine(1).data().size() == 1 && cluster.machine(2).data().size() == 1 &&
               cluster.machine(3).data().size() == 1;
      },
      20))
      << cluster.debug();

  cluster.partition({{1}, {2, 3}});
  const Result<LogIndex> diverged = cluster.node(1).write("diverge");
  ASSERT_TRUE(diverged.ok()) << diverged.error();
  cluster.tick_n(3);
  EXPECT_LT(cluster.node(1).commit_index(), diverged.value()) << cluster.debug();
  EXPECT_TRUE(cluster.log_contains(1, "diverge"));

  // The partitioned former leader keeps its role until it observes a higher
  // vote. The connected majority must still converge on one leader.
  ASSERT_TRUE(cluster.until(
      [&] {
        const bool node2_leads = cluster.node(2).role() == Role::Leader &&
                                 cluster.node(3).current_leader() == 2;
        const bool node3_leads = cluster.node(3).role() == Role::Leader &&
                                 cluster.node(2).current_leader() == 3;
        return node2_leads || node3_leads;
      },
      40))
      << cluster.debug();
  const NodeId leader = cluster.node(2).role() == Role::Leader ? 2 : 3;
  const Result<LogIndex> repaired = cluster.node(leader).write("right");
  ASSERT_TRUE(repaired.ok()) << cluster.debug();
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.machine(2).data().size() == 2 && cluster.machine(3).data().size() == 2;
      },
      20))
      << cluster.debug();

  cluster.heal();
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.agreed_on(leader) && !cluster.log_contains(1, "diverge") &&
               cluster.log_contains(1, "a") && cluster.log_contains(1, "right") &&
               cluster.machine(1).data().size() == 2;
      },
      40))
      << cluster.debug();

  for (NodeId id : {1u, 2u, 3u}) {
    EXPECT_FALSE(cluster.log_contains(id, "diverge")) << cluster.debug();
    ASSERT_EQ(cluster.machine(id).data().size(), 2u) << cluster.debug();
    EXPECT_EQ(cluster.machine(id).data()[0], "a");
    EXPECT_EQ(cluster.machine(id).data()[1], "right");
  }
}

}  // namespace
}  // namespace easyraft
