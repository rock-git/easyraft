// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright 2026 easyraft contributors

#include "cluster.hpp"

#include <gtest/gtest.h>

namespace easyraft {
namespace {

TEST(ClusterPartition, NoCommitWithoutMajorityAndRecoversAfterHeal) {
  test::Cluster cluster({1, 2, 3}, test::fast_config(4), true);
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 20)) << cluster.debug();

  const Result<LogIndex> before = cluster.node(1).write("before");
  ASSERT_TRUE(before.ok());
  ASSERT_TRUE(cluster.until([&] { return cluster.machine(2).data().size() == 1; }, 20))
      << cluster.debug();

  cluster.partition({{1}, {2, 3}});
  const LogIndex commit_before = cluster.node(1).commit_index();
  const Result<LogIndex> lonely = cluster.node(1).write("lonely");
  ASSERT_TRUE(lonely.ok()) << lonely.error();
  cluster.tick_n(12);
  EXPECT_EQ(cluster.node(1).commit_index(), commit_before) << cluster.debug();
  EXPECT_TRUE(cluster.machine(1).data().size() == 1) << cluster.debug();

  // Node 1 is alone, so it remains a leader that cannot commit. Nodes 2 and 3
  // elect a new leader among themselves.
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
  const Result<LogIndex> healed_write = cluster.node(leader).write("majority");
  ASSERT_TRUE(healed_write.ok()) << cluster.debug();
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.machine(2).data().size() == 2 && cluster.machine(3).data().size() == 2;
      },
      20))
      << cluster.debug();
  EXPECT_EQ(cluster.node(1).commit_index(), commit_before) << cluster.debug();

  cluster.heal();
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.leaders().size() == 1 && cluster.machine(1).data().size() == 2 &&
               cluster.machine(2).data().size() == 2 && cluster.machine(3).data().size() == 2 &&
               !cluster.log_contains(1, "lonely");
      },
      40))
      << cluster.debug();

  const NodeId recovered = cluster.leaders().front();
  const Result<LogIndex> after = cluster.node(recovered).write("after");
  ASSERT_TRUE(after.ok()) << cluster.debug();
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.machine(1).data().size() == 3 && cluster.machine(2).data().size() == 3 &&
               cluster.machine(3).data().size() == 3;
      },
      20))
      << cluster.debug();
  for (NodeId id : {1u, 2u, 3u}) {
    EXPECT_EQ(cluster.machine(id).data().back(), "after") << cluster.debug();
    EXPECT_FALSE(cluster.log_contains(id, "lonely")) << cluster.debug();
  }
}

}  // namespace
}  // namespace easyraft
