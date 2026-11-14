// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright 2026 easyraft contributors

#include "cluster.hpp"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

namespace easyraft {
namespace {

std::vector<Membership> membership_log(const RaftNode& node) {
  std::vector<Membership> found;
  for (const LogEntry& entry : node.log()) {
    if (entry.kind != EntryKind::Membership) {
      continue;
    }
    const std::optional<Membership> decoded = Membership::decode(entry.data);
    EXPECT_TRUE(decoded.has_value());
    if (decoded) {
      found.push_back(*decoded);
    }
  }
  return found;
}

bool committed_learners(const test::Cluster& cluster,
                        NodeId id,
                        const std::vector<NodeId>& voters,
                        const std::vector<NodeId>& learners) {
  const RaftNode& node = cluster.node(id);
  if (node.membership().is_joint() || node.membership().voters() != voters ||
      node.membership().learners() != learners) {
    return false;
  }
  LogIndex index = 0;
  for (const LogEntry& entry : node.log()) {
    if (entry.kind == EntryKind::Membership) {
      index = entry.id.index;
    }
  }
  if (index == 0 || node.commit_index() < index) {
    return false;
  }
  for (const LogEntry& entry : node.log()) {
    if (entry.id.index != index) {
      continue;
    }
    const std::optional<Membership> decoded = Membership::decode(entry.data);
    return decoded && !decoded->is_joint() && decoded->voters() == voters &&
           decoded->learners() == learners;
  }
  return false;
}

TEST(Learner, InitializeAcceptsALearnerAndRejectsOverlap) {
  auto storage = std::make_shared<MemoryLogStorage>();
  auto machine = std::make_shared<MemoryStateMachine>();
  RaftNode node(4, test::fast_config(4), storage, machine, nullptr);
  EXPECT_EQ(node.initialize(Membership{{1, 2}, {2}}).error(), Error::InvalidMembership);
  EXPECT_EQ(node.initialize(Membership{{1, 2}, {0}}).error(), Error::InvalidMembership);
  EXPECT_EQ(node.initialize(Membership{{0, 1}, {2}}).error(), Error::InvalidMembership);
  EXPECT_EQ(node.initialize(Membership::joint_of({1, 2}, {1, 2, 4})).error(), Error::InvalidMembership);
  EXPECT_EQ(node.add_learner(4).error(), Error::NotInitialized);

  ASSERT_TRUE(node.initialize(Membership{{1, 2, 3}, {4}}).ok());
  EXPECT_TRUE(node.membership().is_learner(4));
  EXPECT_FALSE(node.membership().contains(4));
  EXPECT_EQ(node.membership().quorum(), 2u);
  for (int tick = 0; tick < 8; ++tick) {
    node.tick();
  }
  EXPECT_EQ(node.role(), Role::Follower);
  EXPECT_EQ(node.term(), 0u);
}

TEST(Learner, AddAndRemoveReplicatesCommittedEntries) {
  test::Cluster cluster({1, 2, 3}, test::fast_config(4), true);
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 30)) << cluster.debug();
  ASSERT_TRUE(cluster.node(1).write("before").ok());
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.machine(2).data().size() == 1 && cluster.machine(3).data().size() == 1;
      },
      20))
      << cluster.debug();

  cluster.add_node(4);
  const Result<LogIndex> added = cluster.node(1).add_learner(4);
  ASSERT_TRUE(added.ok()) << added.error();
  const std::vector<NodeId> voters{1, 2, 3};
  const std::vector<NodeId> learners{4};
  ASSERT_TRUE(cluster.until(
      [&] {
        return committed_learners(cluster, 1, voters, learners) &&
               committed_learners(cluster, 4, voters, learners) &&
               cluster.machine(4).data().size() == 1 && cluster.machine(4).data().front() == "before";
      },
      40))
      << cluster.debug();
  EXPECT_EQ(cluster.node(1).membership().quorum(), 2u);
  EXPECT_EQ(cluster.node(4).role(), Role::Follower);
  const std::vector<Membership> added_log = membership_log(cluster.node(1));
  ASSERT_EQ(added_log.size(), 1u);
  EXPECT_FALSE(added_log.front().is_joint());
  EXPECT_EQ(added_log.front().learners(), learners);
  EXPECT_TRUE(added_log.front().has_prior_learners());
  EXPECT_TRUE(added_log.front().prior_learners().empty());

  ASSERT_TRUE(cluster.node(1).write("after").ok());
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.machine(4).data().size() == 2 && cluster.machine(4).data().back() == "after";
      },
      30))
      << cluster.debug();

  const Term learner_term = cluster.node(4).term();
  cluster.isolate(4);
  cluster.tick_n(50);
  EXPECT_EQ(cluster.node(4).role(), Role::Follower);
  EXPECT_NE(cluster.node(4).role(), Role::Candidate);
  EXPECT_EQ(cluster.node(4).term(), learner_term) << cluster.debug();
  cluster.heal();

  const Result<LogIndex> removed = cluster.node(1).remove_learner(4);
  ASSERT_TRUE(removed.ok()) << removed.error();
  ASSERT_TRUE(cluster.until(
      [&] {
        return committed_learners(cluster, 1, voters, {}) &&
               cluster.node(1).commit_index() >= removed.value();
      },
      30))
      << cluster.debug();
  EXPECT_FALSE(cluster.node(1).membership().is_learner(4));

  const Result<LogIndex> gone = cluster.node(1).write("gone");
  ASSERT_TRUE(gone.ok()) << gone.error();
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.machine(2).data().size() == 3 && cluster.machine(3).data().size() == 3 &&
               cluster.machine(2).data().back() == "gone";
      },
      30))
      << cluster.debug();
  cluster.tick_n(10);
  ASSERT_EQ(cluster.machine(4).data().size(), 2u) << cluster.debug();
  EXPECT_EQ(cluster.machine(4).data().back(), "after");
}

TEST(Learner, MajorityStaysTwoAndALearnerCannotBeElected) {
  test::Cluster cluster({1, 2, 3}, test::fast_config(4), true);
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 30)) << cluster.debug();
  cluster.add_node(4);
  ASSERT_TRUE(cluster.node(1).add_learner(4).ok()) << cluster.debug();
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.node(1).membership().learners() == std::vector<NodeId>{4} &&
               cluster.node(4).membership().is_learner(4);
      },
      40))
      << cluster.debug();
  EXPECT_EQ(cluster.node(1).membership().voters(), (std::vector<NodeId>{1, 2, 3}));
  EXPECT_EQ(cluster.node(1).membership().quorum(), 2u);

  // One other voter is a majority. The learner's vote is not required, and
  // counting it would make this a 4-node quorum that {1, 2} cannot satisfy.
  cluster.partition({{1, 2}, {3}, {4}});
  const Result<LogIndex> committed = cluster.node(1).write("pair");
  ASSERT_TRUE(committed.ok()) << committed.error();
  ASSERT_TRUE(cluster.until([&] { return cluster.node(1).commit_index() >= committed.value(); }, 20))
      << cluster.debug();
  EXPECT_EQ(cluster.machine(2).data().size(), 1u);

  // The leader plus the learner is not a majority of the voters.
  cluster.partition({{1, 4}, {2}, {3}});
  const Result<LogIndex> blocked = cluster.node(1).write("learner-only");
  ASSERT_TRUE(blocked.ok()) << blocked.error();
  cluster.tick_n(10);
  EXPECT_LT(cluster.node(1).commit_index(), blocked.value()) << cluster.debug();

  const Term learner_term = cluster.node(4).term();
  cluster.stop(1);
  cluster.stop(2);
  cluster.stop(3);
  cluster.tick_n(100);
  EXPECT_EQ(cluster.node(4).role(), Role::Follower) << cluster.debug();
  EXPECT_EQ(cluster.node(4).term(), learner_term);
  EXPECT_TRUE(cluster.leaders().empty());
}

TEST(Learner, NewLearnerCatchesUpFromASnapshot) {
  Config config = test::fast_config(4);
  config.max_snapshot_chunk = 4;
  test::Cluster cluster({1, 2, 3}, config, true);
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 30)) << cluster.debug();
  ASSERT_TRUE(cluster.node(1).write("alpha").ok());
  ASSERT_TRUE(cluster.node(1).write("beta").ok());
  ASSERT_TRUE(cluster.until(
      [&] { return cluster.machine(2).data().size() == 2 && cluster.machine(3).data().size() == 2; },
      20))
      << cluster.debug();
  ASSERT_TRUE(cluster.node(1).trigger_snapshot().ok()) << cluster.debug();
  EXPECT_GT(cluster.node(1).first_available_index(), 1u);
  EXPECT_FALSE(cluster.log_contains(1, "alpha"));

  cluster.add_node(4);
  ASSERT_TRUE(cluster.node(1).add_learner(4).ok()) << cluster.debug();
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.node(4).snapshot_meta().has_value() && cluster.machine(4).data().size() == 2 &&
               cluster.node(4).membership().is_learner(4) &&
               cluster.node(4).membership().voters() == std::vector<NodeId>{1, 2, 3};
      },
      80))
      << cluster.debug();
  EXPECT_EQ(cluster.machine(4).data()[0], "alpha");
  EXPECT_EQ(cluster.machine(4).data()[1], "beta");
  EXPECT_FALSE(cluster.log_contains(4, "alpha"));
  EXPECT_FALSE(cluster.log_contains(4, "beta"));
  EXPECT_GT(cluster.node(4).first_available_index(), 1u);
  EXPECT_EQ(cluster.node(1).membership().quorum(), 2u);
  EXPECT_EQ(cluster.node(4).role(), Role::Follower);
}

TEST(Learner, PromoteCaughtUpLearnerThroughJointConsensus) {
  test::Cluster cluster({1, 2, 3}, test::fast_config(4), true);
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 30)) << cluster.debug();
  ASSERT_TRUE(cluster.node(1).write("kept").ok());
  ASSERT_TRUE(cluster.until([&] { return cluster.machine(2).data().size() == 1; }, 20))
      << cluster.debug();
  cluster.add_node(4);
  ASSERT_TRUE(cluster.node(1).add_learner(4).ok());
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.machine(4).data().size() == 1 && cluster.node(4).membership().is_learner(4);
      },
      40))
      << cluster.debug();

  const Result<LogIndex> changed = cluster.node(1).change_membership({1, 2, 3, 4});
  ASSERT_TRUE(changed.ok()) << changed.error();
  const std::vector<NodeId> four{1, 2, 3, 4};
  ASSERT_TRUE(cluster.until(
      [&] {
        return committed_learners(cluster, 1, four, {}) && committed_learners(cluster, 4, four, {});
      },
      80))
      << cluster.debug();
  EXPECT_FALSE(cluster.node(4).membership().is_learner(4));
  EXPECT_TRUE(cluster.node(4).membership().contains(4));
  EXPECT_EQ(cluster.node(1).membership().quorum(), 3u);

  const std::vector<Membership> entries = membership_log(cluster.node(1));
  ASSERT_GE(entries.size(), 2u);
  const Membership& joint = entries[entries.size() - 2];
  const Membership& uniform = entries.back();
  EXPECT_TRUE(joint.is_joint());
  EXPECT_EQ(joint.configs()[0], (std::vector<NodeId>{1, 2, 3}));
  EXPECT_EQ(joint.configs()[1], four);
  EXPECT_TRUE(joint.learners().empty());
  EXPECT_EQ(joint.prior_learners(), (std::vector<NodeId>{4}));
  EXPECT_FALSE(uniform.is_joint());
  EXPECT_EQ(uniform.voters(), four);
  EXPECT_TRUE(uniform.learners().empty());

  cluster.partition({{1, 2}, {3}, {4}});
  const Result<LogIndex> blocked = cluster.node(1).write("needs-four");
  ASSERT_TRUE(blocked.ok()) << blocked.error();
  cluster.tick_n(8);
  EXPECT_LT(cluster.node(1).commit_index(), blocked.value()) << cluster.debug();

  cluster.partition({{1, 2, 4}, {3}});
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.node(1).commit_index() >= blocked.value() &&
               cluster.machine(4).data().size() == 2 && cluster.machine(4).data().back() == "needs-four";
      },
      40))
      << cluster.debug();
}

TEST(Learner, VoterChangePreservesUnrelatedLearners) {
  test::Cluster cluster({1, 2, 3}, test::fast_config(4), true);
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 30)) << cluster.debug();
  cluster.add_node(4);
  ASSERT_TRUE(cluster.node(1).add_learner(4).ok());
  ASSERT_TRUE(cluster.until([&] { return cluster.node(4).membership().is_learner(4); }, 40))
      << cluster.debug();
  cluster.add_node(5);
  ASSERT_TRUE(cluster.node(1).change_membership({1, 2, 3, 5}).ok()) << cluster.debug();
  const std::vector<NodeId> voters{1, 2, 3, 5};
  const std::vector<NodeId> learners{4};
  ASSERT_TRUE(cluster.until(
      [&] {
        return committed_learners(cluster, 1, voters, learners) &&
               committed_learners(cluster, 5, voters, learners) &&
               cluster.node(4).membership().is_learner(4);
      },
      80))
      << cluster.debug();
  EXPECT_EQ(cluster.node(1).membership().quorum(), 3u);
  const std::vector<Membership> entries = membership_log(cluster.node(1));
  ASSERT_GE(entries.size(), 2u);
  EXPECT_EQ(entries[entries.size() - 2].learners(), learners);
  EXPECT_EQ(entries.back().learners(), learners);
}

TEST(Learner, DemoteVoterKeepsReplicationAndDropsItsVote) {
  test::Cluster cluster({1, 2, 3}, test::fast_config(4), true);
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 30)) << cluster.debug();
  const Result<LogIndex> demoted = cluster.node(1).demote_voter(3);
  ASSERT_TRUE(demoted.ok()) << demoted.error();
  const std::vector<NodeId> voters{1, 2};
  const std::vector<NodeId> learners{3};
  ASSERT_TRUE(cluster.until(
      [&] {
        return committed_learners(cluster, 1, voters, learners) &&
               committed_learners(cluster, 3, voters, learners);
      },
      40))
      << cluster.debug();
  EXPECT_EQ(cluster.node(1).membership().quorum(), 2u);
  EXPECT_EQ(cluster.node(3).role(), Role::Follower);
  EXPECT_FALSE(cluster.node(3).membership().contains(3));

  const std::vector<Membership> entries = membership_log(cluster.node(1));
  ASSERT_EQ(entries.size(), 2u);
  EXPECT_TRUE(entries[0].is_joint());
  EXPECT_EQ(entries[0].configs()[0], (std::vector<NodeId>{1, 2, 3}));
  EXPECT_EQ(entries[0].configs()[1], voters);
  EXPECT_TRUE(entries[0].learners().empty());
  EXPECT_TRUE(entries[0].has_uniform_learner_override());
  EXPECT_EQ(entries[0].learners_after_joint(), learners);
  EXPECT_FALSE(entries[1].is_joint());
  EXPECT_EQ(entries[1].learners(), learners);

  ASSERT_TRUE(cluster.node(1).write("still-there").ok());
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.machine(3).data().size() == 1 && cluster.machine(3).data().front() == "still-there";
      },
      30))
      << cluster.debug();

  // {1, 3} is not a voter majority after the demotion. It would be if 3 still voted.
  cluster.partition({{1, 3}, {2}});
  const Result<LogIndex> blocked = cluster.node(1).write("not-a-quorum");
  ASSERT_TRUE(blocked.ok()) << blocked.error();
  cluster.tick_n(4);
  EXPECT_LT(cluster.node(1).commit_index(), blocked.value()) << cluster.debug();

  cluster.partition({{1, 2}, {3}});
  ASSERT_TRUE(cluster.until([&] { return cluster.node(1).commit_index() >= blocked.value(); }, 20))
      << cluster.debug();

  const Term learner_term = cluster.node(3).term();
  cluster.isolate(3);
  cluster.tick_n(40);
  EXPECT_EQ(cluster.node(3).role(), Role::Follower) << cluster.debug();
  EXPECT_EQ(cluster.node(3).term(), learner_term);
}

TEST(Learner, RejectsIllegalMembershipChanges) {
  test::Cluster cluster({1, 2, 3}, test::fast_config(4), true);
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 30)) << cluster.debug();

  EXPECT_EQ(cluster.node(2).add_learner(4).error(), Error::NotLeader);
  EXPECT_EQ(cluster.node(2).remove_learner(4).error(), Error::NotLeader);
  EXPECT_EQ(cluster.node(2).demote_voter(3).error(), Error::NotLeader);
  EXPECT_EQ(cluster.node(1).add_learner(0).error(), Error::InvalidMembership);
  EXPECT_EQ(cluster.node(1).add_learner(1).error(), Error::InvalidMembership);
  EXPECT_EQ(cluster.node(1).add_learner(2).error(), Error::InvalidMembership);
  EXPECT_EQ(cluster.node(1).remove_learner(1).error(), Error::InvalidMembership);
  EXPECT_EQ(cluster.node(1).remove_learner(0).error(), Error::InvalidMembership);
  EXPECT_EQ(cluster.node(1).remove_learner(9).error(), Error::InvalidMembership);
  EXPECT_EQ(cluster.node(1).demote_voter(0).error(), Error::InvalidMembership);
  EXPECT_EQ(cluster.node(1).demote_voter(9).error(), Error::InvalidMembership);
  EXPECT_EQ(cluster.node(1).change_membership({}).error(), Error::InvalidMembership);
  EXPECT_EQ(cluster.node(1).change_membership({1, 0}).error(), Error::InvalidMembership);

  cluster.add_node(4);
  ASSERT_TRUE(cluster.node(1).add_learner(4).ok());
  EXPECT_EQ(cluster.node(1).add_learner(4).error(), Error::InvalidMembership);
  EXPECT_EQ(cluster.node(1).demote_voter(4).error(), Error::InvalidMembership);
  ASSERT_TRUE(cluster.until(
      [&] { return committed_learners(cluster, 1, {1, 2, 3}, {4}); }, 30))
      << cluster.debug();

  cluster.drop_append_responses(true);
  const Result<LogIndex> started = cluster.node(1).add_learner(5);
  ASSERT_TRUE(started.ok()) << started.error();
  EXPECT_FALSE(cluster.node(1).membership().is_joint());
  EXPECT_EQ(cluster.node(1).add_learner(6).error(), Error::Rejected);
  EXPECT_EQ(cluster.node(1).remove_learner(4).error(), Error::Rejected);
  EXPECT_EQ(cluster.node(1).remove_learner(5).error(), Error::Rejected);
  EXPECT_EQ(cluster.node(1).demote_voter(3).error(), Error::Rejected);
  EXPECT_EQ(cluster.node(1).change_membership({1, 2}).error(), Error::Rejected);

  test::Cluster alone({1}, test::fast_config(2), false);
  ASSERT_TRUE(alone.until([&] { return alone.node(1).role() == Role::Leader; }, 5)) << alone.debug();
  EXPECT_EQ(alone.node(1).demote_voter(1).error(), Error::InvalidMembership);
  ASSERT_TRUE(alone.node(1).add_learner(2).ok());
  EXPECT_EQ(alone.node(1).membership().learners(), (std::vector<NodeId>{2}));
  EXPECT_EQ(alone.node(1).membership().quorum(), 1u);
  const std::vector<Membership> alone_log = membership_log(alone.node(1));
  ASSERT_EQ(alone_log.size(), 1u);
  EXPECT_FALSE(alone_log.front().is_joint());
}

enum class UncommittedLearnerChange {
  Add,
  Remove,
  Promote,
};

void roll_back_uncommitted_learners(test::StorageMode mode, UncommittedLearnerChange change) {
  // Learner 4 is part of the initial membership, so it is not a log entry.
  // The uncommitted change is the first membership entry, and only its
  // recorded prior learner set can restore 4 after a restart and a conflict.
  test::Cluster cluster({1, 2, 3}, test::fast_config(4), true, mode, {4});
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 30)) << cluster.debug();
  EXPECT_EQ(cluster.node(1).membership().learners(), (std::vector<NodeId>{4}));
  EXPECT_EQ(cluster.node(1).membership().voters(), (std::vector<NodeId>{1, 2, 3}));
  EXPECT_FALSE(cluster.node(1).snapshot_meta().has_value());
  EXPECT_TRUE(membership_log(cluster.node(1)).empty());

  cluster.isolate(1);
  if (change == UncommittedLearnerChange::Add) {
    ASSERT_TRUE(cluster.node(1).add_learner(5).ok()) << cluster.debug();
    EXPECT_EQ(cluster.node(1).membership().learners(), (std::vector<NodeId>{4, 5}));
  } else if (change == UncommittedLearnerChange::Remove) {
    ASSERT_TRUE(cluster.node(1).remove_learner(4).ok()) << cluster.debug();
    EXPECT_TRUE(cluster.node(1).membership().learners().empty());
  } else {
    ASSERT_TRUE(cluster.node(1).change_membership({1, 2, 3, 4}).ok()) << cluster.debug();
    EXPECT_TRUE(cluster.node(1).membership().is_joint());
    EXPECT_TRUE(cluster.node(1).membership().learners().empty());
  }
  EXPECT_GT(cluster.node(1).last_log_index(), cluster.node(1).commit_index());

  cluster.restart(1);
  EXPECT_FALSE(cluster.node(1).snapshot_meta().has_value());
  cluster.stop(1);

  ASSERT_TRUE(cluster.until(
      [&] {
        const std::vector<NodeId> leaders = cluster.leaders();
        return leaders.size() == 1 && leaders.front() != 1;
      },
      40))
      << cluster.debug();
  const NodeId leader = cluster.leaders().front();
  ASSERT_TRUE(cluster.node(leader).write("conflict").ok()) << cluster.debug();
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.machine(2).data().size() == 1 && cluster.machine(2).data().front() == "conflict" &&
               cluster.machine(3).data().size() == 1 && cluster.machine(3).data().front() == "conflict";
      },
      30))
      << cluster.debug();

  cluster.heal();
  cluster.start(1);
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.node(1).membership().voters() == std::vector<NodeId>{1, 2, 3} &&
               cluster.node(1).membership().learners() == std::vector<NodeId>{4} &&
               !cluster.node(1).membership().is_joint() && cluster.log_contains(1, "conflict");
      },
      80))
      << cluster.debug();
  EXPECT_FALSE(cluster.node(1).snapshot_meta().has_value());
  EXPECT_EQ(cluster.node(1).membership().quorum(), 2u);
}

class LearnerRestart : public ::testing::TestWithParam<test::StorageMode> {};

TEST_P(LearnerRestart, UncommittedAddRollsBackAfterRestart) {
  roll_back_uncommitted_learners(GetParam(), UncommittedLearnerChange::Add);
}

TEST_P(LearnerRestart, UncommittedRemovalRollsBackAfterRestart) {
  roll_back_uncommitted_learners(GetParam(), UncommittedLearnerChange::Remove);
}

TEST_P(LearnerRestart, UncommittedPromotionRollsBackAfterRestart) {
  roll_back_uncommitted_learners(GetParam(), UncommittedLearnerChange::Promote);
}

TEST_P(LearnerRestart, RestoresLearnersAfterCompaction) {
  test::Cluster cluster({1, 2, 3}, test::fast_config(4), true, GetParam());
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 30)) << cluster.debug();
  ASSERT_TRUE(cluster.node(1).write("kept").ok());
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.machine(1).data().size() == 1 && cluster.machine(2).data().size() == 1 &&
               cluster.machine(3).data().size() == 1;
      },
      20))
      << cluster.debug();
  cluster.add_node(4);
  ASSERT_TRUE(cluster.node(1).add_learner(4).ok());
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.machine(4).data().size() == 1 && cluster.node(4).membership().is_learner(4);
      },
      40))
      << cluster.debug();
  ASSERT_TRUE(cluster.node(1).write("also").ok());
  ASSERT_TRUE(cluster.until([&] { return cluster.machine(4).data().size() == 2; }, 30))
      << cluster.debug();

  ASSERT_TRUE(cluster.node(1).trigger_snapshot().ok()) << cluster.debug();
  ASSERT_TRUE(cluster.node(4).trigger_snapshot().ok()) << cluster.debug();
  const auto leader_meta = cluster.node(1).snapshot_meta();
  ASSERT_TRUE(leader_meta.has_value());
  EXPECT_EQ(leader_meta->membership.voters(), (std::vector<NodeId>{1, 2, 3}));
  EXPECT_EQ(leader_meta->membership.learners(), (std::vector<NodeId>{4}));
  EXPECT_EQ(cluster.node(4).snapshot_meta()->membership.learners(), (std::vector<NodeId>{4}));
  EXPECT_FALSE(cluster.log_contains(1, "kept"));
  EXPECT_FALSE(cluster.log_contains(4, "kept"));

  cluster.restart(1);
  cluster.restart(4);
  EXPECT_EQ(cluster.node(1).role(), Role::Follower);
  EXPECT_EQ(cluster.node(1).membership().voters(), (std::vector<NodeId>{1, 2, 3}));
  EXPECT_EQ(cluster.node(1).membership().learners(), (std::vector<NodeId>{4}));
  EXPECT_EQ(cluster.node(1).membership().quorum(), 2u);
  EXPECT_EQ(cluster.node(4).membership().learners(), (std::vector<NodeId>{4}));
  EXPECT_FALSE(cluster.node(4).membership().contains(4));
  ASSERT_EQ(cluster.machine(1).data().size(), 2u) << cluster.debug();
  EXPECT_EQ(cluster.machine(1).data()[0], "kept");
  EXPECT_EQ(cluster.machine(1).data()[1], "also");
  ASSERT_EQ(cluster.machine(4).data().size(), 2u);
  EXPECT_EQ(cluster.machine(4).data()[1], "also");

  ASSERT_TRUE(cluster.until(
      [&] {
        const std::vector<NodeId> leaders = cluster.leaders();
        return leaders.size() == 1 && cluster.node(4).current_leader() == leaders.front() &&
               cluster.node(4).membership().is_learner(4);
      },
      40))
      << cluster.debug();
  const NodeId leader = cluster.leaders().front();
  ASSERT_TRUE(cluster.node(leader).write("later").ok()) << cluster.debug();
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.machine(4).data().size() == 3 && cluster.machine(4).data().back() == "later" &&
               cluster.machine(1).data().size() == 3;
      },
      40))
      << cluster.debug();
  EXPECT_EQ(cluster.node(leader).membership().learners(), (std::vector<NodeId>{4}));
  EXPECT_EQ(cluster.node(4).role(), Role::Follower);
}

INSTANTIATE_TEST_SUITE_P(
    MemoryAndFile,
    LearnerRestart,
    ::testing::Values(test::StorageMode::Memory, test::StorageMode::File),
    [](const ::testing::TestParamInfo<test::StorageMode>& param) {
      return param.param == test::StorageMode::Memory ? std::string("Memory") : std::string("File");
    });

}  // namespace
}  // namespace easyraft
