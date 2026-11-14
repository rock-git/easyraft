// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright 2026 easyraft contributors

#include "cluster.hpp"

#include "easyraft/file.hpp"
#include "easyraft/memory.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <vector>

namespace easyraft {
namespace {

bool uniform_quorum(const RaftNode& node, std::size_t election, std::size_t commit) {
  return !node.membership().is_joint() && node.membership().election_quorum() == election &&
         node.membership().quorum() == commit;
}

TEST(QuorumDefinition, MajorityStaysTheDefaultAndCustomThresholdsRoundTrip) {
  const Membership majority{{1, 2, 3}};
  EXPECT_TRUE(majority.config_quorum(0).is_default());
  EXPECT_EQ(majority.election_quorum(), 2u);
  EXPECT_EQ(majority.quorum(), 2u);
  EXPECT_TRUE(majority.quorums_intersect());
  EXPECT_EQ(static_cast<unsigned char>(majority.encode()[0]), 1u);
  EXPECT_FALSE(majority.is_election_quorum([](NodeId id) { return id == 1; }));
  EXPECT_TRUE(majority.is_commit_quorum([](NodeId id) { return id == 1 || id == 2; }));
  EXPECT_FALSE(majority.is_commit_quorum([](NodeId id) { return id == 4; }));

  EXPECT_FALSE(normalize_quorum(5, Quorum{2, 2}));
  EXPECT_FALSE(normalize_quorum(5, Quorum{6, 1}));
  EXPECT_FALSE(normalize_quorum(5, Quorum{0, 2}));
  EXPECT_EQ(normalize_quorum(5, Quorum{3, 3}), Quorum{});
  const std::optional<Quorum> small_commit = normalize_quorum(5, Quorum{4, 2});
  ASSERT_TRUE(small_commit.has_value());
  EXPECT_EQ(small_commit->election, 4u);
  EXPECT_EQ(small_commit->commit, 2u);

  const Membership custom{{5, 1, 2, 4, 3}, {}, Quorum{4, 2}};
  EXPECT_EQ(custom.voters(), (std::vector<NodeId>{1, 2, 3, 4, 5}));
  EXPECT_EQ(custom.election_quorum(), 4u);
  EXPECT_EQ(custom.quorum(), 2u);
  EXPECT_TRUE(custom.quorums_intersect());
  EXPECT_EQ(static_cast<unsigned char>(custom.encode()[0]), 3u);
  const auto decoded = Membership::decode(custom.encode());
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(*decoded, custom);
  EXPECT_FALSE(decoded->has_prior_quorum());
  EXPECT_FALSE(custom.is_commit_quorum([](NodeId id) { return id == 1; }));
  EXPECT_TRUE(custom.is_commit_quorum([](NodeId id) { return id == 1 || id == 5; }));
  EXPECT_FALSE(custom.is_election_quorum([](NodeId id) { return id == 1 || id == 2 || id == 3; }));
  EXPECT_TRUE(custom.is_election_quorum([](NodeId id) { return id >= 2; }));

  const auto logged = Membership::decode(custom.encode_for_log({}, Quorum{}));
  ASSERT_TRUE(logged.has_value());
  EXPECT_EQ(*logged, custom);
  EXPECT_TRUE(logged->has_prior_quorum());
  EXPECT_TRUE(logged->prior_quorum().is_default());

  // Each side is a legal threshold quorum. A majority of {1,2,3} can miss a
  // majority of {3,4,5}, and the joint is still legal: an election and a
  // commit each have to satisfy every config, so they meet inside each one.
  const std::vector<NodeId> old_voters{1, 2, 3};
  const std::vector<NodeId> new_voters{3, 4, 5};
  EXPECT_FALSE(election_intersects_commit(old_voters, {}, new_voters, {}));
  const Membership joint = Membership::joint_of(old_voters, new_voters);
  EXPECT_TRUE(joint.quorums_intersect());

  const Membership flexible = Membership::joint_of({1, 2, 3, 4, 5}, {1, 2, 3}, {}, {},
                                                   Quorum{4, 2}, Quorum{2, 2});
  EXPECT_TRUE(flexible.quorums_intersect());
  EXPECT_EQ(flexible.election_threshold(0), 4u);
  EXPECT_EQ(flexible.commit_threshold(0), 2u);
  EXPECT_EQ(flexible.election_threshold(1), 2u);
  EXPECT_EQ(flexible.commit_threshold(1), 2u);
  EXPECT_FALSE(flexible.is_election_quorum([](NodeId id) { return id == 1 || id == 2 || id == 3; }));
  EXPECT_TRUE(flexible.is_election_quorum([](NodeId id) { return id <= 4; }));
  EXPECT_FALSE(flexible.is_commit_quorum([](NodeId id) { return id == 1 || id == 4; }));
  EXPECT_TRUE(flexible.is_commit_quorum([](NodeId id) { return id == 1 || id == 2; }));
  const auto flexible_decoded = Membership::decode(flexible.encode());
  ASSERT_TRUE(flexible_decoded.has_value());
  EXPECT_EQ(*flexible_decoded, flexible);

  const Membership learners{{1, 2, 3, 4, 5}, {6}, Quorum{4, 2}};
  EXPECT_EQ(learners.learners(), (std::vector<NodeId>{6}));
  EXPECT_FALSE(learners.is_commit_quorum([](NodeId id) { return id == 1 || id == 6; }));
  EXPECT_TRUE(learners.is_commit_quorum([](NodeId id) { return id == 1 || id == 2; }));
  EXPECT_EQ(static_cast<unsigned char>(learners.encode()[0]), 3u);
}

std::uint32_t file_version(const std::filesystem::path& path) {
  std::ifstream in(path, std::ios::binary);
  char bytes[8] = {};
  in.read(bytes, 8);
  if (in.gcount() != 8) {
    return 0;
  }
  std::uint32_t version = 0;
  for (int i = 0; i < 4; ++i) {
    version |= static_cast<std::uint32_t>(static_cast<unsigned char>(bytes[4 + i])) << (8 * i);
  }
  return version;
}

TEST(QuorumDefinition, FileStorageKeepsMajorityFilesAndRestoresCustomQuorums) {
  const auto root = std::filesystem::temp_directory_path() / "easyraft-quorum-file";
  std::filesystem::remove_all(root);
  std::filesystem::create_directories(root);
  const Membership custom{{1, 2, 3, 4, 5}, {6}, Quorum{4, 2}};
  const Membership majority{{1, 2, 3}};
  {
    FileLogStorage storage(root);
    ASSERT_TRUE(storage.save_membership(majority).ok());
  }
  EXPECT_EQ(file_version(root / "membership"), 1u);
  {
    FileLogStorage storage(root);
    ASSERT_TRUE(storage.save_membership(custom).ok());
    Snapshot snapshot;
    snapshot.meta.last_included = LogId{1, 1, 3};
    snapshot.meta.membership = custom;
    snapshot.meta.snapshot_id = snapshot_id_from(snapshot.meta.last_included);
    snapshot.data = "state";
    ASSERT_TRUE(storage.save_snapshot(snapshot).ok());
  }
  EXPECT_EQ(file_version(root / "membership"), 3u);
  {
    FileLogStorage storage(root);
    const auto loaded = storage.load_membership();
    ASSERT_TRUE(loaded.ok());
    ASSERT_TRUE(loaded.value().has_value());
    EXPECT_EQ(*loaded.value(), custom);
    EXPECT_EQ(loaded.value()->election_quorum(), 4u);
    EXPECT_EQ(loaded.value()->quorum(), 2u);
    const auto image = storage.load_snapshot();
    ASSERT_TRUE(image.ok());
    ASSERT_TRUE(image.value().has_value());
    EXPECT_EQ(image.value()->meta.membership, custom);
  }
  std::filesystem::remove_all(root);
}

TEST(QuorumApi, RejectsIllegalCallsAndAcceptsANoOp) {
  auto storage = std::make_shared<MemoryLogStorage>();
  auto machine = std::make_shared<MemoryStateMachine>();
  RaftNode node(1, test::fast_config(2), storage, machine, nullptr);
  EXPECT_EQ(node.change_quorum(Quorum{4, 2}).error(), Error::NotInitialized);
  EXPECT_EQ(node.initialize(Membership{{1, 2, 3, 4, 5}, {}, Quorum{2, 2}}).error(),
            Error::InvalidMembership);
  EXPECT_EQ(node.initialize(Membership{{1, 2, 3, 4, 5}, {}, Quorum{6, 1}}).error(),
            Error::InvalidMembership);
  ASSERT_TRUE(node.initialize(Membership{{1, 2, 3, 4, 5}, {}, Quorum{4, 2}}).ok());
  EXPECT_EQ(node.change_quorum(Quorum{2, 2}).error(), Error::NotLeader);

  test::Cluster cluster({1, 2, 3, 4, 5}, test::fast_config(4), true);
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 40)) << cluster.debug();
  EXPECT_EQ(cluster.node(2).change_quorum(Quorum{4, 2}).error(), Error::NotLeader);
  EXPECT_EQ(cluster.node(1).change_quorum(Quorum{2, 2}).error(), Error::InvalidMembership);
  EXPECT_EQ(cluster.node(1).change_quorum(Quorum{0, 1}).error(), Error::InvalidMembership);
  EXPECT_EQ(cluster.node(1).change_membership({1, 2, 3, 4, 5, 6}, Quorum{2, 2}).error(),
            Error::InvalidMembership);
  EXPECT_EQ(cluster.node(1).change_membership({}, Quorum{4, 2}).error(), Error::InvalidMembership);
  EXPECT_EQ(cluster.node(1).change_membership({1, 0}, Quorum{1, 1}).error(),
            Error::InvalidMembership);

  const std::size_t before = cluster.node(1).log().size();
  const Result<LogIndex> same = cluster.node(1).change_quorum(Quorum{3, 3});
  ASSERT_TRUE(same.ok()) << same.error();
  EXPECT_EQ(cluster.node(1).log().size(), before);
  EXPECT_TRUE(uniform_quorum(cluster.node(1), 3, 3));

  cluster.drop_append_responses(true);
  ASSERT_TRUE(cluster.node(1).change_quorum(Quorum{4, 3}).ok()) << cluster.debug();
  EXPECT_EQ(cluster.node(1).change_quorum(Quorum{4, 2}).error(), Error::Rejected);
  EXPECT_EQ(cluster.node(1).change_membership({1, 2, 3, 4}).error(), Error::Rejected);
}

TEST(QuorumCluster, DefaultMajorityStillIgnoresLearners) {
  test::Cluster cluster({1, 2, 3}, test::fast_config(4), true, test::StorageMode::Memory, {4});
  cluster.add_node(4);
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 40)) << cluster.debug();
  EXPECT_TRUE(uniform_quorum(cluster.node(1), 2, 2));
  EXPECT_TRUE(cluster.node(4).membership().is_learner(4));

  cluster.partition({{1, 4}, {2}, {3}});
  const Result<LogIndex> with_learner = cluster.node(1).write("learner");
  ASSERT_TRUE(with_learner.ok()) << with_learner.error();
  cluster.tick_n(8);
  EXPECT_LT(cluster.node(1).commit_index(), with_learner.value()) << cluster.debug();
  EXPECT_TRUE(cluster.log_contains(4, "learner"));

  cluster.partition({{1, 2, 4}, {3}});
  ASSERT_TRUE(cluster.until(
      [&] { return cluster.node(1).commit_index() >= with_learner.value(); }, 20))
      << cluster.debug();
  EXPECT_EQ(cluster.machine(1).data().back(), "learner");
  EXPECT_FALSE(cluster.log_contains(3, "learner"));
}

TEST(QuorumCluster, SmallerCommitQuorumDoesNotNeedAMajority) {
  test::Cluster cluster({1, 2, 3, 4, 5}, test::fast_config(4), true, test::StorageMode::Memory, {6},
                        Quorum{4, 2});
  cluster.add_node(6);
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 50)) << cluster.debug();
  EXPECT_TRUE(uniform_quorum(cluster.node(1), 4, 2));

  cluster.partition({{1, 6}, {2}, {3}, {4}, {5}});
  const Result<LogIndex> learner_only = cluster.node(1).write("learner-only");
  ASSERT_TRUE(learner_only.ok()) << learner_only.error();
  cluster.tick_n(8);
  EXPECT_LT(cluster.node(1).commit_index(), learner_only.value()) << cluster.debug();

  cluster.partition({{1, 2, 6}, {3}, {4}, {5}});
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.node(1).commit_index() >= learner_only.value() &&
               cluster.machine(2).data().size() == 1;
      },
      20))
      << cluster.debug();
  EXPECT_EQ(cluster.machine(1).data().back(), "learner-only");
  EXPECT_FALSE(cluster.log_contains(3, "learner-only"));
  EXPECT_FALSE(cluster.log_contains(5, "learner-only"));
}

TEST(QuorumCluster, LearnerChangeKeepsTheCustomQuorum) {
  test::Cluster cluster({1, 2, 3, 4, 5}, test::fast_config(4), true, test::StorageMode::Memory, {},
                        Quorum{4, 2});
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 50)) << cluster.debug();
  cluster.add_node(6);
  ASSERT_TRUE(cluster.node(1).add_learner(6).ok()) << cluster.debug();
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.node(1).membership().is_learner(6) && cluster.node(6).membership().is_learner(6) &&
               uniform_quorum(cluster.node(1), 4, 2) && uniform_quorum(cluster.node(6), 4, 2);
      },
      40))
      << cluster.debug();
}

TEST(QuorumCluster, SmallerCommitQuorumStillNeedsTheElectionQuorum) {
  test::Cluster cluster({1, 2, 3, 4, 5}, test::fast_config(4), true, test::StorageMode::Memory, {},
                        Quorum{4, 2});
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 50)) << cluster.debug();
  cluster.stop(1);
  cluster.partition({{2, 3}, {4}, {5}});
  for (int tick = 0; tick < 24; ++tick) {
    cluster.tick();
    EXPECT_TRUE(cluster.leaders().empty()) << cluster.debug();
  }

  cluster.partition({{2, 3, 4, 5}});
  ASSERT_TRUE(cluster.until([&] { return cluster.leaders().size() == 1; }, 40)) << cluster.debug();
  const NodeId leader = cluster.leaders().front();
  EXPECT_NE(leader, 1u);
  EXPECT_TRUE(uniform_quorum(cluster.node(leader), 4, 2));
}

TEST(QuorumCluster, SmallerElectionQuorumStillNeedsTheCommitQuorum) {
  test::Cluster cluster({1, 2, 3, 4, 5}, test::fast_config(4), true, test::StorageMode::Memory, {},
                        Quorum{2, 4});
  cluster.partition({{1, 2}, {3}, {4}, {5}});
  ASSERT_TRUE(cluster.until([&] { return cluster.node(1).role() == Role::Leader; }, 20))
      << cluster.debug();
  EXPECT_EQ(cluster.node(2).current_leader(), 1u);
  EXPECT_TRUE(uniform_quorum(cluster.node(1), 2, 4));

  const Result<LogIndex> written = cluster.node(1).write("needs-four");
  ASSERT_TRUE(written.ok()) << written.error();
  cluster.tick_n(8);
  EXPECT_LT(cluster.node(1).commit_index(), written.value()) << cluster.debug();
  EXPECT_TRUE(cluster.log_contains(2, "needs-four"));
  EXPECT_FALSE(cluster.log_contains(3, "needs-four"));

  cluster.partition({{1, 2, 3, 4}, {5}});
  ASSERT_TRUE(cluster.until(
      [&] { return cluster.node(1).commit_index() >= written.value() &&
                   cluster.machine(4).data().size() == 1; },
      30))
      << cluster.debug();
  EXPECT_EQ(cluster.machine(1).data().back(), "needs-four");
  EXPECT_FALSE(cluster.log_contains(5, "needs-four"));
}

TEST(QuorumCluster, JointChangeUsesEachConfigsOwnQuorum) {
  test::Cluster cluster({1, 2, 3, 4, 5}, test::fast_config(4), true, test::StorageMode::Memory, {},
                        Quorum{4, 2});
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 50)) << cluster.debug();

  // {1,4} is a commit quorum of the old config and not of {1,2,3}.
  cluster.partition({{1, 4}, {2}, {3}, {5}});
  ASSERT_TRUE(cluster.node(1).change_membership({1, 2, 3}, Quorum{2, 2}).ok()) << cluster.debug();
  const Result<LogIndex> old_only = cluster.node(1).write("old-only");
  ASSERT_TRUE(old_only.ok()) << old_only.error();
  cluster.tick_n(8);
  EXPECT_TRUE(cluster.node(1).membership().is_joint());
  EXPECT_EQ(cluster.node(1).membership().commit_threshold(0), 2u);
  EXPECT_EQ(cluster.node(1).membership().commit_threshold(1), 2u);
  EXPECT_EQ(cluster.node(1).membership().election_threshold(0), 4u);
  EXPECT_LT(cluster.node(1).commit_index(), old_only.value()) << cluster.debug();

  // {1,2} satisfies both commit rules. It is not an election quorum of the old
  // config, and the commit does not wait for one.
  cluster.partition({{1, 2}, {3}, {4}, {5}});
  ASSERT_TRUE(cluster.until(
      [&] {
        return uniform_quorum(cluster.node(1), 2, 2) && uniform_quorum(cluster.node(2), 2, 2) &&
               cluster.node(1).commit_index() >= old_only.value();
      },
      40))
      << cluster.debug();
  EXPECT_EQ(cluster.node(1).membership().voters(), (std::vector<NodeId>{1, 2, 3}));
  // Node 3 is the other new voter and was partitioned off, so the commit used
  // {1, 2} for both configs. Node 4 stored the entry in the first partition
  // and was not required again.
  EXPECT_FALSE(cluster.log_contains(3, "old-only"));
  EXPECT_FALSE(cluster.log_contains(5, "old-only"));
}

TEST(QuorumCluster, LeaderCanAdoptASmallerCommitQuorum) {
  test::Cluster cluster({1, 2, 3, 4, 5}, test::fast_config(4), true);
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 40)) << cluster.debug();
  ASSERT_TRUE(cluster.node(1).change_quorum(Quorum{4, 2}).ok()) << cluster.debug();
  ASSERT_TRUE(cluster.until(
      [&] {
        return uniform_quorum(cluster.node(1), 4, 2) && uniform_quorum(cluster.node(5), 4, 2);
      },
      50))
      << cluster.debug();

  cluster.partition({{1, 2}, {3}, {4}, {5}});
  const Result<LogIndex> written = cluster.node(1).write("after-change");
  ASSERT_TRUE(written.ok()) << written.error();
  ASSERT_TRUE(cluster.until(
      [&] { return cluster.node(1).commit_index() >= written.value(); }, 20))
      << cluster.debug();
  EXPECT_EQ(cluster.machine(2).data().back(), "after-change");
  EXPECT_FALSE(cluster.log_contains(3, "after-change"));
}

TEST(QuorumCluster, DirectQuorumChangeSkipsJointWhenTheDefinitionsCross) {
  test::Cluster cluster({1, 2, 3, 4, 5}, test::fast_config(4), true);
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 40)) << cluster.debug();
  const std::size_t before = cluster.node(1).log().size();
  ASSERT_TRUE(cluster.node(1).change_quorum(Quorum{4, 3}).ok()) << cluster.debug();
  EXPECT_EQ(cluster.node(1).log().size(), before + 1);
  EXPECT_FALSE(cluster.node(1).membership().is_joint());
  EXPECT_TRUE(uniform_quorum(cluster.node(1), 4, 3));

  cluster.partition({{1, 2}, {3}, {4}, {5}});
  const Result<LogIndex> written = cluster.node(1).write("needs-three");
  ASSERT_TRUE(written.ok()) << written.error();
  cluster.tick_n(8);
  EXPECT_LT(cluster.node(1).commit_index(), written.value()) << cluster.debug();

  cluster.partition({{1, 2, 3}, {4}, {5}});
  ASSERT_TRUE(cluster.until(
      [&] { return cluster.node(1).commit_index() >= written.value(); }, 20))
      << cluster.debug();
}

TEST(QuorumCluster, DemoteRejectsAQuorumTheSmallerSetCannotForm) {
  test::Cluster cluster({1, 2, 3, 4, 5}, test::fast_config(4), true, test::StorageMode::Memory, {},
                        Quorum{5, 1});
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 50)) << cluster.debug();
  EXPECT_EQ(cluster.node(1).demote_voter(5).error(), Error::InvalidMembership);
  EXPECT_TRUE(uniform_quorum(cluster.node(1), 5, 1));
}

class QuorumRestart : public ::testing::TestWithParam<test::StorageMode> {};

TEST_P(QuorumRestart, RestoresCustomQuorumAfterRestartAndCompaction) {
  test::Cluster cluster({1, 2, 3, 4, 5}, test::fast_config(4), true, GetParam(), {}, Quorum{4, 2});
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 50)) << cluster.debug();
  ASSERT_TRUE(cluster.node(1).write("kept").ok());
  ASSERT_TRUE(cluster.until([&] { return cluster.machine(1).data().size() == 1; }, 20))
      << cluster.debug();

  cluster.restart(1);
  EXPECT_EQ(cluster.node(1).role(), Role::Follower);
  EXPECT_TRUE(uniform_quorum(cluster.node(1), 4, 2));
  EXPECT_EQ(cluster.node(1).membership().learners(), (std::vector<NodeId>{}));

  ASSERT_TRUE(cluster.until([&] { return cluster.leaders().size() == 1; }, 40)) << cluster.debug();
  const NodeId leader = cluster.leaders().front();
  ASSERT_TRUE(cluster.node(leader).trigger_snapshot().ok()) << cluster.debug();
  const auto meta = cluster.node(leader).snapshot_meta();
  ASSERT_TRUE(meta.has_value());
  EXPECT_EQ(meta->membership.election_quorum(), 4u);
  EXPECT_EQ(meta->membership.quorum(), 2u);
  EXPECT_FALSE(cluster.log_contains(leader, "kept"));

  cluster.restart(leader);
  EXPECT_TRUE(uniform_quorum(cluster.node(leader), 4, 2));
  ASSERT_EQ(cluster.machine(leader).data().size(), 1u);
  EXPECT_EQ(cluster.machine(leader).data().front(), "kept");
  const auto restored = cluster.node(leader).snapshot_meta();
  ASSERT_TRUE(restored.has_value());
  EXPECT_EQ(restored->membership, meta->membership);
}

TEST_P(QuorumRestart, UncommittedQuorumChangeRollsBack) {
  test::Cluster cluster({1, 2, 3, 4, 5}, test::fast_config(4), true, GetParam());
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 40)) << cluster.debug();
  cluster.isolate(1);
  ASSERT_TRUE(cluster.node(1).change_quorum(Quorum{4, 3}).ok()) << cluster.debug();
  EXPECT_TRUE(uniform_quorum(cluster.node(1), 4, 3));
  EXPECT_GT(cluster.node(1).last_log_index(), cluster.node(1).commit_index());

  cluster.restart(1);
  EXPECT_TRUE(uniform_quorum(cluster.node(1), 4, 3));
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
        return cluster.machine(2).data().size() == 1 && cluster.machine(3).data().size() == 1;
      },
      30))
      << cluster.debug();

  cluster.heal();
  cluster.start(1);
  ASSERT_TRUE(cluster.until(
      [&] {
        return uniform_quorum(cluster.node(1), 3, 3) && !cluster.node(1).membership().is_joint() &&
               cluster.log_contains(1, "conflict");
      },
      80))
      << cluster.debug();
}

TEST_P(QuorumRestart, UncommittedJointQuorumChangeRollsBack) {
  test::Cluster cluster({1, 2, 3, 4, 5}, test::fast_config(4), true, GetParam());
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 40)) << cluster.debug();
  cluster.isolate(1);
  ASSERT_TRUE(cluster.node(1).change_quorum(Quorum{4, 2}).ok()) << cluster.debug();
  EXPECT_TRUE(cluster.node(1).membership().is_joint());
  EXPECT_EQ(cluster.node(1).membership().commit_threshold(0), 3u);
  EXPECT_EQ(cluster.node(1).membership().commit_threshold(1), 2u);

  cluster.restart(1);
  EXPECT_TRUE(cluster.node(1).membership().is_joint());
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
      [&] { return cluster.machine(2).data().size() == 1 && cluster.machine(4).data().size() == 1; },
      30))
      << cluster.debug();

  cluster.heal();
  cluster.start(1);
  ASSERT_TRUE(cluster.until(
      [&] {
        return uniform_quorum(cluster.node(1), 3, 3) && cluster.log_contains(1, "conflict");
      },
      80))
      << cluster.debug();
}

INSTANTIATE_TEST_SUITE_P(
    MemoryAndFile,
    QuorumRestart,
    ::testing::Values(test::StorageMode::Memory, test::StorageMode::File),
    [](const ::testing::TestParamInfo<test::StorageMode>& param) {
      return param.param == test::StorageMode::Memory ? std::string("Memory") : std::string("File");
    });

}  // namespace
}  // namespace easyraft
