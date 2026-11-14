// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright 2026 easyraft contributors

#include "cluster.hpp"

#include "easyraft/memory.hpp"
#include "easyraft/types.hpp"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

namespace easyraft {
namespace {

TEST(Membership, UniformQuorumMatchesTheMajority) {
  const Membership membership{{3, 1, 2, 2}};
  EXPECT_FALSE(membership.is_joint());
  ASSERT_EQ(membership.configs().size(), 1u);
  EXPECT_EQ(membership.voters(), (std::vector<NodeId>{1, 2, 3}));
  EXPECT_EQ(membership.quorum(), 2u);
  EXPECT_TRUE(membership.contains(2));
  EXPECT_FALSE(membership.contains(4));

  EXPECT_FALSE(membership.is_quorum([](NodeId id) { return id == 1; }));
  EXPECT_TRUE(membership.is_quorum([](NodeId id) { return id == 1 || id == 3; }));
}

TEST(Membership, JointQuorumNeedsEveryConfig) {
  const Membership joint = Membership::joint_of({1, 2, 3}, {3, 4, 5});
  EXPECT_TRUE(joint.is_joint());
  ASSERT_EQ(joint.configs().size(), 2u);
  EXPECT_EQ(joint.configs()[0], (std::vector<NodeId>{1, 2, 3}));
  EXPECT_EQ(joint.configs()[1], (std::vector<NodeId>{3, 4, 5}));
  EXPECT_EQ(joint.voters(), (std::vector<NodeId>{1, 2, 3, 4, 5}));
  EXPECT_EQ(joint.quorum(), 0u);
  EXPECT_TRUE(joint.contains(1));
  EXPECT_TRUE(joint.contains(5));
  EXPECT_FALSE(joint.contains(6));

  // A majority of the old set and a disjoint majority of the new set.
  EXPECT_TRUE(joint.is_quorum([](NodeId id) {
    return id == 1 || id == 2 || id == 4 || id == 5;
  }));
  // Old majority only.
  EXPECT_FALSE(joint.is_quorum([](NodeId id) { return id == 1 || id == 2; }));
  // New majority only.
  EXPECT_FALSE(joint.is_quorum([](NodeId id) { return id == 4 || id == 5; }));
  // Overlap on one node still covers both majorities.
  EXPECT_TRUE(joint.is_quorum([](NodeId id) { return id == 1 || id == 3 || id == 4; }));
}

TEST(Membership, EncodeRoundTripsUniformAndJoint) {
  const Membership uniform{{2, 2, 1}};
  const auto decoded_uniform = Membership::decode(uniform.encode());
  ASSERT_TRUE(decoded_uniform.has_value());
  EXPECT_EQ(*decoded_uniform, uniform);
  EXPECT_TRUE(uniform.learners().empty());
  EXPECT_EQ(static_cast<unsigned char>(uniform.encode()[0]), 1u);

  const Membership joint = Membership::joint_of({4, 1}, {1, 2, 9});
  const auto decoded_joint = Membership::decode(joint.encode());
  ASSERT_TRUE(decoded_joint.has_value());
  EXPECT_EQ(*decoded_joint, joint);
  EXPECT_TRUE(decoded_joint->is_joint());

  EXPECT_FALSE(Membership::decode("").has_value());
  EXPECT_FALSE(Membership::decode(std::string("\x02\x01", 2)).has_value());
  const std::string truncated = uniform.encode().substr(0, 3);
  EXPECT_FALSE(Membership::decode(truncated).has_value());
}

TEST(Membership, LearnersAreNotPartOfTheQuorum) {
  const Membership membership{{3, 1, 2, 2}, {5, 4, 4}};
  EXPECT_EQ(membership.voters(), (std::vector<NodeId>{1, 2, 3}));
  EXPECT_EQ(membership.learners(), (std::vector<NodeId>{4, 5}));
  EXPECT_EQ(membership.quorum(), 2u);
  EXPECT_TRUE(membership.contains(2));
  EXPECT_FALSE(membership.contains(4));
  EXPECT_TRUE(membership.is_learner(4));
  EXPECT_FALSE(membership.is_learner(1));
  EXPECT_FALSE(membership.learners_overlap_voters());
  EXPECT_EQ(membership.replication_targets(), (std::vector<NodeId>{1, 2, 3, 4, 5}));
  EXPECT_FALSE(membership.is_quorum([](NodeId id) { return id == 1 || id == 4 || id == 5; }));
  EXPECT_TRUE(membership.is_quorum([](NodeId id) { return id == 1 || id == 3; }));

  const Membership overlap{{1, 2}, {2, 3}};
  EXPECT_TRUE(overlap.learners_overlap_voters());
  EXPECT_TRUE(overlap.contains(2));
  EXPECT_TRUE(overlap.is_learner(2));
}

TEST(Membership, LearnerEncodingRoundTripsAndKeepsVersionOne) {
  // Version 1, one config, voters 1 and 2. Locked so a learner extension cannot
  // rewrite the bytes a voter-only config already stores.
  const std::string version1("\x01\x01\x02\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00"
                             "\x02\x00\x00\x00\x00\x00\x00\x00",
                             22);
  EXPECT_EQ((Membership{{1, 2}}).encode(), version1);
  const auto legacy = Membership::decode(version1);
  ASSERT_TRUE(legacy.has_value());
  EXPECT_EQ(legacy->voters(), (std::vector<NodeId>{1, 2}));
  EXPECT_TRUE(legacy->learners().empty());
  EXPECT_FALSE(legacy->has_prior_learners());

  const Membership with_learners{{1, 2, 3}, {6, 4}};
  EXPECT_EQ(static_cast<unsigned char>(with_learners.encode()[0]), 2u);
  const auto decoded = Membership::decode(with_learners.encode());
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(*decoded, with_learners);
  EXPECT_FALSE(decoded->has_prior_learners());

  const auto logged = Membership::decode(with_learners.encode_for_log({6}));
  ASSERT_TRUE(logged.has_value());
  EXPECT_EQ(*logged, with_learners);
  EXPECT_TRUE(logged->has_prior_learners());
  EXPECT_EQ(logged->prior_learners(), (std::vector<NodeId>{6}));
  const auto stripped = Membership::decode(logged->encode());
  ASSERT_TRUE(stripped.has_value());
  EXPECT_EQ(*stripped, with_learners);
  EXPECT_FALSE(stripped->has_prior_learners());

  const Membership promoted = Membership::joint_of({1, 2, 3}, {1, 2, 3, 4}, {4, 5});
  EXPECT_TRUE(promoted.is_joint());
  EXPECT_EQ(promoted.learners(), (std::vector<NodeId>{5}));
  EXPECT_TRUE(promoted.contains(4));
  EXPECT_FALSE(promoted.is_learner(4));
  EXPECT_FALSE(promoted.has_uniform_learner_override());
  const auto promoted_decoded = Membership::decode(promoted.encode_for_log({4, 5}));
  ASSERT_TRUE(promoted_decoded.has_value());
  EXPECT_EQ(*promoted_decoded, promoted);
  EXPECT_EQ(promoted_decoded->prior_learners(), (std::vector<NodeId>{4, 5}));

  const Membership demoted = Membership::joint_of({1, 2, 3}, {1, 2}, {4}, {3, 4});
  EXPECT_EQ(demoted.learners(), (std::vector<NodeId>{4}));
  EXPECT_TRUE(demoted.contains(3));
  EXPECT_FALSE(demoted.is_learner(3));
  EXPECT_TRUE(demoted.has_uniform_learner_override());
  EXPECT_EQ(demoted.learners_after_joint(), (std::vector<NodeId>{3, 4}));
  const auto demoted_decoded = Membership::decode(demoted.encode());
  ASSERT_TRUE(demoted_decoded.has_value());
  EXPECT_EQ(*demoted_decoded, demoted);
  EXPECT_EQ(demoted_decoded->learners_after_joint(), (std::vector<NodeId>{3, 4}));
  EXPECT_FALSE(Membership::decode(demoted.encode().substr(0, 4)).has_value());
}

TEST(Membership, VersionTwoDecodeCollapsesDuplicateVoters) {
  auto append_u32 = [](std::string& out, std::uint32_t value) {
    for (int shift = 0; shift < 32; shift += 8) {
      out.push_back(static_cast<char>((value >> shift) & 0xffu));
    }
  };
  auto append_u64 = [](std::string& out, std::uint64_t value) {
    for (int shift = 0; shift < 64; shift += 8) {
      out.push_back(static_cast<char>((value >> shift) & 0xffu));
    }
  };
  // Version 2, one config, voters {1, 1, 2}, learner {3}, no extra sections.
  std::string bytes;
  bytes.push_back(2);
  bytes.push_back(1);
  append_u32(bytes, 3);
  append_u64(bytes, 1);
  append_u64(bytes, 1);
  append_u64(bytes, 2);
  append_u32(bytes, 1);
  append_u64(bytes, 3);
  bytes.push_back(0);

  const auto decoded = Membership::decode(bytes);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->configs()[0], (std::vector<NodeId>{1, 2}));
  EXPECT_EQ(decoded->voters(), (std::vector<NodeId>{1, 2}));
  EXPECT_EQ(decoded->learners(), (std::vector<NodeId>{3}));
  EXPECT_EQ(decoded->quorum(), 2u);
  EXPECT_FALSE(decoded->is_quorum([](NodeId id) { return id == 1; }));
  EXPECT_TRUE(decoded->is_quorum([](NodeId id) { return id == 1 || id == 2; }));
  EXPECT_EQ(*decoded, (Membership{{1, 2}, {3}}));
}

TEST(Membership, MemoryStorageAcceptsJointMembership) {
  MemoryLogStorage storage;
  const Membership joint = Membership::joint_of({1, 2, 3}, {2, 3, 4});
  ASSERT_TRUE(storage.save_membership(joint).ok());
  const auto loaded = storage.load_membership();
  ASSERT_TRUE(loaded.ok());
  ASSERT_TRUE(loaded.value().has_value());
  EXPECT_EQ(*loaded.value(), joint);

  LogEntry entry;
  entry.id = LogId{3, 1, 1};
  entry.kind = EntryKind::Membership;
  entry.data = joint.encode();
  ASSERT_TRUE(storage.append({entry}).ok());
  const auto log = storage.read(1, 1);
  ASSERT_TRUE(log.ok());
  ASSERT_EQ(log.value().size(), 1u);
  EXPECT_EQ(log.value().front().kind, EntryKind::Membership);
  const auto from_log = Membership::decode(log.value().front().data);
  ASSERT_TRUE(from_log.has_value());
  EXPECT_EQ(*from_log, joint);
}

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

LogIndex latest_membership_index(const RaftNode& node) {
  LogIndex index = 0;
  for (const LogEntry& entry : node.log()) {
    if (entry.kind == EntryKind::Membership) {
      index = entry.id.index;
    }
  }
  return index;
}

bool committed_uniform(const test::Cluster& cluster,
                       NodeId id,
                       const std::vector<NodeId>& voters) {
  const RaftNode& node = cluster.node(id);
  if (node.membership().is_joint() || node.membership().voters() != voters) {
    return false;
  }
  const LogIndex index = latest_membership_index(node);
  if (index == 0 || node.commit_index() < index) {
    return false;
  }
  for (const LogEntry& entry : node.log()) {
    if (entry.id.index != index) {
      continue;
    }
    const std::optional<Membership> decoded = Membership::decode(entry.data);
    return decoded && !decoded->is_joint() && decoded->voters() == voters;
  }
  return false;
}

TEST(MembershipChange, InitializeAllowsANonMemberAndRejectsJoint) {
  auto storage = std::make_shared<MemoryLogStorage>();
  auto machine = std::make_shared<MemoryStateMachine>();
  RaftNode node(4, test::fast_config(4), storage, machine, nullptr);
  const Result<void> joint = node.initialize(Membership::joint_of({1, 2, 3}, {1, 2, 3, 4}));
  EXPECT_EQ(joint.error(), Error::InvalidMembership);

  ASSERT_TRUE(node.initialize(Membership{{1, 2, 3}}).ok());
  EXPECT_FALSE(node.membership().contains(4));
  for (int tick = 0; tick < 6; ++tick) {
    node.tick();
  }
  EXPECT_EQ(node.role(), Role::Follower);
  EXPECT_EQ(node.change_membership({1, 2, 3, 4}).error(), Error::NotLeader);
}

TEST(MembershipChange, AddVoterThenNewVoterIsInTheWriteQuorum) {
  test::Cluster cluster({1, 2, 3}, test::fast_config(4), true);
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 30)) << cluster.debug();
  cluster.add_node(4);

  const Result<LogIndex> changed = cluster.node(1).change_membership({1, 2, 3, 4});
  ASSERT_TRUE(changed.ok()) << changed.error();
  const std::vector<NodeId> four{1, 2, 3, 4};
  ASSERT_TRUE(cluster.until(
      [&] { return committed_uniform(cluster, 1, four) && committed_uniform(cluster, 4, four); },
      40))
      << cluster.debug();

  const std::vector<Membership> entries = membership_log(cluster.node(1));
  ASSERT_EQ(entries.size(), 2u);
  EXPECT_TRUE(entries[0].is_joint());
  EXPECT_EQ(entries[0].configs()[0], (std::vector<NodeId>{1, 2, 3}));
  EXPECT_EQ(entries[0].configs()[1], four);
  EXPECT_FALSE(entries[1].is_joint());
  EXPECT_EQ(entries[1].voters(), four);
  EXPECT_LT(changed.value(), latest_membership_index(cluster.node(1)));
  EXPECT_GE(cluster.node(1).commit_index(), latest_membership_index(cluster.node(1)));

  cluster.partition({{1, 2}, {3}, {4}});
  const Result<LogIndex> blocked = cluster.node(1).write("needs-four");
  ASSERT_TRUE(blocked.ok()) << blocked.error();
  cluster.tick_n(6);
  EXPECT_LT(cluster.node(1).commit_index(), blocked.value()) << cluster.debug();

  cluster.partition({{1, 2, 4}, {3}});
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.node(1).commit_index() >= blocked.value() &&
               cluster.node(4).commit_index() >= blocked.value() &&
               cluster.machine(4).data().size() == 1;
      },
      30))
      << cluster.debug();
  EXPECT_EQ(cluster.machine(4).data().front(), "needs-four");
  EXPECT_EQ(cluster.machine(1).data().front(), "needs-four");
}

TEST(MembershipChange, RemovedVoterIsNotRequiredToCommit) {
  test::Cluster cluster({1, 2, 3}, test::fast_config(4), true);
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 30)) << cluster.debug();
  ASSERT_TRUE(cluster.node(1).change_membership({1, 2}).ok());
  const std::vector<NodeId> two{1, 2};
  ASSERT_TRUE(cluster.until(
      [&] { return committed_uniform(cluster, 1, two) && committed_uniform(cluster, 2, two); }, 40))
      << cluster.debug();

  cluster.stop(3);
  const Result<LogIndex> written = cluster.node(1).write("without-3");
  ASSERT_TRUE(written.ok()) << written.error();
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.node(1).commit_index() >= written.value() &&
               cluster.machine(1).data().size() == 1 && cluster.machine(2).data().size() == 1;
      },
      30))
      << cluster.debug();
  EXPECT_EQ(cluster.machine(2).data().front(), "without-3");
  EXPECT_TRUE(cluster.machine(3).data().empty());
}

TEST(MembershipChange, RejectsFollowerEmptyZeroAndInFlight) {
  test::Cluster cluster({1, 2, 3}, test::fast_config(4), true);
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 30)) << cluster.debug();

  const Result<LogIndex> follower = cluster.node(2).change_membership({1, 2});
  EXPECT_EQ(follower.error(), Error::NotLeader);

  const std::size_t before = cluster.node(1).log().size();
  EXPECT_EQ(cluster.node(1).change_membership({}).error(), Error::InvalidMembership);
  EXPECT_EQ(cluster.node(1).change_membership({1, 0, 2}).error(), Error::InvalidMembership);
  EXPECT_EQ(cluster.node(1).change_membership({3, 2, 1, 1}).error(), Error::Ok);
  EXPECT_EQ(cluster.node(1).log().size(), before);

  cluster.add_node(4);
  cluster.drop_append_responses(true);
  const Result<LogIndex> started = cluster.node(1).change_membership({1, 2, 3, 4});
  ASSERT_TRUE(started.ok()) << started.error();
  EXPECT_EQ(cluster.node(1).change_membership({1, 2}).error(), Error::Rejected);
  EXPECT_TRUE(cluster.node(1).membership().is_joint());
  EXPECT_EQ(cluster.node(1).log().size(), before + 1);
}

TEST(MembershipChange, JointCommitNeedsBothMajorities) {
  {
    test::Cluster cluster({1, 2, 3}, test::fast_config(4), true);
    ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 30)) << cluster.debug();
    cluster.add_node(4);
    cluster.add_node(5);
    cluster.partition({{1, 2}, {3}, {4}, {5}});
    ASSERT_TRUE(cluster.node(1).change_membership({3, 4, 5}).ok());
    const Result<LogIndex> written = cluster.node(1).write("old-only");
    ASSERT_TRUE(written.ok()) << written.error();
    cluster.tick_n(8);
    const LogIndex joint_index = latest_membership_index(cluster.node(1));
    EXPECT_GT(joint_index, 0u);
    EXPECT_LT(cluster.node(1).commit_index(), joint_index) << cluster.debug();
    EXPECT_LT(cluster.node(1).commit_index(), written.value()) << cluster.debug();
    EXPECT_TRUE(cluster.node(1).membership().is_joint());

    cluster.heal();
    const std::vector<NodeId> next{3, 4, 5};
    ASSERT_TRUE(cluster.until(
        [&] {
          for (NodeId id : next) {
            if (cluster.machine(id).data().size() != 1 ||
                cluster.machine(id).data().front() != "old-only") {
              return false;
            }
            if (!committed_uniform(cluster, id, next)) {
              return false;
            }
          }
          return true;
        },
        80))
        << cluster.debug();
  }
  {
    test::Cluster cluster({1, 2, 3}, test::fast_config(4), true);
    ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 30)) << cluster.debug();
    cluster.add_node(4);
    cluster.add_node(5);
    cluster.partition({{1, 4, 5}, {2}, {3}});
    ASSERT_TRUE(cluster.node(1).change_membership({3, 4, 5}).ok());
    const Result<LogIndex> written = cluster.node(1).write("new-only");
    ASSERT_TRUE(written.ok()) << written.error();
    cluster.tick_n(8);
    const LogIndex joint_index = latest_membership_index(cluster.node(1));
    EXPECT_LT(cluster.node(1).commit_index(), joint_index) << cluster.debug();
    EXPECT_LT(cluster.node(1).commit_index(), written.value()) << cluster.debug();
    EXPECT_TRUE(cluster.node(4).membership().is_joint());
    EXPECT_TRUE(cluster.node(5).membership().is_joint());
    EXPECT_FALSE(cluster.node(2).membership().is_joint());

    cluster.heal();
    const std::vector<NodeId> next{3, 4, 5};
    ASSERT_TRUE(cluster.until(
        [&] {
          for (NodeId id : next) {
            if (cluster.machine(id).data().size() != 1 ||
                cluster.machine(id).data().front() != "new-only") {
              return false;
            }
          }
          return committed_uniform(cluster, 4, next);
        },
        80))
        << cluster.debug();
  }
}

TEST(MembershipChange, RemovedLeaderStepsDownAndRemainingElect) {
  test::Cluster cluster({1, 2, 3}, test::fast_config(4), true);
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 30)) << cluster.debug();
  ASSERT_TRUE(cluster.node(1).change_membership({2, 3}).ok());
  const std::vector<NodeId> remaining{2, 3};
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.node(1).role() != Role::Leader &&
               committed_uniform(cluster, 1, remaining) &&
               committed_uniform(cluster, 2, remaining);
      },
      40))
      << cluster.debug();

  ASSERT_TRUE(cluster.until(
      [&] {
        const std::vector<NodeId> leaders = cluster.leaders();
        if (leaders.size() != 1 || leaders.front() == 1) {
          return false;
        }
        const NodeId leader = leaders.front();
        return cluster.node(2).current_leader() == leader &&
               cluster.node(3).current_leader() == leader;
      },
      80))
      << cluster.debug();

  const NodeId leader = cluster.leaders().front();
  const Result<LogIndex> written = cluster.node(leader).write("after-removal");
  ASSERT_TRUE(written.ok()) << written.error() << cluster.debug();
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.machine(2).data().size() == 1 && cluster.machine(3).data().size() == 1;
      },
      30))
      << cluster.debug();
  EXPECT_EQ(cluster.machine(2).data().front(), "after-removal");
  EXPECT_EQ(cluster.machine(3).data().front(), "after-removal");
  EXPECT_NE(cluster.node(1).role(), Role::Leader);
}

class MembershipRestart : public ::testing::TestWithParam<test::StorageMode> {};

TEST_P(MembershipRestart, RecoversDuringAndAfterAChange) {
  test::Cluster cluster({1, 2, 3}, test::fast_config(4), true, GetParam());
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 30)) << cluster.debug();
  cluster.add_node(4);
  cluster.drop_append_responses(true);
  ASSERT_TRUE(cluster.node(1).change_membership({1, 2, 3, 4}).ok());
  cluster.tick_n(4);
  ASSERT_TRUE(cluster.node(1).membership().is_joint()) << cluster.debug();
  ASSERT_TRUE(cluster.node(2).membership().is_joint()) << cluster.debug();
  ASSERT_TRUE(cluster.node(4).membership().is_joint()) << cluster.debug();
  EXPECT_LT(cluster.node(1).commit_index(), latest_membership_index(cluster.node(1)));
  const Membership paused = cluster.node(1).membership();

  cluster.restart(1);
  EXPECT_EQ(cluster.node(1).role(), Role::Follower);
  EXPECT_EQ(cluster.node(1).membership(), paused);

  cluster.drop_append_responses(false);
  const std::vector<NodeId> four{1, 2, 3, 4};
  ASSERT_TRUE(cluster.until(
      [&] { return committed_uniform(cluster, 2, four) && committed_uniform(cluster, 4, four); },
      100))
      << cluster.debug();

  for (NodeId id : four) {
    const Membership before = cluster.node(id).membership();
    cluster.restart(id);
    EXPECT_EQ(cluster.node(id).membership(), before) << id << "\n" << cluster.debug();
  }
  ASSERT_TRUE(cluster.until(
      [&] {
        const std::vector<NodeId> leaders = cluster.leaders();
        return leaders.size() == 1 && cluster.node(leaders.front()).membership().voters() == four;
      },
      80))
      << cluster.debug();
  const NodeId leader = cluster.leaders().front();
  ASSERT_TRUE(cluster.node(leader).write("resumed").ok()) << cluster.debug();
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.machine(1).data().size() == 1 && cluster.machine(4).data().size() == 1;
      },
      40))
      << cluster.debug();
  EXPECT_EQ(cluster.machine(1).data().front(), "resumed");
  EXPECT_EQ(cluster.machine(4).data().front(), "resumed");
}

INSTANTIATE_TEST_SUITE_P(
    MemoryAndFile,
    MembershipRestart,
    ::testing::Values(test::StorageMode::Memory, test::StorageMode::File),
    [](const ::testing::TestParamInfo<test::StorageMode>& param) {
      return param.param == test::StorageMode::Memory ? std::string("Memory") : std::string("File");
    });

}  // namespace
}  // namespace easyraft
