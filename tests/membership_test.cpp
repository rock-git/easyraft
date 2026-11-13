// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright 2026 easyraft contributors

#include "easyraft/memory.hpp"
#include "easyraft/types.hpp"

#include <gtest/gtest.h>

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

}  // namespace
}  // namespace easyraft
