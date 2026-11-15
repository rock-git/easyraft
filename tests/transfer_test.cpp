// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright 2026 easyraft contributors

#include "cluster.hpp"

#include "easyraft/memory.hpp"

#include <gtest/gtest.h>

#include <initializer_list>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace easyraft {
namespace {

Config pinned(int timeout) {
  Config config;
  config.heartbeat_interval = 1;
  config.election_timeout_min = timeout;
  config.election_timeout_max = timeout;
  config.enable_pre_vote = true;
  return config;
}

std::uint64_t prevote_probe(const StepResult& step) {
  for (const Outbound& message : step.outbound) {
    if (const auto* request = std::get_if<PreVoteRequest>(&message.payload)) {
      return request->probe;
    }
  }
  return 0;
}

void become_leader(Engine& engine, std::vector<NodeId> granters) {
  std::uint64_t probe = 0;
  for (int tick = 0; tick < 40 && engine.role() != Role::Leader; ++tick) {
    probe = std::max(probe, prevote_probe(engine.on_tick()));
    if (probe != 0 && engine.role() != Role::Leader && engine.term() == 0) {
      for (NodeId id : granters) {
        PreVoteResponse grant;
        grant.probe = probe;
        grant.body.granted = true;
        engine.on_pre_vote_response(id, grant);
      }
    }
    if (engine.role() == Role::Candidate) {
      for (NodeId id : granters) {
        VoteResponse vote;
        vote.granted = true;
        vote.vote = engine.vote();
        engine.on_vote_response(id, vote);
      }
    }
  }
}

Result<void> await_transfer(test::Cluster& cluster, NodeId id, int max_ticks) {
  Result<void> result = Result<void>::failure(Error::Pending);
  cluster.until(
      [&] {
        result = cluster.node(id).poll_leader_transfer();
        return result.error() != Error::Pending;
      },
      max_ticks);
  return result;
}

bool log_has(const RaftNode& node, const std::string& data) {
  for (const LogEntry& entry : node.log()) {
    if (entry.kind == EntryKind::Application && entry.data == data) {
      return true;
    }
  }
  return false;
}

TEST(LeaderTransfer, MovesLeadershipAndThenRejectsTheOldLeader) {
  test::Cluster cluster({1, 2, 3}, test::fast_config(6), true);
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 30)) << cluster.debug();

  ASSERT_TRUE(cluster.node(1).transfer_leader(2).ok()) << cluster.debug();
  EXPECT_EQ(cluster.node(1).poll_leader_transfer().error(), Error::Pending);
  EXPECT_EQ(cluster.node(1).write("during").error(), Error::Rejected);
  EXPECT_EQ(cluster.node(1).linearizable_read().error(), Error::Rejected);
  EXPECT_EQ(cluster.node(1).lease_read().error(), Error::Rejected);
  EXPECT_EQ(cluster.node(1).add_learner(4).error(), Error::Rejected);
  EXPECT_EQ(cluster.node(1).transfer_leader(3).error(), Error::Rejected);

  const Result<void> handed = await_transfer(cluster, 1, 10);
  ASSERT_TRUE(handed.ok()) << handed.error() << "\n" << cluster.debug();
  EXPECT_TRUE(handed.ok());
  ASSERT_TRUE(cluster.agreed_on(2)) << cluster.debug();
  EXPECT_EQ(cluster.node(1).write("old").error(), Error::NotLeader);
  EXPECT_EQ(cluster.node(1).lease_read().error(), Error::NotLeader);

  const Result<LogIndex> written = cluster.node(2).write("new");
  ASSERT_TRUE(written.ok()) << written.error();
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.node(1).commit_index() >= written.value() && log_has(cluster.node(1), "new") &&
               log_has(cluster.node(3), "new");
      },
      20))
      << cluster.debug();
}

TEST(LeaderTransfer, RejectsFollowersLearnersAndUnknownTargets) {
  test::Cluster cluster({1, 2, 3}, test::fast_config(6), true, test::StorageMode::Memory, {4});
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 30)) << cluster.debug();

  EXPECT_EQ(cluster.node(2).transfer_leader(3).error(), Error::NotLeader);
  EXPECT_EQ(cluster.node(2).poll_leader_transfer().error(), Error::Rejected);
  EXPECT_EQ(cluster.node(1).transfer_leader(4).error(), Error::Rejected);
  EXPECT_EQ(cluster.node(1).transfer_leader(99).error(), Error::Rejected);
  EXPECT_EQ(cluster.node(1).transfer_leader(1).error(), Error::Rejected);
  EXPECT_EQ(cluster.node(1).transfer_leader(0).error(), Error::Rejected);
  EXPECT_EQ(cluster.node(1).role(), Role::Leader);
  EXPECT_EQ(cluster.node(1).poll_leader_transfer().error(), Error::Rejected);

  auto storage = std::make_shared<MemoryLogStorage>();
  auto machine = std::make_shared<MemoryStateMachine>();
  RaftNode learner(4, test::fast_config(6), storage, machine, nullptr);
  ASSERT_TRUE(learner.initialize(Membership{{1, 2, 3}, {4}}).ok());
  EXPECT_EQ(learner.transfer_leader(1).error(), Error::NotLeader);
  EXPECT_EQ(learner.poll_leader_transfer().error(), Error::Rejected);
}

TEST(LeaderTransfer, CatchesUpALaggingVoterBeforeCampaigning) {
  test::Cluster cluster({1, 2, 3}, test::fast_config(8), true);
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 30)) << cluster.debug();

  cluster.stop(3);
  ASSERT_TRUE(cluster.node(1).write("one").ok());
  ASSERT_TRUE(cluster.node(1).write("two").ok());
  ASSERT_TRUE(cluster.until(
      [&] { return log_has(cluster.node(2), "one") && log_has(cluster.node(2), "two"); }, 20))
      << cluster.debug();
  EXPECT_FALSE(log_has(cluster.node(3), "one"));

  cluster.start(3);
  ASSERT_TRUE(cluster.node(1).transfer_leader(3).ok()) << cluster.debug();
  const Result<void> handed = await_transfer(cluster, 1, 30);
  ASSERT_TRUE(handed.ok()) << handed.error() << "\n" << cluster.debug();
  ASSERT_TRUE(cluster.agreed_on(3)) << cluster.debug();
  EXPECT_TRUE(log_has(cluster.node(3), "one"));
  EXPECT_TRUE(log_has(cluster.node(3), "two"));
  EXPECT_EQ(cluster.leaders(), std::vector<NodeId>{3});
}

TEST(LeaderTransfer, FlexibleElectionQuorumStillGrantsDuringTheLease) {
  test::Cluster cluster({1, 2, 3, 4, 5}, test::fast_config(8), true, test::StorageMode::Memory, {},
                        Quorum{4, 2});
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 40)) << cluster.debug();
  EXPECT_EQ(cluster.node(1).membership().election_quorum(), 4u);

  ASSERT_TRUE(cluster.node(1).transfer_leader(5).ok()) << cluster.debug();
  const Result<void> handed = await_transfer(cluster, 1, 20);
  ASSERT_TRUE(handed.ok()) << handed.error() << "\n" << cluster.debug();
  ASSERT_TRUE(cluster.agreed_on(5)) << cluster.debug();
  EXPECT_GT(cluster.node(5).term(), 0u);
}

TEST(LeaderTransfer, FailsWhenTheTargetCannotBeReachedAndDoesNotPickAnother) {
  test::Cluster cluster({1, 2, 3, 4, 5}, test::fast_config(8), false);
  ASSERT_TRUE(cluster.until(
      [&] {
        const std::vector<NodeId> leaders = cluster.leaders();
        return leaders.size() == 1 && cluster.agreed_on(leaders.front());
      },
      80))
      << cluster.debug();
  const NodeId leader = cluster.leaders().front();
  NodeId target = leader == 1 ? 2 : 1;
  const Term term = cluster.node(leader).term();

  ASSERT_TRUE(cluster.node(leader).transfer_leader(target).ok()) << cluster.debug();
  EXPECT_EQ(cluster.node(leader).poll_leader_transfer().error(), Error::Pending);
  EXPECT_EQ(cluster.node(leader).write("during").error(), Error::Rejected);

  cluster.isolate(leader);
  const Result<void> failed = await_transfer(cluster, leader, 16);
  EXPECT_EQ(failed.error(), Error::TransferFailed) << cluster.debug();
  EXPECT_EQ(cluster.node(leader).role(), Role::Leader) << cluster.debug();
  EXPECT_EQ(cluster.node(leader).term(), term);
  EXPECT_EQ(cluster.node(leader).current_leader(), leader);
  const Result<LogIndex> resumed = cluster.node(leader).write("resumed");
  EXPECT_TRUE(resumed.ok()) << resumed.error();

  ASSERT_TRUE(cluster.until(
      [&] {
        for (NodeId id : cluster.leaders()) {
          if (id != leader) {
            return true;
          }
        }
        return false;
      },
      40))
      << cluster.debug();
  EXPECT_EQ(cluster.node(leader).role(), Role::Leader);
  EXPECT_EQ(cluster.node(leader).poll_leader_transfer().error(), Error::TransferFailed);

  cluster.heal();
  ASSERT_TRUE(cluster.until([&] { return cluster.node(leader).role() != Role::Leader; }, 40))
      << cluster.debug();
  EXPECT_EQ(cluster.node(leader).poll_leader_transfer().error(), Error::TransferFailed) << cluster.debug();
  EXPECT_NE(cluster.node(leader).current_leader(), leader);
}

TEST(EngineTransfer, JointElectionNeedsEveryConfig) {
  const Membership joint =
      Membership::joint_of({1, 2, 3}, {3, 4, 5}, {6}, {6}, Quorum{3, 1}, Quorum{3, 1});
  Engine leader(1, pinned(8));
  leader.recover(joint, Vote{}, {});
  become_leader(leader, {2, 3, 4, 5});
  ASSERT_EQ(leader.role(), Role::Leader) << leader.debug();
  EXPECT_TRUE(leader.membership().is_joint());

  EXPECT_FALSE(leader.begin_transfer(1).accepted);
  EXPECT_FALSE(leader.begin_transfer(6).accepted);
  EXPECT_FALSE(leader.begin_transfer(9).accepted);
  EXPECT_EQ(leader.poll_transfer().error(), Error::Rejected);

  std::map<NodeId, Engine> followers;
  for (NodeId id : {2, 3, 4, 5}) {
    followers.emplace(id, Engine(id, pinned(8)));
    followers.at(id).recover(joint, Vote{}, {});
  }
  const StepResult replicated = leader.replicate_now();
  for (const Outbound& message : replicated.outbound) {
    if (followers.find(message.to) == followers.end()) {
      continue;
    }
    const auto* request = std::get_if<AppendEntriesRequest>(&message.payload);
    ASSERT_NE(request, nullptr);
    AppendStep appended = followers.at(message.to).on_append(*request);
    EXPECT_TRUE(appended.response.success);
    leader.on_append_response(message.to, appended.response);
  }

  const Vote authorizing = leader.vote();
  const TransferStart started = leader.begin_transfer(5);
  ASSERT_TRUE(started.accepted) << leader.debug();
  const TimeoutNow* timeout = nullptr;
  for (const Outbound& message : started.step.outbound) {
    if (const auto* request = std::get_if<TimeoutNow>(&message.payload)) {
      EXPECT_EQ(message.to, 5u);
      timeout = request;
    }
  }
  ASSERT_NE(timeout, nullptr);
  EXPECT_EQ(timeout->leader, authorizing);

  const StepResult campaign = followers.at(5).on_timeout_now(*timeout);
  EXPECT_EQ(followers.at(5).role(), Role::Candidate);
  EXPECT_EQ(followers.at(5).term(), authorizing.leader.term + 1);
  // A second TimeoutNow must not open another term.
  followers.at(5).on_timeout_now(*timeout);
  EXPECT_EQ(followers.at(5).term(), authorizing.leader.term + 1);

  std::map<NodeId, VoteResponse> grants;
  for (const Outbound& message : campaign.outbound) {
    EXPECT_FALSE(std::holds_alternative<PreVoteRequest>(message.payload));
    const auto* request = std::get_if<RequestVote>(&message.payload);
    ASSERT_NE(request, nullptr) << message.to;
    EXPECT_EQ(request->body.leadership_transfer_from, std::optional<Vote>{authorizing});
    VoteStep step;
    if (message.to == 1) {
      step = leader.on_vote(request->body);
    } else {
      step = followers.at(message.to).on_vote(request->body);
    }
    EXPECT_TRUE(step.response.granted) << message.to;
    grants.emplace(message.to, step.response);
  }
  ASSERT_EQ(grants.size(), 4u);

  auto respond = [&](std::initializer_list<NodeId> from) {
    for (NodeId id : from) {
      followers.at(5).on_vote_response(id, grants.at(id));
    }
  };
  // New config {3,4,5} is covered by 3 and 4 plus the candidate. The old
  // config {1,2,3} is not.
  respond({3, 4});
  EXPECT_EQ(followers.at(5).role(), Role::Candidate) << followers.at(5).debug();
  respond({2});
  EXPECT_EQ(followers.at(5).role(), Role::Candidate) << followers.at(5).debug();
  respond({1});
  EXPECT_EQ(followers.at(5).role(), Role::Leader) << followers.at(5).debug();
  EXPECT_TRUE(followers.at(5).vote().committed);
}

TEST(EngineTransfer, TimeoutNowDoesNotCampaignALearnerOrABehindLog) {
  Engine learner(4, pinned(6));
  const Vote leader_vote{LeaderId{1, 1}, true};
  learner.recover(Membership{{1, 2, 3}, {4}}, leader_vote, {});
  TimeoutNow request;
  request.leader = leader_vote;
  learner.on_timeout_now(request);
  EXPECT_EQ(learner.role(), Role::Follower);
  EXPECT_EQ(learner.term(), 1u);

  Engine behind(2, pinned(6));
  behind.recover(Membership{{1, 2, 3}}, leader_vote, {});
  AppendEntriesRequest heartbeat;
  heartbeat.vote = leader_vote;
  behind.on_append(heartbeat);
  request.last_log_id = LogId{1, 1, 4};
  behind.on_timeout_now(request);
  EXPECT_EQ(behind.role(), Role::Follower);
  EXPECT_EQ(behind.term(), 1u);
}

TEST(EngineTransfer, StepDownFailsTheTransferAndAbortsTheRead) {
  Engine engine(1, pinned(8));
  engine.recover(Membership{{1, 2, 3}}, Vote{}, {});
  become_leader(engine, {2});
  ASSERT_EQ(engine.role(), Role::Leader) << engine.debug();

  const ReadStart read = engine.begin_linearizable_read();
  ASSERT_TRUE(read.accepted);
  const TransferStart started = engine.begin_transfer(2);
  ASSERT_TRUE(started.accepted);
  EXPECT_EQ(engine.poll_transfer().error(), Error::Pending);
  EXPECT_FALSE(engine.propose("paused").accepted);
  EXPECT_FALSE(engine.begin_linearizable_read().accepted);
  EXPECT_EQ(engine.lease_read().error(), Error::Rejected);
  EXPECT_FALSE(engine.change_membership({1, 2, 3, 4}).accepted);

  AppendEntriesRequest higher;
  higher.vote = Vote{LeaderId{engine.term() + 1, 3}, true};
  higher.leader_commit = engine.commit_index();
  const AppendStep stepped = engine.on_append(higher);
  EXPECT_NE(engine.role(), Role::Leader);
  EXPECT_EQ(engine.current_leader(), 3u);
  EXPECT_EQ(engine.poll_transfer().error(), Error::TransferFailed);
  EXPECT_FALSE(engine.propose("after").accepted);

  bool aborted = false;
  for (const Command& command : stepped.step.commands) {
    if (command.kind == CommandKind::ReadAborted && command.read_id == read.id) {
      aborted = true;
    }
  }
  EXPECT_TRUE(aborted);
}

}  // namespace
}  // namespace easyraft
