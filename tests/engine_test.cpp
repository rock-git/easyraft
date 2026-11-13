// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright 2026 easyraft contributors

#include "easyraft/engine.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <string>
#include <variant>

namespace easyraft {
namespace {

LogEntry entry(Term term, NodeId node, LogIndex index, EntryKind kind, std::string data = {}) {
  LogEntry value;
  value.id = LogId{term, node, index};
  value.kind = kind;
  value.data = std::move(data);
  return value;
}

Config pinned(int timeout) {
  Config config;
  config.heartbeat_interval = 1;
  config.election_timeout_min = timeout;
  config.election_timeout_max = timeout;
  config.enable_pre_vote = true;
  return config;
}

TEST(VoteOrder, HigherNodeIdAndCommittedFlag) {
  const Vote low{LeaderId{1, 1}, false};
  const Vote same_term_higher_node{LeaderId{1, 2}, false};
  const Vote committed{LeaderId{1, 2}, true};
  const Vote next_term{LeaderId{2, 1}, false};

  EXPECT_GT(same_term_higher_node, low);
  EXPECT_GT(committed, same_term_higher_node);
  EXPECT_GT(next_term, committed);
  EXPECT_EQ(low, (Vote{LeaderId{1, 1}, false}));
}

TEST(Engine, SingleNodeTickAppendsAndCommits) {
  Engine engine(1, pinned(2));
  engine.recover(Membership{{1}}, Vote{}, {});

  engine.on_tick();
  EXPECT_NE(engine.role(), Role::Leader);
  engine.on_tick();
  EXPECT_EQ(engine.role(), Role::Leader);
  EXPECT_EQ(engine.term(), 1);
  EXPECT_EQ(engine.log().size(), 1u);
  EXPECT_EQ(engine.log().front().kind, EntryKind::Blank);
  EXPECT_EQ(engine.commit_index(), 1u);

  const ProposeResult proposed = engine.propose("hello");
  EXPECT_TRUE(proposed.accepted);
  EXPECT_EQ(proposed.index, 2u);
  EXPECT_EQ(engine.commit_index(), 2u);

  bool applied = false;
  for (const Command& command : proposed.step.commands) {
    if (command.kind == CommandKind::Apply && command.entry.data == "hello") {
      applied = true;
    }
  }
  EXPECT_TRUE(applied);
}

TEST(Engine, PreVoteDoesNotPersistOrBumpTerm) {
  Engine engine(2, pinned(5));
  engine.recover(Membership{{1, 2, 3}}, Vote{}, {});
  const Vote before = engine.vote();

  VoteRequest request;
  request.vote = Vote{LeaderId{1, 3}, false};
  const VoteStep probed = engine.on_pre_vote(request);

  EXPECT_TRUE(probed.response.granted);
  EXPECT_EQ(engine.vote(), before);
  EXPECT_TRUE(probed.step.commands.empty());
  EXPECT_EQ(engine.term(), 0u);
}

TEST(Engine, RealVotePersistsGrant) {
  Engine engine(2, pinned(5));
  engine.recover(Membership{{1, 2, 3}}, Vote{}, {});

  VoteRequest request;
  request.vote = Vote{LeaderId{1, 3}, false};
  const VoteStep granted = engine.on_vote(request);

  EXPECT_TRUE(granted.response.granted);
  EXPECT_EQ(engine.vote().leader.term, 1u);
  EXPECT_EQ(engine.vote().leader.node, 3u);
  EXPECT_FALSE(engine.vote().committed);
  ASSERT_FALSE(granted.step.commands.empty());
  EXPECT_EQ(granted.step.commands.front().kind, CommandKind::SaveVote);
}

TEST(Engine, StaleLogDoesNotGrantOrBumpTerm) {
  Engine engine(2, pinned(5));
  Vote vote;
  vote.leader = LeaderId{4, 1};
  vote.committed = true;
  std::vector<LogEntry> log;
  log.push_back(entry(4, 1, 1, EntryKind::Blank));
  log.push_back(entry(4, 1, 2, EntryKind::Application, "kept"));
  engine.recover(Membership{{1, 2, 3}}, vote, log);

  VoteRequest request;
  request.vote = Vote{LeaderId{6, 3}, false};
  const VoteStep rejected = engine.on_vote(request);

  EXPECT_FALSE(rejected.response.granted);
  EXPECT_EQ(engine.term(), 4u);
  EXPECT_EQ(engine.vote().leader.node, 1u);
  EXPECT_TRUE(rejected.step.commands.empty());
}

TEST(Engine, ConflictingSuffixIsTruncatedAndOverwritten) {
  Engine engine(2, pinned(5));
  Vote vote;
  vote.leader = LeaderId{4, 2};
  vote.committed = false;
  std::vector<LogEntry> log;
  log.push_back(entry(1, 1, 1, EntryKind::Blank));
  log.push_back(entry(1, 1, 2, EntryKind::Application, "a"));
  log.push_back(entry(4, 2, 3, EntryKind::Application, "diverge"));
  engine.recover(Membership{{1, 2, 3}}, vote, std::move(log));

  AppendEntriesRequest request;
  request.vote = Vote{LeaderId{5, 1}, true};
  request.prev_log_id = LogId{1, 1, 2};
  request.leader_commit = 4;
  request.entries.push_back(entry(5, 1, 3, EntryKind::Blank));
  request.entries.push_back(entry(5, 1, 4, EntryKind::Application, "right"));

  const AppendStep appended = engine.on_append(request);
  EXPECT_TRUE(appended.response.success);
  ASSERT_EQ(engine.log().size(), 4u);
  EXPECT_EQ(engine.log()[0].data, "");
  EXPECT_EQ(engine.log()[1].data, "a");
  EXPECT_EQ(engine.log()[2].kind, EntryKind::Blank);
  EXPECT_EQ(engine.log()[2].id.term, 5u);
  EXPECT_EQ(engine.log()[3].data, "right");
  EXPECT_EQ(engine.commit_index(), 4u);

  bool truncated = false;
  for (const Command& command : appended.step.commands) {
    if (command.kind == CommandKind::Truncate && command.index == 3) {
      truncated = true;
    }
  }
  EXPECT_TRUE(truncated);
}

std::uint64_t prevote_probe(const StepResult& step) {
  for (const Outbound& message : step.outbound) {
    if (const auto* request = std::get_if<PreVoteRequest>(&message.payload)) {
      return request->probe;
    }
  }
  return 0;
}

TEST(Engine, NonVoterGrantDoesNotCompleteElection) {
  Engine engine(1, pinned(2));
  engine.recover(Membership{{1, 2, 3}}, Vote{}, {});
  engine.on_tick();
  const StepResult started = engine.on_tick();
  const std::uint64_t probe = prevote_probe(started);
  ASSERT_NE(probe, 0u);

  PreVoteResponse outsider;
  outsider.probe = probe;
  outsider.body.granted = true;
  engine.on_pre_vote_response(99, outsider);
  EXPECT_EQ(engine.term(), 0u);
  EXPECT_EQ(engine.role(), Role::Follower);

  PreVoteResponse voter;
  voter.probe = probe;
  voter.body.granted = true;
  engine.on_pre_vote_response(2, voter);
  EXPECT_EQ(engine.term(), 1u);
  EXPECT_EQ(engine.role(), Role::Candidate);

  VoteResponse forged;
  forged.granted = true;
  forged.vote = engine.vote();
  engine.on_vote_response(99, forged);
  EXPECT_NE(engine.role(), Role::Leader);

  engine.on_vote_response(2, forged);
  EXPECT_EQ(engine.role(), Role::Leader);
}

TEST(Engine, StalePreVoteGrantDoesNotStartElection) {
  Engine engine(1, pinned(2));
  engine.recover(Membership{{1, 2, 3}}, Vote{}, {});
  engine.on_tick();
  const std::uint64_t first = prevote_probe(engine.on_tick());
  ASSERT_NE(first, 0u);
  engine.on_tick();
  const std::uint64_t second = prevote_probe(engine.on_tick());
  ASSERT_NE(second, 0u);
  ASSERT_NE(first, second);

  PreVoteResponse stale;
  stale.probe = first;
  stale.body.granted = true;
  engine.on_pre_vote_response(2, stale);
  EXPECT_EQ(engine.term(), 0u);
  EXPECT_EQ(engine.role(), Role::Follower);

  PreVoteResponse current;
  current.probe = second;
  current.body.granted = true;
  engine.on_pre_vote_response(2, current);
  EXPECT_EQ(engine.term(), 1u);
  EXPECT_EQ(engine.role(), Role::Candidate);
}

TEST(Engine, LeaderHeartbeatCancelsOutstandingPreVote) {
  Engine engine(2, pinned(4));
  Vote leader_vote;
  leader_vote.leader = LeaderId{1, 1};
  leader_vote.committed = true;
  engine.recover(Membership{{1, 2, 3}}, leader_vote, {});

  std::uint64_t probe = 0;
  for (int tick = 0; tick < 4; ++tick) {
    probe = std::max(probe, prevote_probe(engine.on_tick()));
  }
  ASSERT_NE(probe, 0u);
  EXPECT_EQ(engine.term(), 1u);

  AppendEntriesRequest heartbeat;
  heartbeat.vote = leader_vote;
  engine.on_append(heartbeat);

  PreVoteResponse delayed;
  delayed.probe = probe;
  delayed.body.granted = true;
  engine.on_pre_vote_response(3, delayed);
  EXPECT_EQ(engine.role(), Role::Follower);
  EXPECT_EQ(engine.term(), 1u);
}

TEST(Engine, PrevMismatchTruncatesOnlyTheConflictingSuffix) {
  Engine engine(2, pinned(5));
  Vote vote;
  vote.leader = LeaderId{2, 2};
  vote.committed = false;
  std::vector<LogEntry> log;
  log.push_back(entry(1, 1, 1, EntryKind::Application, "keep"));
  log.push_back(entry(1, 1, 2, EntryKind::Application, "match"));
  log.push_back(entry(2, 2, 3, EntryKind::Application, "drop"));
  engine.recover(Membership{{1, 2}}, vote, std::move(log));

  AppendEntriesRequest request;
  request.vote = Vote{LeaderId{3, 1}, true};
  request.prev_log_id = LogId{9, 1, 2};
  request.entries.push_back(entry(3, 1, 3, EntryKind::Application, "new"));

  const AppendStep rejected = engine.on_append(request);
  EXPECT_FALSE(rejected.response.success);
  ASSERT_EQ(engine.log().size(), 1u);
  EXPECT_EQ(engine.log()[0].data, "keep");
  EXPECT_EQ(rejected.response.hint_next, 2u);
}

void grant_prevote(Engine& engine, NodeId from, std::uint64_t probe) {
  PreVoteResponse response;
  response.probe = probe;
  response.body.granted = true;
  engine.on_pre_vote_response(from, response);
}

TEST(Engine, JointQuorumCountsEveryConfig) {
  Engine engine(1, pinned(4));
  engine.recover(Membership::joint_of({1, 2, 3}, {3, 4, 5}), Vote{}, {});

  std::uint64_t probe = 0;
  for (int tick = 0; tick < 4; ++tick) {
    probe = std::max(probe, prevote_probe(engine.on_tick()));
  }
  ASSERT_NE(probe, 0u);
  grant_prevote(engine, 2, probe);
  grant_prevote(engine, 3, probe);
  EXPECT_EQ(engine.term(), 0u);
  grant_prevote(engine, 4, probe);
  EXPECT_EQ(engine.term(), 1u);
  EXPECT_EQ(engine.role(), Role::Candidate);

  VoteResponse granted;
  granted.granted = true;
  granted.vote = engine.vote();
  engine.on_vote_response(2, granted);
  engine.on_vote_response(3, granted);
  EXPECT_NE(engine.role(), Role::Leader);
  engine.on_vote_response(4, granted);
  EXPECT_EQ(engine.role(), Role::Leader);

  const ProposeResult proposed = engine.propose("joint");
  ASSERT_TRUE(proposed.accepted);
  const LogIndex index = proposed.index;
  EXPECT_EQ(engine.commit_index(), 0u);

  AppendEntriesResponse ack;
  ack.success = true;
  ack.request_vote = engine.vote();
  ack.responder_vote = engine.vote().non_committed();
  ack.match_index = index;
  engine.on_append_response(4, ack);
  engine.on_append_response(5, ack);
  EXPECT_EQ(engine.commit_index(), 0u) << "new-config majority alone must not commit";

  engine.on_append_response(2, ack);
  EXPECT_EQ(engine.commit_index(), index);
}

}  // namespace
}  // namespace easyraft
