// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright 2026 easyraft contributors

#pragma once

#include "easyraft/types.hpp"

#include <cstdint>
#include <optional>
#include <variant>
#include <vector>

namespace easyraft {

struct VoteRequest {
  Vote vote{};
  std::optional<LogId> last_log_id{};
};

struct VoteResponse {
  Vote vote{};
  std::optional<LogId> last_log_id{};
  bool granted{false};
};

struct AppendEntriesRequest {
  Vote vote{};
  std::optional<LogId> prev_log_id{};
  std::vector<LogEntry> entries;
  LogIndex leader_commit{0};
};

struct AppendEntriesResponse {
  Vote responder_vote{};
  Vote request_vote{};
  bool success{false};
  // The responder's vote rejected the request. The log was not truncated.
  bool rejected_by_vote{false};
  LogIndex match_index{0};
  // Index the leader was trying to send (prev.index + 1, or 1).
  LogIndex probed_next{1};
  // Next index the leader should try after a conflict.
  LogIndex hint_next{1};
};

// Distinct wrappers so PreVote cannot be confused with a real RequestVote.
// `probe` identifies one PreVote round. A delayed grant from an older round
// must not be counted toward a later round that carries the same hypothetical vote.
struct PreVoteRequest {
  VoteRequest body{};
  std::uint64_t probe{0};
};
struct PreVoteResponse {
  VoteResponse body{};
  std::uint64_t probe{0};
};
struct RequestVote {
  VoteRequest body{};
};
struct RequestVoteResponse {
  VoteResponse body{};
};

using Payload = std::variant<PreVoteRequest,
                             PreVoteResponse,
                             RequestVote,
                             RequestVoteResponse,
                             AppendEntriesRequest,
                             AppendEntriesResponse>;

struct Envelope {
  NodeId from{0};
  NodeId to{0};
  Payload payload{PreVoteRequest{}};
};

}  // namespace easyraft
