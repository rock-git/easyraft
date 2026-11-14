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
  // Bumped when a leader starts a linearizable-read confirmation. A response
  // echoes it so an ack that left before that round cannot confirm the read.
  std::uint64_t heartbeat_seq{0};
  // Leader tick when this request was built. A response echoes it so a delayed
  // ack renews a read lease from the send tick, not from the arrival tick.
  std::uint64_t sent_tick{0};
};

struct AppendEntriesResponse {
  Vote responder_vote{};
  Vote request_vote{};
  bool success{false};
  // The responder's vote rejected the request. The log was not truncated.
  bool rejected_by_vote{false};
  LogIndex match_index{0};
  // Copied from the request. Zero when the sender did not set a round.
  std::uint64_t heartbeat_seq{0};
  // Copied from the request. The leader's read lease expires from this tick.
  std::uint64_t sent_tick{0};
  // Responder's `election_timeout_min`, in ticks. Zero does not count as a
  // read-lease acknowledgement. The leader never trusts a longer interval
  // than its own minimum.
  std::uint64_t voter_timeout_min{0};
  // Index the leader was trying to send (prev.index + 1, or 1).
  LogIndex probed_next{1};
  // Next index the leader should try after a conflict.
  LogIndex hint_next{1};
  // The probe is inside a compacted prefix. `hint_next` is the first index
  // still in the log, and the leader must move forward rather than backward.
  bool compacted{false};
};

// One chunk of a snapshot. `offset` is the byte offset into `meta`'s image.
// `done` marks the last chunk. A follower installs only after that chunk.
struct InstallSnapshotRequest {
  Vote vote{};
  SnapshotMeta meta{};
  std::uint64_t offset{0};
  std::string data;
  bool done{false};
  // Leader tick when this chunk was built. Echoed so a delayed install ack
  // cannot extend a read lease up to the arrival tick.
  std::uint64_t sent_tick{0};
};

struct InstallSnapshotResponse {
  Vote responder_vote{};
  Vote request_vote{};
  bool success{false};
  // The responder's vote rejected the request. Nothing was installed.
  bool rejected_by_vote{false};
  // `meta` is not newer than the responder's snapshot or applied index.
  bool stale{false};
  // The follower has accepted every byte of this image.
  bool installed{false};
  std::uint64_t received{0};
  // Responder's last applied index after handling the request.
  LogIndex follower_index{0};
  // Snapshot id from the request. A leader ignores progress updates for any
  // other id, so a delayed chunk cannot move a newer transfer.
  std::string snapshot_id;
  // Copied from the request.
  std::uint64_t sent_tick{0};
  // Responder's `election_timeout_min`, in ticks. Zero is not read-lease evidence.
  std::uint64_t voter_timeout_min{0};
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
                             AppendEntriesResponse,
                             InstallSnapshotRequest,
                             InstallSnapshotResponse>;

struct Envelope {
  NodeId from{0};
  NodeId to{0};
  Payload payload{PreVoteRequest{}};
};

}  // namespace easyraft
