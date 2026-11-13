// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright 2026 easyraft contributors

#pragma once

#include "easyraft/error.hpp"
#include "easyraft/types.hpp"

#include <optional>
#include <vector>

namespace easyraft {

// Persistent Raft log and hard state.
//
// Phase 1 persists the vote, the fixed membership, and the log. The commit
// index is not persisted: after a restart the node is a follower and a leader
// re-commits the prefix. Truncation deletes the suffix starting at `index`
// (inclusive) and must not be paired with a blind delete-then-append of a
// matching prefix; the engine truncates only a conflicting suffix.
class RaftLogStorage {
 public:
  virtual ~RaftLogStorage() = default;

  virtual Result<void> save_vote(const Vote& vote) = 0;
  virtual Result<std::optional<Vote>> load_vote() = 0;

  virtual Result<void> save_membership(const Membership& membership) = 0;
  virtual Result<std::optional<Membership>> load_membership() = 0;

  virtual Result<void> append(const std::vector<LogEntry>& entries) = 0;
  virtual Result<void> truncate_from(LogIndex index) = 0;
  // Inclusive range. An empty range returns an empty vector.
  virtual Result<std::vector<LogEntry>> read(LogIndex from, LogIndex to) = 0;
};

}  // namespace easyraft
