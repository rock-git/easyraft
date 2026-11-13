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
// Persists the vote, the membership (one uniform config or a joint pair), the
// log, and at most one snapshot of applied state. The commit index is not
// persisted past that snapshot: after a restart the node is a follower and a
// leader re-commits the suffix. Truncation deletes the suffix starting at
// `index` (inclusive) and must not delete a snapshot or the committed prefix
// that snapshot represents. The engine truncates only a conflicting suffix.
//
// `save_snapshot` makes the image durable and does not itself delete log
// entries. `purge_through` then drops the log prefix up through the snapshot's
// last included index. After that purge the first stored index, when the log
// is not empty, is `last_included + 1`.
class RaftLogStorage {
 public:
  virtual ~RaftLogStorage() = default;

  virtual Result<void> save_vote(const Vote& vote) = 0;
  virtual Result<std::optional<Vote>> load_vote() = 0;

  virtual Result<void> save_membership(const Membership& membership) = 0;
  virtual Result<std::optional<Membership>> load_membership() = 0;

  virtual Result<void> save_snapshot(const Snapshot& snapshot) = 0;
  virtual Result<std::optional<Snapshot>> load_snapshot() = 0;
  // Drop log entries at or below `index`. `index` must be the current
  // snapshot's last included index. A second call is a no-op.
  virtual Result<void> purge_through(LogIndex index) = 0;

  virtual Result<void> append(const std::vector<LogEntry>& entries) = 0;
  virtual Result<void> truncate_from(LogIndex index) = 0;
  // Inclusive range. An empty range returns an empty vector. Entries the
  // snapshot has already purged are not returned.
  virtual Result<std::vector<LogEntry>> read(LogIndex from, LogIndex to) = 0;
};

}  // namespace easyraft
