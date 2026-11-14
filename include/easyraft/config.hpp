// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright 2026 easyraft contributors

#pragma once

#include <cstddef>
#include <cstdint>

namespace easyraft {

// Time is measured in ticks injected by RaftNode::tick(). Tests pin min == max
// so elections are deterministic. Production code maps one tick to a wall-clock
// duration of its choosing.
struct Config {
  int heartbeat_interval{1};
  int election_timeout_min{10};
  int election_timeout_max{20};
  bool enable_pre_vote{true};
  std::size_t max_append_entries{64};
  // Applied entries past the current snapshot before this node compacts.
  // Zero disables automatic snapshots, so tests that never opt in keep the
  // full log. A follower may cross the threshold; sending the result is
  // still leader-only.
  std::uint64_t snapshot_threshold{0};
  // Largest snapshot payload in one InstallSnapshot request. Zero sends the
  // whole image in a single request.
  std::size_t max_snapshot_chunk{0};
  // Linearizable reads kept until the caller polls a terminal result or
  // cancels them. Ready, pending, and aborted ids all count. Zero is raised
  // to one so the set cannot grow without a bound.
  std::size_t max_in_flight_reads{1024};

  Config normalized() const {
    Config copy = *this;
    if (copy.heartbeat_interval < 1) {
      copy.heartbeat_interval = 1;
    }
    if (copy.election_timeout_min < copy.heartbeat_interval + 1) {
      copy.election_timeout_min = copy.heartbeat_interval + 1;
    }
    if (copy.election_timeout_max < copy.election_timeout_min) {
      copy.election_timeout_max = copy.election_timeout_min;
    }
    if (copy.max_append_entries == 0) {
      copy.max_append_entries = 1;
    }
    if (copy.max_in_flight_reads == 0) {
      copy.max_in_flight_reads = 1;
    }
    return copy;
  }
};

}  // namespace easyraft
