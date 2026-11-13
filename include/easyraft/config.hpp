// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright 2026 easyraft contributors

#pragma once

#include <cstddef>

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
    return copy;
  }
};

}  // namespace easyraft
