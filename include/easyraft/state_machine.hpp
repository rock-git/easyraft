// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright 2026 easyraft contributors

#pragma once

#include "easyraft/error.hpp"
#include "easyraft/types.hpp"

#include <string_view>

namespace easyraft {

// Applies committed log entries in index order. Blank entries are delivered so
// the implementation can advance its applied index; application data is the
// payload clients care about.
//
// `build_snapshot` captures applied state. `install_snapshot` replaces that
// state with a snapshot; afterwards `last_applied()` is the snapshot's last
// included index. The bytes are opaque to the protocol.
class RaftStateMachine {
 public:
  virtual ~RaftStateMachine() = default;

  virtual Result<void> apply(const LogEntry& entry) = 0;
  virtual LogIndex last_applied() const = 0;
  virtual Result<std::string> build_snapshot() = 0;
  virtual Result<void> install_snapshot(const SnapshotMeta& meta, std::string_view data) = 0;
};

}  // namespace easyraft
