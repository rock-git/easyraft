// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright 2026 easyraft contributors

#pragma once

#include "easyraft/error.hpp"
#include "easyraft/types.hpp"

namespace easyraft {

// Applies committed log entries in index order. Blank entries are delivered so
// the implementation can advance its applied index; application data is the
// payload clients care about. An in-memory machine is enough for phase 1.
class RaftStateMachine {
 public:
  virtual ~RaftStateMachine() = default;

  virtual Result<void> apply(const LogEntry& entry) = 0;
  virtual LogIndex last_applied() const = 0;
};

}  // namespace easyraft
