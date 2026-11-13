// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright 2026 easyraft contributors

#pragma once

#include "easyraft/messages.hpp"

namespace easyraft {

// Transport used by RaftNode. Implementations must not deliver a message
// re-entrantly inside send(); queue it and flush later. The in-process test
// network does this so a tick stays deterministic.
class MessageBus {
 public:
  virtual ~MessageBus() = default;
  virtual void send(const Envelope& message) = 0;
};

}  // namespace easyraft
