// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright 2026 easyraft contributors

#pragma once

#include "easyraft/state_machine.hpp"
#include "easyraft/storage.hpp"

#include <algorithm>
#include <utility>

namespace easyraft {

// In-memory log used by tests and as a reference storage implementation.
class MemoryLogStorage final : public RaftLogStorage {
 public:
  Result<void> save_vote(const Vote& vote) override {
    vote_ = vote;
    has_vote_ = true;
    return Result<void>::success();
  }

  Result<std::optional<Vote>> load_vote() override {
    if (!has_vote_) {
      return Result<std::optional<Vote>>::success(std::nullopt);
    }
    return Result<std::optional<Vote>>::success(vote_);
  }

  Result<void> save_membership(const Membership& membership) override {
    membership_ = membership;
    has_membership_ = true;
    return Result<void>::success();
  }

  Result<std::optional<Membership>> load_membership() override {
    if (!has_membership_) {
      return Result<std::optional<Membership>>::success(std::nullopt);
    }
    return Result<std::optional<Membership>>::success(membership_);
  }

  Result<void> append(const std::vector<LogEntry>& entries) override {
    if (entries.empty()) {
      return Result<void>::success();
    }
    if (entries.front().id.index != log_.size() + 1) {
      return Result<void>::failure(Error::Storage);
    }
    for (std::size_t i = 0; i < entries.size(); ++i) {
      const LogIndex expect = entries.front().id.index + static_cast<LogIndex>(i);
      if (entries[i].id.index != expect) {
        return Result<void>::failure(Error::Storage);
      }
    }
    log_.insert(log_.end(), entries.begin(), entries.end());
    return Result<void>::success();
  }

  Result<void> truncate_from(LogIndex index) override {
    if (index == 0) {
      log_.clear();
      return Result<void>::success();
    }
    if (index > log_.size()) {
      return Result<void>::success();
    }
    log_.resize(static_cast<std::size_t>(index - 1));
    return Result<void>::success();
  }

  Result<std::vector<LogEntry>> read(LogIndex from, LogIndex to) override {
    if (from == 0 || from > to || from > log_.size()) {
      return Result<std::vector<LogEntry>>::success({});
    }
    const LogIndex last = std::min(to, static_cast<LogIndex>(log_.size()));
    std::vector<LogEntry> out;
    out.reserve(static_cast<std::size_t>(last - from + 1));
    for (LogIndex i = from; i <= last; ++i) {
      out.push_back(log_[static_cast<std::size_t>(i - 1)]);
    }
    return Result<std::vector<LogEntry>>::success(std::move(out));
  }

  const Vote& persisted_vote() const { return vote_; }
  const Membership& persisted_membership() const { return membership_; }
  const std::vector<LogEntry>& persisted_log() const { return log_; }
  bool has_membership() const { return has_membership_; }

 private:
  Vote vote_{};
  bool has_vote_{false};
  Membership membership_{};
  bool has_membership_{false};
  std::vector<LogEntry> log_;
};

// Records committed application payloads. Blank and membership entries advance
// the index only.
class MemoryStateMachine final : public RaftStateMachine {
 public:
  Result<void> apply(const LogEntry& entry) override {
    if (entry.id.index <= last_applied_) {
      return Result<void>::success();
    }
    if (entry.id.index != last_applied_ + 1) {
      return Result<void>::failure(Error::Storage);
    }
    last_applied_ = entry.id.index;
    if (entry.kind == EntryKind::Application) {
      data_.push_back(entry.data);
    }
    return Result<void>::success();
  }

  LogIndex last_applied() const override { return last_applied_; }
  const std::vector<std::string>& data() const { return data_; }

 private:
  LogIndex last_applied_{0};
  std::vector<std::string> data_;
};

}  // namespace easyraft
