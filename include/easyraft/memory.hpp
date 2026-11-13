// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright 2026 easyraft contributors

#pragma once

#include "easyraft/state_machine.hpp"
#include "easyraft/storage.hpp"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
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

  Result<void> save_snapshot(const Snapshot& snapshot) override {
    if (!snapshot_acceptable(snapshot)) {
      return Result<void>::failure(Error::Storage);
    }
    snapshot_ = snapshot;
    compacted_through_ = snapshot.meta.last_included.index;
    return Result<void>::success();
  }

  Result<std::optional<Snapshot>> load_snapshot() override {
    return Result<std::optional<Snapshot>>::success(snapshot_);
  }

  Result<void> purge_through(LogIndex index) override {
    if (!snapshot_ || index != snapshot_->meta.last_included.index) {
      return Result<void>::failure(Error::Storage);
    }
    std::vector<LogEntry> kept;
    kept.reserve(log_.size());
    for (const LogEntry& entry : log_) {
      if (entry.id.index > index) {
        kept.push_back(entry);
      }
    }
    log_ = std::move(kept);
    compacted_through_ = index;
    return Result<void>::success();
  }

  Result<void> append(const std::vector<LogEntry>& entries) override {
    if (entries.empty()) {
      return Result<void>::success();
    }
    const LogIndex base = log_.empty() ? compacted_through_ + 1 : log_.back().id.index + 1;
    if (entries.front().id.index != base) {
      return Result<void>::failure(Error::Storage);
    }
    for (std::size_t i = 0; i < entries.size(); ++i) {
      const LogIndex expect = base + static_cast<LogIndex>(i);
      if (entries[i].id.index != expect) {
        return Result<void>::failure(Error::Storage);
      }
    }
    log_.insert(log_.end(), entries.begin(), entries.end());
    return Result<void>::success();
  }

  Result<void> truncate_from(LogIndex index) override {
    // Index 0 clears every log record and leaves a snapshot in place. Any
    // other index at or below the snapshot would discard the committed prefix
    // that image represents.
    if (index == 0) {
      log_.clear();
      return Result<void>::success();
    }
    if (index <= compacted_through_) {
      return Result<void>::failure(Error::Storage);
    }
    if (log_.empty() || index > log_.back().id.index) {
      return Result<void>::success();
    }
    const LogIndex first = log_.front().id.index;
    if (index <= first) {
      log_.clear();
      return Result<void>::success();
    }
    log_.resize(static_cast<std::size_t>(index - first));
    return Result<void>::success();
  }

  Result<std::vector<LogEntry>> read(LogIndex from, LogIndex to) override {
    if (from == 0 || from > to || log_.empty() || from > log_.back().id.index) {
      return Result<std::vector<LogEntry>>::success({});
    }
    const LogIndex first = log_.front().id.index;
    const LogIndex start = std::max(from, first);
    const LogIndex last = std::min(to, log_.back().id.index);
    if (start > last) {
      return Result<std::vector<LogEntry>>::success({});
    }
    std::vector<LogEntry> out;
    out.reserve(static_cast<std::size_t>(last - start + 1));
    for (LogIndex i = start; i <= last; ++i) {
      out.push_back(log_[static_cast<std::size_t>(i - first)]);
    }
    return Result<std::vector<LogEntry>>::success(std::move(out));
  }

  const Vote& persisted_vote() const { return vote_; }
  const Membership& persisted_membership() const { return membership_; }
  const std::vector<LogEntry>& persisted_log() const { return log_; }
  bool has_membership() const { return has_membership_; }

 private:
  bool snapshot_acceptable(const Snapshot& snapshot) const {
    if (snapshot.meta.last_included.index == 0 || snapshot.meta.snapshot_id.empty()) {
      return false;
    }
    if (snapshot_ && snapshot.meta.last_included.index < snapshot_->meta.last_included.index) {
      return false;
    }
    if (snapshot_ && snapshot.meta.last_included.index == snapshot_->meta.last_included.index &&
        snapshot.meta.last_included != snapshot_->meta.last_included) {
      return false;
    }
    return true;
  }

  Vote vote_{};
  bool has_vote_{false};
  Membership membership_{};
  bool has_membership_{false};
  std::optional<Snapshot> snapshot_;
  LogIndex compacted_through_{0};
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

  // Length-prefixed application payloads. Not a disk format; FileLogStorage
  // has its own snapshot record.
  Result<std::string> build_snapshot() override {
    std::string out;
    if (data_.size() > static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
      return Result<std::string>::failure(Error::Storage);
    }
    append_u32(out, static_cast<std::uint32_t>(data_.size()));
    for (const std::string& row : data_) {
      if (row.size() > static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
        return Result<std::string>::failure(Error::Storage);
      }
      append_u32(out, static_cast<std::uint32_t>(row.size()));
      out.append(row);
    }
    return Result<std::string>::success(std::move(out));
  }

  Result<void> install_snapshot(const SnapshotMeta& meta, std::string_view bytes) override {
    if (meta.last_included.index < last_applied_) {
      return Result<void>::failure(Error::Rejected);
    }
    if (bytes.size() < 4) {
      return Result<void>::failure(Error::Storage);
    }
    std::uint32_t count = 0;
    if (!read_u32(bytes, 0, count) ||
        static_cast<std::size_t>(count) > (bytes.size() - 4) / 4) {
      return Result<void>::failure(Error::Storage);
    }
    std::size_t offset = 4;
    std::vector<std::string> rows;
    rows.reserve(count);
    for (std::uint32_t i = 0; i < count; ++i) {
      std::uint32_t len = 0;
      if (!read_u32(bytes, offset, len) || bytes.size() - offset < 4u + static_cast<std::size_t>(len)) {
        return Result<void>::failure(Error::Storage);
      }
      offset += 4;
      rows.emplace_back(bytes.substr(offset, len));
      offset += len;
    }
    if (offset != bytes.size()) {
      return Result<void>::failure(Error::Storage);
    }
    data_ = std::move(rows);
    last_applied_ = meta.last_included.index;
    return Result<void>::success();
  }

  const std::vector<std::string>& data() const { return data_; }

 private:
  static void append_u32(std::string& out, std::uint32_t value) {
    for (int shift = 0; shift < 32; shift += 8) {
      out.push_back(static_cast<char>((value >> shift) & 0xffu));
    }
  }

  static bool read_u32(std::string_view bytes, std::size_t offset, std::uint32_t& out) {
    if (offset > bytes.size() || bytes.size() - offset < 4) {
      return false;
    }
    const auto* data = reinterpret_cast<const unsigned char*>(bytes.data());
    out = static_cast<std::uint32_t>(data[offset]) |
          (static_cast<std::uint32_t>(data[offset + 1]) << 8) |
          (static_cast<std::uint32_t>(data[offset + 2]) << 16) |
          (static_cast<std::uint32_t>(data[offset + 3]) << 24);
    return true;
  }

  LogIndex last_applied_{0};
  std::vector<std::string> data_;
};

}  // namespace easyraft
