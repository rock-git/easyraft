// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright 2026 easyraft contributors

#pragma once

#include "easyraft/storage.hpp"

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

namespace easyraft {

// Durable RaftLogStorage rooted at one directory. Not thread-safe.
//
// Layout:
//   vote         current vote
//   membership   current voter set
//   log          append-only log
//
// `vote` and `membership` are replaced crash-safely: write `name.tmp`, fsync
// the file, rename it onto `name`, and fsync the directory. Creating the
// directory also fsyncs each parent that gained a new entry. A missing
// metadata file loads as nullopt.
//
// `log` starts with magic "ERLG" and version 1. Each record is little-endian
// `uint32 body_len`, `uint32 crc32` of those length bytes, a body, and
// `uint32 crc32(body)`. The body holds index, term, node, kind, data length,
// and data. `append` fsyncs the file before success. `truncate_from`
// ftruncates the suffix and fsyncs.
//
// On open, a final record is dropped only when its length checksum still
// matches and the record is incomplete or its body checksum fails. A length
// checksum mismatch is not a torn tail: open returns Error::Storage and
// leaves the file unchanged. Any other integrity or I/O failure does the
// same. The commit index is not stored.
class FileLogStorage final : public RaftLogStorage {
 public:
  explicit FileLogStorage(std::filesystem::path directory);
  ~FileLogStorage() override;

  FileLogStorage(const FileLogStorage&) = delete;
  FileLogStorage& operator=(const FileLogStorage&) = delete;
  FileLogStorage(FileLogStorage&&) = delete;
  FileLogStorage& operator=(FileLogStorage&&) = delete;

  Result<void> save_vote(const Vote& vote) override;
  Result<std::optional<Vote>> load_vote() override;
  Result<void> save_membership(const Membership& membership) override;
  Result<std::optional<Membership>> load_membership() override;
  Result<void> append(const std::vector<LogEntry>& entries) override;
  Result<void> truncate_from(LogIndex index) override;
  Result<std::vector<LogEntry>> read(LogIndex from, LogIndex to) override;

 private:
  bool open_directory(std::filesystem::path directory);
  bool open_log();
  bool recover_log();
  bool write_log_header();
  Result<void> write_atomic(const char* name, const std::string& bytes);
  Result<std::optional<std::string>> read_file(const char* name) const;

  std::filesystem::path directory_;
  int dir_fd_{-1};
  int log_fd_{-1};
  bool failed_{false};
  std::vector<LogEntry> log_;
  std::vector<std::uint64_t> offsets_;
  std::uint64_t end_offset_{0};
};

}  // namespace easyraft
