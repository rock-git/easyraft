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
//   membership   current uniform or joint voter config
//   log          append-only log, possibly compacted
//   snapshot     one image of applied state, when a snapshot has been saved
//
// `vote`, `membership`, and `snapshot` are replaced crash-safely: write
// `name.tmp`, fsync the file, rename it onto `name`, and fsync the directory.
// Creating the directory also fsyncs each parent that gained a new entry. A
// missing metadata file loads as nullopt. A leftover `snapshot.tmp` or
// `log.rewrite` from a crash is deleted on open and never installed.
//
// A uniform membership is still the version-1 record: magic, version 1,
// voter count, voter ids, crc32. A joint membership is version 2: magic,
// version 2, config count (2), then each config's count and ids, then crc32.
// Version 1 files load as a uniform membership. The snapshot file does not
// change those versions. It stores its own record: magic "ERSS", version 1,
// the last included log id, a snapshot id, `Membership::encode` bytes, the
// opaque state-machine image, and a crc32.
//
// `log` starts with magic "ERLG" and version 1. Each record is little-endian
// `uint32 body_len`, `uint32 crc32` of those length bytes, a body, and
// `uint32 crc32(body)`. The body holds index, term, node, kind, data length,
// and data. Kind 0 is blank, 1 is application, and 2 is membership (`data`
// is the membership encoding). After a purge the first record may be
// `last_included + 1` rather than 1; the records that remain are still
// contiguous. `append` fsyncs the file before success. `truncate_from`
// ftruncates the suffix and fsyncs, and refuses to cut into a snapshot.
// `purge_through` rewrites the kept suffix to `log.rewrite`, fsyncs, renames
// it onto `log`, and fsyncs the directory.
//
// On open, a final record is dropped only when its length checksum still
// matches and the record is incomplete or its body checksum fails. A length
// checksum mismatch is not a torn tail: open returns Error::Storage and
// leaves the file unchanged. Any other integrity or I/O failure does the
// same. A corrupt snapshot is detected before torn-tail recovery, so the open
// fails and the log file is left unchanged. The commit index is not stored
// past the snapshot.
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
  Result<void> save_snapshot(const Snapshot& snapshot) override;
  Result<std::optional<Snapshot>> load_snapshot() override;
  Result<void> purge_through(LogIndex index) override;
  Result<void> append(const std::vector<LogEntry>& entries) override;
  Result<void> truncate_from(LogIndex index) override;
  Result<std::vector<LogEntry>> read(LogIndex from, LogIndex to) override;

 private:
  bool open_directory(std::filesystem::path directory);
  bool open_log();
  bool recover_log();
  bool write_log_header();
  bool rewrite_log(const std::vector<LogEntry>& entries);
  bool snapshot_acceptable(const Snapshot& snapshot) const;
  Result<void> write_atomic(const char* name, const std::string& bytes);
  Result<std::optional<std::string>> read_file(const char* name,
                                                std::uint64_t max_bytes) const;

  std::filesystem::path directory_;
  int dir_fd_{-1};
  int log_fd_{-1};
  bool failed_{false};
  std::optional<Snapshot> snapshot_;
  LogIndex compacted_through_{0};
  std::vector<LogEntry> log_;
  std::vector<std::uint64_t> offsets_;
  std::uint64_t end_offset_{0};
};

}  // namespace easyraft
