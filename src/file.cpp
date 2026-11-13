// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright 2026 easyraft contributors

#include "easyraft/file.hpp"

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace easyraft {
namespace {

constexpr std::uint32_t kVoteMagic = 0x54565245u;         // 'ERVT'
constexpr std::uint32_t kMembershipMagic = 0x424D5245u;   // 'ERMB'
constexpr std::uint32_t kLogMagic = 0x474C5245u;          // 'ERLG'
constexpr std::uint32_t kVersion = 1u;
constexpr std::uint64_t kLogHeaderSize = 8;
constexpr std::size_t kVoteBytes = 29;
constexpr std::uint32_t kMaxRecordBody = 32u * 1024u * 1024u;
constexpr std::uint32_t kEntryPrefix = 29u;
constexpr std::uint32_t kMaxVoters = 100000u;
constexpr std::uint64_t kMaxMetaBytes = 1u << 20;

constexpr const char* kVoteName = "vote";
constexpr const char* kMembershipName = "membership";
constexpr const char* kLogName = "log";

Result<void> storage_error() { return Result<void>::failure(Error::Storage); }

int fsync_retry(int fd) {
  int rc = 0;
  do {
    rc = ::fsync(fd);
  } while (rc < 0 && errno == EINTR);
  return rc;
}

int ftruncate_retry(int fd, off_t size) {
  int rc = 0;
  do {
    rc = ::ftruncate(fd, size);
  } while (rc < 0 && errno == EINTR);
  return rc;
}

class UniqueFd {
 public:
  explicit UniqueFd(int fd = -1) : fd_(fd) {}
  ~UniqueFd() { reset(); }

  UniqueFd(const UniqueFd&) = delete;
  UniqueFd& operator=(const UniqueFd&) = delete;
  UniqueFd(UniqueFd&& other) noexcept : fd_(other.fd_) { other.fd_ = -1; }
  UniqueFd& operator=(UniqueFd&& other) noexcept {
    if (this != &other) {
      reset();
      fd_ = other.fd_;
      other.fd_ = -1;
    }
    return *this;
  }

  int get() const { return fd_; }
  explicit operator bool() const { return fd_ >= 0; }

  void reset() {
    if (fd_ >= 0) {
      ::close(fd_);
      fd_ = -1;
    }
  }

 private:
  int fd_;
};

bool write_all(int fd, const void* data, std::size_t size) {
  const auto* bytes = static_cast<const std::uint8_t*>(data);
  std::size_t done = 0;
  while (done < size) {
    const ssize_t wrote = ::write(fd, bytes + done, size - done);
    if (wrote < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    if (wrote == 0) {
      return false;
    }
    done += static_cast<std::size_t>(wrote);
  }
  return true;
}

bool pread_all(int fd, void* data, std::size_t size, std::uint64_t offset) {
  if (size == 0) {
    return true;
  }
  if (offset > static_cast<std::uint64_t>(std::numeric_limits<off_t>::max())) {
    return false;
  }
  auto* bytes = static_cast<std::uint8_t*>(data);
  std::size_t done = 0;
  while (done < size) {
    const std::uint64_t at = offset + static_cast<std::uint64_t>(done);
    if (at > static_cast<std::uint64_t>(std::numeric_limits<off_t>::max())) {
      return false;
    }
    const ssize_t got = ::pread(fd, bytes + done, size - done, static_cast<off_t>(at));
    if (got < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    if (got == 0) {
      return false;
    }
    done += static_cast<std::size_t>(got);
  }
  return true;
}

bool pwrite_all(int fd, const void* data, std::size_t size, std::uint64_t offset) {
  if (size == 0) {
    return true;
  }
  const auto* bytes = static_cast<const std::uint8_t*>(data);
  std::size_t done = 0;
  while (done < size) {
    const std::uint64_t at = offset + static_cast<std::uint64_t>(done);
    if (at > static_cast<std::uint64_t>(std::numeric_limits<off_t>::max())) {
      return false;
    }
    const ssize_t wrote = ::pwrite(fd, bytes + done, size - done, static_cast<off_t>(at));
    if (wrote < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    if (wrote == 0) {
      return false;
    }
    done += static_cast<std::size_t>(wrote);
  }
  return true;
}

void append_u8(std::string& out, std::uint8_t value) { out.push_back(static_cast<char>(value)); }

void append_u32(std::string& out, std::uint32_t value) {
  for (int shift = 0; shift < 32; shift += 8) {
    out.push_back(static_cast<char>((value >> shift) & 0xffu));
  }
}

void append_u64(std::string& out, std::uint64_t value) {
  for (int shift = 0; shift < 64; shift += 8) {
    out.push_back(static_cast<char>((value >> shift) & 0xffu));
  }
}

bool read_u32(const std::uint8_t* data, std::size_t size, std::size_t offset, std::uint32_t& out) {
  if (offset > size || size - offset < 4) {
    return false;
  }
  out = static_cast<std::uint32_t>(data[offset]) |
        (static_cast<std::uint32_t>(data[offset + 1]) << 8) |
        (static_cast<std::uint32_t>(data[offset + 2]) << 16) |
        (static_cast<std::uint32_t>(data[offset + 3]) << 24);
  return true;
}

bool read_u64(const std::uint8_t* data, std::size_t size, std::size_t offset, std::uint64_t& out) {
  if (offset > size || size - offset < 8) {
    return false;
  }
  out = 0;
  for (int i = 0; i < 8; ++i) {
    out |= static_cast<std::uint64_t>(data[offset + static_cast<std::size_t>(i)])
           << (8 * i);
  }
  return true;
}

std::uint32_t crc32(const std::uint8_t* data, std::size_t size) {
  std::uint32_t crc = 0xffffffffu;
  for (std::size_t i = 0; i < size; ++i) {
    crc ^= data[i];
    for (int bit = 0; bit < 8; ++bit) {
      if ((crc & 1u) != 0u) {
        crc = (crc >> 1) ^ 0xedb88320u;
      } else {
        crc >>= 1;
      }
    }
  }
  return ~crc;
}

std::uint32_t crc32(const std::string& bytes) {
  return crc32(reinterpret_cast<const std::uint8_t*>(bytes.data()), bytes.size());
}

std::string encode_vote(const Vote& vote) {
  std::string out;
  out.reserve(kVoteBytes);
  append_u32(out, kVoteMagic);
  append_u32(out, kVersion);
  append_u64(out, vote.leader.term);
  append_u64(out, vote.leader.node);
  append_u8(out, vote.committed ? 1 : 0);
  const std::uint32_t sum = crc32(out);
  append_u32(out, sum);
  return out;
}

std::optional<Vote> decode_vote(const std::string& bytes) {
  if (bytes.size() != kVoteBytes) {
    return std::nullopt;
  }
  const auto* data = reinterpret_cast<const std::uint8_t*>(bytes.data());
  std::uint32_t magic = 0;
  std::uint32_t version = 0;
  std::uint64_t term = 0;
  std::uint64_t node = 0;
  std::uint32_t stored = 0;
  if (!read_u32(data, bytes.size(), 0, magic) || !read_u32(data, bytes.size(), 4, version) ||
      !read_u64(data, bytes.size(), 8, term) || !read_u64(data, bytes.size(), 16, node) ||
      !read_u32(data, bytes.size(), 25, stored)) {
    return std::nullopt;
  }
  const std::uint8_t committed = data[24];
  if (magic != kVoteMagic || version != kVersion || committed > 1) {
    return std::nullopt;
  }
  if (crc32(data, 25) != stored) {
    return std::nullopt;
  }
  Vote vote;
  vote.leader = LeaderId{term, node};
  vote.committed = committed == 1;
  return vote;
}

std::optional<std::string> encode_membership(const Membership& membership) {
  const std::vector<NodeId>& voters = membership.voters();
  if (voters.size() > kMaxVoters) {
    return std::nullopt;
  }
  std::string out;
  out.reserve(12 + voters.size() * 8 + 4);
  append_u32(out, kMembershipMagic);
  append_u32(out, kVersion);
  append_u32(out, static_cast<std::uint32_t>(voters.size()));
  for (NodeId id : voters) {
    append_u64(out, id);
  }
  const std::uint32_t sum = crc32(out);
  append_u32(out, sum);
  return out;
}

std::optional<Membership> decode_membership(const std::string& bytes) {
  if (bytes.size() < 16) {
    return std::nullopt;
  }
  const auto* data = reinterpret_cast<const std::uint8_t*>(bytes.data());
  std::uint32_t magic = 0;
  std::uint32_t version = 0;
  std::uint32_t count = 0;
  if (!read_u32(data, bytes.size(), 0, magic) || !read_u32(data, bytes.size(), 4, version) ||
      !read_u32(data, bytes.size(), 8, count)) {
    return std::nullopt;
  }
  if (magic != kMembershipMagic || version != kVersion || count > kMaxVoters) {
    return std::nullopt;
  }
  const std::size_t expect = 12 + static_cast<std::size_t>(count) * 8 + 4;
  if (bytes.size() != expect) {
    return std::nullopt;
  }
  std::uint32_t stored = 0;
  if (!read_u32(data, bytes.size(), expect - 4, stored) || crc32(data, expect - 4) != stored) {
    return std::nullopt;
  }
  std::vector<NodeId> voters;
  voters.reserve(count);
  for (std::uint32_t i = 0; i < count; ++i) {
    std::uint64_t id = 0;
    if (!read_u64(data, bytes.size(), 12 + static_cast<std::size_t>(i) * 8, id)) {
      return std::nullopt;
    }
    voters.push_back(id);
  }
  return Membership{std::move(voters)};
}

std::optional<std::string> encode_record(const LogEntry& entry) {
  if (entry.data.size() > static_cast<std::size_t>(kMaxRecordBody - kEntryPrefix)) {
    return std::nullopt;
  }
  std::uint8_t kind = 0;
  if (entry.kind == EntryKind::Blank) {
    kind = 0;
  } else if (entry.kind == EntryKind::Application) {
    kind = 1;
  } else {
    return std::nullopt;
  }
  std::string body;
  body.reserve(static_cast<std::size_t>(kEntryPrefix) + entry.data.size());
  append_u64(body, entry.id.index);
  append_u64(body, entry.id.term);
  append_u64(body, entry.id.node);
  append_u8(body, kind);
  append_u32(body, static_cast<std::uint32_t>(entry.data.size()));
  body.append(entry.data);

  std::string out;
  out.reserve(4 + body.size() + 4);
  append_u32(out, static_cast<std::uint32_t>(body.size()));
  const std::uint32_t sum = crc32(body);
  out.append(body);
  append_u32(out, sum);
  return out;
}

bool decode_entry(const std::uint8_t* body, std::uint32_t body_len, LogEntry& entry) {
  if (body_len < kEntryPrefix) {
    return false;
  }
  std::uint64_t index = 0;
  std::uint64_t term = 0;
  std::uint64_t node = 0;
  std::uint32_t data_len = 0;
  if (!read_u64(body, body_len, 0, index) || !read_u64(body, body_len, 8, term) ||
      !read_u64(body, body_len, 16, node) || !read_u32(body, body_len, 25, data_len)) {
    return false;
  }
  const std::uint8_t kind = body[24];
  if (kind > 1) {
    return false;
  }
  if (static_cast<std::uint64_t>(kEntryPrefix) + data_len != body_len) {
    return false;
  }
  entry.id = LogId{term, node, index};
  entry.kind = kind == 0 ? EntryKind::Blank : EntryKind::Application;
  entry.data.assign(reinterpret_cast<const char*>(body + kEntryPrefix), data_len);
  return true;
}

}  // namespace

FileLogStorage::FileLogStorage(std::filesystem::path directory) {
  if (!open_directory(std::move(directory)) || !open_log() || fsync_retry(dir_fd_) != 0) {
    failed_ = true;
  }
}

FileLogStorage::~FileLogStorage() {
  if (log_fd_ >= 0) {
    ::close(log_fd_);
    log_fd_ = -1;
  }
  if (dir_fd_ >= 0) {
    ::close(dir_fd_);
    dir_fd_ = -1;
  }
}

bool FileLogStorage::open_directory(std::filesystem::path directory) {
  if (directory.empty()) {
    return false;
  }
  std::error_code ec;
  directory_ = std::filesystem::absolute(directory, ec);
  if (ec) {
    return false;
  }
  std::filesystem::create_directories(directory_, ec);
  if (ec) {
    return false;
  }
  dir_fd_ = ::open(directory_.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (dir_fd_ < 0) {
    return false;
  }
  ::unlink((directory_ / "vote.tmp").c_str());
  ::unlink((directory_ / "membership.tmp").c_str());
  return true;
}

bool FileLogStorage::open_log() {
  const auto path = directory_ / kLogName;
  log_fd_ = ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
  if (log_fd_ < 0) {
    return false;
  }
  return recover_log();
}

bool FileLogStorage::write_log_header() {
  std::string header;
  header.reserve(static_cast<std::size_t>(kLogHeaderSize));
  append_u32(header, kLogMagic);
  append_u32(header, kVersion);
  if (ftruncate_retry(log_fd_, 0) != 0 || !pwrite_all(log_fd_, header.data(), header.size(), 0) ||
      fsync_retry(log_fd_) != 0) {
    return false;
  }
  log_.clear();
  offsets_.clear();
  end_offset_ = kLogHeaderSize;
  return true;
}

bool FileLogStorage::recover_log() {
  struct stat st {};
  if (::fstat(log_fd_, &st) != 0 || st.st_size < 0) {
    return false;
  }
  const auto file_size = static_cast<std::uint64_t>(st.st_size);
  if (file_size < kLogHeaderSize) {
    // A crash while creating the file leaves a short header. The log never
    // held a complete record, so rewrite an empty one.
    return write_log_header();
  }

  std::uint8_t header[kLogHeaderSize];
  if (!pread_all(log_fd_, header, sizeof header, 0)) {
    return false;
  }
  std::uint32_t magic = 0;
  std::uint32_t version = 0;
  if (!read_u32(header, sizeof header, 0, magic) || !read_u32(header, sizeof header, 4, version) ||
      magic != kLogMagic || version != kVersion) {
    return false;
  }

  std::vector<LogEntry> entries;
  std::vector<std::uint64_t> offsets;
  std::uint64_t offset = kLogHeaderSize;
  bool torn_tail = false;
  while (offset < file_size) {
    const std::uint64_t remaining = file_size - offset;
    if (remaining < 4) {
      torn_tail = true;
      break;
    }
    std::uint8_t len_bytes[4];
    if (!pread_all(log_fd_, len_bytes, sizeof len_bytes, offset)) {
      return false;
    }
    std::uint32_t body_len = 0;
    if (!read_u32(len_bytes, sizeof len_bytes, 0, body_len)) {
      return false;
    }
    const std::uint64_t need = 4ull + static_cast<std::uint64_t>(body_len) + 4ull;
    if (need > remaining) {
      // The length prefix runs past EOF. That is an incomplete final write.
      torn_tail = true;
      break;
    }
    if (body_len > kMaxRecordBody) {
      return false;
    }
    std::vector<std::uint8_t> body(body_len);
    std::uint8_t crc_bytes[4];
    if (!pread_all(log_fd_, body.data(), body.size(), offset + 4) ||
        !pread_all(log_fd_, crc_bytes, sizeof crc_bytes, offset + 4 + body_len)) {
      return false;
    }
    std::uint32_t stored_crc = 0;
    if (!read_u32(crc_bytes, sizeof crc_bytes, 0, stored_crc)) {
      return false;
    }
    const bool last = offset + need == file_size;
    if (crc32(body.data(), body.size()) != stored_crc) {
      if (!last) {
        return false;
      }
      // A checksum mismatch on the last record is a torn or corrupted tail.
      torn_tail = true;
      break;
    }
    LogEntry entry;
    if (!decode_entry(body.data(), body_len, entry)) {
      return false;
    }
    const LogIndex expected = static_cast<LogIndex>(entries.size()) + 1;
    if (entry.id.index != expected) {
      return false;
    }
    offsets.push_back(offset);
    entries.push_back(std::move(entry));
    offset += need;
  }

  if (torn_tail) {
    if (offset > static_cast<std::uint64_t>(std::numeric_limits<off_t>::max()) ||
        ftruncate_retry(log_fd_, static_cast<off_t>(offset)) != 0 || fsync_retry(log_fd_) != 0) {
      return false;
    }
  }

  log_ = std::move(entries);
  offsets_ = std::move(offsets);
  end_offset_ = offset;
  return true;
}

Result<void> FileLogStorage::write_atomic(const char* name, const std::string& bytes) {
  const auto destination = directory_ / name;
  const auto temporary = directory_ / (std::string(name) + ".tmp");
  UniqueFd fd{::open(temporary.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644)};
  if (!fd || !write_all(fd.get(), bytes.data(), bytes.size()) || fsync_retry(fd.get()) != 0) {
    fd.reset();
    ::unlink(temporary.c_str());
    return storage_error();
  }
  fd.reset();
  if (::rename(temporary.c_str(), destination.c_str()) != 0) {
    ::unlink(temporary.c_str());
    return storage_error();
  }
  if (fsync_retry(dir_fd_) != 0) {
    return storage_error();
  }
  return Result<void>::success();
}

Result<std::optional<std::string>> FileLogStorage::read_file(const char* name) const {
  if (failed_ || dir_fd_ < 0) {
    return Result<std::optional<std::string>>::failure(Error::Storage);
  }
  const auto path = directory_ / name;
  UniqueFd fd{::open(path.c_str(), O_RDONLY | O_CLOEXEC)};
  if (!fd) {
    if (errno == ENOENT) {
      return Result<std::optional<std::string>>::success(std::nullopt);
    }
    return Result<std::optional<std::string>>::failure(Error::Storage);
  }
  struct stat st {};
  if (::fstat(fd.get(), &st) != 0 || st.st_size < 0 ||
      static_cast<std::uint64_t>(st.st_size) > kMaxMetaBytes) {
    return Result<std::optional<std::string>>::failure(Error::Storage);
  }
  std::string bytes(static_cast<std::size_t>(st.st_size), '\0');
  if (!bytes.empty() && !pread_all(fd.get(), bytes.data(), bytes.size(), 0)) {
    return Result<std::optional<std::string>>::failure(Error::Storage);
  }
  return Result<std::optional<std::string>>::success(std::move(bytes));
}

Result<void> FileLogStorage::save_vote(const Vote& vote) {
  if (failed_) {
    return storage_error();
  }
  return write_atomic(kVoteName, encode_vote(vote));
}

Result<std::optional<Vote>> FileLogStorage::load_vote() {
  auto loaded = read_file(kVoteName);
  if (!loaded.ok()) {
    return Result<std::optional<Vote>>::failure(loaded.error());
  }
  if (!loaded.value().has_value()) {
    return Result<std::optional<Vote>>::success(std::nullopt);
  }
  auto vote = decode_vote(*loaded.value());
  if (!vote) {
    return Result<std::optional<Vote>>::failure(Error::Storage);
  }
  return Result<std::optional<Vote>>::success(std::move(*vote));
}

Result<void> FileLogStorage::save_membership(const Membership& membership) {
  if (failed_) {
    return storage_error();
  }
  auto encoded = encode_membership(membership);
  if (!encoded) {
    return storage_error();
  }
  return write_atomic(kMembershipName, *encoded);
}

Result<std::optional<Membership>> FileLogStorage::load_membership() {
  auto loaded = read_file(kMembershipName);
  if (!loaded.ok()) {
    return Result<std::optional<Membership>>::failure(loaded.error());
  }
  if (!loaded.value().has_value()) {
    return Result<std::optional<Membership>>::success(std::nullopt);
  }
  auto membership = decode_membership(*loaded.value());
  if (!membership) {
    return Result<std::optional<Membership>>::failure(Error::Storage);
  }
  return Result<std::optional<Membership>>::success(std::move(*membership));
}

Result<void> FileLogStorage::append(const std::vector<LogEntry>& entries) {
  if (failed_ || log_fd_ < 0 || end_offset_ < kLogHeaderSize) {
    return storage_error();
  }
  if (entries.empty()) {
    return Result<void>::success();
  }
  const LogIndex base = static_cast<LogIndex>(log_.size()) + 1;
  if (entries.front().id.index != base) {
    return storage_error();
  }
  for (std::size_t i = 0; i < entries.size(); ++i) {
    if (entries[i].id.index != base + static_cast<LogIndex>(i)) {
      return storage_error();
    }
  }

  std::string buffer;
  std::vector<std::uint64_t> new_offsets;
  new_offsets.reserve(entries.size());
  std::uint64_t cursor = end_offset_;
  for (const LogEntry& entry : entries) {
    auto encoded = encode_record(entry);
    if (!encoded) {
      return storage_error();
    }
    if (encoded->size() > std::numeric_limits<std::uint64_t>::max() - cursor) {
      return storage_error();
    }
    new_offsets.push_back(cursor);
    cursor += encoded->size();
    buffer.append(*encoded);
  }

  auto rollback = [this]() {
    if (end_offset_ > static_cast<std::uint64_t>(std::numeric_limits<off_t>::max()) ||
        ftruncate_retry(log_fd_, static_cast<off_t>(end_offset_)) != 0 ||
        fsync_retry(log_fd_) != 0) {
      failed_ = true;
    }
  };
  if (!pwrite_all(log_fd_, buffer.data(), buffer.size(), end_offset_)) {
    rollback();
    return storage_error();
  }
  // Success means the new records survive a crash.
  if (fsync_retry(log_fd_) != 0) {
    rollback();
    return storage_error();
  }
  log_.insert(log_.end(), entries.begin(), entries.end());
  offsets_.insert(offsets_.end(), new_offsets.begin(), new_offsets.end());
  end_offset_ = cursor;
  return Result<void>::success();
}

Result<void> FileLogStorage::truncate_from(LogIndex index) {
  if (failed_ || log_fd_ < 0 || end_offset_ < kLogHeaderSize) {
    return storage_error();
  }
  if (index > log_.size()) {
    return Result<void>::success();
  }
  const std::size_t keep = index == 0 ? 0 : static_cast<std::size_t>(index - 1);
  const std::uint64_t new_end =
      index == 0 ? kLogHeaderSize : offsets_[static_cast<std::size_t>(index - 1)];
  if (new_end < kLogHeaderSize || new_end > end_offset_) {
    return storage_error();
  }
  if (new_end == end_offset_ && keep == log_.size()) {
    return Result<void>::success();
  }
  if (new_end > static_cast<std::uint64_t>(std::numeric_limits<off_t>::max()) ||
      ftruncate_retry(log_fd_, static_cast<off_t>(new_end)) != 0) {
    return storage_error();
  }
  log_.resize(keep);
  offsets_.resize(keep);
  end_offset_ = new_end;
  if (fsync_retry(log_fd_) != 0) {
    // The suffix is gone in this process, but it may come back after a crash.
    // A later truncate of the same index would see an empty suffix and return
    // success without another fsync, so the handle must not stay usable.
    failed_ = true;
    return storage_error();
  }
  return Result<void>::success();
}

Result<std::vector<LogEntry>> FileLogStorage::read(LogIndex from, LogIndex to) {
  if (failed_) {
    return Result<std::vector<LogEntry>>::failure(Error::Storage);
  }
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

}  // namespace easyraft
