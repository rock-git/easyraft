// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright 2026 easyraft contributors

#include "easyraft/file.hpp"

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <limits>
#include <new>
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
constexpr std::uint32_t kSnapshotMagic = 0x53535245u;    // 'ERSS'
constexpr std::uint32_t kVersion = 1u;
constexpr std::uint64_t kLogHeaderSize = 8;
constexpr std::size_t kVoteBytes = 29;
constexpr std::uint32_t kEntryPrefix = 29u;
constexpr std::uint64_t kRecordFraming = 8;
constexpr std::uint32_t kMaxVoters = 100000u;
constexpr std::uint32_t kMaxSnapshotId = 1024u;
constexpr std::uint64_t kMaxMetaBytes = 1u << 20;
// magic, version, index, term, node, id length, membership length, data length, crc.
constexpr std::uint64_t kSnapshotOverhead = 52;
// Every byte encode_snapshot can write must fit through load_snapshot.
constexpr std::uint64_t kMaxSnapshotFile =
    kMaxSnapshotDataBytes + kMaxMetaBytes + kMaxSnapshotId + kSnapshotOverhead;

constexpr const char* kVoteName = "vote";
constexpr const char* kMembershipName = "membership";
constexpr const char* kLogName = "log";
constexpr const char* kSnapshotName = "snapshot";
constexpr const char* kLogRewriteName = "log.rewrite";

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

bool fsync_directory_at(const std::filesystem::path& path) {
  UniqueFd fd{::open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC)};
  if (!fd) {
    return false;
  }
  return fsync_retry(fd.get()) == 0;
}

// `existing` is the deepest ancestor that was already present before
// create_directories. Fsync each parent that received a new directory entry.
// When `created` itself already existed, fsync only its immediate parent so a
// previous crash cannot leave that entry unpersisted.
bool fsync_directory_parents(const std::filesystem::path& created,
                             const std::filesystem::path& existing) {
  const std::filesystem::path parent = created.parent_path();
  if (parent.empty() || parent == created) {
    return true;
  }
  std::filesystem::path at = parent;
  while (true) {
    if (!fsync_directory_at(at)) {
      return false;
    }
    if (created == existing || at == existing) {
      return true;
    }
    const std::filesystem::path next = at.parent_path();
    if (next.empty() || next == at) {
      return false;
    }
    at = next;
  }
}

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

std::optional<std::string> encode_uniform_membership(const std::vector<NodeId>& voters) {
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

bool membership_lists_fit(const Membership& membership) {
  for (const std::vector<NodeId>& config : membership.configs()) {
    if (config.size() > kMaxVoters) {
      return false;
    }
  }
  if (membership.learners().size() > kMaxVoters) {
    return false;
  }
  if (membership.has_uniform_learner_override() &&
      membership.learners_after_joint().size() > kMaxVoters) {
    return false;
  }
  return true;
}

std::optional<std::string> encode_membership_v3(const Membership& membership) {
  // Membership::decode rejects a list longer than 100,000 ids. load_membership
  // also refuses a file larger than kMaxMetaBytes, and the version-3 framing
  // is 16 bytes around the payload. Both limits are checked before the file
  // is replaced, so a saved membership can be opened again.
  if (!membership_lists_fit(membership)) {
    return std::nullopt;
  }
  const std::string payload = membership.encode();
  constexpr std::uint64_t kFraming = 16;
  if (payload.size() > kMaxMetaBytes - kFraming) {
    return std::nullopt;
  }
  std::string out;
  out.reserve(16 + payload.size());
  append_u32(out, kMembershipMagic);
  append_u32(out, 3u);
  append_u32(out, static_cast<std::uint32_t>(payload.size()));
  out.append(payload);
  append_u32(out, crc32(out));
  return out;
}

std::optional<std::string> encode_membership(const Membership& membership) {
  // Learners and a pending demotion use version 3. Version 1 stays the single
  // sorted voter list so files written before joint consensus still match
  // what a uniform config writes now.
  if (!membership.learners().empty() || membership.has_uniform_learner_override()) {
    return encode_membership_v3(membership);
  }
  if (!membership.is_joint()) {
    return encode_uniform_membership(membership.voters());
  }
  const std::vector<std::vector<NodeId>>& configs = membership.configs();
  if (configs.size() != 2) {
    return std::nullopt;
  }
  std::size_t id_count = 0;
  for (const std::vector<NodeId>& config : configs) {
    if (config.size() > kMaxVoters) {
      return std::nullopt;
    }
    id_count += config.size();
  }
  std::string out;
  out.reserve(16 + id_count * 8 + configs.size() * 4 + 4);
  append_u32(out, kMembershipMagic);
  append_u32(out, 2u);
  append_u32(out, static_cast<std::uint32_t>(configs.size()));
  for (const std::vector<NodeId>& config : configs) {
    append_u32(out, static_cast<std::uint32_t>(config.size()));
    for (NodeId id : config) {
      append_u64(out, id);
    }
  }
  const std::uint32_t sum = crc32(out);
  append_u32(out, sum);
  return out;
}

std::optional<Membership> decode_uniform_membership(const std::uint8_t* data,
                                                    std::size_t size,
                                                    std::uint32_t count) {
  if (count > kMaxVoters) {
    return std::nullopt;
  }
  const std::size_t expect = 12 + static_cast<std::size_t>(count) * 8 + 4;
  if (size != expect) {
    return std::nullopt;
  }
  std::uint32_t stored = 0;
  if (!read_u32(data, size, expect - 4, stored) || crc32(data, expect - 4) != stored) {
    return std::nullopt;
  }
  std::vector<NodeId> voters;
  voters.reserve(count);
  for (std::uint32_t i = 0; i < count; ++i) {
    std::uint64_t id = 0;
    if (!read_u64(data, size, 12 + static_cast<std::size_t>(i) * 8, id)) {
      return std::nullopt;
    }
    voters.push_back(id);
  }
  return Membership{std::move(voters)};
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
  if (magic != kMembershipMagic) {
    return std::nullopt;
  }
  if (version == kVersion) {
    return decode_uniform_membership(data, bytes.size(), count);
  }
  if (version == 3u) {
    if (count > kMaxMetaBytes) {
      return std::nullopt;
    }
    const std::size_t expect = 12 + static_cast<std::size_t>(count) + 4;
    if (bytes.size() != expect) {
      return std::nullopt;
    }
    std::uint32_t stored = 0;
    if (!read_u32(data, bytes.size(), expect - 4, stored) || crc32(data, expect - 4) != stored) {
      return std::nullopt;
    }
    return Membership::decode(std::string_view(bytes.data() + 12, count));
  }
  if (version != 2u || count < 1 || count > 2) {
    return std::nullopt;
  }
  std::size_t offset = 12;
  std::vector<std::vector<NodeId>> configs;
  configs.reserve(count);
  for (std::uint32_t config_index = 0; config_index < count; ++config_index) {
    std::uint32_t voters = 0;
    if (!read_u32(data, bytes.size(), offset, voters) || voters > kMaxVoters) {
      return std::nullopt;
    }
    offset += 4;
    if (bytes.size() - offset < static_cast<std::size_t>(voters) * 8 + 4) {
      return std::nullopt;
    }
    std::vector<NodeId> ids;
    ids.reserve(voters);
    for (std::uint32_t i = 0; i < voters; ++i) {
      std::uint64_t id = 0;
      if (!read_u64(data, bytes.size(), offset, id)) {
        return std::nullopt;
      }
      offset += 8;
      ids.push_back(id);
    }
    configs.push_back(std::move(ids));
  }
  if (bytes.size() != offset + 4) {
    return std::nullopt;
  }
  std::uint32_t stored = 0;
  if (!read_u32(data, bytes.size(), offset, stored) || crc32(data, offset) != stored) {
    return std::nullopt;
  }
  if (configs.size() == 1) {
    return Membership{std::move(configs[0])};
  }
  return Membership::joint_of(std::move(configs[0]), std::move(configs[1]));
}

std::optional<std::string> encode_snapshot(const Snapshot& snapshot) {
  if (snapshot.meta.last_included.index == 0 || snapshot.meta.snapshot_id.empty() ||
      snapshot.meta.snapshot_id.size() > kMaxSnapshotId ||
      snapshot.data.size() > kMaxSnapshotDataBytes) {
    return std::nullopt;
  }
  const std::string membership = snapshot.meta.membership.encode();
  if (membership.size() > kMaxMetaBytes) {
    return std::nullopt;
  }
  std::string out;
  out.reserve(48 + snapshot.meta.snapshot_id.size() + membership.size() + snapshot.data.size());
  append_u32(out, kSnapshotMagic);
  append_u32(out, kVersion);
  append_u64(out, snapshot.meta.last_included.index);
  append_u64(out, snapshot.meta.last_included.term);
  append_u64(out, snapshot.meta.last_included.node);
  append_u32(out, static_cast<std::uint32_t>(snapshot.meta.snapshot_id.size()));
  out.append(snapshot.meta.snapshot_id);
  append_u32(out, static_cast<std::uint32_t>(membership.size()));
  out.append(membership);
  append_u64(out, static_cast<std::uint64_t>(snapshot.data.size()));
  out.append(snapshot.data);
  append_u32(out, crc32(out));
  return out;
}

std::optional<Snapshot> decode_snapshot(const std::string& bytes) {
  if (bytes.size() < 48) {
    return std::nullopt;
  }
  const auto* data = reinterpret_cast<const std::uint8_t*>(bytes.data());
  std::uint32_t stored = 0;
  if (!read_u32(data, bytes.size(), bytes.size() - 4, stored) ||
      crc32(data, bytes.size() - 4) != stored) {
    return std::nullopt;
  }
  std::uint32_t magic = 0;
  std::uint32_t version = 0;
  std::uint64_t index = 0;
  std::uint64_t term = 0;
  std::uint64_t node = 0;
  std::uint32_t id_len = 0;
  if (!read_u32(data, bytes.size(), 0, magic) || !read_u32(data, bytes.size(), 4, version) ||
      !read_u64(data, bytes.size(), 8, index) || !read_u64(data, bytes.size(), 16, term) ||
      !read_u64(data, bytes.size(), 24, node) || !read_u32(data, bytes.size(), 32, id_len)) {
    return std::nullopt;
  }
  if (magic != kSnapshotMagic || version != kVersion || index == 0 || id_len == 0 ||
      id_len > kMaxSnapshotId) {
    return std::nullopt;
  }
  std::size_t offset = 36;
  if (bytes.size() - offset < static_cast<std::size_t>(id_len) + 4) {
    return std::nullopt;
  }
  Snapshot snapshot;
  snapshot.meta.last_included = LogId{term, node, index};
  snapshot.meta.snapshot_id.assign(bytes.data() + offset, id_len);
  offset += id_len;
  std::uint32_t membership_len = 0;
  if (!read_u32(data, bytes.size(), offset, membership_len) || membership_len > kMaxMetaBytes) {
    return std::nullopt;
  }
  offset += 4;
  if (bytes.size() - offset < static_cast<std::size_t>(membership_len) + 8) {
    return std::nullopt;
  }
  const auto membership = Membership::decode(
      std::string_view(bytes.data() + offset, membership_len));
  if (!membership) {
    return std::nullopt;
  }
  snapshot.meta.membership = *membership;
  offset += membership_len;
  std::uint64_t data_len = 0;
  if (!read_u64(data, bytes.size(), offset, data_len) || data_len > kMaxSnapshotDataBytes) {
    return std::nullopt;
  }
  offset += 8;
  if (bytes.size() - offset != static_cast<std::size_t>(data_len) + 4) {
    return std::nullopt;
  }
  snapshot.data.assign(bytes.data() + offset, static_cast<std::size_t>(data_len));
  return snapshot;
}

std::optional<std::string> encode_record(const LogEntry& entry) {
  // body_len is a uint32. Anything larger cannot be framed; this is the same
  // set of payloads MemoryLogStorage will store, short of that format limit.
  if (entry.data.size() >
      static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max() - kEntryPrefix)) {
    return std::nullopt;
  }
  std::uint8_t kind = 0;
  if (entry.kind == EntryKind::Blank) {
    kind = 0;
  } else if (entry.kind == EntryKind::Application) {
    kind = 1;
  } else if (entry.kind == EntryKind::Membership) {
    kind = 2;
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
  out.reserve(static_cast<std::size_t>(kRecordFraming) + body.size() + 4);
  append_u32(out, static_cast<std::uint32_t>(body.size()));
  // Checksum the length by itself so a flipped length is not mistaken for a
  // torn tail and used to delete every later record.
  const std::uint32_t length_crc =
      crc32(reinterpret_cast<const std::uint8_t*>(out.data()), out.size());
  append_u32(out, length_crc);
  out.append(body);
  append_u32(out, crc32(body));
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
  if (kind > 2) {
    return false;
  }
  if (static_cast<std::uint64_t>(kEntryPrefix) + data_len != body_len) {
    return false;
  }
  entry.id = LogId{term, node, index};
  if (kind == 0) {
    entry.kind = EntryKind::Blank;
  } else if (kind == 1) {
    entry.kind = EntryKind::Application;
  } else {
    entry.kind = EntryKind::Membership;
  }
  entry.data.assign(reinterpret_cast<const char*>(body + kEntryPrefix), data_len);
  return true;
}

}  // namespace

FileLogStorage::FileLogStorage(std::filesystem::path directory) {
  if (!open_directory(std::move(directory))) {
    failed_ = true;
    return;
  }
  // Check the snapshot before torn-tail recovery. A corrupt image must not
  // truncate the log on the way to reporting the failure.
  const Result<std::optional<Snapshot>> snapshot = load_snapshot();
  if (!snapshot.ok()) {
    failed_ = true;
    fsync_retry(dir_fd_);
    return;
  }
  if (!open_log() || fsync_retry(dir_fd_) != 0) {
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
  std::filesystem::path existing = directory_;
  while (!existing.empty()) {
    std::error_code exists_ec;
    if (std::filesystem::exists(existing, exists_ec)) {
      break;
    }
    if (exists_ec) {
      return false;
    }
    const std::filesystem::path parent = existing.parent_path();
    if (parent.empty() || parent == existing) {
      return false;
    }
    existing = parent;
  }
  std::filesystem::create_directories(directory_, ec);
  if (ec || !fsync_directory_parents(directory_, existing)) {
    return false;
  }
  dir_fd_ = ::open(directory_.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (dir_fd_ < 0) {
    return false;
  }
  ::unlink((directory_ / "vote.tmp").c_str());
  ::unlink((directory_ / "membership.tmp").c_str());
  ::unlink((directory_ / "snapshot.tmp").c_str());
  ::unlink((directory_ / kLogRewriteName).c_str());
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
    if (remaining < kRecordFraming) {
      // Not enough bytes for an authenticated length. The final write stopped
      // before its framing was complete.
      torn_tail = true;
      break;
    }
    std::uint8_t len_bytes[4];
    std::uint8_t length_crc_bytes[4];
    if (!pread_all(log_fd_, len_bytes, sizeof len_bytes, offset) ||
        !pread_all(log_fd_, length_crc_bytes, sizeof length_crc_bytes, offset + 4)) {
      return false;
    }
    std::uint32_t body_len = 0;
    std::uint32_t length_crc = 0;
    if (!read_u32(len_bytes, sizeof len_bytes, 0, body_len) ||
        !read_u32(length_crc_bytes, sizeof length_crc_bytes, 0, length_crc)) {
      return false;
    }
    if (crc32(len_bytes, sizeof len_bytes) != length_crc) {
      // The length no longer matches its checksum, so this is not a torn
      // append. Leave every later record on disk.
      return false;
    }
    const std::uint64_t need = kRecordFraming + static_cast<std::uint64_t>(body_len) + 4ull;
    if (need > remaining) {
      // The length is authentic and the body runs past EOF: an incomplete
      // final write, with no intact record hidden behind it.
      torn_tail = true;
      break;
    }
    std::vector<std::uint8_t> body;
    try {
      body.resize(body_len);
    } catch (const std::bad_alloc&) {
      return false;
    }
    std::uint8_t crc_bytes[4];
    if (!pread_all(log_fd_, body.data(), body.size(), offset + kRecordFraming) ||
        !pread_all(log_fd_, crc_bytes, sizeof crc_bytes, offset + kRecordFraming + body_len)) {
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
    // A purged log starts at last_included + 1. Records after that stay contiguous.
    if (entry.id.index == 0) {
      return false;
    }
    if (!entries.empty() && entry.id.index != entries.back().id.index + 1) {
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

Result<std::optional<std::string>> FileLogStorage::read_file(const char* name,
                                                              std::uint64_t max_bytes) const {
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
      static_cast<std::uint64_t>(st.st_size) > max_bytes) {
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
  auto loaded = read_file(kVoteName, kMaxMetaBytes);
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
  auto loaded = read_file(kMembershipName, kMaxMetaBytes);
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
  const LogIndex base = log_.empty() ? compacted_through_ + 1 : log_.back().id.index + 1;
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
  // Index 0 drops every log record and leaves the snapshot file alone.
  if (index != 0 && index <= compacted_through_) {
    return storage_error();
  }
  if (index != 0 && (log_.empty() || index > log_.back().id.index)) {
    return Result<void>::success();
  }
  std::size_t keep = 0;
  std::uint64_t new_end = kLogHeaderSize;
  if (index != 0 && !log_.empty() && index > log_.front().id.index) {
    keep = static_cast<std::size_t>(index - log_.front().id.index);
    new_end = offsets_[keep];
  }
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

bool FileLogStorage::snapshot_acceptable(const Snapshot& snapshot) const {
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

Result<void> FileLogStorage::save_snapshot(const Snapshot& snapshot) {
  if (failed_) {
    return storage_error();
  }
  if (!snapshot_acceptable(snapshot)) {
    return storage_error();
  }
  const std::optional<std::string> encoded = encode_snapshot(snapshot);
  if (!encoded) {
    return storage_error();
  }
  const Result<void> written = write_atomic(kSnapshotName, *encoded);
  if (!written.ok()) {
    return written;
  }
  snapshot_ = snapshot;
  compacted_through_ = snapshot.meta.last_included.index;
  return Result<void>::success();
}

Result<std::optional<Snapshot>> FileLogStorage::load_snapshot() {
  if (failed_ || dir_fd_ < 0) {
    return Result<std::optional<Snapshot>>::failure(Error::Storage);
  }
  auto loaded = read_file(kSnapshotName, kMaxSnapshotFile);
  if (!loaded.ok()) {
    return Result<std::optional<Snapshot>>::failure(loaded.error());
  }
  if (!loaded.value().has_value()) {
    if (snapshot_) {
      return Result<std::optional<Snapshot>>::failure(Error::Storage);
    }
    return Result<std::optional<Snapshot>>::success(std::nullopt);
  }
  std::optional<Snapshot> decoded = decode_snapshot(*loaded.value());
  if (!decoded || !snapshot_acceptable(*decoded)) {
    return Result<std::optional<Snapshot>>::failure(Error::Storage);
  }
  snapshot_ = std::move(*decoded);
  compacted_through_ = std::max(compacted_through_, snapshot_->meta.last_included.index);
  return Result<std::optional<Snapshot>>::success(snapshot_);
}

bool FileLogStorage::rewrite_log(const std::vector<LogEntry>& entries) {
  const auto temporary = directory_ / kLogRewriteName;
  UniqueFd fd{::open(temporary.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644)};
  if (!fd) {
    return false;
  }
  std::string image;
  append_u32(image, kLogMagic);
  append_u32(image, kVersion);
  std::vector<std::uint64_t> offsets;
  offsets.reserve(entries.size());
  std::uint64_t cursor = kLogHeaderSize;
  for (const LogEntry& entry : entries) {
    auto encoded = encode_record(entry);
    if (!encoded || encoded->size() > std::numeric_limits<std::uint64_t>::max() - cursor) {
      fd.reset();
      ::unlink(temporary.c_str());
      return false;
    }
    offsets.push_back(cursor);
    cursor += encoded->size();
    image.append(*encoded);
  }
  if (!write_all(fd.get(), image.data(), image.size()) || fsync_retry(fd.get()) != 0) {
    fd.reset();
    ::unlink(temporary.c_str());
    return false;
  }
  fd.reset();
  const auto destination = directory_ / kLogName;
  if (::rename(temporary.c_str(), destination.c_str()) != 0) {
    ::unlink(temporary.c_str());
    return false;
  }
  if (fsync_retry(dir_fd_) != 0) {
    // The new name may already be visible. Refuse further use; a restart
    // loads whichever log the directory entry points at.
    failed_ = true;
    return false;
  }
  if (log_fd_ >= 0) {
    ::close(log_fd_);
    log_fd_ = -1;
  }
  log_fd_ = ::open(destination.c_str(), O_RDWR | O_CLOEXEC);
  if (log_fd_ < 0) {
    failed_ = true;
    return false;
  }
  log_ = entries;
  offsets_ = std::move(offsets);
  end_offset_ = cursor;
  return true;
}

Result<void> FileLogStorage::purge_through(LogIndex index) {
  if (failed_ || log_fd_ < 0 || end_offset_ < kLogHeaderSize) {
    return storage_error();
  }
  if (!snapshot_ || index != snapshot_->meta.last_included.index) {
    return storage_error();
  }
  std::vector<LogEntry> kept;
  kept.reserve(log_.size());
  for (const LogEntry& entry : log_) {
    if (entry.id.index > index) {
      kept.push_back(entry);
    }
  }
  if (kept.size() != log_.size()) {
    if (!rewrite_log(kept)) {
      return storage_error();
    }
  }
  compacted_through_ = index;
  return Result<void>::success();
}

}  // namespace easyraft
