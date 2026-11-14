// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright 2026 easyraft contributors

#include "cluster.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <string>
#include <system_error>
#include <vector>

#include <unistd.h>

namespace easyraft {
namespace {

class TempDir {
 public:
  TempDir() {
    static std::atomic<std::uint64_t> seq{0};
    path_ = std::filesystem::temp_directory_path() /
            ("easyraft-file-" + std::to_string(::getpid()) + "-" + std::to_string(seq.fetch_add(1)));
    std::filesystem::create_directories(path_);
  }

  TempDir(const TempDir&) = delete;
  TempDir& operator=(const TempDir&) = delete;

  ~TempDir() {
    std::error_code ec;
    std::filesystem::remove_all(path_, ec);
  }

  const std::filesystem::path& path() const { return path_; }

 private:
  std::filesystem::path path_;
};

LogEntry entry(Term term, NodeId node, LogIndex index, EntryKind kind, std::string data = {}) {
  LogEntry value;
  value.id = LogId{term, node, index};
  value.kind = kind;
  value.data = std::move(data);
  return value;
}

LogEntry app(LogIndex index, std::string data) {
  return entry(1, 1, index, EntryKind::Application, std::move(data));
}

std::string read_bytes(const std::filesystem::path& path) {
  std::ifstream in(path, std::ios::binary);
  return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

void write_bytes(const std::filesystem::path& path, const std::string& bytes) {
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  ASSERT_TRUE(out.good());
  out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
  out.flush();
  ASSERT_TRUE(out.good());
}

bool has_payload(const std::vector<LogEntry>& entries, const std::string& data) {
  for (const LogEntry& item : entries) {
    if (item.kind == EntryKind::Application && item.data == data) {
      return true;
    }
  }
  return false;
}

TEST(FileLogStorage, MissingVoteAndMembershipAreEmpty) {
  TempDir dir;
  FileLogStorage storage(dir.path());

  const auto vote = storage.load_vote();
  ASSERT_TRUE(vote.ok()) << vote.error();
  EXPECT_FALSE(vote.value().has_value());

  const auto membership = storage.load_membership();
  ASSERT_TRUE(membership.ok()) << membership.error();
  EXPECT_FALSE(membership.value().has_value());
}

TEST(FileLogStorage, SaveLoadVoteAndMembership) {
  TempDir dir;
  const std::filesystem::path path = dir.path();
  Vote first;
  first.leader = LeaderId{4, 2};
  first.committed = false;
  const Membership membership({3, 1, 2});

  {
    FileLogStorage storage(path);
    ASSERT_TRUE(storage.save_vote(first).ok());
    ASSERT_TRUE(storage.save_membership(membership).ok());
    EXPECT_FALSE(std::filesystem::exists(path / "vote.tmp"));
    EXPECT_FALSE(std::filesystem::exists(path / "membership.tmp"));

    const auto vote = storage.load_vote();
    ASSERT_TRUE(vote.ok());
    ASSERT_TRUE(vote.value().has_value());
    EXPECT_EQ(vote.value()->leader.term, 4u);
    EXPECT_EQ(vote.value()->leader.node, 2u);
    EXPECT_FALSE(vote.value()->committed);

    const auto loaded = storage.load_membership();
    ASSERT_TRUE(loaded.ok());
    ASSERT_TRUE(loaded.value().has_value());
    EXPECT_EQ(loaded.value()->voters(), membership.voters());

    Vote established = first;
    established.committed = true;
    established.leader.term = 9;
    ASSERT_TRUE(storage.save_vote(established).ok());
    const Membership replaced({8});
    ASSERT_TRUE(storage.save_membership(replaced).ok());
  }

  FileLogStorage reopened(path);
  const auto vote = reopened.load_vote();
  ASSERT_TRUE(vote.ok());
  ASSERT_TRUE(vote.value().has_value());
  EXPECT_EQ(vote.value()->leader.term, 9u);
  EXPECT_EQ(vote.value()->leader.node, 2u);
  EXPECT_TRUE(vote.value()->committed);
  const auto loaded = reopened.load_membership();
  ASSERT_TRUE(loaded.ok());
  ASSERT_TRUE(loaded.value().has_value());
  ASSERT_EQ(loaded.value()->voters().size(), 1u);
  EXPECT_EQ(loaded.value()->voters().front(), 8u);
}

std::uint32_t test_crc32(const std::uint8_t* data, std::size_t size) {
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

void append_le_u32(std::string& out, std::uint32_t value) {
  for (int shift = 0; shift < 32; shift += 8) {
    out.push_back(static_cast<char>((value >> shift) & 0xffu));
  }
}

void append_le_u64(std::string& out, std::uint64_t value) {
  for (int shift = 0; shift < 64; shift += 8) {
    out.push_back(static_cast<char>((value >> shift) & 0xffu));
  }
}

TEST(FileLogStorage, LegacyUniformMembershipStillLoads) {
  TempDir dir;
  // Version 1 layout from before joint configs: magic ERMB, version 1,
  // count, ids, crc32. Ids are intentionally unsorted.
  std::string bytes;
  append_le_u32(bytes, 0x424D5245u);
  append_le_u32(bytes, 1u);
  append_le_u32(bytes, 3u);
  append_le_u64(bytes, 3u);
  append_le_u64(bytes, 1u);
  append_le_u64(bytes, 2u);
  const auto* raw = reinterpret_cast<const std::uint8_t*>(bytes.data());
  append_le_u32(bytes, test_crc32(raw, bytes.size()));
  write_bytes(dir.path() / "membership", bytes);

  FileLogStorage storage(dir.path());
  const auto loaded = storage.load_membership();
  ASSERT_TRUE(loaded.ok()) << loaded.error();
  ASSERT_TRUE(loaded.value().has_value());
  EXPECT_FALSE(loaded.value()->is_joint());
  EXPECT_EQ(loaded.value()->voters(), (std::vector<NodeId>{1, 2, 3}));
}

TEST(FileLogStorage, JointMembershipAndMembershipEntriesRoundTrip) {
  TempDir dir;
  const std::filesystem::path path = dir.path();
  const Membership joint = Membership::joint_of({3, 1, 2}, {2, 4, 4});
  LogEntry membership_entry;
  membership_entry.id = LogId{2, 1, 1};
  membership_entry.kind = EntryKind::Membership;
  membership_entry.data = joint.encode();
  std::string binary("bin");
  binary.push_back('\0');
  binary.append("end");

  {
    FileLogStorage storage(path);
    ASSERT_TRUE(storage.save_membership(joint).ok());
    ASSERT_TRUE(storage
                    .append({membership_entry, entry(2, 1, 2, EntryKind::Application, binary)})
                    .ok());
  }

  FileLogStorage reopened(path);
  const auto loaded = reopened.load_membership();
  ASSERT_TRUE(loaded.ok()) << loaded.error();
  ASSERT_TRUE(loaded.value().has_value());
  EXPECT_EQ(*loaded.value(), joint);
  EXPECT_TRUE(loaded.value()->is_joint());
  EXPECT_EQ(loaded.value()->configs()[0], (std::vector<NodeId>{1, 2, 3}));
  EXPECT_EQ(loaded.value()->configs()[1], (std::vector<NodeId>{2, 4}));

  const auto log = reopened.read(1, 2);
  ASSERT_TRUE(log.ok()) << log.error();
  ASSERT_EQ(log.value().size(), 2u);
  EXPECT_EQ(log.value()[0].kind, EntryKind::Membership);
  EXPECT_EQ(log.value()[0].data, joint.encode());
  const auto decoded = Membership::decode(log.value()[0].data);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(*decoded, joint);
  EXPECT_EQ(log.value()[1].kind, EntryKind::Application);
  EXPECT_EQ(log.value()[1].data, binary);

  const Membership uniform{{9, 8}};
  ASSERT_TRUE(reopened.save_membership(uniform).ok());
  const auto replaced = reopened.load_membership();
  ASSERT_TRUE(replaced.ok());
  ASSERT_TRUE(replaced.value().has_value());
  EXPECT_FALSE(replaced.value()->is_joint());
  EXPECT_EQ(replaced.value()->voters(), (std::vector<NodeId>{8, 9}));
}

TEST(FileLogStorage, LearnerMembershipAndSnapshotRoundTrip) {
  TempDir dir;
  const std::filesystem::path path = dir.path();
  const Membership learners{{1, 2, 3}, {5, 4}};
  const Membership demotion = Membership::joint_of({1, 2, 3}, {1, 2}, {4}, {3, 4});
  Snapshot snapshot;
  snapshot.meta.last_included = LogId{2, 1, 2};
  snapshot.meta.membership = learners;
  snapshot.meta.snapshot_id = snapshot_id_from(snapshot.meta.last_included);
  snapshot.data = std::string("learn\0ers", 9);

  {
    FileLogStorage storage(path);
    ASSERT_TRUE(storage.save_membership(learners).ok());
    ASSERT_TRUE(storage.save_snapshot(snapshot).ok());
    const auto loaded = storage.load_membership();
    ASSERT_TRUE(loaded.ok());
    ASSERT_TRUE(loaded.value().has_value());
    EXPECT_EQ(*loaded.value(), learners);
    EXPECT_EQ(loaded.value()->learners(), (std::vector<NodeId>{4, 5}));
    EXPECT_EQ(loaded.value()->quorum(), 2u);
    const auto image = storage.load_snapshot();
    ASSERT_TRUE(image.ok());
    ASSERT_TRUE(image.value().has_value());
    EXPECT_EQ(image.value()->meta.membership, learners);
    EXPECT_EQ(image.value()->meta.membership.learners(), (std::vector<NodeId>{4, 5}));

    ASSERT_TRUE(storage.save_membership(demotion).ok());
  }

  FileLogStorage reopened(path);
  const auto loaded = reopened.load_membership();
  ASSERT_TRUE(loaded.ok()) << loaded.error();
  ASSERT_TRUE(loaded.value().has_value());
  EXPECT_EQ(*loaded.value(), demotion);
  EXPECT_TRUE(loaded.value()->is_joint());
  EXPECT_EQ(loaded.value()->learners(), (std::vector<NodeId>{4}));
  EXPECT_TRUE(loaded.value()->has_uniform_learner_override());
  EXPECT_EQ(loaded.value()->learners_after_joint(), (std::vector<NodeId>{3, 4}));
  const auto image = reopened.load_snapshot();
  ASSERT_TRUE(image.ok()) << image.error();
  ASSERT_TRUE(image.value().has_value());
  EXPECT_EQ(image.value()->meta.membership.learners(), (std::vector<NodeId>{4, 5}));
  EXPECT_EQ(image.value()->data, snapshot.data);
}

TEST(FileLogStorage, AppendAndReadPayloads) {
  TempDir dir;
  FileLogStorage storage(dir.path());
  ASSERT_TRUE(storage.append({}).ok());

  std::string binary("bin");
  binary.push_back('\0');
  binary.push_back(static_cast<char>(0xff));
  binary.append("end");

  const std::vector<LogEntry> written{
      entry(1, 1, 1, EntryKind::Blank),
      entry(1, 1, 2, EntryKind::Blank, "marker"),
      entry(2, 4, 3, EntryKind::Application, "hello"),
      entry(2, 4, 4, EntryKind::Application, binary),
  };
  ASSERT_TRUE(storage.append(written).ok());

  const auto all = storage.read(1, 4);
  ASSERT_TRUE(all.ok());
  ASSERT_EQ(all.value().size(), written.size());
  for (std::size_t i = 0; i < written.size(); ++i) {
    EXPECT_EQ(all.value()[i].id.term, written[i].id.term);
    EXPECT_EQ(all.value()[i].id.node, written[i].id.node);
    EXPECT_EQ(all.value()[i].id.index, written[i].id.index);
    EXPECT_EQ(all.value()[i].kind, written[i].kind);
    EXPECT_EQ(all.value()[i].data, written[i].data);
  }

  const auto middle = storage.read(2, 3);
  ASSERT_TRUE(middle.ok());
  ASSERT_EQ(middle.value().size(), 2u);
  EXPECT_EQ(middle.value()[0].data, "marker");
  EXPECT_EQ(middle.value()[1].data, "hello");

  const auto one = storage.read(4, 4);
  ASSERT_TRUE(one.ok());
  ASSERT_EQ(one.value().size(), 1u);
  EXPECT_EQ(one.value()[0].data, binary);

  const auto tail = storage.read(3, 100);
  ASSERT_TRUE(tail.ok());
  ASSERT_EQ(tail.value().size(), 2u);

  const auto empty_from = storage.read(0, 4);
  ASSERT_TRUE(empty_from.ok());
  EXPECT_TRUE(empty_from.value().empty());
  const auto empty_range = storage.read(3, 2);
  ASSERT_TRUE(empty_range.ok());
  EXPECT_TRUE(empty_range.value().empty());
  const auto past_end = storage.read(5, 9);
  ASSERT_TRUE(past_end.ok());
  EXPECT_TRUE(past_end.value().empty());
}

TEST(FileLogStorage, TruncateDropsOnlyTheSuffix) {
  TempDir dir;
  const std::filesystem::path path = dir.path();
  const auto log_path = path / "log";
  {
    FileLogStorage storage(path);
    ASSERT_TRUE(storage.append({
                               app(1, "keep-one"),
                               app(2, "keep-two"),
                               app(3, "drop-three"),
                               app(4, "drop-four"),
                           })
                    .ok());
    ASSERT_TRUE(storage.truncate_from(100).ok());
    ASSERT_EQ(storage.read(1, 10).value().size(), 4u);

    ASSERT_TRUE(storage.truncate_from(3).ok());
    const auto kept = storage.read(1, 10);
    ASSERT_TRUE(kept.ok());
    ASSERT_EQ(kept.value().size(), 2u);
    EXPECT_EQ(kept.value()[0].data, "keep-one");
    EXPECT_EQ(kept.value()[1].data, "keep-two");
  }

  const std::string bytes = read_bytes(log_path);
  EXPECT_NE(bytes.find("keep-one"), std::string::npos);
  EXPECT_NE(bytes.find("keep-two"), std::string::npos);
  EXPECT_EQ(bytes.find("drop-three"), std::string::npos);
  EXPECT_EQ(bytes.find("drop-four"), std::string::npos);

  {
    FileLogStorage storage(path);
    const auto kept = storage.read(1, 10);
    ASSERT_TRUE(kept.ok());
    ASSERT_EQ(kept.value().size(), 2u);
    EXPECT_EQ(kept.value()[1].id.index, 2u);
    ASSERT_TRUE(storage.append({app(3, "keep-three")}).ok());
    ASSERT_TRUE(storage.truncate_from(0).ok());
    const auto cleared = storage.read(1, 10);
    ASSERT_TRUE(cleared.ok());
    EXPECT_TRUE(cleared.value().empty());
    ASSERT_TRUE(storage.append({app(1, "fresh")}).ok());
  }

  FileLogStorage storage(path);
  const auto reopened = storage.read(1, 10);
  ASSERT_TRUE(reopened.ok());
  ASSERT_EQ(reopened.value().size(), 1u);
  EXPECT_EQ(reopened.value()[0].data, "fresh");
}

TEST(FileLogStorage, ReopenRestoresVoteMembershipAndLog) {
  TempDir dir;
  const std::filesystem::path path = dir.path();
  Vote vote;
  vote.leader = LeaderId{6, 1};
  vote.committed = true;
  const Membership membership({1, 2, 3});
  std::string binary("a");
  binary.push_back('\0');
  binary.push_back('b');

  {
    FileLogStorage storage(path);
    ASSERT_TRUE(storage.save_vote(vote).ok());
    ASSERT_TRUE(storage.save_membership(membership).ok());
    ASSERT_TRUE(storage.append({
                               entry(6, 1, 1, EntryKind::Blank),
                               entry(6, 1, 2, EntryKind::Application, binary),
                           })
                    .ok());
  }

  FileLogStorage storage(path);
  const auto loaded_vote = storage.load_vote();
  ASSERT_TRUE(loaded_vote.ok());
  ASSERT_TRUE(loaded_vote.value().has_value());
  EXPECT_EQ(loaded_vote.value()->leader.term, vote.leader.term);
  EXPECT_EQ(loaded_vote.value()->leader.node, vote.leader.node);
  EXPECT_EQ(loaded_vote.value()->committed, vote.committed);
  const auto loaded_membership = storage.load_membership();
  ASSERT_TRUE(loaded_membership.ok());
  ASSERT_TRUE(loaded_membership.value().has_value());
  EXPECT_EQ(loaded_membership.value()->voters(), membership.voters());
  const auto loaded_log = storage.read(1, 10);
  ASSERT_TRUE(loaded_log.ok());
  ASSERT_EQ(loaded_log.value().size(), 2u);
  EXPECT_EQ(loaded_log.value()[0].kind, EntryKind::Blank);
  EXPECT_EQ(loaded_log.value()[1].data, binary);
}

TEST(FileLogStorage, TornTailIsDiscarded) {
  TempDir dir;
  const auto log_path = dir.path() / "log";
  {
    FileLogStorage storage(dir.path());
    ASSERT_TRUE(storage.append({app(1, "one"), app(2, "two"), app(3, "three")}).ok());
  }

  const auto clean_size = std::filesystem::file_size(log_path);
  {
    std::string bytes = read_bytes(log_path);
    ASSERT_GT(bytes.size(), 20u);
    bytes.resize(bytes.size() - 10);
    write_bytes(log_path, bytes);
  }

  {
    FileLogStorage storage(dir.path());
    const auto kept = storage.read(1, 10);
    ASSERT_TRUE(kept.ok()) << kept.error();
    ASSERT_EQ(kept.value().size(), 2u);
    EXPECT_EQ(kept.value()[0].data, "one");
    EXPECT_EQ(kept.value()[1].data, "two");
    const auto recovered_size = std::filesystem::file_size(log_path);
    EXPECT_LT(recovered_size, clean_size);
    ASSERT_TRUE(storage.append({app(3, "replacement")}).ok());
  }

  std::string bytes = read_bytes(log_path);
  bytes.append("\x01\x02\x03", 3);
  write_bytes(log_path, bytes);
  const auto dirty_size = std::filesystem::file_size(log_path);

  FileLogStorage storage(dir.path());
  const auto kept = storage.read(1, 10);
  ASSERT_TRUE(kept.ok()) << kept.error();
  ASSERT_EQ(kept.value().size(), 3u);
  EXPECT_EQ(kept.value()[2].data, "replacement");
  EXPECT_LT(std::filesystem::file_size(log_path), dirty_size);

  FileLogStorage again(dir.path());
  const auto stable = again.read(1, 10);
  ASSERT_TRUE(stable.ok());
  ASSERT_EQ(stable.value().size(), 3u);
  EXPECT_EQ(stable.value()[2].data, "replacement");
}

TEST(FileLogStorage, CorruptLastChecksumIsDiscarded) {
  TempDir dir;
  const auto log_path = dir.path() / "log";
  {
    FileLogStorage storage(dir.path());
    ASSERT_TRUE(storage.append({app(1, "one"), app(2, "two"), app(3, "three")}).ok());
  }
  std::string bytes = read_bytes(log_path);
  ASSERT_FALSE(bytes.empty());
  bytes.back() = static_cast<char>(static_cast<unsigned char>(bytes.back()) ^ 0xffu);
  write_bytes(log_path, bytes);
  const auto dirty_size = bytes.size();

  FileLogStorage storage(dir.path());
  const auto kept = storage.read(1, 10);
  ASSERT_TRUE(kept.ok()) << kept.error();
  ASSERT_EQ(kept.value().size(), 2u);
  EXPECT_EQ(kept.value()[0].data, "one");
  EXPECT_EQ(kept.value()[1].data, "two");
  EXPECT_LT(std::filesystem::file_size(log_path), dirty_size);
  EXPECT_EQ(read_bytes(log_path).find("three"), std::string::npos);
}

TEST(FileLogStorage, CorruptLengthPrefixDoesNotTruncate) {
  TempDir dir;
  const auto log_path = dir.path() / "log";
  {
    FileLogStorage storage(dir.path());
    ASSERT_TRUE(storage.append({app(1, "one"), app(2, "two"), app(3, "three")}).ok());
  }
  std::string bytes = read_bytes(log_path);
  ASSERT_GT(bytes.size(), 16u);
  const auto original_size = bytes.size();
  // First record's body_len. Its length checksum stays put, so this is not a
  // torn tail and must not swallow the later records.
  bytes[8] = static_cast<char>(static_cast<unsigned char>(bytes[8]) ^ 0xffu);
  write_bytes(log_path, bytes);

  FileLogStorage storage(dir.path());
  const auto failed = storage.read(1, 10);
  EXPECT_FALSE(failed.ok());
  EXPECT_EQ(failed.error(), Error::Storage);
  EXPECT_EQ(std::filesystem::file_size(log_path), original_size);
  EXPECT_NE(read_bytes(log_path).find("three"), std::string::npos);
}

TEST(FileLogStorage, MiddleChecksumFailureIsFatal) {
  TempDir dir;
  const auto log_path = dir.path() / "log";
  {
    FileLogStorage storage(dir.path());
    ASSERT_TRUE(storage.append({app(1, "one"), app(2, "two"), app(3, "three")}).ok());
  }
  std::string bytes = read_bytes(log_path);
  ASSERT_GT(bytes.size(), 40u);
  const auto original_size = bytes.size();
  bytes[20] = static_cast<char>(static_cast<unsigned char>(bytes[20]) ^ 0xffu);
  write_bytes(log_path, bytes);

  FileLogStorage storage(dir.path());
  const auto failed = storage.read(1, 10);
  EXPECT_FALSE(failed.ok());
  EXPECT_EQ(failed.error(), Error::Storage);
  EXPECT_EQ(std::filesystem::file_size(log_path), original_size);
  EXPECT_FALSE(storage.append({app(4, "nope")}).ok());
}

TEST(FileLogStorage, ShortHeaderIsRewrittenEmpty) {
  TempDir dir;
  const auto log_path = dir.path() / "log";
  {
    FileLogStorage storage(dir.path());
  }
  {
    std::string bytes = read_bytes(log_path);
    ASSERT_GE(bytes.size(), 8u);
    bytes.resize(3);
    write_bytes(log_path, bytes);
  }

  FileLogStorage storage(dir.path());
  const auto kept = storage.read(1, 10);
  ASSERT_TRUE(kept.ok()) << kept.error();
  EXPECT_TRUE(kept.value().empty());
  ASSERT_TRUE(storage.append({app(1, "fresh")}).ok());
  const auto again = storage.read(1, 1);
  ASSERT_TRUE(again.ok());
  ASSERT_EQ(again.value().size(), 1u);
  EXPECT_EQ(again.value()[0].data, "fresh");
}

TEST(FileLogStorage, CorruptHeaderIsFatal) {
  TempDir dir;
  const auto log_path = dir.path() / "log";
  {
    FileLogStorage storage(dir.path());
    ASSERT_TRUE(storage.append({app(1, "one")}).ok());
  }
  std::string bytes = read_bytes(log_path);
  ASSERT_FALSE(bytes.empty());
  const auto original_size = bytes.size();
  bytes[0] = static_cast<char>(static_cast<unsigned char>(bytes[0]) ^ 0xffu);
  write_bytes(log_path, bytes);

  FileLogStorage storage(dir.path());
  const auto failed = storage.read(1, 1);
  EXPECT_FALSE(failed.ok());
  EXPECT_EQ(failed.error(), Error::Storage);
  EXPECT_EQ(std::filesystem::file_size(log_path), original_size);
}

TEST(FileLogStorage, NonContiguousAppendDoesNotCorrupt) {
  TempDir dir;
  const std::filesystem::path path = dir.path();
  const auto log_path = path / "log";
  {
    FileLogStorage storage(path);
    const auto skipped_front = storage.append({app(2, "skip")});
    EXPECT_FALSE(skipped_front.ok());
    EXPECT_EQ(skipped_front.error(), Error::Storage);
    ASSERT_TRUE(storage.append({app(1, "one"), app(2, "two")}).ok());
    const auto size_before = std::filesystem::file_size(log_path);

    const auto skipped = storage.append({app(4, "skip")});
    EXPECT_FALSE(skipped.ok());
    EXPECT_EQ(skipped.error(), Error::Storage);

    const auto gapped = storage.append({app(3, "three"), app(5, "five")});
    EXPECT_FALSE(gapped.ok());
    EXPECT_EQ(gapped.error(), Error::Storage);
    EXPECT_EQ(std::filesystem::file_size(log_path), size_before);

    const auto still = storage.read(1, 10);
    ASSERT_TRUE(still.ok());
    ASSERT_EQ(still.value().size(), 2u);
    EXPECT_EQ(still.value()[0].data, "one");
    EXPECT_EQ(still.value()[1].data, "two");

    ASSERT_TRUE(storage.append({app(3, "three")}).ok());
  }

  FileLogStorage storage(path);
  const auto kept = storage.read(1, 10);
  ASSERT_TRUE(kept.ok());
  ASSERT_EQ(kept.value().size(), 3u);
  EXPECT_EQ(kept.value()[0].data, "one");
  EXPECT_EQ(kept.value()[1].data, "two");
  EXPECT_EQ(kept.value()[2].data, "three");
}

TEST(FileLogStorage, CorruptVoteDoesNotDropTheLog) {
  TempDir dir;
  const std::filesystem::path path = dir.path();
  {
    FileLogStorage storage(path);
    Vote vote;
    vote.leader = LeaderId{2, 1};
    vote.committed = true;
    ASSERT_TRUE(storage.save_vote(vote).ok());
    ASSERT_TRUE(storage.append({app(1, "kept")}).ok());
  }
  const auto vote_path = path / "vote";
  std::string bytes = read_bytes(vote_path);
  ASSERT_FALSE(bytes.empty());
  bytes.back() = static_cast<char>(static_cast<unsigned char>(bytes.back()) ^ 0xffu);
  write_bytes(vote_path, bytes);

  FileLogStorage storage(path);
  const auto vote = storage.load_vote();
  EXPECT_FALSE(vote.ok());
  EXPECT_EQ(vote.error(), Error::Storage);
  const auto kept = storage.read(1, 10);
  ASSERT_TRUE(kept.ok()) << kept.error();
  ASSERT_EQ(kept.value().size(), 1u);
  EXPECT_EQ(kept.value()[0].data, "kept");
}

TEST(ClusterPersistence, FileRestartRecoversVoteAndLogAndDropsUncommitted) {
  test::Cluster cluster({1, 2, 3}, test::fast_config(4), true, test::StorageMode::File);
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 20)) << cluster.debug();

  const Result<LogIndex> committed = cluster.node(1).write("kept");
  ASSERT_TRUE(committed.ok());
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.machine(1).data().size() == 1 && cluster.machine(2).data().size() == 1 &&
               cluster.machine(3).data().size() == 1;
      },
      20))
      << cluster.debug();

  cluster.partition({{1}, {2, 3}});
  const Result<LogIndex> uncommitted = cluster.node(1).write("transient");
  ASSERT_TRUE(uncommitted.ok()) << uncommitted.error();
  const Term term_before_restart = cluster.node(1).term();
  const std::size_t log_size = cluster.node(1).log().size();
  ASSERT_TRUE(cluster.log_contains(1, "transient"));
  EXPECT_EQ(cluster.node(1).vote().leader.node, 1u);
  EXPECT_TRUE(cluster.node(1).vote().committed);

  {
    FileLogStorage saved(cluster.data_directory(1));
    const auto vote = saved.load_vote();
    ASSERT_TRUE(vote.ok()) << vote.error();
    ASSERT_TRUE(vote.value().has_value());
    EXPECT_EQ(vote.value()->leader.term, term_before_restart);
    EXPECT_EQ(vote.value()->leader.node, 1u);
    EXPECT_TRUE(vote.value()->committed);
    const auto stored = saved.read(1, std::numeric_limits<LogIndex>::max());
    ASSERT_TRUE(stored.ok()) << stored.error();
    EXPECT_EQ(stored.value().size(), log_size);
    EXPECT_TRUE(has_payload(stored.value(), "kept"));
    EXPECT_TRUE(has_payload(stored.value(), "transient"));
  }

  cluster.restart(1);

  EXPECT_EQ(cluster.node(1).role(), Role::Follower) << cluster.debug();
  EXPECT_EQ(cluster.node(1).term(), term_before_restart) << cluster.debug();
  EXPECT_FALSE(cluster.node(1).vote().committed);
  EXPECT_EQ(cluster.node(1).vote().leader.term, term_before_restart);
  EXPECT_EQ(cluster.node(1).vote().leader.node, 1u);
  EXPECT_EQ(cluster.node(1).log().size(), log_size);
  EXPECT_TRUE(cluster.log_contains(1, "kept"));
  EXPECT_TRUE(cluster.log_contains(1, "transient"));
  EXPECT_TRUE(cluster.machine(1).data().empty());

  {
    FileLogStorage reloaded(cluster.data_directory(1));
    const auto vote = reloaded.load_vote();
    ASSERT_TRUE(vote.ok()) << vote.error();
    ASSERT_TRUE(vote.value().has_value());
    EXPECT_EQ(vote.value()->leader.term, term_before_restart);
    EXPECT_FALSE(vote.value()->committed);
    const auto stored = reloaded.read(1, std::numeric_limits<LogIndex>::max());
    ASSERT_TRUE(stored.ok()) << stored.error();
    EXPECT_EQ(stored.value().size(), log_size);
    EXPECT_TRUE(has_payload(stored.value(), "kept"));
    EXPECT_TRUE(has_payload(stored.value(), "transient"));
  }

  ASSERT_TRUE(cluster.until(
      [&] {
        const std::vector<NodeId> leaders = cluster.leaders();
        if (leaders.size() != 1 || leaders.front() == 1) {
          return false;
        }
        return cluster.node(2).term() == cluster.node(3).term() &&
               cluster.node(2).current_leader() == leaders.front() &&
               cluster.node(3).current_leader() == leaders.front();
      },
      40))
      << cluster.debug();

  const NodeId leader = cluster.leaders().front();
  const Result<LogIndex> replacement = cluster.node(leader).write("replacement");
  ASSERT_TRUE(replacement.ok()) << cluster.debug();
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.machine(2).data().size() == 2 && cluster.machine(3).data().size() == 2;
      },
      20))
      << cluster.debug();
  EXPECT_FALSE(cluster.log_contains(2, "transient"));
  EXPECT_FALSE(cluster.log_contains(3, "transient"));

  cluster.heal();
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.agreed_on(leader) && !cluster.log_contains(1, "transient") &&
               cluster.log_contains(1, "kept") && cluster.log_contains(1, "replacement") &&
               cluster.machine(1).data().size() == 2;
      },
      40))
      << cluster.debug();

  for (NodeId id : {2u, 3u}) {
    ASSERT_EQ(cluster.machine(id).data().size(), 2u) << cluster.debug();
    EXPECT_EQ(cluster.machine(id).data()[0], "kept");
    EXPECT_EQ(cluster.machine(id).data()[1], "replacement");
    EXPECT_FALSE(cluster.log_contains(id, "transient"));
  }
  EXPECT_EQ(cluster.machine(1).data()[0], "kept");
  EXPECT_EQ(cluster.machine(1).data()[1], "replacement");
}

}  // namespace
}  // namespace easyraft
