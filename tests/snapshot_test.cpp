// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright 2026 easyraft contributors

#include "cluster.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <string>
#include <system_error>
#include <variant>
#include <vector>

#include <unistd.h>

namespace easyraft {
namespace {

class TempDir {
 public:
  TempDir() {
    static std::atomic<std::uint64_t> seq{0};
    path_ = std::filesystem::temp_directory_path() /
            ("easyraft-snap-" + std::to_string(::getpid()) + "-" + std::to_string(seq.fetch_add(1)));
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

Snapshot image_of(LogId id, Membership membership, std::string data) {
  Snapshot snapshot;
  snapshot.meta.last_included = id;
  snapshot.meta.membership = std::move(membership);
  snapshot.meta.snapshot_id = snapshot_id_from(id);
  snapshot.data = std::move(data);
  return snapshot;
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

Config pinned(int timeout) {
  Config config;
  config.heartbeat_interval = 1;
  config.election_timeout_min = timeout;
  config.election_timeout_max = timeout;
  config.enable_pre_vote = true;
  return config;
}

TEST(SnapshotStateMachine, BuildAndInstallReplaceAppliedState) {
  MemoryStateMachine built;
  ASSERT_TRUE(built.apply(entry(1, 1, 1, EntryKind::Blank)).ok());
  std::string binary("a");
  binary.push_back('\0');
  binary.push_back('b');
  ASSERT_TRUE(built.apply(entry(1, 1, 2, EntryKind::Application, binary)).ok());
  ASSERT_TRUE(built.apply(entry(1, 1, 3, EntryKind::Application, "c")).ok());
  const auto bytes = built.build_snapshot();
  ASSERT_TRUE(bytes.ok()) << bytes.error();

  MemoryStateMachine installed;
  const SnapshotMeta meta = image_of(LogId{1, 1, 3}, Membership{{1, 2}}, *bytes).meta;
  ASSERT_TRUE(installed.install_snapshot(meta, *bytes).ok());
  EXPECT_EQ(installed.last_applied(), 3u);
  ASSERT_EQ(installed.data().size(), 2u);
  EXPECT_EQ(installed.data()[0], binary);
  EXPECT_EQ(installed.data()[1], "c");

  EXPECT_EQ(installed.install_snapshot(image_of(LogId{1, 1, 2}, Membership{{1}}, {}).meta, *bytes).error(),
            Error::Rejected);
  EXPECT_EQ(installed.last_applied(), 3u);
  EXPECT_EQ(installed.data().size(), 2u);

  EXPECT_FALSE(installed.install_snapshot(meta, "nope").ok());
  EXPECT_EQ(installed.last_applied(), 3u);
}

TEST(MemoryLogStorage, SnapshotSurvivesPurgeAndSuffixTruncate) {
  MemoryLogStorage storage;
  ASSERT_TRUE(storage.append({app(1, "one"), app(2, "two"), app(3, "three"), app(4, "four")}).ok());
  const Snapshot snapshot =
      image_of(LogId{2, 1, 2}, Membership::joint_of({1, 2}, {2, 3}), "state");
  ASSERT_TRUE(storage.save_snapshot(snapshot).ok());

  const auto loaded = storage.load_snapshot();
  ASSERT_TRUE(loaded.ok());
  ASSERT_TRUE(loaded.value().has_value());
  EXPECT_EQ(loaded.value()->meta, snapshot.meta);
  EXPECT_EQ(loaded.value()->data, "state");
  ASSERT_EQ(storage.read(1, 10).value().size(), 4u);

  EXPECT_FALSE(storage.truncate_from(2).ok());
  EXPECT_EQ(storage.read(1, 10).value().size(), 4u);
  EXPECT_TRUE(storage.load_snapshot().value().has_value());

  ASSERT_TRUE(storage.truncate_from(4).ok());
  ASSERT_EQ(storage.read(1, 10).value().size(), 3u);
  EXPECT_EQ(storage.read(1, 10).value().back().data, "three");

  ASSERT_TRUE(storage.purge_through(2).ok());
  const auto kept = storage.read(1, 10);
  ASSERT_TRUE(kept.ok());
  ASSERT_EQ(kept.value().size(), 1u);
  EXPECT_EQ(kept.value().front().id.index, 3u);
  EXPECT_EQ(kept.value().front().data, "three");
  EXPECT_FALSE(storage.append({app(1, "nope")}).ok());
  ASSERT_TRUE(storage.append({app(4, "four")}).ok());

  const Snapshot older = image_of(LogId{1, 1, 1}, Membership{{1}}, "old");
  EXPECT_FALSE(storage.save_snapshot(older).ok());
  EXPECT_EQ(storage.load_snapshot().value()->meta.last_included.index, 2u);
  EXPECT_FALSE(storage.purge_through(1).ok());

  ASSERT_TRUE(storage.truncate_from(0).ok());
  EXPECT_TRUE(storage.read(1, 10).value().empty());
  EXPECT_EQ(storage.load_snapshot().value()->data, "state");
  EXPECT_FALSE(storage.append({app(1, "reset")}).ok());
  ASSERT_TRUE(storage.append({app(3, "after")}).ok());
  EXPECT_EQ(storage.read(1, 10).value().front().data, "after");
}

TEST(FileLogStorage, SnapshotRoundTripPurgeAndReopen) {
  TempDir dir;
  const std::filesystem::path path = dir.path();
  const Snapshot snapshot =
      image_of(LogId{4, 2, 2}, Membership::joint_of({3, 1}, {1, 4}), std::string("bin\0ary", 7));
  {
    FileLogStorage storage(path);
    ASSERT_TRUE(storage.append({app(1, "one"), app(2, "two"), app(3, "three"), app(4, "four")}).ok());
    ASSERT_TRUE(storage.save_snapshot(snapshot).ok());
    EXPECT_FALSE(std::filesystem::exists(path / "snapshot.tmp"));
    ASSERT_EQ(storage.read(1, 10).value().size(), 4u);
    EXPECT_FALSE(storage.truncate_from(2).ok());
    ASSERT_TRUE(storage.purge_through(2).ok());
    const auto kept = storage.read(1, 10);
    ASSERT_TRUE(kept.ok());
    ASSERT_EQ(kept.value().size(), 2u);
    EXPECT_EQ(kept.value()[0].id.index, 3u);
    EXPECT_EQ(kept.value()[0].data, "three");
    EXPECT_EQ(kept.value()[1].data, "four");
    ASSERT_TRUE(storage.truncate_from(4).ok());
    EXPECT_EQ(storage.read(1, 10).value().size(), 1u);
    EXPECT_EQ(storage.load_snapshot().value()->meta, snapshot.meta);
  }

  FileLogStorage reopened(path);
  const auto loaded = reopened.load_snapshot();
  ASSERT_TRUE(loaded.ok()) << loaded.error();
  ASSERT_TRUE(loaded.value().has_value());
  EXPECT_EQ(loaded.value()->meta, snapshot.meta);
  EXPECT_EQ(loaded.value()->data, snapshot.data);
  EXPECT_TRUE(loaded.value()->meta.membership.is_joint());
  const auto kept = reopened.read(1, 10);
  ASSERT_TRUE(kept.ok());
  ASSERT_EQ(kept.value().size(), 1u);
  EXPECT_EQ(kept.value().front().data, "three");
  EXPECT_FALSE(reopened.append({app(2, "rewind")}).ok());
  ASSERT_TRUE(reopened.append({app(4, "next")}).ok());
  EXPECT_FALSE(reopened.truncate_from(2).ok());
  EXPECT_EQ(reopened.load_snapshot().value()->meta.last_included.index, 2u);
}

TEST(FileLogStorage, IncompleteSnapshotAndPurgeFilesAreIgnored) {
  TempDir dir;
  const auto path = dir.path();
  const Snapshot snapshot = image_of(LogId{1, 1, 2}, Membership{{1, 2, 3}}, "kept-state");
  {
    FileLogStorage storage(path);
    ASSERT_TRUE(storage.append({app(1, "one"), app(2, "two"), app(3, "three")}).ok());
    ASSERT_TRUE(storage.save_snapshot(snapshot).ok());
  }
  write_bytes(path / "snapshot.tmp", std::string("partial-snapshot"));
  write_bytes(path / "log.rewrite", std::string("partial-rewrite"));

  {
    FileLogStorage storage(path);
    const auto loaded = storage.load_snapshot();
    ASSERT_TRUE(loaded.ok()) << loaded.error();
    ASSERT_TRUE(loaded.value().has_value());
    EXPECT_EQ(loaded.value()->data, "kept-state");
    ASSERT_EQ(storage.read(1, 10).value().size(), 3u);
    EXPECT_FALSE(std::filesystem::exists(path / "snapshot.tmp"));
    EXPECT_FALSE(std::filesystem::exists(path / "log.rewrite"));
    ASSERT_TRUE(storage.purge_through(2).ok());
    ASSERT_EQ(storage.read(1, 10).value().size(), 1u);
    EXPECT_EQ(storage.read(1, 10).value().front().data, "three");
  }

  FileLogStorage again(path);
  EXPECT_EQ(again.read(1, 10).value().front().data, "three");
  EXPECT_EQ(again.load_snapshot().value()->data, "kept-state");
}

TEST(FileLogStorage, CorruptSnapshotFailsOpenAndLeavesTheLog) {
  TempDir dir;
  const auto path = dir.path();
  {
    FileLogStorage storage(path);
    ASSERT_TRUE(storage.append({app(1, "survive")}).ok());
    ASSERT_TRUE(storage.save_snapshot(image_of(LogId{1, 1, 1}, Membership{{1}}, "state")).ok());
  }
  const auto log_bytes = read_bytes(path / "log");
  std::string broken = read_bytes(path / "snapshot");
  ASSERT_FALSE(broken.empty());
  broken.back() = static_cast<char>(static_cast<unsigned char>(broken.back()) ^ 0xffu);
  write_bytes(path / "snapshot", broken);

  FileLogStorage storage(path);
  EXPECT_FALSE(storage.load_snapshot().ok());
  EXPECT_FALSE(storage.read(1, 10).ok());
  EXPECT_EQ(read_bytes(path / "log"), log_bytes);
  EXPECT_NE(log_bytes.find("survive"), std::string::npos);
}

TEST(FileLogStorage, PurgedSuffixKeepsTornTailRecovery) {
  TempDir dir;
  const auto path = dir.path();
  const auto log_path = path / "log";
  const Snapshot snapshot = image_of(LogId{1, 1, 1}, Membership{{1, 2}}, "state");
  {
    FileLogStorage storage(path);
    ASSERT_TRUE(storage.append({app(1, "covered"), app(2, "keep-two"), app(3, "keep-three"),
                                app(4, "drop-four")})
                    .ok());
    ASSERT_TRUE(storage.save_snapshot(snapshot).ok());
    ASSERT_TRUE(storage.purge_through(1).ok());
  }
  {
    std::string bytes = read_bytes(log_path);
    ASSERT_GT(bytes.size(), 20u);
    bytes.resize(bytes.size() - 10);
    write_bytes(log_path, bytes);
  }

  FileLogStorage storage(path);
  const auto kept = storage.read(1, 10);
  ASSERT_TRUE(kept.ok()) << kept.error();
  ASSERT_EQ(kept.value().size(), 2u);
  EXPECT_EQ(kept.value()[0].id.index, 2u);
  EXPECT_EQ(kept.value()[0].data, "keep-two");
  EXPECT_EQ(kept.value()[1].data, "keep-three");
  EXPECT_EQ(storage.load_snapshot().value()->data, "state");
  EXPECT_FALSE(storage.append({app(2, "rewind")}).ok());
  ASSERT_TRUE(storage.append({app(4, "replacement")}).ok());
  EXPECT_EQ(storage.read(2, 10).value().back().data, "replacement");
}

TEST(EngineSnapshot, RecoverUsesSnapshotMembershipThenLaterLogEntries) {
  std::vector<LogEntry> log;
  log.push_back(app(1, "old"));
  log.push_back(app(2, "covered"));
  LogEntry membership_entry;
  membership_entry.id = LogId{3, 1, 3};
  membership_entry.kind = EntryKind::Membership;
  membership_entry.data = Membership::joint_of({1, 2, 3}, {3, 4}).encode();
  log.push_back(membership_entry);
  log.push_back(app(4, "later"));

  Engine engine(2, pinned(5));
  const Snapshot snapshot = image_of(LogId{1, 1, 2}, Membership{{1, 2, 3}}, "state");
  const StepResult recovered =
      engine.recover(Membership{{9}}, Vote{}, log, 0, snapshot);
  EXPECT_EQ(engine.membership(), Membership::joint_of({1, 2, 3}, {3, 4}));
  ASSERT_EQ(engine.log().size(), 2u);
  EXPECT_EQ(engine.log().front().id.index, 3u);
  EXPECT_EQ(engine.log().back().data, "later");
  EXPECT_EQ(engine.compacted_index(), 2u);
  EXPECT_EQ(engine.commit_index(), 2u);
  EXPECT_EQ(engine.last_applied(), 2u);
  EXPECT_EQ(engine.first_available_index(), 3u);
  bool purged = false;
  for (const Command& command : recovered.commands) {
    if (command.kind == CommandKind::Purge && command.index == 2) {
      purged = true;
    }
    EXPECT_NE(command.kind, CommandKind::Truncate);
  }
  EXPECT_TRUE(purged);

  Engine bare(2, pinned(5));
  const StepResult only_snapshot =
      bare.recover(Membership{{9}}, Vote{}, {app(1, "old"), app(2, "covered")}, 0, snapshot);
  EXPECT_EQ(bare.membership(), (Membership{{1, 2, 3}}));
  EXPECT_TRUE(bare.log().empty());
  EXPECT_EQ(bare.last_index(), 2u);
  bool saved = false;
  for (const Command& command : only_snapshot.commands) {
    if (command.kind == CommandKind::SaveMembership) {
      saved = true;
      EXPECT_EQ(command.membership, (Membership{{1, 2, 3}}));
    }
  }
  EXPECT_TRUE(saved);
}

TEST(EngineSnapshot, StaleInstallIsIgnoredAndANewerImageKeepsAMatchingSuffix) {
  Engine engine(2, pinned(5));
  Vote vote;
  vote.leader = LeaderId{4, 1};
  vote.committed = true;
  std::vector<LogEntry> log;
  log.push_back(entry(1, 1, 1, EntryKind::Blank));
  log.push_back(entry(1, 1, 2, EntryKind::Application, "a"));
  log.push_back(entry(1, 1, 3, EntryKind::Application, "b"));
  log.push_back(entry(1, 1, 4, EntryKind::Application, "c"));
  log.push_back(entry(2, 1, 5, EntryKind::Application, "keep"));
  const Snapshot current = image_of(LogId{1, 1, 3}, Membership{{1, 2}}, "state");
  engine.recover(Membership{{1, 2}}, vote, log, 3, current);
  ASSERT_EQ(engine.log().size(), 2u);
  EXPECT_EQ(engine.log().front().data, "c");

  InstallSnapshotRequest stale;
  stale.vote = vote;
  stale.meta = image_of(LogId{1, 1, 2}, Membership{{1, 2}}, "older").meta;
  stale.data = "older";
  stale.done = true;
  const InstallStep rejected = engine.on_install(stale);
  EXPECT_TRUE(rejected.response.stale);
  EXPECT_FALSE(rejected.response.installed);
  EXPECT_TRUE(rejected.step.commands.empty());
  EXPECT_EQ(engine.compacted_index(), 3u);
  EXPECT_EQ(engine.log().front().data, "c");
  EXPECT_EQ(rejected.response.follower_index, 3u);

  InstallSnapshotRequest same = stale;
  same.meta = current.meta;
  same.data = current.data;
  EXPECT_TRUE(engine.on_install(same).response.stale);

  const std::string payload = "next-state";
  InstallSnapshotRequest newer;
  newer.vote = vote;
  newer.meta = image_of(LogId{1, 1, 4}, Membership{{1, 2, 3}}, payload).meta;
  newer.data = payload.substr(0, 4);
  newer.offset = 0;
  newer.done = false;
  const InstallStep partial = engine.on_install(newer);
  EXPECT_TRUE(partial.response.success);
  EXPECT_FALSE(partial.response.installed);
  EXPECT_EQ(partial.response.received, 4u);
  EXPECT_EQ(engine.compacted_index(), 3u);

  newer.offset = 4;
  newer.data = payload.substr(4);
  newer.done = true;
  const InstallStep installed = engine.on_install(newer);
  EXPECT_TRUE(installed.response.installed) << "empty done chunk finishes the image";
  EXPECT_EQ(engine.compacted_index(), 4u);
  EXPECT_EQ(engine.last_applied(), 4u);
  EXPECT_EQ(engine.membership(), (Membership{{1, 2, 3}}));
  ASSERT_EQ(engine.log().size(), 1u);
  EXPECT_EQ(engine.log().front().id.index, 5u);
  EXPECT_EQ(engine.log().front().data, "keep");
  bool saved = false;
  bool purged = false;
  for (const Command& command : installed.step.commands) {
    if (command.kind == CommandKind::SaveSnapshot && !command.build_snapshot &&
        command.snapshot.data == "next-state") {
      saved = true;
    }
    if (command.kind == CommandKind::Purge && command.index == 4) {
      purged = true;
    }
    EXPECT_NE(command.kind, CommandKind::Truncate);
  }
  EXPECT_TRUE(saved);
  EXPECT_TRUE(purged);
}

TEST(EngineSnapshot, ConflictingSnapshotDropsTheSuffix) {
  Engine engine(2, pinned(5));
  Vote vote;
  vote.leader = LeaderId{3, 1};
  vote.committed = true;
  std::vector<LogEntry> log{entry(1, 1, 1, EntryKind::Application, "a"),
                            entry(9, 9, 2, EntryKind::Application, "diverge"),
                            entry(9, 9, 3, EntryKind::Application, "tail")};
  engine.recover(Membership{{1, 2}}, vote, std::move(log), 1);

  InstallSnapshotRequest request;
  request.vote = vote;
  request.meta = image_of(LogId{1, 1, 2}, Membership{{2, 4}}, "snap").meta;
  request.data = "snap";
  request.done = true;
  const InstallStep installed = engine.on_install(request);
  EXPECT_TRUE(installed.response.installed);
  EXPECT_TRUE(engine.log().empty());
  EXPECT_EQ(engine.membership(), (Membership{{2, 4}}));
  EXPECT_EQ(engine.last_applied(), 2u);
  bool truncated = false;
  for (const Command& command : installed.step.commands) {
    if (command.kind == CommandKind::Truncate && command.index == 3) {
      truncated = true;
    }
  }
  EXPECT_TRUE(truncated);
}

TEST(EngineSnapshot, LeaderSendsSnapshotToANewVoter) {
  Engine engine(1, pinned(2));
  engine.recover(Membership{{1}}, Vote{}, {});
  engine.on_tick();
  engine.on_tick();
  ASSERT_EQ(engine.role(), Role::Leader);
  ASSERT_TRUE(engine.propose("hello").accepted);
  const SnapshotBuild built = engine.begin_snapshot();
  ASSERT_TRUE(built.started);
  EXPECT_TRUE(engine.log().empty());
  EXPECT_EQ(engine.compacted_index(), 2u);
  bool build = false;
  for (const Command& command : built.step.commands) {
    if (command.kind == CommandKind::SaveSnapshot && command.build_snapshot) {
      build = true;
      EXPECT_EQ(command.snapshot.meta.last_included.index, 2u);
      EXPECT_EQ(command.snapshot.meta.membership, (Membership{{1}}));
    }
  }
  EXPECT_TRUE(build);
  engine.store_snapshot_bytes("bytes");

  const ProposeResult changed = engine.change_membership({1, 2});
  ASSERT_TRUE(changed.accepted);
  bool saw_snapshot = false;
  for (const Outbound& message : changed.step.outbound) {
    if (message.to != 2) {
      continue;
    }
    const auto* request = std::get_if<InstallSnapshotRequest>(&message.payload);
    ASSERT_NE(request, nullptr);
    EXPECT_EQ(request->offset, 0u);
    EXPECT_EQ(request->data, "bytes");
    EXPECT_TRUE(request->done);
    EXPECT_EQ(request->meta.last_included.index, 2u);
    saw_snapshot = true;
  }
  EXPECT_TRUE(saw_snapshot);
}

TEST(NodeSnapshot, OpenPurgesAPrefixLeftBehindByACrash) {
  TempDir dir;
  auto storage = std::make_shared<FileLogStorage>(dir.path());
  MemoryStateMachine seeded;
  ASSERT_TRUE(seeded.apply(entry(1, 1, 1, EntryKind::Blank)).ok());
  ASSERT_TRUE(seeded.apply(entry(1, 1, 2, EntryKind::Application, "kept")).ok());
  const auto bytes = seeded.build_snapshot();
  ASSERT_TRUE(bytes.ok());
  ASSERT_TRUE(storage->save_membership(Membership{{1, 2}}).ok());
  Vote vote;
  vote.leader = LeaderId{2, 1};
  vote.committed = true;
  ASSERT_TRUE(storage->save_vote(vote).ok());
  ASSERT_TRUE(storage
                  ->append({entry(1, 1, 1, EntryKind::Blank),
                            entry(1, 1, 2, EntryKind::Application, "kept"),
                            entry(2, 1, 3, EntryKind::Application, "suffix")})
                  .ok());
  ASSERT_TRUE(storage->save_snapshot(image_of(LogId{1, 1, 2}, Membership{{1, 2}}, *bytes)).ok());
  ASSERT_EQ(storage->read(1, 10).value().size(), 3u);

  auto machine = std::make_shared<MemoryStateMachine>();
  RaftNode node(1, pinned(6), storage, machine, nullptr);
  ASSERT_TRUE(node.initialized());
  EXPECT_EQ(node.compacted_index(), 2u);
  EXPECT_EQ(node.first_available_index(), 3u);
  ASSERT_EQ(node.log().size(), 1u);
  EXPECT_EQ(node.log().front().data, "suffix");
  EXPECT_EQ(node.membership(), (Membership{{1, 2}}));
  ASSERT_EQ(machine->data().size(), 1u);
  EXPECT_EQ(machine->data().front(), "kept");
  const auto stored = storage->read(1, 10);
  ASSERT_TRUE(stored.ok());
  ASSERT_EQ(stored.value().size(), 1u);
  EXPECT_EQ(stored.value().front().data, "suffix");
}

class SnapshotCluster : public ::testing::TestWithParam<test::StorageMode> {};

TEST_P(SnapshotCluster, RestartRestoresSnapshotAndReplaysTheSuffix) {
  test::Cluster cluster({1, 2, 3}, test::fast_config(4), true, GetParam());
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 30)) << cluster.debug();
  ASSERT_TRUE(cluster.node(1).write("kept").ok());
  ASSERT_TRUE(cluster.node(1).write("also").ok());
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.machine(1).data().size() == 2 && cluster.machine(2).data().size() == 2 &&
               cluster.machine(3).data().size() == 2;
      },
      30))
      << cluster.debug();
  ASSERT_TRUE(cluster.node(1).trigger_snapshot().ok()) << cluster.debug();
  ASSERT_TRUE(cluster.node(1).write("later").ok());
  ASSERT_TRUE(cluster.until(
      [&] { return cluster.machine(2).data().size() == 3 && cluster.machine(3).data().size() == 3; },
      30))
      << cluster.debug();

  const auto meta = cluster.node(1).snapshot_meta();
  ASSERT_TRUE(meta.has_value());
  EXPECT_EQ(meta->membership, (Membership{{1, 2, 3}}));
  EXPECT_FALSE(cluster.log_contains(1, "kept"));
  EXPECT_TRUE(cluster.log_contains(1, "later"));
  EXPECT_GT(cluster.node(1).first_available_index(), 1u);

  cluster.restart(1);
  ASSERT_TRUE(cluster.node(1).snapshot_meta().has_value());
  ASSERT_EQ(cluster.machine(1).data().size(), 2u) << cluster.debug();
  EXPECT_EQ(cluster.machine(1).data()[0], "kept");
  EXPECT_EQ(cluster.machine(1).data()[1], "also");
  EXPECT_TRUE(cluster.log_contains(1, "later"));
  EXPECT_FALSE(cluster.log_contains(1, "kept"));
  EXPECT_EQ(cluster.node(1).membership(), (Membership{{1, 2, 3}}));

  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.machine(1).data().size() == 3 && cluster.machine(1).data().back() == "later";
      },
      40))
      << cluster.debug();
}

TEST_P(SnapshotCluster, UncommittedSuffixIsNotCompacted) {
  test::Cluster cluster({1, 2, 3}, test::fast_config(4), true, GetParam());
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 30)) << cluster.debug();
  ASSERT_TRUE(cluster.node(1).write("kept").ok());
  ASSERT_TRUE(cluster.until(
      [&] { return cluster.machine(1).data().size() == 1 && cluster.machine(2).data().size() == 1; },
      20))
      << cluster.debug();
  cluster.isolate(1);
  ASSERT_TRUE(cluster.node(1).write("transient").ok()) << cluster.debug();
  ASSERT_TRUE(cluster.node(1).trigger_snapshot().ok()) << cluster.debug();

  const auto meta = cluster.node(1).snapshot_meta();
  ASSERT_TRUE(meta.has_value());
  EXPECT_FALSE(cluster.log_contains(1, "kept"));
  EXPECT_TRUE(cluster.log_contains(1, "transient"));
  LogIndex transient_index = 0;
  for (const LogEntry& item : cluster.node(1).log()) {
    if (item.data == "transient") {
      transient_index = item.id.index;
    }
  }
  EXPECT_GT(transient_index, meta->last_included.index);
  ASSERT_EQ(cluster.machine(1).data().size(), 1u);
  EXPECT_EQ(cluster.machine(1).data().front(), "kept");
  EXPECT_EQ(cluster.node(1).snapshot_meta()->membership, (Membership{{1, 2, 3}}));
}

TEST_P(SnapshotCluster, LaggingFollowerCatchesUpFromASnapshot) {
  Config config = test::fast_config(4);
  config.max_snapshot_chunk = 3;
  test::Cluster cluster({1, 2, 3}, config, true, GetParam());
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 30)) << cluster.debug();
  cluster.stop(3);
  ASSERT_TRUE(cluster.node(1).write("alpha").ok());
  ASSERT_TRUE(cluster.node(1).write("beta").ok());
  ASSERT_TRUE(cluster.until(
      [&] { return cluster.machine(1).data().size() == 2 && cluster.machine(2).data().size() == 2; },
      20))
      << cluster.debug();
  ASSERT_TRUE(cluster.node(1).trigger_snapshot().ok()) << cluster.debug();
  ASSERT_TRUE(cluster.node(1).write("gamma").ok());
  ASSERT_TRUE(cluster.until([&] { return cluster.machine(2).data().size() == 3; }, 20))
      << cluster.debug();
  EXPECT_GT(cluster.node(1).first_available_index(), 1u);
  EXPECT_FALSE(cluster.log_contains(1, "alpha"));

  cluster.restart(3);
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.node(3).snapshot_meta().has_value() && cluster.machine(3).data().size() == 3;
      },
      40))
      << cluster.debug();
  EXPECT_EQ(cluster.machine(3).data()[0], "alpha");
  EXPECT_EQ(cluster.machine(3).data()[1], "beta");
  EXPECT_EQ(cluster.machine(3).data()[2], "gamma");
  EXPECT_FALSE(cluster.log_contains(3, "alpha"));
  EXPECT_FALSE(cluster.log_contains(3, "beta"));
  EXPECT_TRUE(cluster.log_contains(3, "gamma"));
  EXPECT_EQ(cluster.node(3).snapshot_meta()->last_included.index,
            cluster.node(1).snapshot_meta()->last_included.index);
}

TEST_P(SnapshotCluster, NewVoterCatchesUpFromASnapshot) {
  Config config = test::fast_config(4);
  config.max_snapshot_chunk = 4;
  test::Cluster cluster({1, 2, 3}, config, true, GetParam());
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 30)) << cluster.debug();
  ASSERT_TRUE(cluster.node(1).write("alpha").ok());
  ASSERT_TRUE(cluster.node(1).write("beta").ok());
  ASSERT_TRUE(cluster.until(
      [&] { return cluster.machine(2).data().size() == 2 && cluster.machine(3).data().size() == 2; },
      20))
      << cluster.debug();
  ASSERT_TRUE(cluster.node(1).trigger_snapshot().ok()) << cluster.debug();
  EXPECT_FALSE(cluster.log_contains(1, "alpha"));

  cluster.add_node(4);
  ASSERT_TRUE(cluster.node(1).change_membership({1, 2, 3, 4}).ok()) << cluster.debug();
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.node(4).snapshot_meta().has_value() && cluster.machine(4).data().size() == 2 &&
               cluster.node(4).membership().voters() == std::vector<NodeId>{1, 2, 3, 4};
      },
      80))
      << cluster.debug();
  EXPECT_EQ(cluster.machine(4).data()[0], "alpha");
  EXPECT_EQ(cluster.machine(4).data()[1], "beta");
  EXPECT_FALSE(cluster.log_contains(4, "alpha"));
  EXPECT_FALSE(cluster.log_contains(4, "beta"));
  EXPECT_GT(cluster.node(4).first_available_index(), 1u);
}

TEST_P(SnapshotCluster, FollowerCompactsLocallyAndStillReplicates) {
  test::Cluster cluster({1, 2, 3}, test::fast_config(4), true, GetParam());
  ASSERT_TRUE(cluster.until([&] { return cluster.agreed_on(1); }, 30)) << cluster.debug();
  ASSERT_TRUE(cluster.node(1).write("kept").ok());
  ASSERT_TRUE(cluster.until([&] { return cluster.machine(2).data().size() == 1; }, 20))
      << cluster.debug();
  ASSERT_EQ(cluster.node(2).role(), Role::Follower);
  ASSERT_TRUE(cluster.node(2).trigger_snapshot().ok()) << cluster.debug();
  EXPECT_FALSE(cluster.log_contains(2, "kept"));
  EXPECT_TRUE(cluster.log_contains(1, "kept"));
  ASSERT_EQ(cluster.machine(2).data().size(), 1u);
  EXPECT_EQ(cluster.machine(2).data().front(), "kept");

  ASSERT_TRUE(cluster.node(1).write("next").ok());
  ASSERT_TRUE(cluster.until(
      [&] { return cluster.machine(2).data().size() == 2 && cluster.machine(2).data().back() == "next"; },
      20))
      << cluster.debug();
  EXPECT_TRUE(cluster.log_contains(2, "next"));
}

TEST(SnapshotCluster, ThresholdCompactsASingleNode) {
  Config config = test::fast_config(2);
  config.snapshot_threshold = 2;
  test::Cluster cluster({1}, config, false);
  ASSERT_TRUE(cluster.until([&] { return cluster.node(1).role() == Role::Leader; }, 5))
      << cluster.debug();
  EXPECT_FALSE(cluster.node(1).snapshot_meta().has_value());
  ASSERT_TRUE(cluster.node(1).write("one").ok());
  ASSERT_TRUE(cluster.node(1).snapshot_meta().has_value()) << cluster.debug();
  EXPECT_EQ(cluster.node(1).compacted_index(), cluster.node(1).last_log_index());
  EXPECT_FALSE(cluster.log_contains(1, "one"));
  ASSERT_EQ(cluster.machine(1).data().size(), 1u);
  EXPECT_EQ(cluster.machine(1).data().front(), "one");
  EXPECT_EQ(cluster.node(1).trigger_snapshot().error(), Error::Rejected);

  cluster.restart(1);
  ASSERT_EQ(cluster.machine(1).data().size(), 1u);
  EXPECT_EQ(cluster.machine(1).data().front(), "one");
}

INSTANTIATE_TEST_SUITE_P(
    MemoryAndFile,
    SnapshotCluster,
    ::testing::Values(test::StorageMode::Memory, test::StorageMode::File),
    [](const ::testing::TestParamInfo<test::StorageMode>& param) {
      return param.param == test::StorageMode::Memory ? std::string("Memory") : std::string("File");
    });

}  // namespace
}  // namespace easyraft
