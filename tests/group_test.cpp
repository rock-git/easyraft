// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright 2026 easyraft contributors

#include "easyraft/easyraft.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <map>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <variant>
#include <vector>

#include <unistd.h>

namespace easyraft {
namespace {

LogEntry app(LogIndex index, std::string data) {
  LogEntry entry;
  entry.id = LogId{1, 1, index};
  entry.kind = EntryKind::Application;
  entry.data = std::move(data);
  return entry;
}

TEST(MemoryGroupStore, LogsVotesAndSnapshotsStayApart) {
  MemoryGroupStore store;
  const std::shared_ptr<MemoryLogStorage> first = store.open(1);
  const std::shared_ptr<MemoryLogStorage> second = store.open(2);
  EXPECT_NE(first.get(), second.get());
  EXPECT_EQ(store.open(1).get(), first.get());
  EXPECT_EQ(store.find(1), first.get());
  EXPECT_EQ(store.find(3), nullptr);
  EXPECT_TRUE(store.contains(1));
  EXPECT_FALSE(store.contains(3));
  EXPECT_EQ(store.groups(), (std::vector<GroupId>{1, 2}));

  Vote vote1;
  vote1.leader = LeaderId{4, 1};
  vote1.committed = true;
  Vote vote2;
  vote2.leader = LeaderId{7, 3};
  ASSERT_TRUE(first->save_vote(vote1).ok());
  ASSERT_TRUE(second->save_vote(vote2).ok());
  ASSERT_TRUE(first->save_membership(Membership{{1, 2}}).ok());
  ASSERT_TRUE(second->save_membership(Membership{{3}}).ok());
  ASSERT_TRUE(first->append({app(1, "group-one-payload")}).ok());
  ASSERT_TRUE(second->append({app(1, "group-two-payload")}).ok());

  Snapshot snap1;
  snap1.meta.last_included = LogId{1, 1, 1};
  snap1.meta.membership = Membership{{1, 2}};
  snap1.meta.snapshot_id = snapshot_id_from(snap1.meta.last_included);
  snap1.data = "snap-group-1";
  Snapshot snap2 = snap1;
  snap2.meta.last_included = LogId{2, 3, 1};
  snap2.meta.membership = Membership{{3}};
  snap2.meta.snapshot_id = snapshot_id_from(snap2.meta.last_included);
  snap2.data = "snap-group-2";
  ASSERT_TRUE(first->save_snapshot(snap1).ok());
  ASSERT_TRUE(second->save_snapshot(snap2).ok());

  EXPECT_EQ(first->persisted_vote(), vote1);
  EXPECT_EQ(second->persisted_vote(), vote2);
  EXPECT_EQ(first->persisted_membership().voters(), (std::vector<NodeId>{1, 2}));
  EXPECT_EQ(second->persisted_membership().voters(), (std::vector<NodeId>{3}));
  ASSERT_EQ(first->persisted_log().size(), 1u);
  ASSERT_EQ(second->persisted_log().size(), 1u);
  EXPECT_EQ(first->persisted_log().front().data, "group-one-payload");
  EXPECT_EQ(second->persisted_log().front().data, "group-two-payload");
  ASSERT_TRUE(first->load_snapshot().value().has_value());
  ASSERT_TRUE(second->load_snapshot().value().has_value());
  EXPECT_EQ(first->load_snapshot().value()->data, "snap-group-1");
  EXPECT_EQ(second->load_snapshot().value()->data, "snap-group-2");
}

Config pinned(int timeout) {
  Config config;
  config.heartbeat_interval = 1;
  config.election_timeout_min = timeout;
  config.election_timeout_max = timeout;
  config.enable_pre_vote = true;
  return config;
}

class RecordingBus final : public MessageBus {
 public:
  void send(const Envelope& message) override { sent.push_back(message); }

  std::vector<Envelope> sent;
};

bool log_has(const RaftNode& node, const std::string& data) {
  for (const LogEntry& entry : node.log()) {
    if (entry.kind == EntryKind::Application && entry.data == data) {
      return true;
    }
  }
  return false;
}

TEST(RaftGroups, TwoGroupsOnOneNodeCommitApart) {
  MemoryGroupStore store;
  const std::shared_ptr<MemoryLogStorage> first = store.open(10);
  const std::shared_ptr<MemoryLogStorage> second = store.open(20);
  auto alpha = std::make_shared<MemoryStateMachine>();
  auto beta = std::make_shared<MemoryStateMachine>();
  auto extra_machine = std::make_shared<MemoryStateMachine>();
  RaftGroups host(1, nullptr);

  ASSERT_TRUE(host.add(10, pinned(2), first, alpha).ok());
  ASSERT_TRUE(host.add(20, pinned(2), second, beta).ok());
  EXPECT_EQ(host.add(10, pinned(2), store.open(30), extra_machine).error(), Error::AlreadyInitialized);
  EXPECT_EQ(host.add(30, pinned(2), first, extra_machine).error(), Error::Rejected);
  EXPECT_EQ(host.add(30, pinned(2), store.open(30), alpha).error(), Error::Rejected);
  EXPECT_EQ(host.add(30, pinned(2), nullptr, extra_machine).error(), Error::Rejected);
  EXPECT_EQ(host.add(30, pinned(2), store.open(30), nullptr).error(), Error::Rejected);
  EXPECT_EQ(host.id(), 1u);
  EXPECT_EQ(host.groups(), (std::vector<GroupId>{10, 20}));
  EXPECT_EQ(host.find(30), nullptr);

  ASSERT_TRUE(host.find(10)->initialize(Membership{{1}}).ok());
  ASSERT_TRUE(host.find(20)->initialize(Membership{{1}}).ok());
  EXPECT_EQ(host.find(10)->group(), 10u);
  EXPECT_EQ(host.find(20)->group(), 20u);

  for (int tick = 0; tick < 4; ++tick) {
    host.tick();
  }
  ASSERT_EQ(host.find(10)->role(), Role::Leader);
  ASSERT_EQ(host.find(20)->role(), Role::Leader);

  const Result<LogIndex> wrote_alpha = host.find(10)->write("alpha");
  const Result<LogIndex> wrote_beta = host.find(20)->write("beta");
  ASSERT_TRUE(wrote_alpha.ok()) << wrote_alpha.error();
  ASSERT_TRUE(wrote_beta.ok()) << wrote_beta.error();
  EXPECT_GE(host.find(10)->commit_index(), wrote_alpha.value());
  EXPECT_GE(host.find(20)->commit_index(), wrote_beta.value());
  ASSERT_EQ(alpha->data().size(), 1u);
  ASSERT_EQ(beta->data().size(), 1u);
  EXPECT_EQ(alpha->data().front(), "alpha");
  EXPECT_EQ(beta->data().front(), "beta");
  EXPECT_TRUE(log_has(*host.find(10), "alpha"));
  EXPECT_FALSE(log_has(*host.find(10), "beta"));
  EXPECT_TRUE(log_has(*host.find(20), "beta"));
  EXPECT_FALSE(log_has(*host.find(20), "alpha"));
  EXPECT_EQ(first->persisted_vote().leader.node, 1u);
  EXPECT_EQ(second->persisted_vote().leader.node, 1u);
  ASSERT_FALSE(first->persisted_log().empty());
  ASSERT_FALSE(second->persisted_log().empty());
  EXPECT_EQ(first->persisted_log().back().data, "alpha");
  EXPECT_EQ(second->persisted_log().back().data, "beta");
}

TEST(RaftGroups, RoutesAMessageToItsGroupOnly) {
  RecordingBus bus;
  MemoryGroupStore store;
  RaftGroups host(1, &bus);
  ASSERT_TRUE(host.add(10, pinned(8), store.open(10), std::make_shared<MemoryStateMachine>()).ok());
  ASSERT_TRUE(host.add(20, pinned(8), store.open(20), std::make_shared<MemoryStateMachine>()).ok());
  ASSERT_TRUE(host.find(10)->initialize(Membership{{1, 2}}).ok());
  ASSERT_TRUE(host.find(20)->initialize(Membership{{1, 2}}).ok());

  RequestVote vote;
  vote.body.vote.leader = LeaderId{3, 2};
  Envelope message;
  message.from = 2;
  message.to = 1;
  message.group = 10;
  message.payload = vote;
  host.on_message(message);
  EXPECT_EQ(host.find(10)->term(), 3u);
  EXPECT_EQ(host.find(20)->term(), 0u);
  ASSERT_EQ(bus.sent.size(), 1u);
  EXPECT_EQ(bus.sent[0].group, 10u);
  EXPECT_EQ(bus.sent[0].to, 2u);
  EXPECT_TRUE(std::holds_alternative<RequestVoteResponse>(bus.sent[0].payload));

  vote.body.vote.leader = LeaderId{5, 2};
  message.group = 20;
  message.payload = vote;
  host.on_message(message);
  EXPECT_EQ(host.find(10)->term(), 3u);
  EXPECT_EQ(host.find(20)->term(), 5u);
  ASSERT_EQ(bus.sent.size(), 2u);
  EXPECT_EQ(bus.sent[1].group, 20u);

  const Vote left = host.find(10)->vote();
  const Vote right = host.find(20)->vote();
  vote.body.vote.leader = LeaderId{9, 2};
  message.group = 99;
  message.payload = vote;
  host.on_message(message);
  message.to = 4;
  message.group = 10;
  host.on_message(message);

  AppendEntriesRequest append;
  append.vote.leader = LeaderId{11, 2};
  append.vote.committed = true;
  append.leader_commit = 1;
  LogEntry foreign;
  foreign.id = LogId{11, 2, 1};
  foreign.kind = EntryKind::Application;
  foreign.data = "other-group";
  append.entries.push_back(foreign);
  message.to = 1;
  message.group = 20;
  message.payload = append;
  host.find(10)->on_message(message);

  EXPECT_EQ(host.find(10)->vote(), left);
  EXPECT_EQ(host.find(20)->vote(), right);
  EXPECT_FALSE(log_has(*host.find(10), "other-group"));
  EXPECT_FALSE(log_has(*host.find(20), "other-group"));
  EXPECT_EQ(bus.sent.size(), 2u);
}

Snapshot persisted_image() {
  Snapshot snapshot;
  snapshot.meta.last_included = LogId{1, 1, 1};
  snapshot.meta.membership = Membership{{1}};
  snapshot.meta.snapshot_id = snapshot_id_from(snapshot.meta.last_included);
  snapshot.data.assign(4, '\0');
  return snapshot;
}

class RecoveryCallbackMachine final : public RaftStateMachine {
 public:
  RaftGroups* host{nullptr};
  std::shared_ptr<RaftLogStorage> storage;
  std::weak_ptr<RaftStateMachine> self;
  Error same_group{Error::Ok};
  Error shared_storage{Error::Ok};
  Error shared_machine{Error::Ok};
  Error fresh{Error::Ok};
  bool opening_was_hidden{false};
  bool callback_ran{false};

  Result<void> apply(const LogEntry& entry) override {
    last_ = entry.id.index;
    return Result<void>::success();
  }

  LogIndex last_applied() const override { return last_; }

  Result<std::string> build_snapshot() override { return Result<std::string>::success(std::string(4, '\0')); }

  Result<void> install_snapshot(const SnapshotMeta& meta, std::string_view) override {
    if (callback_ran) {
      last_ = meta.last_included.index;
      return Result<void>::success();
    }
    callback_ran = true;
    last_ = meta.last_included.index;
    auto other_storage = std::make_shared<MemoryLogStorage>();
    auto other_machine = std::make_shared<MemoryStateMachine>();
    same_group = host->add(7, pinned(4), other_storage, other_machine).error();
    shared_storage = host->add(8, pinned(4), storage, std::make_shared<MemoryStateMachine>()).error();
    shared_machine =
        host->add(9, pinned(4), std::make_shared<MemoryLogStorage>(), self.lock()).error();
    fresh = host->add(11, pinned(4), std::make_shared<MemoryLogStorage>(),
                      std::make_shared<MemoryStateMachine>())
                 .error();
    opening_was_hidden = host->find(7) == nullptr;
    host->tick();
    Envelope message;
    message.to = host->id();
    message.group = 7;
    message.payload = RequestVote{};
    host->on_message(message);
    return Result<void>::success();
  }

 private:
  LogIndex last_{0};
};

class FailingSnapshotMachine final : public RaftStateMachine {
 public:
  Result<void> apply(const LogEntry&) override { return Result<void>::success(); }
  LogIndex last_applied() const override { return 0; }
  Result<std::string> build_snapshot() override { return Result<std::string>::success(std::string{}); }
  Result<void> install_snapshot(const SnapshotMeta&, std::string_view) override {
    return Result<void>::failure(Error::Storage);
  }
};

TEST(RaftGroups, ReservesTheGroupBeforeRecoveryCallbacks) {
  auto storage = std::make_shared<MemoryLogStorage>();
  ASSERT_TRUE(storage->save_membership(Membership{{1}}).ok());
  ASSERT_TRUE(storage->save_snapshot(persisted_image()).ok());
  auto machine = std::make_shared<RecoveryCallbackMachine>();
  RaftGroups host(1, nullptr);
  machine->host = &host;
  machine->storage = storage;
  machine->self = machine;

  const Result<void> added = host.add(7, pinned(4), storage, machine);
  ASSERT_TRUE(added.ok()) << added.error();
  EXPECT_TRUE(machine->callback_ran);
  EXPECT_EQ(machine->same_group, Error::AlreadyInitialized);
  EXPECT_EQ(machine->shared_storage, Error::Rejected);
  EXPECT_EQ(machine->shared_machine, Error::Rejected);
  EXPECT_EQ(machine->fresh, Error::Ok);
  EXPECT_TRUE(machine->opening_was_hidden);
  ASSERT_NE(host.find(7), nullptr);
  ASSERT_NE(host.find(11), nullptr);
  EXPECT_EQ(host.find(7)->group(), 7u);
  EXPECT_TRUE(host.find(7)->initialized());
  EXPECT_GE(host.find(7)->last_log_index(), 1u);
  EXPECT_EQ(host.find(11)->group(), 11u);
  EXPECT_FALSE(host.find(11)->initialized());
  EXPECT_EQ(host.find(11)->last_log_index(), 0u);
  EXPECT_EQ(host.groups(), (std::vector<GroupId>{7, 11}));
  EXPECT_EQ(host.find(8), nullptr);
  EXPECT_EQ(host.find(9), nullptr);
}

TEST(RaftGroups, DropsTheReservationWhenRecoveryFails) {
  auto storage = std::make_shared<MemoryLogStorage>();
  ASSERT_TRUE(storage->save_membership(Membership{{1}}).ok());
  ASSERT_TRUE(storage->save_snapshot(persisted_image()).ok());
  RaftGroups host(1, nullptr);
  const Result<void> failed = host.add(4, pinned(4), storage, std::make_shared<FailingSnapshotMachine>());
  EXPECT_EQ(failed.error(), Error::Storage);
  EXPECT_EQ(host.find(4), nullptr);
  EXPECT_TRUE(host.groups().empty());

  const Result<void> added = host.add(4, pinned(4), storage, std::make_shared<MemoryStateMachine>());
  ASSERT_TRUE(added.ok()) << added.error();
  ASSERT_NE(host.find(4), nullptr);
  EXPECT_TRUE(host.find(4)->initialized());
  EXPECT_EQ(host.find(4)->group(), 4u);
  EXPECT_EQ(host.groups(), (std::vector<GroupId>{4}));
}

constexpr GroupId kAlpha = 10;
constexpr GroupId kBeta = 20;
const std::vector<NodeId> kNodes{1, 2, 3};

std::string read_file(const std::filesystem::path& path) {
  std::ifstream in(path, std::ios::binary);
  return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

bool file_contains(const std::filesystem::path& path, std::string_view needle) {
  const std::string bytes = read_file(path);
  return bytes.find(needle) != std::string::npos;
}

// Three processes, each hosting the same set of Raft groups. One bus delivers
// by node id. RaftGroups then delivers by the envelope's group.
class GroupCluster {
 public:
  struct Spec {
    GroupId id{0};
    int timeout{4};
    bool reverse{false};
  };

  enum class StoreKind { Memory, File };

  GroupCluster(std::vector<NodeId> nodes, std::vector<Spec> specs, StoreKind kind = StoreKind::Memory)
      : ids_(std::move(nodes)), kind_(kind) {
    if (ids_.empty() || specs.empty()) {
      throw std::invalid_argument("group cluster needs nodes and groups");
    }
    if (kind_ == StoreKind::File) {
      static std::atomic<std::uint64_t> seq{0};
      root_ = std::filesystem::temp_directory_path() /
              ("easyraft-groups-" + std::to_string(::getpid()) + "-" + std::to_string(seq.fetch_add(1)));
      std::filesystem::create_directories(root_);
    }
    for (const Spec& spec : specs) {
      group_ids_.push_back(spec.id);
    }
    for (std::size_t index = 0; index < ids_.size(); ++index) {
      const NodeId id = ids_[index];
      hosts_.emplace(id, std::make_unique<RaftGroups>(id, &bus_));
      bus_.bind(id, [this, id](const Envelope& message) { hosts_.at(id)->on_message(message); });
      for (const Spec& spec : specs) {
        Config config;
        config.heartbeat_interval = 1;
        config.enable_pre_vote = true;
        config.snapshot_threshold = 0;
        const int step =
            spec.reverse ? static_cast<int>(ids_.size() - 1 - index) : static_cast<int>(index);
        const int timeout = spec.timeout + step * 4;
        config.election_timeout_min = timeout;
        config.election_timeout_max = timeout;
        config = config.normalized();
        const Key id_group{id, spec.id};
        configs_.emplace(id_group, config);
        std::shared_ptr<RaftLogStorage> storage;
        if (kind_ == StoreKind::File) {
          storage = FileLogStorage::open_group(root_ / std::to_string(id), spec.id);
        } else {
          storage = memory_[id].open(spec.id);
        }
        stores_.emplace(id_group, storage);
        auto machine = std::make_shared<MemoryStateMachine>();
        machines_.emplace(id_group, machine);
        const Result<void> added = hosts_.at(id)->add(spec.id, config, storage, machine);
        if (!added.ok()) {
          throw std::runtime_error("add group failed");
        }
        const Result<void> started = hosts_.at(id)->find(spec.id)->initialize(Membership{ids_});
        if (!started.ok()) {
          throw std::runtime_error("initialize group failed");
        }
      }
    }
  }

  ~GroupCluster() {
    hosts_.clear();
    stores_.clear();
    if (!root_.empty()) {
      std::error_code ec;
      std::filesystem::remove_all(root_, ec);
    }
  }

  void tick() {
    for (NodeId id : ids_) {
      hosts_.at(id)->tick();
    }
    bus_.drain();
  }

  template <typename Pred>
  bool until(Pred pred, int max_ticks) {
    if (pred()) {
      return true;
    }
    for (int i = 0; i < max_ticks; ++i) {
      tick();
      if (pred()) {
        return true;
      }
    }
    return false;
  }

  void restart() {
    bus_.clear();
    for (NodeId id : ids_) {
      hosts_.at(id).reset();
      for (GroupId group : group_ids_) {
        const Key id_group{id, group};
        machines_.at(id_group) = std::make_shared<MemoryStateMachine>();
        if (kind_ == StoreKind::File) {
          stores_.at(id_group).reset();
          stores_.at(id_group) = FileLogStorage::open_group(root_ / std::to_string(id), group);
        }
      }
      hosts_.at(id) = std::make_unique<RaftGroups>(id, &bus_);
      for (GroupId group : group_ids_) {
        const Key id_group{id, group};
        const Result<void> added =
            hosts_.at(id)->add(group, configs_.at(id_group), stores_.at(id_group), machines_.at(id_group));
        if (!added.ok() || hosts_.at(id)->find(group) == nullptr ||
            !hosts_.at(id)->find(group)->initialized()) {
          throw std::runtime_error("restart failed");
        }
      }
    }
  }

  RaftGroups& host(NodeId id) { return *hosts_.at(id); }
  RaftNode& node(NodeId id, GroupId group) {
    RaftNode* found = hosts_.at(id)->find(group);
    if (found == nullptr) {
      throw std::logic_error("missing group");
    }
    return *found;
  }
  MemoryStateMachine& machine(NodeId id, GroupId group) { return *machines_.at(Key{id, group}); }

  std::filesystem::path directory(NodeId id, GroupId group) const {
    return FileLogStorage::group_directory(root_ / std::to_string(id), group);
  }

  bool agreed(GroupId group, NodeId leader) {
    if (node(leader, group).role() != Role::Leader) {
      return false;
    }
    const Term term = node(leader, group).term();
    for (NodeId id : ids_) {
      const RaftNode& current = node(id, group);
      if (current.term() != term) {
        return false;
      }
      if (id == leader) {
        continue;
      }
      if (current.role() == Role::Leader || current.current_leader() != leader) {
        return false;
      }
    }
    return true;
  }

  bool every_machine_has(GroupId group, const std::string& data) {
    for (NodeId id : ids_) {
      const std::vector<std::string>& rows = machine(id, group).data();
      bool found = false;
      for (const std::string& row : rows) {
        if (row == data) {
          found = true;
          break;
        }
      }
      if (!found) {
        return false;
      }
    }
    return true;
  }

  bool no_machine_has(GroupId group, const std::string& data) {
    for (NodeId id : ids_) {
      for (const std::string& row : machine(id, group).data()) {
        if (row == data) {
          return false;
        }
      }
    }
    return true;
  }

  std::string debug() {
    std::ostringstream out;
    for (GroupId group : group_ids_) {
      out << "group " << group << "\n";
      for (NodeId id : ids_) {
        const RaftNode& current = node(id, group);
        out << "  n" << id << " " << current.role() << " term " << current.term() << " leader "
            << current.current_leader().value_or(0) << " commit " << current.commit_index() << " vote "
            << current.vote() << " log " << current.log().size();
        out << " applied";
        for (const std::string& row : machine(id, group).data()) {
          out << " [" << row << "]";
        }
        out << "\n";
      }
    }
    return out.str();
  }

 private:
  using Key = std::pair<NodeId, GroupId>;

  class Bus final : public MessageBus {
   public:
    void bind(NodeId id, std::function<void(const Envelope&)> handler) { handlers_[id] = std::move(handler); }

    void send(const Envelope& message) override { queue_.push_back(message); }

    void clear() { queue_.clear(); }

    void drain() {
      int steps = 0;
      while (!queue_.empty()) {
        if (++steps > 100000) {
          throw std::runtime_error("in-process network did not quiesce");
        }
        Envelope message = std::move(queue_.front());
        queue_.pop_front();
        const auto handler = handlers_.find(message.to);
        if (handler == handlers_.end()) {
          continue;
        }
        handler->second(message);
      }
    }

   private:
    std::deque<Envelope> queue_;
    std::map<NodeId, std::function<void(const Envelope&)>> handlers_;
  };

  std::vector<NodeId> ids_;
  std::vector<GroupId> group_ids_;
  StoreKind kind_;
  std::filesystem::path root_;
  Bus bus_;
  std::map<NodeId, MemoryGroupStore> memory_;
  std::map<Key, Config> configs_;
  std::map<Key, std::shared_ptr<RaftLogStorage>> stores_;
  std::map<Key, std::shared_ptr<MemoryStateMachine>> machines_;
  std::map<NodeId, std::unique_ptr<RaftGroups>> hosts_;
};

std::vector<GroupCluster::Spec> two_groups() {
  return {GroupCluster::Spec{kAlpha, 4, false}, GroupCluster::Spec{kBeta, 4, true}};
}

bool both_elected(GroupCluster& cluster) {
  return cluster.agreed(kAlpha, 1) && cluster.agreed(kBeta, 3);
}

TEST(MultiGroup, ElectsAndCommitsIndependently) {
  GroupCluster cluster(kNodes, two_groups());
  ASSERT_TRUE(cluster.until([&] { return both_elected(cluster); }, 40)) << cluster.debug();

  const Term alpha_term = cluster.node(1, kAlpha).term();
  const Term beta_term = cluster.node(3, kBeta).term();
  const Result<LogIndex> alpha = cluster.node(1, kAlpha).write("alpha");
  const Result<LogIndex> beta = cluster.node(3, kBeta).write("beta");
  ASSERT_TRUE(alpha.ok()) << alpha.error();
  ASSERT_TRUE(beta.ok()) << beta.error();
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.every_machine_has(kAlpha, "alpha") && cluster.every_machine_has(kBeta, "beta");
      },
      30))
      << cluster.debug();

  for (NodeId id : kNodes) {
    EXPECT_EQ(cluster.machine(id, kAlpha).data(), (std::vector<std::string>{"alpha"})) << cluster.debug();
    EXPECT_EQ(cluster.machine(id, kBeta).data(), (std::vector<std::string>{"beta"})) << cluster.debug();
    EXPECT_TRUE(log_has(cluster.node(id, kAlpha), "alpha"));
    EXPECT_FALSE(log_has(cluster.node(id, kAlpha), "beta"));
    EXPECT_TRUE(log_has(cluster.node(id, kBeta), "beta"));
    EXPECT_FALSE(log_has(cluster.node(id, kBeta), "alpha"));
    EXPECT_EQ(cluster.node(id, kAlpha).vote().leader.node, 1u);
    EXPECT_EQ(cluster.node(id, kBeta).vote().leader.node, 3u);
    EXPECT_TRUE(cluster.node(id, kAlpha).vote().committed);
    EXPECT_TRUE(cluster.node(id, kBeta).vote().committed);
    EXPECT_EQ(cluster.node(id, kAlpha).term(), alpha_term);
    EXPECT_EQ(cluster.node(id, kBeta).term(), beta_term);
    EXPECT_EQ(cluster.node(id, kAlpha).membership().voters(), kNodes);
    EXPECT_EQ(cluster.node(id, kBeta).membership().voters(), kNodes);
    EXPECT_TRUE(cluster.node(id, kAlpha).membership().learners().empty());
    EXPECT_TRUE(cluster.node(id, kBeta).membership().learners().empty());
    EXPECT_GT(cluster.node(id, kAlpha).commit_index(), 0u);
    EXPECT_GT(cluster.node(id, kBeta).commit_index(), 0u);
  }
  EXPECT_TRUE(cluster.no_machine_has(kAlpha, "beta"));
  EXPECT_TRUE(cluster.no_machine_has(kBeta, "alpha"));
}

TEST(MultiGroup, MembershipChangeLeavesTheOtherGroupAlone) {
  GroupCluster cluster(kNodes, two_groups());
  ASSERT_TRUE(cluster.until([&] { return both_elected(cluster); }, 40)) << cluster.debug();
  const Term beta_term = cluster.node(1, kBeta).term();
  std::map<NodeId, std::size_t> beta_log;
  for (NodeId id : kNodes) {
    beta_log.emplace(id, cluster.node(id, kBeta).log().size());
  }

  ASSERT_TRUE(cluster.node(1, kAlpha).add_learner(4).ok()) << cluster.debug();
  ASSERT_TRUE(cluster.until(
      [&] {
        for (NodeId id : kNodes) {
          const RaftNode& node = cluster.node(id, kAlpha);
          if (!node.membership().is_learner(4) || node.commit_index() < node.last_log_index()) {
            return false;
          }
        }
        return true;
      },
      40))
      << cluster.debug();

  EXPECT_TRUE(cluster.agreed(kBeta, 3)) << cluster.debug();
  for (NodeId id : kNodes) {
    EXPECT_TRUE(cluster.node(id, kAlpha).membership().is_learner(4)) << id;
    EXPECT_EQ(cluster.node(id, kAlpha).membership().voters(), kNodes);
    EXPECT_FALSE(cluster.node(id, kBeta).membership().is_learner(4)) << id;
    EXPECT_TRUE(cluster.node(id, kBeta).membership().learners().empty());
    EXPECT_EQ(cluster.node(id, kBeta).membership().voters(), kNodes);
    EXPECT_EQ(cluster.node(id, kBeta).term(), beta_term);
    EXPECT_EQ(cluster.node(id, kBeta).vote().leader.node, 3u);
    EXPECT_EQ(cluster.node(id, kBeta).log().size(), beta_log.at(id));
    EXPECT_EQ(cluster.node(id, kBeta).role(), id == 3 ? Role::Leader : Role::Follower);
  }
}

TEST(MultiGroup, TransferLeavesTheOtherGroupAlone) {
  GroupCluster cluster(kNodes, two_groups());
  ASSERT_TRUE(cluster.until([&] { return both_elected(cluster); }, 40)) << cluster.debug();
  const Term alpha_term = cluster.node(1, kAlpha).term();

  ASSERT_TRUE(cluster.node(3, kBeta).transfer_leader(2).ok()) << cluster.debug();
  EXPECT_EQ(cluster.node(3, kBeta).poll_leader_transfer().error(), Error::Pending);
  const Result<LogIndex> during = cluster.node(1, kAlpha).write("during-transfer");
  ASSERT_TRUE(during.ok()) << during.error() << "\n" << cluster.debug();

  Result<void> handed = Result<void>::failure(Error::Pending);
  ASSERT_TRUE(cluster.until(
      [&] {
        handed = cluster.node(3, kBeta).poll_leader_transfer();
        return handed.error() != Error::Pending;
      },
      40))
      << handed.error() << "\n" << cluster.debug();
  ASSERT_TRUE(handed.ok()) << handed.error() << "\n" << cluster.debug();
  EXPECT_TRUE(cluster.agreed(kBeta, 2)) << cluster.debug();
  EXPECT_TRUE(cluster.agreed(kAlpha, 1)) << cluster.debug();
  EXPECT_EQ(cluster.node(1, kAlpha).term(), alpha_term);
  EXPECT_EQ(cluster.node(2, kAlpha).term(), alpha_term);
  EXPECT_EQ(cluster.node(3, kAlpha).term(), alpha_term);

  const Result<LogIndex> after = cluster.node(2, kBeta).write("after-transfer");
  ASSERT_TRUE(after.ok()) << after.error() << "\n" << cluster.debug();
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.every_machine_has(kAlpha, "during-transfer") &&
               cluster.every_machine_has(kBeta, "after-transfer") &&
               cluster.no_machine_has(kAlpha, "after-transfer") &&
               cluster.no_machine_has(kBeta, "during-transfer");
      },
      30))
      << cluster.debug();
  EXPECT_TRUE(cluster.agreed(kAlpha, 1)) << cluster.debug();
  EXPECT_EQ(cluster.node(1, kAlpha).term(), alpha_term);
}

TEST(MultiGroup, ForeignEnvelopeDoesNotChangeLogVoteOrSnapshot) {
  GroupCluster cluster(kNodes, two_groups());
  ASSERT_TRUE(cluster.until([&] { return both_elected(cluster); }, 40)) << cluster.debug();
  ASSERT_TRUE(cluster.node(1, kAlpha).write("alpha").ok());
  ASSERT_TRUE(cluster.until([&] { return cluster.every_machine_has(kAlpha, "alpha"); }, 30))
      << cluster.debug();
  ASSERT_TRUE(cluster.node(1, kAlpha).trigger_snapshot().ok()) << cluster.debug();
  ASSERT_TRUE(cluster.node(1, kAlpha).snapshot_meta().has_value());

  const Vote beta_vote = cluster.node(2, kBeta).vote();
  const std::size_t beta_log = cluster.node(2, kBeta).log().size();
  const LogIndex beta_commit = cluster.node(2, kBeta).commit_index();
  const Role beta_role = cluster.node(2, kBeta).role();
  EXPECT_FALSE(cluster.node(2, kBeta).snapshot_meta().has_value());

  AppendEntriesRequest append;
  append.vote.leader = LeaderId{80, 1};
  append.vote.committed = true;
  append.leader_commit = 4;
  LogEntry smuggled;
  smuggled.id = LogId{80, 1, 1};
  smuggled.kind = EntryKind::Application;
  smuggled.data = "smuggled";
  append.entries.push_back(smuggled);
  Envelope message;
  message.from = 1;
  message.to = 2;
  message.group = kAlpha;
  message.payload = append;
  cluster.node(2, kBeta).on_message(message);

  RequestVote vote;
  vote.body.vote.leader = LeaderId{90, 1};
  vote.body.last_log_id = LogId{90, 1, 20};
  message.payload = vote;
  cluster.node(2, kBeta).on_message(message);

  InstallSnapshotRequest snapshot;
  snapshot.vote.leader = LeaderId{91, 1};
  snapshot.vote.committed = true;
  snapshot.meta = *cluster.node(1, kAlpha).snapshot_meta();
  snapshot.meta.last_included = LogId{91, 1, 6};
  snapshot.meta.snapshot_id = "6-91-1";
  snapshot.data = "smuggled-snapshot";
  snapshot.done = true;
  message.payload = snapshot;
  cluster.node(2, kBeta).on_message(message);

  TimeoutNow timeout;
  timeout.leader.leader = LeaderId{92, 1};
  timeout.leader.committed = true;
  message.payload = timeout;
  cluster.node(2, kBeta).on_message(message);

  PreVoteRequest prevote;
  prevote.body.vote.leader = LeaderId{93, 1};
  prevote.probe = 3;
  message.payload = prevote;
  cluster.node(2, kBeta).on_message(message);

  message.group = 99;
  message.payload = vote;
  cluster.host(2).on_message(message);
  message.to = 9;
  message.group = kBeta;
  cluster.host(2).on_message(message);

  const RaftNode& beta = cluster.node(2, kBeta);
  EXPECT_EQ(beta.vote(), beta_vote);
  EXPECT_EQ(beta.log().size(), beta_log);
  EXPECT_EQ(beta.commit_index(), beta_commit);
  EXPECT_EQ(beta.role(), beta_role);
  EXPECT_FALSE(beta.snapshot_meta().has_value());
  EXPECT_FALSE(beta.failed());
  EXPECT_FALSE(log_has(beta, "smuggled"));
  EXPECT_EQ(cluster.machine(2, kBeta).data().size(), 0u);
  EXPECT_TRUE(cluster.node(1, kAlpha).snapshot_meta().has_value());
  EXPECT_TRUE(log_has(cluster.node(2, kAlpha), "alpha"));
}

TEST(MultiGroup, FileStoresRecoverEachGroupSeparately) {
  GroupCluster cluster(kNodes, two_groups(), GroupCluster::StoreKind::File);
  ASSERT_TRUE(cluster.until([&] { return both_elected(cluster); }, 40)) << cluster.debug();
  ASSERT_TRUE(cluster.node(1, kAlpha).write("alpha").ok());
  ASSERT_TRUE(cluster.node(3, kBeta).write("beta").ok());
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.every_machine_has(kAlpha, "alpha") && cluster.every_machine_has(kBeta, "beta") &&
               cluster.node(1, kAlpha).commit_index() >= cluster.node(1, kAlpha).last_log_index() &&
               cluster.node(3, kBeta).commit_index() >= cluster.node(3, kBeta).last_log_index();
      },
      30))
      << cluster.debug();

  const Term alpha_term = cluster.node(1, kAlpha).term();
  const Term beta_term = cluster.node(3, kBeta).term();
  for (NodeId id : kNodes) {
    const std::filesystem::path alpha_dir = cluster.directory(id, kAlpha);
    const std::filesystem::path beta_dir = cluster.directory(id, kBeta);
    EXPECT_NE(alpha_dir, beta_dir);
    EXPECT_EQ(alpha_dir.filename(), "10");
    EXPECT_EQ(beta_dir.filename(), "20");
    EXPECT_EQ(alpha_dir.parent_path().filename(), "groups");
    EXPECT_TRUE(std::filesystem::is_regular_file(alpha_dir / "log"));
    EXPECT_TRUE(std::filesystem::is_regular_file(beta_dir / "log"));
    EXPECT_FALSE(std::filesystem::exists(alpha_dir.parent_path().parent_path() / "log"));
    EXPECT_TRUE(file_contains(alpha_dir / "log", "alpha"));
    EXPECT_FALSE(file_contains(alpha_dir / "log", "beta"));
    EXPECT_TRUE(file_contains(beta_dir / "log", "beta"));
    EXPECT_FALSE(file_contains(beta_dir / "log", "alpha"));
    EXPECT_TRUE(std::filesystem::is_regular_file(alpha_dir / "vote"));
    EXPECT_TRUE(std::filesystem::is_regular_file(beta_dir / "vote"));
    EXPECT_NE(read_file(alpha_dir / "vote"), read_file(beta_dir / "vote"));
  }

  for (NodeId id : kNodes) {
    ASSERT_TRUE(cluster.node(id, kAlpha).trigger_snapshot().ok()) << id << "\n" << cluster.debug();
    EXPECT_TRUE(cluster.node(id, kAlpha).snapshot_meta().has_value());
    EXPECT_FALSE(cluster.node(id, kBeta).snapshot_meta().has_value());
    EXPECT_TRUE(file_contains(cluster.directory(id, kAlpha) / "snapshot", "alpha"));
    EXPECT_FALSE(file_contains(cluster.directory(id, kAlpha) / "snapshot", "beta"));
    EXPECT_FALSE(std::filesystem::exists(cluster.directory(id, kBeta) / "snapshot"));
  }

  cluster.restart();

  for (NodeId id : kNodes) {
    const RaftNode& alpha = cluster.node(id, kAlpha);
    const RaftNode& beta = cluster.node(id, kBeta);
    EXPECT_TRUE(alpha.initialized());
    EXPECT_TRUE(beta.initialized());
    EXPECT_EQ(alpha.role(), Role::Follower) << cluster.debug();
    EXPECT_EQ(beta.role(), Role::Follower) << cluster.debug();
    EXPECT_EQ(alpha.term(), alpha_term);
    EXPECT_EQ(beta.term(), beta_term);
    EXPECT_EQ(alpha.vote().leader.node, 1u);
    EXPECT_EQ(beta.vote().leader.node, 3u);
    EXPECT_EQ(alpha.vote().committed, id != 1);
    EXPECT_EQ(beta.vote().committed, id != 3);
    EXPECT_TRUE(alpha.snapshot_meta().has_value());
    EXPECT_FALSE(beta.snapshot_meta().has_value());
    EXPECT_EQ(cluster.machine(id, kAlpha).data(), (std::vector<std::string>{"alpha"}));
    EXPECT_TRUE(cluster.machine(id, kBeta).data().empty());
    EXPECT_FALSE(log_has(alpha, "beta"));
    EXPECT_TRUE(log_has(beta, "beta"));
    EXPECT_FALSE(log_has(beta, "alpha"));
    EXPECT_TRUE(file_contains(cluster.directory(id, kBeta) / "log", "beta"));
    EXPECT_FALSE(file_contains(cluster.directory(id, kBeta) / "log", "alpha"));
    EXPECT_TRUE(file_contains(cluster.directory(id, kAlpha) / "snapshot", "alpha"));
    EXPECT_FALSE(file_contains(cluster.directory(id, kAlpha) / "snapshot", "beta"));
    EXPECT_FALSE(alpha.failed());
    EXPECT_FALSE(beta.failed());
  }

  ASSERT_TRUE(cluster.until([&] { return both_elected(cluster); }, 40)) << cluster.debug();
  ASSERT_TRUE(cluster.until(
      [&] {
        return cluster.every_machine_has(kBeta, "beta") && cluster.every_machine_has(kAlpha, "alpha") &&
               cluster.no_machine_has(kAlpha, "beta") && cluster.no_machine_has(kBeta, "alpha");
      },
      30))
      << cluster.debug();
}

}  // namespace
}  // namespace easyraft
