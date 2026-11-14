// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright 2026 easyraft contributors

#pragma once

#include "easyraft/config.hpp"
#include "easyraft/error.hpp"
#include "easyraft/messages.hpp"
#include "easyraft/types.hpp"

#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <vector>

namespace easyraft {

// I/O the runtime must perform to mirror an engine transition.
// The engine has already applied the same change to its in-memory shadow.
enum class CommandKind {
  SaveVote,
  SaveMembership,
  Truncate,
  Append,
  Apply,
  // `build_snapshot` asks the runtime to read the state machine. Otherwise
  // `snapshot.data` is the image to store and install.
  SaveSnapshot,
  // Drop log entries at or below `index` once `SaveSnapshot` has succeeded.
  Purge,
  // A linearizable read may be served at `index`. `read_id` identifies it.
  // The runtime observes this only after Apply commands for that index.
  ReadReady,
  // `read_id` will not become ready. Leadership was lost first.
  ReadAborted,
};

struct Command {
  CommandKind kind{CommandKind::SaveVote};
  Vote vote{};
  Membership membership{};
  LogIndex index{0};
  std::vector<LogEntry> entries;
  LogEntry entry{};
  Snapshot snapshot{};
  bool build_snapshot{false};
  // Set on ReadReady and ReadAborted.
  std::uint64_t read_id{0};
};

struct Outbound {
  NodeId to{0};
  Payload payload{PreVoteRequest{}};
};

struct StepResult {
  std::vector<Command> commands;
  std::vector<Outbound> outbound;
};

struct ProposeResult {
  StepResult step;
  bool accepted{false};
  LogIndex index{0};
};

struct VoteStep {
  StepResult step;
  VoteResponse response;
};

struct AppendStep {
  StepResult step;
  AppendEntriesResponse response;
};

struct InstallStep {
  StepResult step;
  InstallSnapshotResponse response;
};

struct SnapshotBuild {
  StepResult step;
  bool started{false};
};

// A linearizable read accepted by the leader. Heartbeats, when a peer quorum
// is required, are in `step.outbound`. `id` is polled until ReadReady.
struct ReadStart {
  StepResult step;
  bool accepted{false};
  std::uint64_t id{0};
};

// Pure Raft state machine. Time advances only through on_tick(); no wall clock.
//
// The runtime (RaftNode) executes `commands` against storage and the state
// machine, then sends `outbound`. Feeding the replies back in is the whole
// protocol loop, which is what makes the tests deterministic.
class Engine {
 public:
  Engine(NodeId id, Config config);

  // `already_applied` is the state machine's cursor. Entries at or below it are
  // not emitted again when a leader re-commits the prefix.
  // `snapshot`, when present, is applied state. Log entries at or below its
  // last included index are dropped. `already_applied` is raised to that index
  // when the state machine cursor is behind the image.
  StepResult recover(Membership membership,
                     Vote vote,
                     std::vector<LogEntry> log,
                     LogIndex already_applied = 0,
                     std::optional<Snapshot> snapshot = std::nullopt);
  StepResult on_tick();
  ProposeResult propose(std::string data);
  // Compact through `last_applied`. `started` is false when nothing new has
  // been applied since the current snapshot. The runtime builds the bytes,
  // stores them, then calls `store_snapshot_bytes` before the image can be sent.
  SnapshotBuild begin_snapshot();
  void store_snapshot_bytes(std::string data);
  // One replication pass. A leader uses this after a snapshot's bytes are stored.
  StepResult replicate_now();
  // Appends a joint config of the current voters and `voters`, then the
  // uniform target once that joint entry commits. Learners stay learners,
  // except one named in `voters`: that node is promoted and leaves the
  // learner set. The old config keeps its quorum. `quorum` is the new
  // config's definition, and the default is a majority of the new voters.
  // `accepted` is false when this node cannot start a change. The returned
  // index is the joint entry. An unchanged voter set does not append.
  ProposeResult change_membership(std::vector<NodeId> voters, Quorum quorum = {});
  // Changes the quorum of the current voter set. One uniform entry when every
  // old election quorum intersects every new commit quorum and the reverse.
  // Otherwise a joint config of the same voters. `accepted` is false when the
  // definition does not intersect or this node cannot start a change.
  ProposeResult change_quorum(Quorum quorum);
  // One uniform membership entry. Learners do not vote, so this does not
  // enter joint consensus. The returned index is that entry.
  ProposeResult add_learner(NodeId learner);
  ProposeResult remove_learner(NodeId learner);
  // Joint consensus removes `voter` and the uniform target adds it as a
  // learner. During the joint config it is still a voter.
  ProposeResult demote_voter(NodeId voter);
  // Leader only. Records the commit index once this term has a committed
  // entry (the blank entry, if that is still all this leader has committed),
  // then confirms leadership with AppendEntries acks from a commit quorum of
  // every voter config. Learners are not part of that quorum. `accepted` is
  // false when this node is not the leader. The read is not served until
  // `last_applied` reaches the recorded index.
  ReadStart begin_linearizable_read();
  bool linearizable_read_pending(std::uint64_t id) const;
  // Leader only, and only while `leader_quorum_lease_valid` is still true.
  // Returns the applied commit index. `LeaseExpired` when the existing lease
  // does not cover the read; the call does not return state and does not
  // start a quorum round. `Rejected` when the lease is valid but this term
  // has not yet committed and applied an entry. Followers return `NotLeader`.
  Result<LogIndex> lease_read() const;

  VoteStep on_pre_vote(const VoteRequest& request);
  StepResult on_pre_vote_response(NodeId from, const PreVoteResponse& response);
  VoteStep on_vote(const VoteRequest& request);
  StepResult on_vote_response(NodeId from, const VoteResponse& response);
  AppendStep on_append(const AppendEntriesRequest& request);
  StepResult on_append_response(NodeId from, const AppendEntriesResponse& response);
  InstallStep on_install(const InstallSnapshotRequest& request);
  StepResult on_install_response(NodeId from, const InstallSnapshotResponse& response);

  bool initialized() const { return initialized_; }
  const Membership& membership() const { return membership_; }
  // Joint config still in effect, or a membership entry not yet committed.
  bool membership_changing() const;
  Role role() const { return role_; }
  const Vote& vote() const { return vote_; }
  Term term() const { return vote_.leader.term; }
  std::optional<NodeId> current_leader() const;
  LogIndex commit_index() const { return commit_index_; }
  LogIndex last_index() const { return last_index_; }
  LogIndex last_applied() const { return last_applied_; }
  // Zero when no snapshot is stored. Otherwise the last included index.
  LogIndex compacted_index() const;
  // First log index that can still be sent or stored. `compacted_index() + 1`.
  LogIndex first_available_index() const { return compacted_index() + 1; }
  std::optional<SnapshotMeta> snapshot_meta() const;
  const std::vector<LogEntry>& log() const { return log_; }
  std::string debug() const;

 private:
  struct Progress {
    LogIndex next_index{1};
    LogIndex match_index{0};
    std::uint64_t last_ack_tick{0};
    std::uint64_t snapshot_offset{0};
    std::string snapshot_id;
  };

  struct IncomingSnapshot {
    SnapshotMeta meta{};
    std::string data;
  };

  struct Campaign {
    Vote vote{};
    std::set<NodeId> grants;
    std::uint64_t probe{0};
  };

  // One client linearizable read. `read_index` stays empty until this leader
  // has committed an entry from its current term. Acks are voters whose
  // AppendEntries response carried `heartbeat_seq >= confirm_seq`.
  struct PendingRead {
    std::uint64_t id{0};
    std::optional<LogIndex> read_index;
    bool needs_anchor{false};
    bool confirming{false};
    std::uint64_t confirm_seq{0};
    std::set<NodeId> acks;
    bool confirmed{false};
    bool aborted{false};
  };

  const LogEntry* find(LogIndex index) const;
  std::optional<LogId> last_log_id() const;
  bool has_quorum(const std::set<NodeId>& grants) const;
  std::optional<LogIndex> quorum_match_index() const;
  std::uint64_t sample_timeout();
  bool follower_lease_valid() const;
  bool leader_quorum_lease_valid() const;
  void note_leader_contact();
  bool update_vote(StepResult& out, const Vote& incoming);
  void become_leader(StepResult& out);
  void step_down(StepResult& out);
  bool current_term_committed() const;
  bool acks_commit_quorum(const std::set<NodeId>& acks) const;
  void begin_confirmation(StepResult& out, PendingRead& read);
  void assign_read_anchors();
  void note_read_ack(StepResult& out, NodeId from, std::uint64_t seq);
  void serve_reads(StepResult& out);
  void abort_reads(StepResult& out);
  void append_local(StepResult& out, EntryKind kind, std::string data);
  void truncate_from(StepResult& out, LogIndex index);
  void advance_commit(StepResult& out);
  void adopt_membership(StepResult& out, Membership next);
  Membership infer_base(const Membership& stored) const;
  Membership membership_from_log() const;
  void sync_progress();
  void continue_membership(StepResult& out);
  void maybe_auto_snapshot(StepResult& out);
  void start_snapshot(StepResult& out);
  void install_snapshot(StepResult& out, Snapshot snapshot);
  Membership membership_at(LogIndex index) const;
  std::vector<NodeId> peers() const;
  void replicate_all(StepResult& out);
  void send_append(StepResult& out, NodeId to);
  void send_snapshot(StepResult& out, NodeId to);
  void send_vote_requests(StepResult& out, bool pre_vote, const Vote& vote);
  void pre_elect(StepResult& out);
  void elect(StepResult& out);
  void establish_leader(StepResult& out);
  void enqueue_applies(StepResult& out);

  NodeId id_;
  Config config_;
  bool initialized_{false};
  Membership membership_{};
  // Membership in effect when the log has no membership entry. Inferred on
  // recover from the first joint entry's old config, or from the stored one.
  Membership base_membership_{};
  bool continuing_{false};
  Vote vote_{};
  Role role_{Role::Follower};
  bool leading_{false};
  std::vector<LogEntry> log_;
  std::optional<Snapshot> snapshot_;
  // False while a local snapshot has been compacted in memory but the runtime
  // has not yet copied the state-machine bytes in.
  bool snapshot_data_ready_{false};
  std::optional<IncomingSnapshot> incoming_;
  LogIndex last_index_{0};
  LogIndex commit_index_{0};
  LogIndex last_applied_{0};
  std::uint64_t now_{0};
  // Copied onto AppendEntries and echoed by the follower. Read confirmation
  // bumps it so an older in-flight ack cannot be counted.
  std::uint64_t heartbeat_seq_{0};
  std::uint64_t election_timeout_ticks_{1};
  std::uint64_t next_election_at_{1};
  std::uint64_t ticks_since_heartbeat_{0};
  std::uint64_t last_leader_contact_{0};
  bool ever_contacted_{false};
  std::uint32_t rng_{1};
  std::optional<Campaign> campaign_;
  std::optional<Campaign> pre_campaign_;
  std::uint64_t next_probe_{0};
  std::map<NodeId, Progress> progress_;
  std::vector<PendingRead> reads_;
  std::uint64_t next_read_id_{1};
};

}  // namespace easyraft
