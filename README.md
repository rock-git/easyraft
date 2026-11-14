# easyraft

easyraft is a small C++20 implementation of the [Raft](https://raft.github.io/) consensus protocol. Phase 1 is the core algorithm: leader election, log replication, flexible quorums, joint-consensus membership changes, learners, log snapshots, linearizable and lease reads, and restart from a persisted vote, membership, log, and snapshot.

The design follows the OpenRaft 0.10 line (`release-0.10`, published as 0.10.0): a pure, tick-driven engine decides, and a thin runtime performs storage and network I/O. This is an independent implementation inspired by that design. It is not an official OpenRaft port and it does not contain OpenRaft source code. OpenRaft is a Databend project and is derived from the original async-raft work. See `NOTICE`.

## Phase 1 scope

Included:

- Leader election with PreVote (enabled by default)
- AppendEntries replication, including conflict truncation that deletes only the divergent suffix
- Commit when a commit quorum of every voter config has an entry from the current leader
- Persistence of the vote, the membership (including its quorum), the log, and the latest snapshot (`MemoryLogStorage` or `FileLogStorage`), and recovery after the node object is rebuilt
- Flexible quorums. The default for every voter config is a majority, `floor(n / 2) + 1`, used for both elections and commits. A config may instead name an election quorum and a commit quorum, as long as every election quorum intersects every commit quorum
- Voter-set changes through joint consensus: a joint config is committed, then the uniform target. Each side uses its own quorum definition
- Learners: non-voting replicators that receive the log and snapshots and do not count toward quorum
- Snapshots of applied state, crash-safe log compaction, and `InstallSnapshot` catch-up for a lagging voter, a new voter, or a learner
- Linearizable reads (a ReadIndex round on the leader) and lease reads that reuse the leader's existing lease

Not in this phase: leader transfer, multi-raft, chaos frameworks, OpenTelemetry, and a standalone server or key-value application. Followers do not forward reads.

## Protocol notes

Time is a tick count, not a wall clock. `RaftNode::tick()` advances one unit. Tests set `election_timeout_min == election_timeout_max` so campaigns are reproducible. Heartbeats are empty AppendEntries.

Votes use OpenRaft's default leader id `(term, node_id)`, totally ordered, plus a `committed` bit. A node grants a request when the candidate's vote is at least the vote it has stored and the candidate's log is at least as up to date. A higher node id can supersede another candidate in the same term. `committed` marks an established leader and is greater than that leader's non-committed vote.

PreVote asks peers whether they would grant `term + 1` and changes nothing locally. A single-node voter set skips the round and campaigns immediately. A follower that still has a leader lease (a recent AppendEntries from the leader it follows) rejects both PreVote and RequestVote, so a partitioned node cannot inflate the term and knock over a healthy leader when it returns.

On winning an election the leader appends a blank entry. Only entries written by the current leader move the commit index; once one of those is on a commit quorum of every config in the effective membership, the prefix before it is committed too. That is how entries from the previous term become committed, and how a restarted leader avoids treating an unreplicated suffix as committed. PreVote and RequestVote count an election quorum. The leader lease counts a commit quorum, the same set that can advance the commit index. Linearizable reads confirm leadership with that same commit quorum. Lease reads are local and are allowed only while that lease is still valid.

`initialize()` stores the first uniform voter set, optionally with learners and a quorum. A node id is a voter or a learner or neither, never both. Id 0 is reserved for both sets. `RaftNode::change_membership` is leader-only. It appends a joint membership (the current voters and the target voters) and, once that entry commits, appends the uniform target. The old config keeps its quorum. The new config uses the quorum passed to `change_membership`, or a majority when that argument is omitted. Learners are kept, except a learner named in the target voter set: that node is promoted and leaves the learner set in the joint entry. The effective membership is the last membership entry still in the log, or the membership recorded in the latest snapshot when the remaining log has none, or the initialized set when there is neither. A membership entry commits only when a commit quorum of each voter config in that effective membership has it, so during the joint phase a quorum of the old set alone or the new set alone is not enough. Learners are not part of that count, and a learner's match index cannot advance the commit index. After the uniform target commits, a leader that is no longer a voter steps down, and a removed node is not part of the quorum. A new voter or learner catches up by replicating the log, or by `InstallSnapshot` once the leader has compacted the prefix. An empty set, voter id 0, a learner id that is already a voter, a quorum that does not intersect or cannot be formed, a change from a follower, and a second change while one is in flight are rejected.

Learners replicate and do not vote. `RaftNode::add_learner` and `RaftNode::remove_learner` are leader-only. Each appends one uniform membership entry and does not enter joint consensus, because the voter set and its quorum do not change. The leader sends `AppendEntries` and, when the learner's next index is below the first available log index, `InstallSnapshot`, the same way it does for a voting follower. A learner never starts an election, never grants a vote, and its match index cannot advance the commit index. `RaftNode::demote_voter` is the opposite of promotion: joint consensus removes the node from the voter set, and the uniform target adds it as a learner. During the joint config it is still a voter of the old set, so it is not in the learner set yet. Demoting the last voter is rejected. Demotion keeps the current quorum counts when they still intersect on the smaller set, and rejects the demotion when they do not.

## Flexible quorums

A `Quorum` is two thresholds on one voter config: `election` and `commit`. Zero means the majority of that config, `floor(n / 2) + 1`. Passing the majority counts explicitly is stored as zeros, so a default config stays on the existing encoding. Learners are not part of `n` and are not part of either threshold.

The intersection rule is `election + commit > n`, with both thresholds in `1 .. n`. Every set of `election` voters then shares a node with every set of `commit` voters, which is the Flexible Paxos condition this library checks. A field left at 0 is the majority, not a threshold of zero. A resolved threshold above `n`, or a pair that fails the inequality, is `Error::InvalidMembership`. With the library's totally ordered leader id, PreVote and RequestVote use only the election quorum. Commit and the leader lease use only the commit quorum. A commit quorum of 2 on 5 voters can commit with the leader and one follower. An election quorum of 4 on those same voters still needs four votes.

In a joint config each side keeps its own definition. An election needs the election quorum of every side, and a commit needs the commit quorum of every side. That is enough for the joint election sets to intersect the joint commit sets, including when a majority of the old voters can miss a majority of the new voters. Requiring the old and new voter sets themselves to cross-intersect would reject today's replacements, such as `{1, 2, 3}` to `{3, 4, 5}`.

`RaftNode::change_quorum` changes the definition of the current voters. When every old election quorum intersects every new commit quorum and the reverse, it appends one uniform membership entry, the same shape as a learner change. Otherwise it appends a joint config of the same voters and, once that commits, the uniform target. Shrinking the commit quorum on the same voters usually fails the direct check, because an old election quorum can miss a new commit quorum, so that change goes through the joint config. `change_membership` with a new voter set always uses joint consensus. The old side keeps its current quorum and the new side uses the `Quorum` argument, which defaults to a majority. An unchanged voter set does not append. A non-leader returns `Error::NotLeader`. A change that is already in flight returns `Error::Rejected`.

## Linearizable reads and lease reads

Both reads are leader-only. A follower or a learner returns `Error::NotLeader`. This library does not forward a read to the current leader; the caller does that, if it wants to.

`RaftNode::linearizable_read` starts a ReadIndex round and returns an id. `RaftNode::poll_linearizable_read` returns the read index when the round is finished. `Error::Pending` means the caller should tick and deliver RPCs, then poll again. `Error::NotLeader` means leadership was lost before the read was served, so the index must not be used. `Error::Rejected` means this node never started that id. A successful poll is the commit index recorded for the read. The state machine has applied at least that index. The library does not copy state-machine bytes; the caller reads its own `RaftStateMachine` knowing the applied prefix covers the index.

The leader records that index only after it has committed an entry from its own term. Until then the blank entry appended at election (or a later entry of this term) has to commit. That is the same rule that stops a leader from committing a prefix it does not know is committed. The index is then confirmed with a fresh round of AppendEntries. Acks that were already in flight do not count: each confirmation bumps a heartbeat sequence and the follower echoes it. A commit quorum of every voter config must ack. That is the flexible commit quorum, including a joint config's separate sides, not a hard-coded majority. Learners are not voters, so a learner ack cannot confirm the read. On a single node the leader is the whole commit quorum and the poll can succeed in the same call, once the entry is applied.

The read is not served before `last_applied` reaches the recorded index. `ReadReady` is handed to the runtime only after the Apply commands for that index, and the node publishes the poll result only after those applies have run. A read that has not been published is dropped if the node steps down. A result already returned stays with the caller; it was confirmed while this node was still the leader.

`RaftNode::lease_read` skips the quorum round. It reuses the leader's acknowledgements, narrowed so a read cannot outlive the voters' protection. The vote-rejection lease is unchanged: it still refreshes when a reply arrives and still treats the stamps written at election as contact. A read lease does not. A peer counts only after a real `AppendEntries` or `InstallSnapshot` ack. The clock is the leader tick stored on that request, which the follower echoes, so a delayed reply does not move the deadline to the arrival tick. The peer stays fresh for the shorter of this leader's `election_timeout_min` and the `election_timeout_min` the voter reports. Learners are not voters. The call also requires a committed entry from the current term, and the state machine's own `last_applied` must already cover the returned index. A callback still inside `apply` therefore cannot observe a later index the engine has queued but the state machine has not finished.

`Error::LeaseExpired` means that read lease does not cover the call. No index is returned, and the call does not fall back to `linearizable_read`. `Error::Rejected` means the lease is valid but this term's commit is not applied on the state machine yet. `Error::NotLeader` means this node is not the leader. A node can still report `Role::Leader` after the read lease expires, until it sees a higher term. Lease reads are refused in that window.

One tick has to be the same duration on every node. The engine has no wall clock and does not correct drift between nodes. `election_timeout_min` on each voter is the shortest election timeout that voter will use. A read lease is safe under those two conditions because a new election needs an election quorum, every election quorum intersects every commit quorum, and a voter will not grant a vote while its own minimum timeout has not elapsed since the request it acknowledged. `heartbeat_interval`, `election_timeout_min`, and `election_timeout_max` are unchanged.

`poll_linearizable_read` consumes a ready or aborted id. `Error::Pending` does not. `RaftNode::cancel_read` drops a pending, ready, or aborted id. Ready, pending, and aborted ids together cannot exceed `Config::max_in_flight_reads` (the default is 1024). A further `linearizable_read` returns `Error::Rejected` until one of them is polled or cancelled. A read started from inside a state-machine `apply` callback is not visible to `poll_linearizable_read` until that outermost `apply` returns and the state-machine cursor covers the index.

```cpp
const auto id = node.linearizable_read();
// tick and deliver RPCs until poll_linearizable_read(id) is not Pending
const auto index = node.poll_linearizable_read(*id);

const auto leased = node.lease_read();
```

A follower accepts AppendEntries only after it accepts the leader's vote. If `prev_log_id` does not match, it truncates at that index and rejects. If the prefix matches, identical entries are kept and only the conflicting tail is deleted before the new suffix is appended. The commit index never advances past the prefix just checked against the leader. A prev id that names the snapshot's last included entry matches that compacted prefix, and truncation never deletes the snapshot.

## Snapshots

A snapshot is applied state only. Its meta is the last included log id `(index, term, node)`, the effective membership at that index (one uniform config or a joint pair, including that config's learners and quorum), and a snapshot id of the form `index-term-node`. The bytes are whatever `RaftStateMachine::build_snapshot` returned. An uncommitted suffix is not included and is not purged.

`Config::snapshot_threshold` is how many applied entries since the current snapshot cause a node to compact. The default is 0, so existing callers do not compact unless they opt in. `RaftNode::trigger_snapshot` compacts through `last_applied` on a leader or a follower and returns `Error::Rejected` when nothing new has been applied. Building and storing the image is local. Only the leader sends it.

The runtime builds the bytes, stores the snapshot, and only then deletes the log prefix through the last included index. The first remaining index is `last_included + 1`, or the log is empty. `truncate_from` of a conflicting suffix does not delete that snapshot or the committed prefix it stands for. On restart the state machine installs the snapshot, so its applied index is the snapshot index, and any later log is applied again once a leader commits it. If a crash lands after the snapshot is durable and before the prefix is deleted, opening the node finishes the purge.

`FileLogStorage` writes `snapshot` the same way it writes `vote` and `membership`: a temp file, fsync, rename, and a directory fsync. A leftover `snapshot.tmp` is removed on open. The purge rewrites the kept suffix to `log.rewrite`, fsyncs, renames it onto `log`, and fsyncs the directory. A crash before the rename leaves the previous log, and torn-tail recovery still applies to whatever suffix remains. The log header stays version 1. A membership file with no learners and a majority quorum stays version 1 when it is uniform and version 2 when it is joint. Learners, or any non-default quorum, use version 3, described below. The snapshot stores `Membership::encode` inside its own record. A non-default quorum is inside that payload; the snapshot file stays version 1.

When a follower's `next_index` is below the leader's first available index, the leader sends `InstallSnapshot` instead of `AppendEntries` from index 1. That is also how a voter or learner added after compaction catches up. A request carries `offset`, `data`, and `done`. `Config::max_snapshot_chunk` limits the payload; 0 sends the whole image in one request. The follower installs the image only when the last included index is newer than both its snapshot and its applied index. A stale image is rejected and left unused. After a successful install the log has no conflicting prefix, commit and applied are at least the snapshot index, the state machine holds the snapshot, and membership is the snapshot membership unless a later membership entry is still in the log.

## Layout

| Path | Role |
| --- | --- |
| `include/easyraft/engine.hpp` | Pure protocol state machine |
| `include/easyraft/node.hpp` | Runtime: timeouts, heartbeats, client writes, membership changes, reads, RPC dispatch |
| `include/easyraft/storage.hpp` | `RaftLogStorage` |
| `include/easyraft/state_machine.hpp` | `RaftStateMachine` |
| `include/easyraft/network.hpp` | `MessageBus` |
| `include/easyraft/memory.hpp` | In-memory storage and state machine |
| `include/easyraft/file.hpp` | Durable directory storage (`FileLogStorage`) |
| `tests/cluster.hpp` | In-process cluster with a fake clock |

`MessageBus::send` must queue. Delivering inside `send` re-enters the engine. The test cluster drains the queue after each tick.

## Build and test

Requires CMake 3.24+, a C++20 compiler, and network access on the first configure so GoogleTest 1.15.2 can be fetched. No Boost and no other runtime dependencies. Voter and learner ids start at 1; id 0 is reserved.

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j
ctest --test-dir build --output-on-failure
```

Sanitizers:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug -DEASYRAFT_SANITIZERS=ON
cmake --build build -j
ctest --test-dir build --output-on-failure
```

GitHub Actions runs the sanitized build on every pull request.

## Using a node

```cpp
auto storage = std::make_shared<easyraft::MemoryLogStorage>();
auto machine = std::make_shared<easyraft::MemoryStateMachine>();
easyraft::RaftNode node(1, easyraft::Config{}, storage, machine, /*bus=*/nullptr);
node.initialize(easyraft::Membership{{1}});
node.tick();  // repeat until role() == Role::Leader
auto index = node.write("value");
```

On a multi-node cluster, pass a `MessageBus` and deliver envelopes back through `RaftNode::on_message`. `write` returns `Error::NotLeader` when the node cannot accept the proposal. `current_leader()` is the established leader, when this node knows one. `linearizable_read` and `lease_read` return `Error::NotLeader` from a follower or a learner. A linearizable read on the leader stays `Error::Pending` until `poll_linearizable_read` returns the read index. The state machine has applied at least that index. `lease_read` returns that index only while the read lease is valid and the state machine has applied it. Otherwise it returns `Error::LeaseExpired` or `Error::Rejected` and does not substitute a quorum read. `poll_linearizable_read` consumes a finished id. `cancel_read` drops one. `Config::max_in_flight_reads` bounds how many ids a node will hold.

```cpp
auto changed = node.change_membership({1, 2, 4});
auto added = node.add_learner(5);
auto removed = node.remove_learner(5);
auto demoted = node.demote_voter(2);
// Five voters: commit with any 2, elect with any 4.
auto quorum = node.change_quorum(easyraft::Quorum{4, 2});
```

`change_membership` returns `Error::NotLeader` from a follower, `Error::InvalidMembership` for an empty set, voter id 0, or a quorum that does not intersect, and `Error::Rejected` while a joint config is in effect or a membership entry is still uncommitted. The returned index is the joint entry. Calling it with the current voter set does not append. A second argument sets the new config's quorum; omitting it installs a majority. A learner listed in the new voter set is promoted: the joint entry removes it from the learner set. Other learners are unchanged. A process may `initialize` with a uniform set that does not include itself, then catch up when a leader adds it as a voter or as a learner.

```cpp
node.initialize(easyraft::Membership{{1, 2, 3, 4, 5}, {}, easyraft::Quorum{4, 2}});
```

`add_learner` and `remove_learner` return `Error::NotLeader` from a follower and `Error::InvalidMembership` for id 0, a voter, a duplicate learner, or a learner that is not in the set. `demote_voter` returns `Error::InvalidMembership` for id 0, a node that is not a voter, the last remaining voter, or a quorum the smaller set cannot form. All three return `Error::Rejected` while a membership entry is still uncommitted. `add_learner` and `remove_learner` return the uniform entry's index. `demote_voter` returns the joint entry's index. `change_quorum` returns `Error::NotLeader` from a follower, `Error::InvalidMembership` when the definition does not intersect or cannot be formed, and `Error::Rejected` while a membership change is in flight. An unchanged definition does not append.

`FileLogStorage` is the same `RaftLogStorage` contract, rooted at a directory. It stores `vote`, `membership`, an append-only `log`, and at most one `snapshot`. A uniform membership with no learners and a majority quorum is the original version-1 voter list. A joint membership with no learners and majority quorums is version 2 and round-trips as the old config plus the new one; a version-1 file still loads as a uniform majority set. A membership that records learners, a joint demotion's follow-up learner set, or a non-default quorum is version 3 and stores `Membership::encode`. Membership log entries use kind 2 and carry `Membership::encode_for_log`. That payload is version 1 only when the entry records no current learners, no follow-up learner set, no prior learner set, and no quorum other than the majority. It is version 2 when learners are present and the quorum is still the majority, including removal of the last learner, which still records who the learners were. It is version 3 when a config's election or commit threshold is not the majority, or when the entry records the quorum from before the change so a truncation can restore it. `MemoryLogStorage` stores the same `Membership` value. Restart from either storage honors the restored thresholds. A snapshot's membership payload uses that same encoding, so compaction keeps a non-default quorum without a new snapshot version. Metadata files, including `snapshot`, are replaced with a temp file, fsync, rename, and a directory fsync. Creating the directory fsyncs each parent that gained a new entry. Log appends and truncations fsync before they return success. Opening the directory drops a torn or checksum-failed final record when its length checksum still matches. A corrupt length prefix is left on disk and reported as a storage error. A corrupt `snapshot` fails the open and leaves the log file unchanged. The commit index is not stored past the snapshot.

```cpp
auto storage = std::make_shared<easyraft::FileLogStorage>("/var/lib/easyraft/1");
auto machine = std::make_shared<easyraft::MemoryStateMachine>();
easyraft::RaftNode node(1, easyraft::Config{}, storage, machine, /*bus=*/nullptr);
node.initialize(easyraft::Membership{{1}});
```

```cpp
node.trigger_snapshot();
```

`trigger_snapshot` returns `Error::Rejected` when the applied index has not moved past the current snapshot.

`Config::heartbeat_interval`, `election_timeout_min`, and `election_timeout_max` are all in ticks. `enable_pre_vote` defaults to true. `snapshot_threshold` defaults to 0. `max_snapshot_chunk` defaults to 0, which sends a snapshot in one request.

## License

MIT OR Apache-2.0. See `LICENSE-MIT`, `LICENSE-APACHE`, and `NOTICE`.
