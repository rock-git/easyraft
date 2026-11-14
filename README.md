# easyraft

easyraft is a small C++20 implementation of the [Raft](https://raft.github.io/) consensus protocol. Phase 1 is the core algorithm: leader election, log replication, majority commit, joint-consensus membership changes, learners, log snapshots, and restart from a persisted vote, membership, log, and snapshot.

The design follows the OpenRaft 0.10 line (`release-0.10`, published as 0.10.0): a pure, tick-driven engine decides, and a thin runtime performs storage and network I/O. This is an independent implementation inspired by that design. It is not an official OpenRaft port and it does not contain OpenRaft source code. OpenRaft is a Databend project and is derived from the original async-raft work. See `NOTICE`.

## Phase 1 scope

Included:

- Leader election with PreVote (enabled by default)
- AppendEntries replication, including conflict truncation that deletes only the divergent suffix
- Commit when a majority of every voter config has an entry from the current leader
- Persistence of the vote, the membership, the log, and the latest snapshot (`MemoryLogStorage` or `FileLogStorage`), and recovery after the node object is rebuilt
- A standard majority quorum: `floor(n / 2) + 1`, taken separately for each config in a joint membership
- Voter-set changes through joint consensus: a joint config is committed, then the uniform target
- Learners: non-voting replicators that receive the log and snapshots and do not count toward quorum
- Snapshots of applied state, crash-safe log compaction, and `InstallSnapshot` catch-up for a lagging voter, a new voter, or a learner

Not in this phase: flexible quorums, linearizable or lease reads, leader transfer, multi-raft, chaos frameworks, OpenTelemetry, and a standalone server or key-value application.

## Protocol notes

Time is a tick count, not a wall clock. `RaftNode::tick()` advances one unit. Tests set `election_timeout_min == election_timeout_max` so campaigns are reproducible. Heartbeats are empty AppendEntries.

Votes use OpenRaft's default leader id `(term, node_id)`, totally ordered, plus a `committed` bit. A node grants a request when the candidate's vote is at least the vote it has stored and the candidate's log is at least as up to date. A higher node id can supersede another candidate in the same term. `committed` marks an established leader and is greater than that leader's non-committed vote.

PreVote asks peers whether they would grant `term + 1` and changes nothing locally. A single-node voter set skips the round and campaigns immediately. A follower that still has a leader lease (a recent AppendEntries from the leader it follows) rejects both PreVote and RequestVote, so a partitioned node cannot inflate the term and knock over a healthy leader when it returns.

On winning an election the leader appends a blank entry. Only entries written by the current leader move the commit index; once one of those is on a majority of every config in the effective membership, the prefix before it is committed too. That is how entries from the previous term become committed, and how a restarted leader avoids treating an unreplicated suffix as committed.

`initialize()` stores the first uniform voter set, optionally with learners. A node id is a voter or a learner or neither, never both. Id 0 is reserved for both sets. `RaftNode::change_membership` is leader-only. It appends a joint membership (the current voters and the target voters) and, once that entry commits, appends the uniform target. Learners are kept, except a learner named in the target voter set: that node is promoted and leaves the learner set in the joint entry. The effective membership is the last membership entry still in the log, or the membership recorded in the latest snapshot when the remaining log has none, or the initialized set when there is neither. A membership entry commits only when a majority of each voter config in that effective membership has it, so during the joint phase a majority of the old set alone or the new set alone is not enough. Learners are not part of that count. After the uniform target commits, a leader that is no longer a voter steps down, and a removed node is not part of the quorum. A new voter or learner catches up by replicating the log, or by `InstallSnapshot` once the leader has compacted the prefix. An empty set, voter id 0, a learner id that is already a voter, a change from a follower, and a second change while one is in flight are rejected.

Learners replicate and do not vote. `RaftNode::add_learner` and `RaftNode::remove_learner` are leader-only. Each appends one uniform membership entry and does not enter joint consensus, because the voter quorum does not change. The leader sends `AppendEntries` and, when the learner's next index is below the first available log index, `InstallSnapshot`, the same way it does for a voting follower. A learner never starts an election, never grants a vote, and its match index cannot advance the commit index. `RaftNode::demote_voter` is the opposite of promotion: joint consensus removes the node from the voter set, and the uniform target adds it as a learner. During the joint config it is still a voter of the old set, so it is not in the learner set yet. Demoting the last voter is rejected.

A follower accepts AppendEntries only after it accepts the leader's vote. If `prev_log_id` does not match, it truncates at that index and rejects. If the prefix matches, identical entries are kept and only the conflicting tail is deleted before the new suffix is appended. The commit index never advances past the prefix just checked against the leader. A prev id that names the snapshot's last included entry matches that compacted prefix, and truncation never deletes the snapshot.

## Snapshots

A snapshot is applied state only. Its meta is the last included log id `(index, term, node)`, the effective membership at that index (one uniform config or a joint pair, including that config's learners), and a snapshot id of the form `index-term-node`. The bytes are whatever `RaftStateMachine::build_snapshot` returned. An uncommitted suffix is not included and is not purged.

`Config::snapshot_threshold` is how many applied entries since the current snapshot cause a node to compact. The default is 0, so existing callers do not compact unless they opt in. `RaftNode::trigger_snapshot` compacts through `last_applied` on a leader or a follower and returns `Error::Rejected` when nothing new has been applied. Building and storing the image is local. Only the leader sends it.

The runtime builds the bytes, stores the snapshot, and only then deletes the log prefix through the last included index. The first remaining index is `last_included + 1`, or the log is empty. `truncate_from` of a conflicting suffix does not delete that snapshot or the committed prefix it stands for. On restart the state machine installs the snapshot, so its applied index is the snapshot index, and any later log is applied again once a leader commits it. If a crash lands after the snapshot is durable and before the prefix is deleted, opening the node finishes the purge.

`FileLogStorage` writes `snapshot` the same way it writes `vote` and `membership`: a temp file, fsync, rename, and a directory fsync. A leftover `snapshot.tmp` is removed on open. The purge rewrites the kept suffix to `log.rewrite`, fsyncs, renames it onto `log`, and fsyncs the directory. A crash before the rename leaves the previous log, and torn-tail recovery still applies to whatever suffix remains. The log header stays version 1. A membership file with no learners stays version 1 when it is uniform and version 2 when it is joint. Learners use version 3, described below. The snapshot stores `Membership::encode` inside its own record.

When a follower's `next_index` is below the leader's first available index, the leader sends `InstallSnapshot` instead of `AppendEntries` from index 1. That is also how a voter or learner added after compaction catches up. A request carries `offset`, `data`, and `done`. `Config::max_snapshot_chunk` limits the payload; 0 sends the whole image in one request. The follower installs the image only when the last included index is newer than both its snapshot and its applied index. A stale image is rejected and left unused. After a successful install the log has no conflicting prefix, commit and applied are at least the snapshot index, the state machine holds the snapshot, and membership is the snapshot membership unless a later membership entry is still in the log.

## Layout

| Path | Role |
| --- | --- |
| `include/easyraft/engine.hpp` | Pure protocol state machine |
| `include/easyraft/node.hpp` | Runtime: timeouts, heartbeats, client writes, membership changes, RPC dispatch |
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

On a multi-node cluster, pass a `MessageBus` and deliver envelopes back through `RaftNode::on_message`. `write` returns `Error::NotLeader` when the node cannot accept the proposal. `current_leader()` is the established leader, when this node knows one.

```cpp
auto changed = node.change_membership({1, 2, 4});
auto added = node.add_learner(5);
auto removed = node.remove_learner(5);
auto demoted = node.demote_voter(2);
```

`change_membership` returns `Error::NotLeader` from a follower, `Error::InvalidMembership` for an empty set or voter id 0, and `Error::Rejected` while a joint config is in effect or a membership entry is still uncommitted. The returned index is the joint entry. Calling it with the current voter set does not append. A learner listed in the new voter set is promoted: the joint entry removes it from the learner set. Other learners are unchanged. A process may `initialize` with a uniform set that does not include itself, then catch up when a leader adds it as a voter or as a learner.

`add_learner` and `remove_learner` return `Error::NotLeader` from a follower and `Error::InvalidMembership` for id 0, a voter, a duplicate learner, or a learner that is not in the set. `demote_voter` returns `Error::InvalidMembership` for id 0, a node that is not a voter, or the last remaining voter. All three return `Error::Rejected` while a membership entry is still uncommitted. `add_learner` and `remove_learner` return the uniform entry's index. `demote_voter` returns the joint entry's index.

`FileLogStorage` is the same `RaftLogStorage` contract, rooted at a directory. It stores `vote`, `membership`, an append-only `log`, and at most one `snapshot`. A uniform membership with no learners is the original version-1 voter list. A joint membership with no learners is version 2 and round-trips as the old config plus the new one; a version-1 file still loads as a uniform set. A membership that records learners, or a joint demotion's follow-up learner set, is version 3 and stores `Membership::encode`. Membership log entries use kind 2 and carry that payload: version 1 when the learner set is empty, and version 2 when it is not. Metadata files, including `snapshot`, are replaced with a temp file, fsync, rename, and a directory fsync. Creating the directory fsyncs each parent that gained a new entry. Log appends and truncations fsync before they return success. Opening the directory drops a torn or checksum-failed final record when its length checksum still matches. A corrupt length prefix is left on disk and reported as a storage error. A corrupt `snapshot` fails the open and leaves the log file unchanged. The commit index is not stored past the snapshot.

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
