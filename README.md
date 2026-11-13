# easyraft

easyraft is a small C++20 implementation of the [Raft](https://raft.github.io/) consensus protocol. Phase 1 is the core algorithm: leader election, log replication, majority commit, and restart from a persisted vote and log.

The design follows the OpenRaft 0.10 line (`release-0.10`, published as 0.10.0): a pure, tick-driven engine decides, and a thin runtime performs storage and network I/O. This is an independent implementation inspired by that design. It is not an official OpenRaft port and it does not contain OpenRaft source code. OpenRaft is a Databend project and is derived from the original async-raft work. See `NOTICE`.

## Phase 1 scope

Included:

- Leader election with PreVote (enabled by default)
- AppendEntries replication, including conflict truncation that deletes only the divergent suffix
- Commit when a majority of the fixed voter set has an entry from the current leader
- Persistence of the vote, the static membership, and the log (`MemoryLogStorage` or `FileLogStorage`), and recovery after the node object is rebuilt
- A standard majority quorum: `floor(n / 2) + 1`
- Membership fixed at `initialize()`. Every member is a voter

Not in this phase: snapshots, log compaction, dynamic membership, joint consensus, learners, flexible quorums, linearizable or lease reads, leader transfer, snapshot streaming, multi-raft, chaos frameworks, OpenTelemetry, and a standalone server or key-value application.

## Protocol notes

Time is a tick count, not a wall clock. `RaftNode::tick()` advances one unit. Tests set `election_timeout_min == election_timeout_max` so campaigns are reproducible. Heartbeats are empty AppendEntries.

Votes use OpenRaft's default leader id `(term, node_id)`, totally ordered, plus a `committed` bit. A node grants a request when the candidate's vote is at least the vote it has stored and the candidate's log is at least as up to date. A higher node id can supersede another candidate in the same term. `committed` marks an established leader and is greater than that leader's non-committed vote.

PreVote asks peers whether they would grant `term + 1` and changes nothing locally. A single-node voter set skips the round and campaigns immediately. A follower that still has a leader lease (a recent AppendEntries from the leader it follows) rejects both PreVote and RequestVote, so a partitioned node cannot inflate the term and knock over a healthy leader when it returns.

On winning an election the leader appends a blank entry. Only entries written by the current leader move the commit index; once one of those is on a majority, the prefix before it is committed too. That is how entries from the previous term become committed, and how a restarted leader avoids treating an unreplicated suffix as committed.

A follower accepts AppendEntries only after it accepts the leader's vote. If `prev_log_id` does not match, it truncates at that index and rejects. If the prefix matches, identical entries are kept and only the conflicting tail is deleted before the new suffix is appended. The commit index never advances past the prefix just checked against the leader.

## Layout

| Path | Role |
| --- | --- |
| `include/easyraft/engine.hpp` | Pure protocol state machine |
| `include/easyraft/node.hpp` | Runtime: timeouts, heartbeats, client writes, RPC dispatch |
| `include/easyraft/storage.hpp` | `RaftLogStorage` |
| `include/easyraft/state_machine.hpp` | `RaftStateMachine` |
| `include/easyraft/network.hpp` | `MessageBus` |
| `include/easyraft/memory.hpp` | In-memory storage and state machine |
| `include/easyraft/file.hpp` | Durable directory storage (`FileLogStorage`) |
| `tests/cluster.hpp` | In-process cluster with a fake clock |

`MessageBus::send` must queue. Delivering inside `send` re-enters the engine. The test cluster drains the queue after each tick.

## Build and test

Requires CMake 3.24+, a C++20 compiler, and network access on the first configure so GoogleTest 1.15.2 can be fetched. No Boost and no other runtime dependencies. Voter ids start at 1; id 0 is reserved.

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

`FileLogStorage` is the same `RaftLogStorage` contract, rooted at a directory. It stores `vote`, `membership`, and an append-only `log`. Metadata files are replaced with a temp file, fsync, rename, and a directory fsync. Creating the directory fsyncs each parent that gained a new entry. Log appends and truncations fsync before they return success. Opening the directory drops a torn or checksum-failed final record when its length checksum still matches. A corrupt length prefix is left on disk and reported as a storage error. The commit index is not stored, and this class does not snapshot or compact the log.

```cpp
auto storage = std::make_shared<easyraft::FileLogStorage>("/var/lib/easyraft/1");
auto machine = std::make_shared<easyraft::MemoryStateMachine>();
easyraft::RaftNode node(1, easyraft::Config{}, storage, machine, /*bus=*/nullptr);
node.initialize(easyraft::Membership{{1}});
```

`Config::heartbeat_interval`, `election_timeout_min`, and `election_timeout_max` are all in ticks. `enable_pre_vote` defaults to true.

## License

MIT OR Apache-2.0. See `LICENSE-MIT`, `LICENSE-APACHE`, and `NOTICE`.
