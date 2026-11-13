// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright 2026 easyraft contributors

#include "easyraft/engine.hpp"

#include <algorithm>
#include <sstream>
#include <utility>

namespace easyraft {
namespace {

Command save_vote_command(const Vote& vote) {
  Command command;
  command.kind = CommandKind::SaveVote;
  command.vote = vote;
  return command;
}

Command save_membership_command(const Membership& membership) {
  Command command;
  command.kind = CommandKind::SaveMembership;
  command.membership = membership;
  return command;
}

struct RaiseFlag {
  bool& flag;
  explicit RaiseFlag(bool& raised) : flag(raised) { flag = true; }
  ~RaiseFlag() { flag = false; }
  RaiseFlag(const RaiseFlag&) = delete;
  RaiseFlag& operator=(const RaiseFlag&) = delete;
};

Command truncate_command(LogIndex index) {
  Command command;
  command.kind = CommandKind::Truncate;
  command.index = index;
  return command;
}

Command append_command(std::vector<LogEntry> entries) {
  Command command;
  command.kind = CommandKind::Append;
  command.entries = std::move(entries);
  return command;
}

Command apply_command(LogEntry entry) {
  Command command;
  command.kind = CommandKind::Apply;
  command.entry = std::move(entry);
  return command;
}

}  // namespace

Engine::Engine(NodeId id, Config config)
    : id_(id),
      config_(config.normalized()),
      rng_(static_cast<std::uint32_t>(id * 2654435761u) | 1u) {
  election_timeout_ticks_ = sample_timeout();
  next_election_at_ = election_timeout_ticks_;
}

StepResult Engine::recover(Membership membership,
                            Vote vote,
                            std::vector<LogEntry> log,
                            LogIndex already_applied) {
  StepResult out;
  log_ = std::move(log);
  last_index_ = log_.empty() ? 0 : log_.back().id.index;
  commit_index_ = 0;
  last_applied_ = already_applied > last_index_ ? last_index_ : already_applied;
  leading_ = false;
  role_ = Role::Follower;
  campaign_.reset();
  pre_campaign_.reset();
  progress_.clear();
  ever_contacted_ = false;
  base_membership_ = infer_base(membership);
  membership_ = membership_from_log();
  if (membership_ != membership) {
    out.commands.push_back(save_membership_command(membership_));
  }
  // A restarted process is not an established leader. Keep the term so a
  // higher vote is still required, but clear the committed bit that would
  // otherwise make this node resume leadership without a quorum.
  if (vote.committed && vote.leader.node == id_) {
    vote.committed = false;
    out.commands.push_back(save_vote_command(vote));
  }
  vote_ = vote;
  initialized_ = true;
  return out;
}

StepResult Engine::on_tick() {
  StepResult out;
  if (!initialized_) {
    return out;
  }
  ++now_;
  if (leading_) {
    ++ticks_since_heartbeat_;
    if (ticks_since_heartbeat_ >= static_cast<std::uint64_t>(config_.heartbeat_interval)) {
      ticks_since_heartbeat_ = 0;
      replicate_all(out);
    }
    return out;
  }
  if (now_ >= next_election_at_) {
    election_timeout_ticks_ = sample_timeout();
    next_election_at_ = now_ + election_timeout_ticks_;
    if (!membership_.contains(id_)) {
      return out;
    }
    if (config_.enable_pre_vote) {
      pre_elect(out);
    } else {
      elect(out);
    }
  }
  return out;
}

ProposeResult Engine::propose(std::string data) {
  ProposeResult result;
  if (!initialized_ || role_ != Role::Leader) {
    return result;
  }
  append_local(result.step, EntryKind::Application, std::move(data));
  result.accepted = true;
  result.index = last_index_;
  replicate_all(result.step);
  return result;
}

ProposeResult Engine::change_membership(std::vector<NodeId> voters) {
  ProposeResult result;
  if (!initialized_ || !leading_ || role_ != Role::Leader) {
    return result;
  }
  std::sort(voters.begin(), voters.end());
  voters.erase(std::unique(voters.begin(), voters.end()), voters.end());
  if (voters.empty() || voters.front() == 0 || membership_changing()) {
    return result;
  }
  if (voters == membership_.voters()) {
    result.accepted = true;
    result.index = last_index_;
    return result;
  }
  const Membership joint = Membership::joint_of(membership_.voters(), std::move(voters));
  const LogIndex joint_index = last_index_ + 1;
  append_local(result.step, EntryKind::Membership, joint.encode());
  result.accepted = true;
  result.index = joint_index;
  replicate_all(result.step);
  return result;
}

bool Engine::membership_changing() const {
  if (membership_.is_joint()) {
    return true;
  }
  for (const LogEntry& entry : log_) {
    if (entry.kind == EntryKind::Membership && entry.id.index > commit_index_) {
      return true;
    }
  }
  return false;
}

VoteStep Engine::on_pre_vote(const VoteRequest& request) {
  VoteStep step;
  step.response.vote = vote_;
  step.response.last_log_id = last_log_id();
  step.response.granted = false;
  if (!initialized_ || !membership_.contains(id_)) {
    return step;
  }

  // PreVote is a pure query: nothing is persisted and the term does not move.
  if ((leading_ && leader_quorum_lease_valid()) || follower_lease_valid()) {
    return step;
  }
  if (!log_is_up_to_date(request.last_log_id, last_log_id())) {
    return step;
  }
  step.response.granted = request.vote >= vote_;
  return step;
}

StepResult Engine::on_pre_vote_response(NodeId from, const PreVoteResponse& response) {
  StepResult out;
  // Only the fixed voter set can complete a probe, and only the probe that is
  // still in flight. A delayed grant from the previous round is ignored.
  if (!membership_.contains(from) || from == id_) {
    return out;
  }
  if (!pre_campaign_ || response.probe != pre_campaign_->probe) {
    return out;
  }
  if (response.body.granted) {
    pre_campaign_->grants.insert(from);
    if (has_quorum(pre_campaign_->grants)) {
      elect(out);
    }
    return out;
  }
  // Their log is strictly ahead of ours; wait before campaigning again.
  if (response.body.last_log_id && !log_is_up_to_date(last_log_id(), response.body.last_log_id)) {
    next_election_at_ = now_ + election_timeout_ticks_;
  }
  // Catch up to a higher term without granting leadership or a lease.
  if (response.body.vote > vote_) {
    update_vote(out, response.body.vote.non_committed());
  }
  return out;
}

VoteStep Engine::on_vote(const VoteRequest& request) {
  VoteStep step;
  step.response.vote = vote_;
  step.response.last_log_id = last_log_id();
  step.response.granted = false;
  if (!initialized_ || !membership_.contains(id_)) {
    return step;
  }

  const bool from_current_leader = vote_.committed && request.vote.leader.node == vote_.leader.node &&
                                   request.vote.leader.node != 0;
  if (leading_ && leader_quorum_lease_valid() && !from_current_leader) {
    return step;
  }
  if (follower_lease_valid() && !from_current_leader) {
    return step;
  }
  if (!log_is_up_to_date(request.last_log_id, last_log_id())) {
    return step;
  }
  if (!update_vote(step.step, request.vote)) {
    step.response.vote = vote_;
    return step;
  }
  // Giving a real vote waits out one election timeout, as in the Raft paper.
  next_election_at_ = now_ + election_timeout_ticks_;
  step.response.granted = true;
  step.response.vote = vote_;
  step.response.last_log_id = last_log_id();
  return step;
}

StepResult Engine::on_vote_response(NodeId from, const VoteResponse& response) {
  StepResult out;
  if (!membership_.contains(from) || from == id_) {
    return out;
  }
  if (!campaign_) {
    return out;
  }
  if (response.granted && response.vote == campaign_->vote) {
    campaign_->grants.insert(from);
    if (has_quorum(campaign_->grants)) {
      establish_leader(out);
    }
    return out;
  }
  if (response.last_log_id && !log_is_up_to_date(last_log_id(), response.last_log_id)) {
    next_election_at_ = now_ + election_timeout_ticks_;
  }
  if (response.vote > vote_) {
    update_vote(out, response.vote.non_committed());
  }
  return out;
}

AppendStep Engine::on_append(const AppendEntriesRequest& request) {
  AppendStep step;
  step.response.request_vote = request.vote;
  step.response.responder_vote = vote_;
  step.response.probed_next = request.prev_log_id ? request.prev_log_id->index + 1 : 1;
  step.response.hint_next = step.response.probed_next;
  if (!initialized_) {
    step.response.rejected_by_vote = true;
    return step;
  }

  if (!update_vote(step.step, request.vote)) {
    step.response.rejected_by_vote = true;
    step.response.responder_vote = vote_;
    return step;
  }
  // Accepted the leader's vote, so this node should not start an election
  // while that leader is still probing the log.
  if (!leading_) {
    note_leader_contact();
  }

  if (request.prev_log_id) {
    const LogEntry* local = find(request.prev_log_id->index);
    if (local == nullptr || local->id != *request.prev_log_id) {
      // Drop only the conflicting suffix. A missing index is not local data.
      if (local != nullptr && request.prev_log_id->index > commit_index_) {
        truncate_from(step.step, request.prev_log_id->index);
      }
      step.response.success = false;
      step.response.hint_next = last_index_ + 1;
      step.response.responder_vote = vote_;
      return step;
    }
  }

  std::size_t mismatch = request.entries.size();
  for (std::size_t i = 0; i < request.entries.size(); ++i) {
    const LogEntry* local = find(request.entries[i].id.index);
    if (local == nullptr || local->id != request.entries[i].id) {
      mismatch = i;
      break;
    }
  }
  if (mismatch < request.entries.size()) {
    const LogIndex conflict_at = request.entries[mismatch].id.index;
    if (conflict_at > last_index_ + 1) {
      step.response.success = false;
      step.response.hint_next = last_index_ + 1;
      step.response.responder_vote = vote_;
      return step;
    }
    if (conflict_at <= commit_index_) {
      // A committed prefix must never be rolled back. Refuse the append.
      step.response.success = false;
      step.response.hint_next = commit_index_ + 1;
      step.response.responder_vote = vote_;
      return step;
    }
    truncate_from(step.step, conflict_at);
    std::vector<LogEntry> tail(request.entries.begin() + static_cast<std::ptrdiff_t>(mismatch),
                               request.entries.end());
    if (!tail.empty()) {
      std::optional<Membership> appended_membership;
      for (const LogEntry& item : tail) {
        if (item.kind != EntryKind::Membership) {
          continue;
        }
        if (auto decoded = Membership::decode(item.data)) {
          appended_membership = std::move(*decoded);
        }
      }
      log_.insert(log_.end(), tail.begin(), tail.end());
      last_index_ = log_.back().id.index;
      step.step.commands.push_back(append_command(std::move(tail)));
      if (appended_membership) {
        adopt_membership(step.step, std::move(*appended_membership));
      }
    }
  }

  if (request.entries.empty()) {
    step.response.match_index = request.prev_log_id ? request.prev_log_id->index : 0;
  } else {
    step.response.match_index = request.entries.back().id.index;
  }
  step.response.success = true;

  const LogIndex consistent = std::min(step.response.match_index, last_index_);
  if (request.leader_commit > commit_index_) {
    const LogIndex next_commit = std::min(request.leader_commit, consistent);
    if (next_commit > commit_index_) {
      commit_index_ = next_commit;
      enqueue_applies(step.step);
    }
  }
  step.response.responder_vote = vote_;
  return step;
}

StepResult Engine::on_append_response(NodeId from, const AppendEntriesResponse& response) {
  StepResult out;
  if (response.responder_vote > vote_) {
    // Adopt the non-committed form so a peer cannot appoint us leader, while
    // the higher (term, node) still supersedes this leader.
    update_vote(out, response.responder_vote.non_committed());
  }
  if (!leading_ || response.request_vote != vote_) {
    return out;
  }
  auto found = progress_.find(from);
  if (found == progress_.end()) {
    return out;
  }
  Progress& progress = found->second;
  if (response.success) {
    if (response.match_index > progress.match_index) {
      progress.match_index = response.match_index;
      progress.next_index = response.match_index + 1;
    }
    progress.last_ack_tick = now_;
    const LogIndex before = commit_index_;
    advance_commit(out);
    if (commit_index_ > before) {
      replicate_all(out);
    } else if (progress.next_index <= last_index_) {
      send_append(out, from);
    }
    return out;
  }
  if (response.rejected_by_vote) {
    return out;
  }
  // Conflict: walk the probe backward, but never into the matched prefix.
  if (response.probed_next > progress.match_index + 1) {
    LogIndex hint = response.hint_next;
    if (hint >= response.probed_next) {
      hint = response.probed_next - 1;
    }
    if (hint < progress.match_index + 1) {
      hint = progress.match_index + 1;
    }
    if (hint < 1) {
      hint = 1;
    }
    progress.next_index = hint;
    send_append(out, from);
  }
  return out;
}

std::optional<NodeId> Engine::current_leader() const {
  if (role_ == Role::Leader) {
    return id_;
  }
  if (vote_.committed && vote_.leader.node != 0) {
    return vote_.leader.node;
  }
  return std::nullopt;
}

std::string Engine::debug() const {
  std::ostringstream out;
  out << "n" << id_ << " " << role_ << " vote=" << vote_ << " commit=" << commit_index_
      << " last=" << last_index_ << " log=[";
  for (std::size_t i = 0; i < log_.size(); ++i) {
    if (i != 0) {
      out << ", ";
    }
    const LogEntry& entry = log_[i];
    out << entry.id;
    if (entry.kind == EntryKind::Blank) {
      out << " blank";
    } else if (entry.kind == EntryKind::Membership) {
      out << " membership";
    } else {
      out << " \"" << entry.data << "\"";
    }
  }
  out << "] membership=";
  const std::vector<std::vector<NodeId>>& configs = membership_.configs();
  for (std::size_t c = 0; c < configs.size(); ++c) {
    if (c != 0) {
      out << "|";
    }
    out << "[";
    for (std::size_t i = 0; i < configs[c].size(); ++i) {
      if (i != 0) {
        out << ",";
      }
      out << configs[c][i];
    }
    out << "]";
  }
  return out.str();
}

const LogEntry* Engine::find(LogIndex index) const {
  if (index == 0 || index > log_.size()) {
    return nullptr;
  }
  const LogEntry& entry = log_[static_cast<std::size_t>(index - 1)];
  if (entry.id.index != index) {
    return nullptr;
  }
  return &entry;
}

std::optional<LogId> Engine::last_log_id() const {
  if (log_.empty()) {
    return std::nullopt;
  }
  return log_.back().id;
}

std::uint64_t Engine::sample_timeout() {
  const int min_timeout = config_.election_timeout_min;
  const int max_timeout = config_.election_timeout_max;
  if (max_timeout <= min_timeout) {
    return static_cast<std::uint64_t>(min_timeout);
  }
  rng_ = rng_ * 1664525u + 1013904223u;
  const auto span = static_cast<std::uint32_t>(max_timeout - min_timeout);
  const std::uint32_t pick = rng_ % (span + 1u);
  return static_cast<std::uint64_t>(min_timeout) + pick;
}

bool Engine::follower_lease_valid() const {
  if (!ever_contacted_ || !vote_.committed || vote_.leader.node == 0 || vote_.leader.node == id_) {
    return false;
  }
  return now_ - last_leader_contact_ < election_timeout_ticks_;
}

bool Engine::leader_quorum_lease_valid() const {
  if (!leading_) {
    return false;
  }
  return membership_.is_quorum([&](NodeId voter) {
    if (voter == id_) {
      return true;
    }
    const auto found = progress_.find(voter);
    return found != progress_.end() &&
           now_ - found->second.last_ack_tick < election_timeout_ticks_;
  });
}

void Engine::note_leader_contact() {
  ever_contacted_ = true;
  last_leader_contact_ = now_;
  next_election_at_ = now_ + election_timeout_ticks_;
  // A heartbeat from the current leader cancels a probe that has not yet
  // bumped the term. A delayed PreVote grant must not start an election.
  const bool pending = campaign_.has_value() || pre_campaign_.has_value();
  campaign_.reset();
  pre_campaign_.reset();
  if (pending && role_ == Role::Candidate) {
    role_ = Role::Follower;
  }
}

bool Engine::update_vote(StepResult& out, const Vote& incoming) {
  if (!(incoming >= vote_)) {
    return false;
  }
  if (!(incoming > vote_)) {
    if (incoming.committed && incoming.leader.node != id_) {
      note_leader_contact();
    }
    return true;
  }

  vote_ = incoming;
  out.commands.push_back(save_vote_command(vote_));
  if (vote_.committed && vote_.leader.node == id_) {
    become_leader(out);
    return true;
  }

  step_down();
  const bool self_campaign = campaign_ && campaign_->vote == vote_;
  if (self_campaign) {
    role_ = Role::Candidate;
  } else {
    role_ = Role::Follower;
    campaign_.reset();
    pre_campaign_.reset();
  }
  if (vote_.committed && vote_.leader.node != id_) {
    note_leader_contact();
  }
  return true;
}

void Engine::become_leader(StepResult& out) {
  if (leading_) {
    role_ = Role::Leader;
    return;
  }
  leading_ = true;
  role_ = Role::Leader;
  campaign_.reset();
  pre_campaign_.reset();
  progress_.clear();
  ticks_since_heartbeat_ = 0;
  // The blank entry is this term's commit anchor. Entries from earlier
  // leaders commit only once this entry (or a later one of ours) is on a
  // majority. That is the Raft rule OpenRaft keeps.
  append_local(out, EntryKind::Blank, {});
  for (NodeId voter : membership_.voters()) {
    if (voter == id_) {
      continue;
    }
    Progress progress;
    progress.next_index = std::max<LogIndex>(last_index_, 1);
    progress.match_index = 0;
    progress.last_ack_tick = now_;
    progress_.emplace(voter, progress);
  }
  replicate_all(out);
}

void Engine::step_down() {
  if (leading_) {
    leading_ = false;
    progress_.clear();
  }
  role_ = Role::Follower;
}

void Engine::append_local(StepResult& out, EntryKind kind, std::string data) {
  LogEntry entry;
  entry.id.term = vote_.leader.term;
  entry.id.node = id_;
  entry.id.index = last_index_ + 1;
  entry.kind = kind;
  entry.data = std::move(data);
  log_.push_back(entry);
  last_index_ = entry.id.index;
  const bool membership_entry = entry.kind == EntryKind::Membership;
  std::vector<LogEntry> batch;
  batch.push_back(std::move(entry));
  out.commands.push_back(append_command(std::move(batch)));
  if (membership_entry) {
    if (auto decoded = Membership::decode(log_.back().data)) {
      adopt_membership(out, std::move(*decoded));
    }
  }
  advance_commit(out);
}

void Engine::truncate_from(StepResult& out, LogIndex index) {
  if (index == 0 || index > last_index_ || index <= commit_index_) {
    return;
  }
  log_.resize(static_cast<std::size_t>(index - 1));
  last_index_ = log_.empty() ? 0 : log_.back().id.index;
  Membership next = membership_from_log();
  if (next != membership_) {
    membership_ = std::move(next);
    // Persist the restored config before deleting the suffix. A crash in
    // between leaves the log entry in place, and recover prefers the log.
    out.commands.push_back(save_membership_command(membership_));
    if (leading_) {
      sync_progress();
    }
  }
  out.commands.push_back(truncate_command(index));
}

bool Engine::has_quorum(const std::set<NodeId>& grants) const {
  return membership_.is_quorum([&](NodeId id) { return grants.find(id) != grants.end(); });
}

std::optional<LogIndex> Engine::quorum_match_index() const {
  if (membership_.configs().empty()) {
    return std::nullopt;
  }
  std::optional<LogIndex> agreed;
  for (const std::vector<NodeId>& config : membership_.configs()) {
    if (config.empty()) {
      return std::nullopt;
    }
    std::vector<LogIndex> matched;
    matched.reserve(config.size());
    for (NodeId voter : config) {
      if (voter == id_) {
        matched.push_back(last_index_);
      } else {
        const auto found = progress_.find(voter);
        matched.push_back(found == progress_.end() ? 0 : found->second.match_index);
      }
    }
    std::sort(matched.begin(), matched.end());
    const std::size_t need = config.size() / 2 + 1;
    if (need == 0 || need > matched.size()) {
      return std::nullopt;
    }
    const LogIndex majority = matched[matched.size() - need];
    if (!agreed || majority < *agreed) {
      agreed = majority;
    }
  }
  return agreed;
}

void Engine::advance_commit(StepResult& out) {
  if (!leading_ || membership_.empty()) {
    return;
  }
  // The highest index stored on a majority of every config. Self is fully
  // caught up. One pass over the voters, not over the uncommitted suffix.
  const std::optional<LogIndex> agreed = quorum_match_index();
  if (!agreed || *agreed <= commit_index_) {
    return;
  }
  const LogIndex majority_index = *agreed;
  // Only an entry from this leader may advance commit. Every index at or
  // below majority_index is on a majority, so walk back to the newest such entry.
  for (LogIndex index = majority_index; index > commit_index_; --index) {
    const LogEntry* entry = find(index);
    if (entry != nullptr && entry->id.term == vote_.leader.term && entry->id.node == id_) {
      commit_index_ = index;
      enqueue_applies(out);
      continue_membership(out);
      return;
    }
  }
}

void Engine::replicate_all(StepResult& out) {
  if (!leading_) {
    return;
  }
  for (NodeId voter : membership_.voters()) {
    if (voter == id_) {
      continue;
    }
    send_append(out, voter);
  }
}

void Engine::send_append(StepResult& out, NodeId to) {
  auto found = progress_.find(to);
  if (found == progress_.end()) {
    return;
  }
  Progress& progress = found->second;
  if (progress.next_index == 0) {
    progress.next_index = 1;
  }
  if (progress.next_index > last_index_ + 1) {
    progress.next_index = last_index_ + 1;
  }

  AppendEntriesRequest request;
  request.vote = vote_;
  request.leader_commit = commit_index_;
  const LogIndex next = progress.next_index;
  if (next > 1) {
    if (const LogEntry* prev = find(next - 1)) {
      request.prev_log_id = prev->id;
    }
  }
  std::size_t sent = 0;
  for (LogIndex index = next; index <= last_index_ && sent < config_.max_append_entries; ++index) {
    const LogEntry* entry = find(index);
    if (entry == nullptr) {
      break;
    }
    request.entries.push_back(*entry);
    ++sent;
  }
  Outbound message;
  message.to = to;
  message.payload = std::move(request);
  out.outbound.push_back(std::move(message));
}

void Engine::send_vote_requests(StepResult& out, bool pre_vote, const Vote& vote) {
  VoteRequest request;
  request.vote = vote;
  request.last_log_id = last_log_id();
  for (NodeId voter : membership_.voters()) {
    if (voter == id_) {
      continue;
    }
    Outbound message;
    message.to = voter;
    if (pre_vote) {
      PreVoteRequest wrapped;
      wrapped.body = request;
      wrapped.probe = pre_campaign_ ? pre_campaign_->probe : 0;
      message.payload = std::move(wrapped);
    } else {
      message.payload = RequestVote{request};
    }
    out.outbound.push_back(std::move(message));
  }
}

void Engine::pre_elect(StepResult& out) {
  if (leading_ || follower_lease_valid()) {
    return;
  }
  // A new probe supersedes an unfinished campaign so a late vote response
  // cannot still appoint this node.
  campaign_.reset();
  Vote probed;
  probed.leader.term = vote_.leader.term + 1;
  probed.leader.node = id_;
  probed.committed = false;
  Campaign campaign;
  campaign.vote = probed;
  campaign.grants.insert(id_);
  campaign.probe = ++next_probe_;
  pre_campaign_ = campaign;
  if (has_quorum(pre_campaign_->grants)) {
    elect(out);
    return;
  }
  role_ = Role::Follower;
  send_vote_requests(out, true, probed);
}

void Engine::elect(StepResult& out) {
  pre_campaign_.reset();
  if (leading_ && leader_quorum_lease_valid()) {
    return;
  }
  step_down();
  Vote candidate_vote;
  candidate_vote.leader.term = vote_.leader.term + 1;
  candidate_vote.leader.node = id_;
  candidate_vote.committed = false;

  Campaign campaign;
  campaign.vote = candidate_vote;
  campaign.grants.insert(id_);
  campaign_ = campaign;
  update_vote(out, candidate_vote);
  if (!campaign_) {
    return;
  }
  if (has_quorum(campaign_->grants)) {
    establish_leader(out);
    return;
  }
  role_ = Role::Candidate;
  send_vote_requests(out, false, candidate_vote);
}

void Engine::adopt_membership(StepResult& out, Membership next) {
  if (next == membership_) {
    return;
  }
  membership_ = std::move(next);
  out.commands.push_back(save_membership_command(membership_));
  if (leading_) {
    sync_progress();
  }
}

Membership Engine::infer_base(const Membership& stored) const {
  for (const LogEntry& entry : log_) {
    if (entry.kind != EntryKind::Membership) {
      continue;
    }
    const std::optional<Membership> decoded = Membership::decode(entry.data);
    if (!decoded || !decoded->is_joint() || decoded->configs().size() != 2) {
      return stored;
    }
    return Membership{decoded->configs().front()};
  }
  return stored;
}

Membership Engine::membership_from_log() const {
  Membership current = base_membership_;
  for (const LogEntry& entry : log_) {
    if (entry.kind != EntryKind::Membership) {
      continue;
    }
    if (std::optional<Membership> decoded = Membership::decode(entry.data)) {
      current = std::move(*decoded);
    }
  }
  return current;
}

void Engine::sync_progress() {
  if (!leading_) {
    return;
  }
  std::set<NodeId> wanted(membership_.voters().begin(), membership_.voters().end());
  wanted.erase(id_);
  for (auto it = progress_.begin(); it != progress_.end();) {
    if (wanted.find(it->first) == wanted.end()) {
      it = progress_.erase(it);
    } else {
      ++it;
    }
  }
  for (NodeId voter : wanted) {
    if (progress_.find(voter) != progress_.end()) {
      continue;
    }
    Progress progress;
    // There is no snapshot, so a newly added voter is sent the log from index 1.
    progress.next_index = 1;
    progress.match_index = 0;
    progress.last_ack_tick = now_;
    progress_.emplace(voter, progress);
  }
}

void Engine::continue_membership(StepResult& out) {
  if (!leading_ || continuing_) {
    return;
  }
  RaiseFlag guard(continuing_);
  const LogEntry* latest = nullptr;
  for (const LogEntry& entry : log_) {
    if (entry.kind == EntryKind::Membership) {
      latest = &entry;
    }
  }
  if (latest == nullptr || latest->id.index > commit_index_) {
    return;
  }
  std::optional<Membership> decoded = Membership::decode(latest->data);
  if (!decoded) {
    return;
  }
  if (decoded->is_joint()) {
    if (decoded->configs().size() != 2) {
      return;
    }
    // The uniform target becomes effective when it is appended. It commits
    // under that new config, which no longer requires the old voters.
    const Membership uniform{decoded->configs()[1]};
    append_local(out, EntryKind::Membership, uniform.encode());
    return;
  }
  if (!decoded->contains(id_)) {
    step_down();
    campaign_.reset();
    pre_campaign_.reset();
  }
}

void Engine::establish_leader(StepResult& out) {
  if (!campaign_) {
    return;
  }
  Vote committed = campaign_->vote;
  committed.committed = true;
  update_vote(out, committed);
}

void Engine::enqueue_applies(StepResult& out) {
  while (last_applied_ < commit_index_) {
    ++last_applied_;
    const LogEntry* entry = find(last_applied_);
    if (entry == nullptr) {
      --last_applied_;
      break;
    }
    out.commands.push_back(apply_command(*entry));
  }
}

}  // namespace easyraft
