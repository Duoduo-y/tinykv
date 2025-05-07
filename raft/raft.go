// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math"
	"math/rand"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// CountAgreeAppend uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int

	//新加的！！！
	electionTimeoutTick int
	countreject         int
	countagree          int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pbf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	Prs := make(map[uint64]*Progress)
	votes := make(map[uint64]bool)
	hardState, constate, _ := c.Storage.InitialState()

	var ID []uint64
	if len(c.peers) > 0 {
		ID = c.peers
		// log.DIYf(log.LOG_DIY4, "", "id from config: %v", ID)
	} else if len(constate.Nodes) > 0 {
		ID = constate.Nodes
		// log.DIYf(log.LOG_DIY4, "", "id from constate: %v", ID)
	}
	for _, id := range ID {
		Prs[id] = &Progress{
			Match: 0,
			Next:  1,
		}
		if id == hardState.Vote {
			votes[id] = true
		} else {
			votes[id] = false
		}
	}
	// log.DIYf(log.LOG_DIY2, "DIY", "creat new raft ID: %v", c.ID)
	// Your Code Here (2A).
	// lastIndex, _ := c.Storage.LastIndex()
	// term, _ := c.Storage.Term(lastIndex)
	return &Raft{
		id:                  c.ID,
		Term:                hardState.Term,
		Vote:                hardState.Vote,
		RaftLog:             newLog(c.Storage),
		Prs:                 Prs,
		State:               StateFollower,
		votes:               votes,
		Lead:                hardState.Vote,
		heartbeatTimeout:    c.HeartbeatTick,
		electionTimeout:     c.ElectionTick,
		countreject:         0,
		countagree:          0,
		heartbeatElapsed:    0,
		electionElapsed:     0,
		electionTimeoutTick: c.ElectionTick + rand.Intn(c.ElectionTick),
	}
}

func (r *Raft) randomElectionTimeout(electionTimeout int) int {
	// Your Code Here (2A).
	return electionTimeout + rand.Intn(electionTimeout)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.

// 在发送附加日志 RPC 的时候，领导人会把新的日志条目前紧挨着的条目的索引位置和任期号包含在日志内。
// 如果跟随者在它的日志中找不到包含相同索引位置和任期号的条目，那么他就会拒绝接收新的日志条目。
func (r *Raft) sendAppend(to uint64) bool {
	if len(r.RaftLog.entries) == 0 {
		return false
	}

	PreEntryIndex := r.Prs[to].Next - 1
	PreEntryTerm, err1 := r.RaftLog.Term(PreEntryIndex)

	//sedentry为空就是要更新committed
	var sendentry []*pb.Entry
	// _, err2 := r.RaftLog.Term(r.Prs[to].Next)

	if err1 == ErrUnavailable {
		return false
	} else {
		for i := r.Prs[to].Next; i <= r.RaftLog.LastIndex(); i++ {
			sendentry = append(sendentry, &r.RaftLog.entries[i-r.RaftLog.Offset])
		}
	}

	appendmessage := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		LogTerm: PreEntryTerm, //term和index是append的前一个entry的
		Index:   PreEntryIndex,
		Entries: sendentry, //发送的是要append的entry
		Commit:  r.RaftLog.committed,
	}

	r.msgs = append(r.msgs, appendmessage)
	return sendentry != nil
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	if r.State == StateLeader {
		//为什么不step？？
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgHeartbeat,
			From:    r.id,
			To:      to,
			Term:    r.Term,
			Commit:  r.RaftLog.committed,
		})
	}
}

func (r *Raft) sendVoterequire(to uint64) {
	logterm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		LogTerm: logterm,
		Index:   r.RaftLog.LastIndex(),
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State != StateLeader {
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeoutTick {
			// log.DIYf(log.LOG_DIY4, "TimeOut", "ID:%d, state %v, time out tick %d, electionElapsedtick %d", r.id, r.State, r.electionTimeoutTick, r.electionElapsed)
			//发起选举
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
			})
		}
	} else {
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
			})
			r.heartbeatElapsed = 0
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	for id := range r.votes {
		r.votes[id] = false
		r.Prs[id].Next = 1
		r.Prs[id].Match = 0
	}
	r.Term = term
	r.Lead = lead
	r.electionElapsed = 0
	r.Vote = lead
	r.electionTimeoutTick = r.randomElectionTimeout(r.electionTimeout)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.countagree = 1
	r.countreject = 0
	r.Term++
	r.Vote = r.id
	for id := range r.votes {
		if id == r.id {
			r.votes[id] = true
		} else {
			r.votes[id] = false
		}
		r.Prs[id].Next = 1
		r.Prs[id].Match = 0
	}
	r.electionElapsed = 0
	r.electionTimeoutTick = r.randomElectionTimeout(r.electionTimeout)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	log.DIYf(log.LOG_DIY1, "", "become leader %d", r.id)
	r.State = StateLeader
	r.Vote = 0
	r.Lead = r.id
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	noopentry := pb.Entry{
		Term:  r.Term,
		Index: r.RaftLog.LastIndex() + 1,
		Data:  nil,
	}
	r.RaftLog.entries = append(r.RaftLog.entries, noopentry)
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
		// r.RaftLog.stabled = r.RaftLog.LastIndex()
	}
	for id := range r.Prs {
		if id == r.id {
			r.Prs[id].Match = r.RaftLog.LastIndex()
			r.Prs[id].Next = r.Prs[id].Match + 1
		} else {
			r.Prs[id].Match = 0
			r.Prs[id].Next = r.RaftLog.LastIndex()
		}
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch m.MsgType {

	//-----------Leader Selection--------------------//
	// 'MessageType_MsgHup' is a local message used for election. If an election timeout happened,
	// the node should pass 'MessageType_MsgHup' to its Step method and start a new election.
	case pb.MessageType_MsgHup:
		switch r.State {
		case StateFollower, StateCandidate:
			r.becomeCandidate()
			//只有一共节点，直接当选！！
			if len(r.votes) == 1 {
				r.becomeLeader()
			}
			for id := range r.Prs {
				if id != r.id {
					r.sendVoterequire(id)
				}
			}
		case StateLeader:
		}

		// 'MessageType_MsgRequestVote' requests votes for election.
	case pb.MessageType_MsgRequestVote:
		switch r.State {
		case StateFollower, StateCandidate, StateLeader:
			// log.DIYf(log.LOG_DIY4, "!!", "%d receive voterequire from %d", m.To, m.From)
			r.handleVoterequire(m)
		}

		// 'MessageType_MsgRequestVoteResponse' contains responses from voting request.
	case pb.MessageType_MsgRequestVoteResponse:
		// log.DIYf(log.LOG_DIY1, "receive voteresponse", "%d vote for %d, now state is %v", m.From, m.To, r.State)
		switch r.State {
		case StateFollower:
		case StateCandidate:
			r.votes[m.From] = !m.Reject
			if m.Reject {
				r.countreject++
				if r.countreject >= int(math.Floor(float64(len(r.Prs))/2))+1 {
					r.becomeFollower(r.Term, m.From)
					return nil
				}
			} else {
				r.countagree++
				log.DIYf(log.LOG_DIY1, "receive agree", "%d vote for %d, countagree is %d", m.From, m.To, r.countagree)
				if r.countagree >= int(math.Floor(float64(len(r.Prs))/2))+1 {
					if r.State == StateCandidate {
						r.becomeLeader()
						r.Step(pb.Message{
							MsgType: pb.MessageType_MsgPropose,
							From:    r.id,
							To:      r.id,
							// Entries: []*pb.Entry{&noopentry},
							// Term:    r.Term,
						})
					}

				}
			}
		case StateLeader:
		}

		//-----------Heartbeat--------------------//
	// 'MessageType_MsgBeat' is a local message that signals the leader to send a heartbeat
	// of the 'MessageType_MsgHeartbeat' type to its followers.
	case pb.MessageType_MsgBeat:
		switch r.State {
		case StateFollower:
		case StateCandidate:

		case StateLeader:
			for id := range r.Prs {
				if id != r.id {
					r.sendHeartbeat(id)
				}
			}
		}
		// 'MessageType_MsgHeartbeat' sends heartbeat from leader to its followers.
	case pb.MessageType_MsgHeartbeat:
		switch r.State {
		case StateFollower:
			fallthrough
		case StateCandidate:
			r.handleHeartbeat(m)
		case StateLeader:
		}

	// 'MessageType_MsgHeartbeatResponse' is a response to 'MessageType_MsgHeartbeat'.
	case pb.MessageType_MsgHeartbeatResponse:
		switch r.State {
		case StateFollower:
		case StateCandidate:
		case StateLeader:
			r.handleHeartbeatResponse(m)
		}

		//--------------Log Replication--------------------//
	// 'MessageType_MsgPropose' is a local message that proposes to append data to the leader's log entries.
	case pb.MessageType_MsgPropose:
		switch r.State {
		case StateFollower:
			fallthrough
		case StateCandidate:
			return ErrProposalDropped
		case StateLeader:
			for _, entry := range m.Entries {
				r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
					EntryType: entry.EntryType,
					Term:      r.Term,
					Index:     r.RaftLog.LastIndex() + 1,
					Data:      entry.Data,
				})
			}
			r.Prs[r.id].Match = r.RaftLog.LastIndex()
			r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
			if len(r.Prs) == 1 {
				r.RaftLog.committed = r.RaftLog.LastIndex()
				// r.RaftLog.stabled = r.RaftLog.LastIndex()
				return nil
			}
			for id := range r.Prs {
				if id != r.id {
					r.sendAppend(id)
				}
			}

		}

	// 'MessageType_MsgAppend' contains log entries to replicate.
	case pb.MessageType_MsgAppend:
		switch r.State {
		case StateFollower, StateCandidate, StateLeader:
			r.handleAppendEntries(m)
		}

	// 'MessageType_MsgAppendResponse' is response to log replication request('MessageType_MsgAppend').
	case pb.MessageType_MsgAppendResponse:
		switch r.State {
		case StateFollower:
		case StateCandidate:
		case StateLeader:
			r.handleAppendEntriesResponse(m)
		}

	// 'MessageType_MsgSnapshot' requests to install a snapshot message.
	case pb.MessageType_MsgSnapshot:
		switch r.State {
		case StateFollower:
		case StateCandidate:
		case StateLeader:
		}

	// 'MessageType_MsgTransferLeader' requests the leader to transfer its leadership.
	case pb.MessageType_MsgTransferLeader:
		switch r.State {
		case StateFollower:
		case StateCandidate:
		case StateLeader:
		}
	// 'MessageType_MsgTimeoutNow' send from the leader to the leadership transfer target, to let
	// the transfer target timeout immediately and start a new election.
	case pb.MessageType_MsgTimeoutNow:
		switch r.State {
		case StateFollower:
		case StateCandidate:
		case StateLeader:
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	entryTerm, err := r.RaftLog.Term(m.Index)
	rejectmessage := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Index:   0,
		Reject:  true,
	}
	agreemessage := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		// LogTerm: entryTerm,
		// Index:  , //matchIndex
		Reject: false,
		// Commit:  r.RaftLog.committed,
	}
	//过期信息
	if r.Term > m.Term {
		r.msgs = append(r.msgs, rejectmessage)
	} else {

		r.becomeFollower(m.Term, m.From)

		if err == ErrUnavailable {
			// 没有找到，回退
			r.msgs = append(r.msgs, rejectmessage)
		} else {
			//term不一致
			if entryTerm != m.LogTerm {
				// index对应的entry存在但term不一致，回退
				r.msgs = append(r.msgs, rejectmessage)
			} else {
				isstablechange := false
				agreemessage.Index = m.Index //matchIndex

				if len(m.Entries) != 0 {
					for _, entry := range m.Entries {
						if entry.Index > r.RaftLog.LastIndex() {
							r.RaftLog.entries = append(r.RaftLog.entries, *entry)
						} else {
							if r.RaftLog.entries[entry.Index-r.RaftLog.Offset].Term != entry.Term {
								if !isstablechange {
									r.RaftLog.stabled = entry.Index - 1
									isstablechange = true
								}
								//截断,改变stable
								r.RaftLog.entries = r.RaftLog.entries[:entry.Index-r.RaftLog.Offset]
								r.RaftLog.entries = append(r.RaftLog.entries, *entry)
							}
						}

					}
					agreemessage.Index = m.Entries[len(m.Entries)-1].Index //matchIndex
				}

				if m.Commit > r.RaftLog.committed {
					r.RaftLog.committed = min(m.Commit, agreemessage.Index)
				}
				agreemessage.Commit = r.RaftLog.committed
				r.msgs = append(r.msgs, agreemessage)
				log.DIYf(log.LOG_DIY4, "committed", "ID:%d, state %v, committed %d", r.id, r.State, m.Index)

				for _, entry := range r.RaftLog.entries {
					log.DIYf(log.LOG_DIY4, "committed", "ID:%d,entries %v", r.id, string(entry.Data))
				}
			}
		}
	}
	// // r.RaftLog.committed = m.Commit
	// if r.Term > m.Term {
	// 	r.msgs = append(r.msgs, rejectmessage)
	// 	return
	// } else {
	// 	r.becomeFollower(m.Term, m.From)
	// 	if (m.Index - r.RaftLog.Offset + 1) < uint64(len(r.RaftLog.entries)) {
	// 		//从头覆盖
	// 		if len(m.Entries) > 0 {
	// 			r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[0])
	// 		}
	// 		r.msgs = append(r.msgs, pb.Message{
	// 			MsgType: pb.MessageType_MsgAppendResponse,
	// 			To:      m.From,
	// 			From:    r.id,
	// 			Term:    r.Term,
	// 			Reject:  false,
	// 			LogTerm: PreEntryTerm,
	// 			Index:   m.Index,
	// 		})
	// 		return
	// 	} else if (m.Index - r.RaftLog.Offset + 1) == uint64(len(r.RaftLog.entries)) {
	// 		preterm, err := r.RaftLog.Term(m.Index)
	// 		if err != nil || preterm != m.LogTerm {
	// 			//回退
	// 			r.msgs = append(r.msgs, rejectmessage)
	// 			return
	// 		} else {
	// 			//可以append
	// 			if len(m.Entries) > 0 {
	// 				r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[0])
	// 			}
	// 			r.msgs = append(r.msgs, pb.Message{
	// 				MsgType: pb.MessageType_MsgAppendResponse,
	// 				To:      m.From,
	// 				From:    r.id,
	// 				Term:    r.Term,
	// 				Reject:  false,
	// 				LogTerm: PreEntryTerm,
	// 				Index:   m.Index,
	// 				Commit:  m.Commit,
	// 			})
	// 			return
	// 		}
	// 	} else {
	// 		//回退
	// 		r.msgs = append(r.msgs, rejectmessage)
	// 		return
	// 	}
	// }
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if m.Reject {
		if m.Term > r.Term {
			r.becomeFollower(m.Term, m.From)
			return
		}
		r.Prs[m.From].Next--
		r.sendAppend(m.From)
	} else {
		r.Prs[m.From].Match = m.Index
		r.Prs[m.From].Next = m.Index + 1
		if r.RaftLog.committed < m.Index {
			if r.canCommitted(m) {
				//广播新的committed
				for id := range r.Prs {
					if id != r.id {
						r.sendAppend(id)
					}
				}
			}
		}
	}
}

func (r *Raft) canCommitted(m pb.Message) bool {
	count := 1
	for id := range r.Prs {
		//仅提交本周期的日志
		if id != r.id && r.Prs[id].Match >= m.Index && r.Term == r.RaftLog.entries[m.Index-r.RaftLog.Offset].Term {
			count++
		}
	}
	if count >= int(math.Floor(float64(len(r.Prs))/2))+1 {
		r.RaftLog.committed = m.Index
		log.DIYf(log.LOG_DIY4, "committed", "ID:%d, state %v, committed %d", r.id, r.State, m.Index)

		for _, entry := range r.RaftLog.entries {
			log.DIYf(log.LOG_DIY4, "committed", "ID:%d, entries %v", r.id, string(entry.Data))
		}
		return true
	}
	return false
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	// log.DIYf(log.LOG_DIY4, "Receive heartbeat", "ID:%d, state %v, heart beat time out tick %d, heartbeatElapsed %d", r.id, r.State, r.heartbeatTimeout, r.heartbeatElapsed)

	if r.Term > m.Term {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgHeartbeatResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
			// Commit:  r.RaftLog.committed,
		})
	} else {
		r.becomeFollower(m.Term, m.From)
		if m.Commit > r.RaftLog.committed && m.Commit <= r.RaftLog.LastIndex() {
			r.RaftLog.committed = m.Commit
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgHeartbeatResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  false,
			Index:   r.RaftLog.LastIndex(),
			LogTerm: func() uint64 {
				term, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
				return term
			}(),
			Commit: r.RaftLog.committed,
		})
	}

}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if m.Reject {
		r.becomeFollower(m.Term, m.From)
	} else {
		// log.DIYf(log.LOG_DIY4, "heartbeatrespond", "Leader %d  Receive heartbeat response from %d  ", r.id, m.From)
		//通过 m.Commit 判断节点是否落后了，如果是，则进行日志追加；
		if m.Commit < r.RaftLog.committed { //还是比match？？？
			r.sendAppend(m.From)
		}
	}

}

func (r *Raft) handleVoterequire(m pb.Message) {
	logterm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())

	RejectVoteMessage := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		LogTerm: logterm,
		Index:   r.RaftLog.LastIndex(),
		Reject:  true,
	}

	if r.Term > m.Term {
		//拒绝投票
		r.msgs = append(r.msgs, RejectVoteMessage)
		// log.DIYf(log.LOG_DIY1, "reject", "%d vote for %d", m.To, m.From)
		return
	} else if r.Term == m.Term && r.Vote != None && r.Vote != m.From {
		//一个Term只能投一张票，拒绝投票
		r.msgs = append(r.msgs, RejectVoteMessage)
		// log.DIYf(log.LOG_DIY1, "reject", "%d vote for %d", m.To, m.From)

		return
	} else {
		//比较日志新旧。如果两份日志最后的条目的任期号不同，那么任期号大的日志更加新。
		// 如果两份日志最后的条目任期号相同，那么日志比较长的那个就更加新。
		// r.Term = m.Term
		r.becomeFollower(m.Term, None)
		if (logterm == m.LogTerm && r.RaftLog.LastIndex() > m.Index) || (logterm > m.LogTerm) {
			r.msgs = append(r.msgs, RejectVoteMessage)
			// log.DIYf(log.LOG_DIY1, "reject", "%d vote for %d", m.To, m.From)
		} else {
			//投票
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
				LogTerm: logterm,
				Index:   r.RaftLog.LastIndex(),
				Reject:  false,
			})
			r.Vote = m.From
			log.DIYf(log.LOG_DIY1, "agree", "%d vote for %d", m.To, m.From)
		}
	}

}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
