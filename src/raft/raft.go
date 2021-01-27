package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"../labgob"
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

const (
	heartbeatTime = 100
	electionTimeoutLower = 300
	randomSection = 150
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Entry struct {
	Command interface{}
	Term int
}


//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 0 leader 1 candidate 2 follower
	state int
	tick int64
	// Persistent state
	currentTerm int
	voteFor int
	logs []Entry

	// volatile state
	commitIndex int
	lastApplied int
	electionTimeout int

	// volatile state on leaders
	nextIndex []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	if rf.state == 0 {
		isleader = true
	}
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, voteFor int
	var logs []Entry
	if d.Decode(&currentTerm) != nil ||
	   	d.Decode(&voteFor) != nil || d.Decode(&logs) != nil {
		fmt.Println("readPersist error")
	} else {
		rf.mu.Lock()
	  	rf.currentTerm = currentTerm
	  	rf.logs = logs
	  	rf.voteFor = voteFor
		rf.mu.Unlock()
	}
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []Entry
	LeaderCommit int
}


type AppendEntriesReply struct {
	Term int
	Success bool

	// for optimize
	ConflictTerm int
	ConflictIndex int
}

// TODO reduce the granularity of lock
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply)  {
	rf.mu.Lock()
	defer func() {
		rf.persist()
		rf.mu.Unlock()
	}()

	reply.Term = rf.currentTerm
	reply.ConflictTerm = -1
	reply.ConflictIndex = -1
	if rf.currentTerm > args.Term {
		DPrintf("[server %v, role %v, term %v] leader [%v] term is smaller\n", rf.me, rf.state, rf.currentTerm, args.LeaderId)
		reply.Success = false
		return
	} else if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.state = 2
	}
	// can't find a entry that has same index and term with args, return false
	if args.PrevLogIndex>=len(rf.logs) {
		reply.Success = false
		reply.ConflictIndex = len(rf.logs)
		reply.ConflictTerm = -1
		return
	}
	if rf.logs[args.PrevLogIndex].Term!=args.PrevLogTerm {
		reply.Success = false
		conflictTerm := rf.logs[args.PrevLogIndex].Term
		reply.ConflictTerm = conflictTerm
		conflictIndex := args.PrevLogIndex
		for rf.logs[conflictIndex-1].Term == conflictTerm {
			conflictIndex--
		}
		reply.ConflictIndex = conflictIndex
		DPrintf("[server %v, role %v, term %v] inconsistent entry in preLogIndex(%v), need to decrease\n", rf.me, rf.state, rf.currentTerm, args.PrevLogIndex)
		return
	}
	// append entries that not exist
	unmatch_idx := -1
	for idx := range args.Entries {
		if len(rf.logs) < (args.PrevLogIndex+2+idx) ||
			rf.logs[(args.PrevLogIndex+1+idx)].Term != args.Entries[idx].Term {
			// unmatch log found
			unmatch_idx = idx
			break
		}
	}

	if unmatch_idx != -1 {
		// there are unmatch entries
		// truncate unmatch Follower entries, and apply Leader entries
		rf.logs = rf.logs[:(args.PrevLogIndex + 1 + unmatch_idx)]
		rf.logs = append(rf.logs, args.Entries[unmatch_idx:]...)
		DPrintf("[server %v, role %v, term %v] Append Entry successfully from [%v], (%v)-(%v)", rf.me, rf.state, rf.currentTerm, args.LeaderId, args.PrevLogIndex + 1 + unmatch_idx, args.PrevLogIndex+len(args.Entries))
	} else {
		if len(args.Entries) == 0 {
			DPrintf("[server %v, role %v, term %v] receive successful heartbeat from [%v]", rf.me, rf.state, rf.currentTerm, args.LeaderId)
		} else {
			DPrintf("[server %v, role %v, term %v] Append Entry successfully from [%v], already have (%v)-(%v)", rf.me, rf.state, rf.currentTerm, args.LeaderId, args.PrevLogIndex+1, args.PrevLogIndex+len(args.Entries))
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > len(rf.logs)-1 {
			rf.setCommitIndexAndApply(len(rf.logs)-1)
		} else {
			rf.setCommitIndexAndApply(args.LeaderCommit)
		}
	}
	rf.state = 2
	rf.electionTimeout = electionTimeoutLower + rand.Intn(randomSection)
	rf.tick = time.Now().UnixNano()
	reply.Success = true
}


func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer func() {
		rf.persist()
		rf.mu.Unlock()
	}()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		DPrintf("[server %v, role %v, term %v] reject vote for [%v], candidate term is small", rf.me, rf.state, rf.currentTerm, args.CandidateId)
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = 2
		rf.voteFor = -1
	}
	if rf.voteFor!=args.CandidateId && rf.voteFor!=-1 {
		reply.VoteGranted = false
		DPrintf("[server %v, role %v, term %v] reject vote for [%v], already voted for [%v]", rf.me, rf.state, rf.currentTerm, args.CandidateId, rf.voteFor)
		return
	}
	lastLogIndex := len(rf.logs)-1
	lastLogTerm := rf.logs[lastLogIndex].Term
	if lastLogTerm > args.LastLogTerm {
		reply.VoteGranted = false
		DPrintf("[server %v, role %v, term %v] reject vote for [%v], not up to date", rf.me, rf.state, rf.currentTerm, args.CandidateId)
		return
	} else if lastLogTerm == args.LastLogTerm {
		if lastLogIndex > args.LastLogIndex {
			reply.VoteGranted = false
			DPrintf("[server %v, role %v, term %v] reject vote for [%v], not up to date", rf.me, rf.state, rf.currentTerm, args.CandidateId)
			return
		}
	}
	rf.electionTimeout = electionTimeoutLower + rand.Intn(randomSection)
	rf.tick = time.Now().UnixNano()
	rf.voteFor = args.CandidateId
	DPrintf("[server %v, role %v, term %v], vote for [%v]\n", rf.me, rf.state, rf.currentTerm, args.CandidateId)
	reply.VoteGranted = true
}


//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	// TODO The granularity of the lock can be reduced
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isLeader = rf.state == 0
	if isLeader {
		DPrintf("[server %v, role %v, term %v] receive command(%v) from client", rf.me, rf.state, rf.currentTerm, command)
		rf.logs = append(rf.logs, Entry{Command: command, Term: term})
		rf.persist()
		index = len(rf.logs) - 1
		// why this step
		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1
	}

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).
	// initialization for leader election
	rf.currentTerm = 0
	rf.applyCh = applyCh
	rf.state = 2
	rf.commitIndex = 0
	rf.lastApplied = 0
	// initial with sentinel node
	rf.logs = []Entry{{Term: -1}}
	rand.Seed(time.Now().UnixNano())
	rf.voteFor = -1
	rf.tick = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	// create a goroutine to start leader election
	go rf.run()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}

func (rf *Raft) run()  {
	// one loop represents an identity switch
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		if state == 2 {
			rf.follower()
		} else if state == 1 {
			rf.candidate()
		} else {
			rf.leader()
		}
	}
}

func (rf *Raft) restartTimer() int64 {
	rf.electionTimeout = electionTimeoutLower + rand.Intn(randomSection)
	rf.mu.Lock()
	lastTick := time.Now().UnixNano()
	rf.tick = lastTick
	rf.mu.Unlock()
	return lastTick
}

//func (rf *Raft) timeout(ch chan struct{}) {
//	for {
//		// wait election timeout
//		rf.mu.Lock()
//		if time.Now().UnixNano()-rf.tick >= int64(rf.electionTimeout*1000000) {
//				rf.mu.Unlock()
//				ch <- struct{}{}
//				return
//		}
//		rf.mu.Unlock()
//		time.Sleep(10 * time.Millisecond)
//	}
//}

// TODO: add cond primitive
func (rf *Raft) follower() {
	lastTick := rf.restartTimer()
	for {
		// wait election timeout
		rf.mu.Lock()
		if rf.tick != lastTick {
			lastTick = rf.tick
		} else {
			if time.Now().UnixNano()-rf.tick >= int64(rf.electionTimeout*1000000) {
				rf.mu.Unlock()
				break
			}
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	rf.mu.Lock()
	DPrintf("[server %v, role %v, term %v] election timeout, change to candidate\n", rf.me, rf.state, rf.currentTerm)
	rf.state = 1
	rf.mu.Unlock()
}

func (rf *Raft) candidate() {
	end := make(chan struct{})
	rf.restartTimer()
	go rf.monitor(end, 1)
	voting := false
	for {
		select {
		case <-end:
			return
		default:
			if !voting {
				rf.mu.Lock()
				rf.currentTerm += 1
				rf.voteFor = rf.me
				rf.persist()
				DPrintf("[server %v, role %v, term %v]: vote for self:[%v]", rf.me, rf.state, rf.currentTerm, rf.me)
				rf.mu.Unlock()
				if rf.collectEnoughVotes() {
					return
				} else {
					voting = true
				}
			} else {
				time.Sleep(10 * time.Millisecond)
			}
		}
	}
}

func (rf *Raft) leader() {
	logNums := len(rf.logs)
	for i:=0; i<len(rf.peers); i++ {
		rf.nextIndex[i] = logNums
		rf.matchIndex[i] = 0
	}

	end := make(chan struct{})
	go rf.monitor(end, 0)
	for {
		select {
		case <-end:
			return
		default:
			rf.sendHeartbeat()
			time.Sleep(heartbeatTime * time.Millisecond)
		}
	}
}

func (rf *Raft) sendHeartbeat() {
	for i:=0; i<len(rf.peers); i++ {
		if i != rf.me {
			rf.mu.Lock()
			currentTerm := rf.currentTerm
			prevLogIndex := rf.nextIndex[i] - 1
			state := rf.state
			entries := make([]Entry, len(rf.logs[(prevLogIndex+1):]))
			copy(entries, rf.logs[(prevLogIndex+1):])
			args := AppendEntriesArgs {
				Term:         currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  rf.logs[prevLogIndex].Term,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()
			go func(i int) {
				var reply AppendEntriesReply
				if len(entries) == 0 {
					DPrintf("[server %v, role %v, term %v] send heartbeat to [%v]", rf.me, state, currentTerm, i)
				} else {
					DPrintf("[server %v, role %v, term %v] Append entry to [%v] from %v to %v", rf.me, state, currentTerm, i, prevLogIndex+1, prevLogIndex+len(entries))
				}
				if rf.sendAppendEntries(i, &args, &reply) {
					rf.mu.Lock()
					if reply.Success {
						DPrintf("[server %v, role %v, term %v] receive successful append entry response from [%v]", rf.me, rf.state, rf.currentTerm, i)
						rf.nextIndex[i] = args.PrevLogIndex+len(args.Entries)+1
						rf.matchIndex[i] = rf.nextIndex[i]-1
						// update committed
						for N := len(rf.logs) - 1; N > rf.commitIndex; N-- {
							count := 0
							for _, matchIndex := range rf.matchIndex {
								if matchIndex >= N {
									count += 1
								}
							}

							if count > len(rf.peers)/2 {
								// most of nodes agreed on rf.log[i]
								rf.setCommitIndexAndApply(N)
								break
							}
						}
					} else {
						if reply.Term > rf.currentTerm {
							DPrintf("[server %v, role %v, term %v] leader term is obsolete, change to follower", rf.me, rf.state, rf.currentTerm)
							rf.currentTerm = reply.Term
							rf.persist()
							rf.state = 2
						} else {
							// can't find a entry that has same index and term with args, decrease prevLogIndex
							DPrintf("[server %v, role %v, term %v] inconsistent entry send to [%v], now to decrease preLogIndex", rf.me, rf.state, rf.currentTerm, i)
							rf.nextIndex[i] = reply.ConflictIndex
						}
					}
					rf.mu.Unlock()
				} else {
					DPrintf("[server %v, role %v, term %v] not receive response from [%v] for append entry, delay or drop may happen", rf.me, state, currentTerm, i)
				}
			}(i)
		}
	}
}

func (rf *Raft) collectEnoughVotes() bool {
	var mu sync.Mutex
	cond := sync.NewCond(&mu)
	votes := 1
	finish := 1
	rf.mu.Lock()
	lastLogIndex := len(rf.logs)-1
	state := rf.state
	currentTerm := rf.currentTerm
	args := &RequestVoteArgs {
		Term:         currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  rf.logs[lastLogIndex].Term,
	}
	rf.mu.Unlock()
	for i:=0; i<len(rf.peers); i++ {
		if i != rf.me {
			go func(i int) {
				DPrintf("[server %v, role %v, term %v], send request vote to [%v]", rf.me, state, currentTerm, i)
				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(i, args, reply)
				mu.Lock()
				if ok  {
					if reply.VoteGranted {
						votes++
						DPrintf("[server %v, role %v, term %v], receive votes from [%v]", rf.me, state, currentTerm, i)
					} else {
						rf.mu.Lock()
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.persist()
							rf.state = 2
						}
						rf.mu.Unlock()
					}
				} else {
					DPrintf("[server %v, role %v, term %v] not receive vote response from [%v], drop or delay may happens", rf.me, state, currentTerm, i)
				}
				finish++
				cond.Broadcast()
				mu.Unlock()
			}(i)
		}
	}
	mu.Lock()
	for votes <= len(rf.peers)/2 && finish != len(rf.peers)-1 {
		cond.Wait()
	}
	if votes > len(rf.peers)/2 {
		DPrintf("[server %v, role %v, term %v], get majority votes\n", rf.me, state, currentTerm)
		rf.mu.Lock()
		rf.state = 0
		rf.mu.Unlock()
		return true
	}
	mu.Unlock()
	DPrintf("[server %v, role %v, term %v] not get enough votes\n", rf.me, state, currentTerm)
	return false
}

func (rf *Raft) setCommitIndexAndApply(commitIndex int)  {
	rf.commitIndex = commitIndex
	// apply to state machine, with a new goroutine
	if rf.commitIndex > rf.lastApplied {
		DPrintf("[server %v, role %v, term %v], apply log from %v to %v\n", rf.me, rf.state, rf.currentTerm, rf.lastApplied+1, rf.commitIndex)
		entriesToApply := append([]Entry{}, rf.logs[(rf.lastApplied+1):(rf.commitIndex+1)]...)

		go func(startIdx int, entries []Entry) {
			for idx, entry := range entries {
				msg := ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: startIdx+idx,
				}
				rf.applyCh <- msg
				rf.mu.Lock()
				if rf.lastApplied < msg.CommandIndex {
					rf.lastApplied = msg.CommandIndex
				}
				rf.mu.Unlock()
			}
		}(rf.lastApplied+1, entriesToApply)
	}
}

// Monitor state change, once the state changed, then send signal to the end execute procedure
// TODO: cond primitive
func (rf *Raft) monitor(end chan struct{}, state int)  {
	for {
		rf.mu.Lock()
		s := rf.state
		if s != state {
			rf.mu.Unlock()
			end <- struct {}{}
			return
		}
		if s!=0 {
			if time.Now().UnixNano()-rf.tick >= int64(rf.electionTimeout*1000000) {
				DPrintf("[server %v, role %v, term %v] election timeout, start new election", rf.me, rf.state, rf.currentTerm)
				rf.mu.Unlock()
				end <- struct{}{}
				return
			}
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}