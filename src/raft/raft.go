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
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

const heartbeatTime = 125

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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 0 leader 1 candidate 2 follower
	state int
	lastContact int64
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
}


func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply)  {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		DPrintf("[server %v, state %v, term %v] receive past heartbeat from %v", rf.me, rf.state, rf.currentTerm, args.LeaderId)
		reply.Success = false
	} else {
		reply.Term = args.Term
		rf.lastContact = time.Now().UnixNano()
		DPrintf("[server %v, state %v, term %v] receive success heartbeat from %v", rf.me, rf.state, rf.currentTerm, args.LeaderId)
		reply.Success = true
	}
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
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = 2
		rf.voteFor = -1
	}
	if rf.voteFor == args.CandidateId || rf.voteFor == -1 {
		rf.lastContact = time.Now().UnixNano()
		rf.voteFor = args.CandidateId
		DPrintf("[server %v, role %v, term %v], vote for [%v]\n", rf.me, rf.state, rf.currentTerm, args.CandidateId)
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
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
	rf.state = 2
	rand.Seed(time.Now().Unix())
	rf.voteFor = -1
	rf.lastContact = 0
	// create a goroutine to start leader election
	go rf.schedule()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}

func (rf *Raft)schedule()  {
	// one loop represents an identity switch
	for {
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

// TODO: add cond primitive
func (rf *Raft)follower() {
	rf.electionTimeout = 300 + rand.Intn(150)
	DPrintf("[server %v, role %v, term %v]: change to follower, sleep [%v]\n", rf.me ,rf.state, rf.currentTerm, rf.electionTimeout)
	rf.mu.Lock()
	rf.lastContact = time.Now().UnixNano()
	rf.mu.Unlock()
	for {
		// wait election timeout
		rf.mu.Lock()
		lastContact := rf.lastContact
		rf.mu.Unlock()
		if time.Now().UnixNano()-lastContact >= int64(rf.electionTimeout*1000000) {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}
	DPrintf("[server %v, role %v, term %v] election timeout\n", rf.me, rf.state, rf.currentTerm)
	rf.mu.Lock()
	rf.state = 1
	rf.mu.Unlock()
}

func (rf *Raft)candidate() {
	end := make(chan struct{})
	go monitor(rf, end, 1)
	for {
		select {
		case <-end:
			return
		default:
			rf.mu.Lock()
			rf.currentTerm += 1
			rf.voteFor = rf.me
			DPrintf("[server %v, role %v, term %v]: vote for self:[%v]", rf.me, rf.state, rf.currentTerm, rf.me)
			rf.mu.Unlock()
			if rf.collectEnoughVotes() {
				return
			} else {
				rf.electionTimeout = 300 + rand.Intn(150)
				DPrintf("[server %v, role %v, term %v] not get enough votes, sleep [%v]", rf.me, rf.state, rf.currentTerm, rf.electionTimeout)
				rf.mu.Lock()
				rf.lastContact = time.Now().UnixNano()
				rf.mu.Unlock()
				for {
					// wait election timeout
					rf.mu.Lock()
					lastContact := rf.lastContact
					rf.mu.Unlock()
					if time.Now().UnixNano()-lastContact >= int64(rf.electionTimeout*1000000) {
						break
					}
					time.Sleep(1 * time.Millisecond)
				}
			}
		}
	}
}

func (rf *Raft)leader() {
	end := make(chan struct{})
	go monitor(rf, end, 0)
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

func (rf *Raft)sendHeartbeat() {
	rf.mu.Lock()
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: 0,
	}
	rf.mu.Unlock()
	for i:=0; i<len(rf.peers); i++ {
		if i != rf.me {
			reply := &AppendEntriesReply{}
			go func(i int) {
				//DPrintf("[server %v, role %v, term %v] send heartbeat to %v", rf.me, rf.state, rf.currentTerm, i)
				if ok := rf.sendAppendEntries(i, args, reply); ok {
					rf.mu.Lock()
					if reply.Term>rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = 2
					}
					rf.mu.Unlock()
				}
			}(i)
		}
	}
}

func (rf *Raft)collectEnoughVotes() bool {
	var mu sync.Mutex
	cond := sync.NewCond(&mu)
	votes := 1
	finish := 1
	rf.mu.Lock()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	rf.mu.Unlock()
	for i:=0; i<len(rf.peers); i++ {
		if i != rf.me {
			reply := &RequestVoteReply{
				Term:        0,
				VoteGranted: false,
			}
			go func(i int) {
				ok:=rf.sendRequestVote(i, args, reply)
				mu.Lock()
				if ok{
					if reply.VoteGranted {
						votes++
						DPrintf("[server %v, role %v, term %v], receive votes from [%v]", rf.me, rf.state, rf.currentTerm, i)
					}
				}
				finish++
				DPrintf("[server %v, role %v, term %v]: finish send requestVote to [%v]", rf.me, rf.state, rf.currentTerm, i)
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
		DPrintf("[server %v, role %v, term %v], get majority votes\n", rf.me, rf.state, rf.currentTerm)
		rf.mu.Lock()
		rf.state = 0
		rf.mu.Unlock()
		return true
	}
	mu.Unlock()
	return false
}

// Monitor state change, once the state changed, then send signal to the end execute procedure
// TODO: cond primitive
func monitor(rf *Raft, end chan struct{}, state int)  {
	for {
		rf.mu.Lock()
		s := rf.state
		rf.mu.Unlock()
		if s != state {
			end <- struct {}{}
			return
		}
		time.Sleep(1 * time.Millisecond)
	}
}