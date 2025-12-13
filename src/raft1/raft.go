package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state         int
	currentTerm   int
	getVotedCount int
	votedFor      int
	commitIndex   int

	nextIndex  []int
	matchIndex []int

	Logs []Entry

	turnFollower chan struct{}
	turnLeader   chan struct{}
}

type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	// in raft1/server.go:86 has use mu.Lock(), so it needn't use lock here
	// rf.mu.Lock()
	// defer func() {
	// 	rf.mu.Unlock()
	// 	log.Printf("&&&&&&& no %v getState unlock  ", rf.me)
	// }()
	// defer rf.mu.Unlock()
	log.Printf("%v state: %v, term: %v", rf.me, rf.state, rf.currentTerm)
	term = rf.currentTerm
	isleader = rf.state == LEADER
	// rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term              int
	LeaderId          int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []Entry
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm // for candicate to update itself
		log.Printf("================ %v refuse to vote to %v, term %v", rf.me, args.CandidateId, rf.currentTerm)
		tester.Annotate(fmt.Sprintf("server %v", rf.me), "refuse to vote", fmt.Sprintf("candiate %v", args.CandidateId))
		return
	}
	log.Printf("++++++++++++++ get server %v voteFor: %v, commitIndex: %v, term: %v, candidate: %v, candidate term: %v, args,LastLogIndex: %v", rf.me, rf.votedFor, rf.commitIndex, rf.currentTerm, args.CandidateId, args.Term, args.LastLogIndex)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId || args.Term > rf.currentTerm) && args.LastLogIndex >= rf.commitIndex {
		log.Printf("===== follower %v get vote request from %v, term: %v, grant\n", rf.me, args.CandidateId, rf.currentTerm)
		rf.state = FOLLOWER
		rf.turnFollower <- struct{}{}
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.getVotedCount = 0
		rf.getVotedCount = 0
		rf.votedFor = args.CandidateId
		tester.Annotate(fmt.Sprintf("server %v", rf.me), "grant vote", fmt.Sprintf("candidate %v,term %v", args.CandidateId, rf.currentTerm))
	}
	rf.currentTerm = args.Term
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	tester.Annotate(fmt.Sprintf("server %v", rf.me), "get hb", fmt.Sprintf("from leader %v", args.LeaderId))
	if args.Term < rf.currentTerm {
		tester.Annotate(fmt.Sprintf("server %v", rf.me), "send hb get newer term,turn follower", fmt.Sprintf("from leader %v", args.LeaderId))
		reply.Term = rf.currentTerm
		return
	}
	rf.turnFollower <- struct{}{}
	rf.state = FOLLOWER
	rf.votedFor = args.LeaderId
	rf.currentTerm = args.Term
	rf.getVotedCount = 0
	reply.Success = true
	log.Printf("********************** server %v get hb from leader %v, leaderTerm: %v, turn follower", rf.me, args.LeaderId, args.Term)
}

func (rf *Raft) StartElection() {
	rf.mu.Lock()
	rf.votedFor = rf.me
	rf.getVotedCount = 1
	rf.currentTerm++
	tester.Annotate(fmt.Sprintf("server %v", rf.me), "start election", fmt.Sprintf("term %v", rf.currentTerm))
	rf.mu.Unlock()
	var wg sync.WaitGroup
	for i := 0; i < len(rf.peers); i++ {
		if rf.me == i {
			continue
		}
		wg.Add(1)
		go func() {
			rf.mu.Lock()
			args := RequestVoteArgs{
				Term:        rf.currentTerm,
				CandidateId: rf.me,
			}
			rf.mu.Unlock()
			log.Printf("------ candiate %v send to %v, term: %v\n", rf.me, i, rf.currentTerm)
			tester.Annotate(fmt.Sprintf("server %v", rf.me), "send vote req", fmt.Sprintf("to %v, term %v", i, rf.currentTerm))
			reply := RequestVoteReply{}
			if ok := rf.sendRequestVote(i, &args, &reply); ok {
				rf.mu.Lock()
				if reply.VoteGranted {
					tester.Annotate(fmt.Sprintf("server %v", rf.me), "get vote", fmt.Sprintf("term %v", rf.currentTerm))
					rf.getVotedCount++
				}
				rf.mu.Unlock()
				log.Printf("##### candiate get voteCount: %v\n", rf.getVotedCount)
				if rf.getVotedCount > len(rf.peers)/2 {
					// rf.turnLeader <- struct{}{}
					rf.mu.Lock()
					log.Printf("server %v turn leader", rf.me)
					rf.state = LEADER
					rf.mu.Unlock()
					tester.Annotate(fmt.Sprintf("server %v", rf.me), "turn leader", fmt.Sprintf("term %v", rf.currentTerm))
					rf.SendHeartBeat()
					log.Printf("send hb finish")
					// tester.Annotate(fmt.Sprintf("server %v", rf.me), "send hb", fmt.Sprintf("term %v", rf.currentTerm))
				}
			} else {
				log.Printf("!!!!!!!!! candidate %v send vote request to %v failed, term %v", rf.me, i, rf.currentTerm)
				tester.Annotate(fmt.Sprintf("server %v", rf.me), "send vote request failed", fmt.Sprintf("to server %v, no %v", i, args.CandidateId))
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func (rf *Raft) SendHeartBeat() {
	var wg sync.WaitGroup
	for i := 0; i < len(rf.peers); i++ {
		if rf.me == i {
			continue
		}
		tester.Annotate(fmt.Sprintf("server %v", rf.me), "send hb", fmt.Sprintf("to %v, term: %v", i, rf.currentTerm))
		wg.Add(1)
		go func() {
			rf.mu.Lock()
			args := AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
			}
			rf.mu.Unlock()
			reply := AppendEntriesReply{}
			if ok := rf.sendAppendEntries(i, &args, &reply); ok {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = FOLLOWER
					tester.Annotate(fmt.Sprintf("server %v", rf.me), "leader turn follower", fmt.Sprintf("term: %v", rf.currentTerm))
					rf.turnFollower <- struct{}{}
				}
				rf.mu.Unlock()
			} else {
				log.Printf("!!!!!!!!!!!!!!!!!! %v send hb to server %v failed", rf.me, i)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	log.Printf("__________send hb to %v", server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	rf.mu.Lock()
	isLeader := rf.state == LEADER
	if rf.state == LEADER {
		entry := Entry{
			Index:   len(rf.Logs),
			Term:    rf.currentTerm,
			Command: command,
		}
		rf.Logs = append(rf.Logs, entry)
		index = entry.Index
		term = entry.Term
	}
	rf.mu.Unlock()
	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		log.Printf("################# server %v, state: %v", rf.me, rf.state)
		switch rf.state {
		case FOLLOWER:
			select {
			case <-time.After(ElectionTimer()):
				log.Printf("*** %v start election, term: %v", rf.me, rf.currentTerm)
				rf.StartElection()
			case <-rf.turnFollower:
				log.Printf("^^^^^^^ %v follower get turn follower signal, voteFor %v", rf.me, rf.votedFor)
			}
		case CANDIDATE:
			select {
			case <-time.After(ElectionTimer()):
				rf.StartElection()
			case <-rf.turnFollower:
				log.Printf("^^^^^^^ %v candidate get turn follower signal, voteFor %v", rf.me, rf.votedFor)
				tester.Annotate(fmt.Sprintf("server %v", rf.me), "turn leader", fmt.Sprintf("term %v", rf.currentTerm))
			}
		case LEADER:
			select {
			case <-rf.turnFollower:
				log.Printf("^^^^^^^ %v leader get turn follower signal", rf.me)
			case <-time.After(50 * time.Millisecond):
				log.Printf("```````````` leader %v term %v send hb", rf.me, rf.currentTerm)
				rf.SendHeartBeat()
			}
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func ElectionTimer() time.Duration {
	timer := 200 + (rand.Intn(200))
	// log.Printf("")
	return time.Duration(timer) * time.Millisecond
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.x
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votedFor = -1
	rf.turnFollower = make(chan struct{}, len(rf.peers))
	// rf.turnLeader = make(chan struct{}, len(rf.peers))

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
