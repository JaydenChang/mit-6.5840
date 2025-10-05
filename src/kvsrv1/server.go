package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Tuple struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	mu      sync.Mutex
	Map     map[string]Tuple
	LockMap map[string]string
	// Your definitions here.
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	kv.Map = map[string]Tuple{}
	kv.LockMap = map[string]string{} // key for lockKey, value for clientId
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	tuple, ok := kv.Map[args.Key]
	// DPrintf("get cmd args: %+v, get tuple: %+v\n", args, tuple)
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}

	reply.Value = tuple.Value
	reply.Version = tuple.Version
	reply.Err = rpc.OK
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	tuple, ok := kv.Map[args.Key]
	if ok {
		// DPrintf("##########.  put cmd input ver: %v, cur ver: %v\n", args.Version, tuple.Version)
		if tuple.Version == args.Version {
			kv.Map[args.Key] = Tuple{
				Version: args.Version + 1,
				Value:   args.Value,
			}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrVersion
		}
		// DPrintf("put cmd reply err: %v\n", reply.Err)
		return
	}
	// DPrintf("no result, get get args: %+v\n", args)
	if args.Version == 0 {
		args.Version = 1
	} else {
		// DPrintf("----- get no key")
		reply.Err = rpc.ErrNoKey
		return
	}
	kv.Map[args.Key] = Tuple{
		Version: args.Version,
		Value:   args.Value,
	}
	reply.Err = rpc.OK
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}

func (kv *KVServer) Acquire(args *rpc.AcquireLockArgs, reply *rpc.AcquireLockReply) {
	// DPrintf("kv svr try acquire lock, id: %v\n", args.ClientId)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// DPrintf("kv svr acquired lock, lockKey: %v, clientId: %v, lockMap: %v\n", args.LockKey, args.ClientId, kv.LockMap[args.LockKey])
	switch kv.LockMap[args.LockKey] {
	case "":
		kv.LockMap[args.LockKey] = args.ClientId
		// DPrintf("get map str: %v, lockKey: %v", kv.LockMap[args.LockKey], args.LockKey)
		// DPrintf("******************************** %v acquire lock", args.ClientId)
		reply.Err = rpc.OK
	case args.ClientId:
		// DPrintf("***** %v lock is occupied", args.ClientId)
		reply.Err = rpc.ErrAcquired
	default:
		// DPrintf("lock is unoccupied, lockKey: %v, clientId: %v, lockVal: %v", args.LockKey, args.ClientId, kv.LockMap[args.LockKey])
		reply.Err = rpc.ErrOccured
	}
}

func (kv *KVServer) Release(args *rpc.ReleaseLockArgs, reply *rpc.ReleaseLockReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// DPrintf("prepare to release lock")
	switch kv.LockMap[args.LockKey] {
	case "":
		// DPrintf("^^^^ lock released")
		reply.Err = rpc.ErrReleased
	case args.ClientId:
		delete(kv.LockMap, args.LockKey)
		// DPrintf("`````````````````````````````````` %v released lock", args.ClientId)
		reply.Err = rpc.OK
	default:
		// DPrintf("lock is unoccupied, lockKey: %v, clientId: %v, lockVal: %v", args.LockKey, args.ClientId, kv.LockMap[args.LockKey])
		reply.Err = rpc.ErrOccured
	}
}
