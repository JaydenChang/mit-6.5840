package kvsrv

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt   *tester.Clnt
	server string
	id     string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	ck.id = kvtest.RandValue(8)
	// You may add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	args := rpc.GetArgs{Key: key}
	reply := rpc.GetReply{}
	retries := 0
	for {
		if ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply); ok {
			if retries > 0 && reply.Err == rpc.ErrVersion {
				return "", 0, rpc.ErrMaybe
			}

			return reply.Value, reply.Version, reply.Err
		}
		time.Sleep(100 * time.Millisecond)
		retries++
	}
	// DPrintf("------ get reply err: %v\n", reply.Err)

}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	args := rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}
	// DPrintf("get ck Put args: %+v\n", args)
	reply := rpc.PutReply{}
	retries := 0
	for {
		if ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply); ok {
			if retries > 0 && reply.Err == rpc.ErrVersion {
				return rpc.ErrMaybe
			}
			// DPrintf("get kv reply err: %v\n", reply.Err)
			return reply.Err
		}
		time.Sleep(100 * time.Millisecond)

		retries++
	}
	// return rpc.OK
}

func (ck *Clerk) Acquire(lockKey string) rpc.Err {
	args := rpc.AcquireLockArgs{
		LockKey:  lockKey,
		ClientId: ck.id,
	}
	var reply rpc.AcquireLockReply
	retry := 0
	for {
		if ok := ck.clnt.Call(ck.server, "KVServer.Acquire", &args, &reply); ok {
			if retry > 0 && reply.Err == rpc.ErrAcquired {
				return rpc.ErrMaybe
			}
			// DPrintf("----- get retry, err: %v\n", reply.Err)
			return reply.Err
		}
		retry++
	}
}

func (ck *Clerk) Release(lockKey string) rpc.Err {
	args := rpc.ReleaseLockArgs{
		LockKey:  lockKey,
		ClientId: ck.id,
	}
	retry := 0
	for {
		var reply rpc.AcquireLockReply
		if ok := ck.clnt.Call(ck.server, "KVServer.Release", &args, &reply); ok {
			if retry > 0 && reply.Err == rpc.ErrReleased {
				return rpc.ErrMaybe
			}
			return reply.Err
		}
		retry++
	}
}
