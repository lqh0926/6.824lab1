package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId int
	seq      int
	mu       sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clientId = int(nrand())
	ck.seq = 1
	return ck
}

func (ck *Clerk) Query(num int) Config {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.ClientId = ck.clientId
	args.ClientSeq = ck.seq
	ck.seq++
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			var ok bool
			done := make(chan bool)
			go func() {
				ok = srv.Call("ShardCtrler.Query", args, &reply)
				done <- true
			}()
			select {
			case <-done:
				if ok && !reply.WrongLeader && reply.Err == OK {
					return reply.Config
				}
			case <-time.After(500 * time.Millisecond):
				ok = false
			}
			if ok && reply.WrongLeader {
				continue
			} else if ok && reply.Err == ErrTimeOut {
				continue
			}

			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientId = ck.clientId
	args.ClientSeq = ck.seq
	ck.seq++

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			var ok bool
			done := make(chan bool)
			go func() {
				ok = srv.Call("ShardCtrler.Join", args, &reply)
				done <- true
			}()
			select {
			case <-done:
				if ok && !reply.WrongLeader && reply.Err == OK {
					return
				}
			case <-time.After(500 * time.Millisecond):
				ok = false
			}
			if ok && reply.WrongLeader {
				continue
			} else if ok && reply.Err == ErrTimeOut {
				continue
			}

			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientId = ck.clientId
	args.ClientSeq = ck.seq
	ck.seq++

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			var ok bool
			done := make(chan bool)
			go func() {
				ok = srv.Call("ShardCtrler.Leave", args, &reply)
				done <- true
			}()
			select {
			case <-done:
				if ok && !reply.WrongLeader && reply.Err == OK {
					return
				}
			case <-time.After(500 * time.Millisecond):
				ok = false
			}
			if ok && reply.WrongLeader {
				continue
			}
			if ok && reply.Err == ErrTimeOut {
				continue
			}

			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientId = ck.clientId
	args.ClientSeq = ck.seq
	ck.seq++

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			var ok bool
			done := make(chan bool)
			go func() {
				ok = srv.Call("ShardCtrler.Move", args, &reply)
				done <- true
			}()
			select {
			case <-done:
				if ok && !reply.WrongLeader && reply.Err == OK {
					return
				}
			case <-time.After(500 * time.Millisecond):
				ok = false
			}
			if ok && reply.WrongLeader {
				continue
			} else if ok && reply.Err == ErrTimeOut {
				continue
			}

			time.Sleep(100 * time.Millisecond)
		}
	}
}
