package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
	"6.5840/shardctrler"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	clientId int
	seq      int
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.clientId = int(nrand())
	ck.seq = 1
	ck.config = ck.sm.Query(-1)

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key
	args.ClientId = ck.clientId
	args.ClinetSeq = ck.seq
	ck.seq++
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				reply := GetReply{}
				//处理Rpc超时
				var ok bool
				done := make(chan bool)
				go func() {

					ok = srv.Call("ShardKV.Get", &args, &reply)
					done <- true
				}()
				select {
				case <-done:
				case <-time.After(2000 * time.Millisecond):
					//打印超时和连接的服务器
					//fmt.Println("time out" + strconv.Itoa(i))
					ok = false
				}
				if ok && reply.Err == OK {
					//打印成功和id和seq
					//fmt.Print("Get success")
					//fmt.Println("id:", ck.clientId, "seq:", args.ClinetSeq, "value:", reply.Value, "key:", key)
					return reply.Value
				} else if ok && reply.Err == ErrWrongLeader {
					//fmt.Print("id:", ck.clientId, "seq:", ck.seq, "value:", reply.Value)
					//fmt.Println("get wrong leader")
					continue
				} else if ok && reply.Err == ErrNoKey {
					return ""
				} else if ok && reply.Err == ErrFail {

					// args.ClinetSeq = ck.seq
					// ck.seq++
					continue
				} else if ok && reply.Err == ErrTimeOut {
					//fmt.Print("id:", ck.clientId, "seq:", ck.seq, "value:", reply.Value)
					//fmt.Println("time out")
					continue
				} else if ok && (reply.Err == ErrWrongGroup) {
					//fmt.Println("wrong group", "seq:", ck.seq, "id:", ck.clientId)

					// args.ClinetSeq = ck.seq
					// ck.seq++
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)

		// ask controller for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.ClientId = ck.clientId
	args.ClinetSeq = ck.seq
	ck.seq++

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := false
				done := make(chan bool)
				go func() {
					if op == "Put" {
						ok = srv.Call("ShardKV.Put", &args, &reply)
					}
					if op == "Append" {
						ok = srv.Call("ShardKV.Append", &args, &reply)
					}
					done <- true
				}()
				select {
				case <-done:
				case <-time.After(2000 * time.Millisecond):

					//fmt.Println("time out" + strconv.Itoa(i))
					ok = false
				}
				if ok && reply.Err == OK {
					//打印成功和id和seq
					//fmt.Print("PutAppend success")
					//fmt.Println("id:", ck.clientId, "seq:", args.ClinetSeq, "value:", value, "key:", key, "gid", gid)
					return
				} else if ok && reply.Err == ErrWrongLeader {
					//fmt.Println("wrong leader")
					continue
				} else if ok && reply.Err == ErrFail {

					// args.ClinetSeq = ck.seq
					// ck.seq++
					continue
				} else if ok && reply.Err == ErrTimeOut {
					//fmt.Println("time out")
					continue
				} else if ok && reply.Err == ErrWrongGroup {
					// args.ClinetSeq = ck.seq
					// ck.seq++
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controller for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
