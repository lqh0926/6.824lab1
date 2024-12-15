package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Opration  string
	Key       string
	Value     string
	ClientId  int
	ClinetSeq int
	Shards    []int
	Confignum int
	Config    shardctrler.Config
	Data      map[string]string
	Seq       map[int]int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	sm           *shardctrler.Clerk
	maxraftstate int // snapshot if log grows this big
	bepulling    map[int]*PullShardReply
	// Your definitions here.
	shards      map[int][]int // shard -> [state and num]
	preconfig   shardctrler.Config
	nowconfig   shardctrler.Config
	dead        int32
	applied     int
	kvMap       map[string]string
	startedmsgs map[int]map[int]chan interface{} //clientid -> clientseq -> repch
	applyedmsgs map[int]int
	replys      map[int]map[int]interface{}
}
type DebugMutex struct {
	mu         sync.Mutex
	lockedAt   string    // 记录上锁的位置
	lockedTime time.Time // 记录上锁的时间
}

// Lock 获取锁并打印调试信息
func (dm *DebugMutex) Lock() {
	// 获取调用者的文件名和行号
	_, file, line, _ := runtime.Caller(1)
	lockLocation := fmt.Sprintf("%s:%d", file, line)

	fmt.Printf("Attempting to lock at %s\n", lockLocation)
	dm.mu.Lock()

	dm.lockedAt = lockLocation
	dm.lockedTime = time.Now()
	fmt.Printf("Locked at %s\n", lockLocation)
}

// Unlock 释放锁并打印调试信息
func (dm *DebugMutex) Unlock() {
	// 获取调用者的文件名和行号
	_, file, line, _ := runtime.Caller(1)
	unlockLocation := fmt.Sprintf("%s:%d", file, line)

	duration := time.Since(dm.lockedTime)
	fmt.Printf("Unlocking at %s (locked at %s, held for %v)\n",
		unlockLocation, dm.lockedAt, duration)

	dm.mu.Unlock()
}
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	//fmt.Println("Get", args.Key)
	kv.mu.Lock()
	if args.ClinetSeq <= kv.applyedmsgs[args.ClientId] {
		if kv.replys[args.ClientId][args.ClinetSeq] != nil {

			reply1 := kv.replys[args.ClientId][args.ClinetSeq].(*GetReply)
			//fmt.Println("b"+reply1.Value+"b", reply1.Err)
			reply.Err = reply1.Err
			reply.Value = reply1.Value
			kv.mu.Unlock()
			//fmt.Print("a")
			return
		}
		//fmt.Print("b")
		kv.mu.Unlock()
		return
	}

	kv.replys[args.ClientId] = make(map[int]interface{})
	op := Op{Opration: "Get", Key: args.Key, ClientId: args.ClientId, ClinetSeq: args.ClinetSeq}
	_, _, isLeader := kv.rf.Start(op)
	repch := make(chan interface{})

	if !isLeader {
		//fmt.Println("not leader")
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	//fmt.Print("c")
	if _, ok := kv.startedmsgs[args.ClientId]; !ok {
		kv.startedmsgs[args.ClientId] = make(map[int]chan interface{})
	}
	kv.startedmsgs[args.ClientId][args.ClinetSeq] = repch
	//kv.startedmsgs[logindex] = map[int]map[int]chan interface{}{args.ClientId: {args.ClinetSeq: repch}}
	kv.mu.Unlock()
	select {
	case <-time.After(200 * time.Millisecond):

		reply.Err = ErrTimeOut
		kv.mu.Lock()

		delete(kv.startedmsgs[args.ClientId], args.ClinetSeq)
		kv.mu.Unlock()
		return

	case reply1 := <-repch:
		kv.mu.Lock()

		delete(kv.startedmsgs[args.ClientId], args.ClinetSeq)
		kv.mu.Unlock()
		if reply1 == nil {
			reply.Err = ErrFail
			return
		}
		reply2 := reply1.(*GetReply)
		if reply2.Err == OK {
			kv.mu.Lock()
			if value, ok := kv.kvMap[args.Key]; ok {
				reply.Err = OK
				reply.Value = value
			} else {
				reply.Err = ErrNoKey
			}
			kv.mu.Unlock()
		} else {
			reply.Err = reply2.Err
		}

	}
}

func (kv *ShardKV) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	//fmt.Println("Put", args.Key, args.Value)
	if args.ClinetSeq <= kv.applyedmsgs[args.ClientId] {
		if kv.replys[args.ClientId][args.ClinetSeq] != nil {
			reply1 := kv.replys[args.ClientId][args.ClinetSeq].(*PutAppendReply)
			reply.Err = reply1.Err
			kv.mu.Unlock()
			//fmt.Print("a")
			return
		}
		//fmt.Print("b")
		kv.mu.Unlock()
		return
	}

	kv.replys[args.ClientId] = make(map[int]interface{})
	op := Op{Opration: "Put", Key: args.Key, Value: args.Value, ClientId: args.ClientId, ClinetSeq: args.ClinetSeq}
	_, _, isLeader := kv.rf.Start(op)
	repch := make(chan interface{})
	if !isLeader {
		//fmt.Println("not leader")
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	//fmt.Print("c")
	if _, ok := kv.startedmsgs[args.ClientId]; !ok {
		kv.startedmsgs[args.ClientId] = make(map[int]chan interface{})
	}
	kv.startedmsgs[args.ClientId][args.ClinetSeq] = repch
	//kv.startedmsgs[logindex] = map[int]map[int]chan interface{}{args.ClientId: {args.ClinetSeq: repch}}

	kv.mu.Unlock()
	select {
	case <-time.After(500 * time.Millisecond):
		kv.mu.Lock()

		delete(kv.startedmsgs[args.ClientId], args.ClinetSeq)
		kv.mu.Unlock()
		reply.Err = ErrTimeOut
		return

	case reply1 := <-repch:

		kv.mu.Lock()

		delete(kv.startedmsgs[args.ClientId], args.ClinetSeq)
		kv.mu.Unlock()
		if reply1 == nil {
			reply.Err = ErrFail
			return
		}
		reply2 := reply1.(*PutAppendReply)
		reply.Err = reply2.Err

	}
}
func (kv *ShardKV) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	if args.ClinetSeq <= kv.applyedmsgs[args.ClientId] {
		if kv.replys[args.ClientId][args.ClinetSeq] != nil {
			reply1 := kv.replys[args.ClientId][args.ClinetSeq].(*PutAppendReply)
			reply.Err = reply1.Err
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		return
	}
	kv.replys[args.ClientId] = make(map[int]interface{})
	op := Op{Opration: "Append", Key: args.Key, Value: args.Value, ClientId: args.ClientId, ClinetSeq: args.ClinetSeq}
	_, _, isLeader := kv.rf.Start(op)
	repch := make(chan interface{})
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	if _, ok := kv.startedmsgs[args.ClientId]; !ok {
		kv.startedmsgs[args.ClientId] = make(map[int]chan interface{})
	}
	kv.startedmsgs[args.ClientId][args.ClinetSeq] = repch
	//kv.startedmsgs[logindex] = map[int]map[int]chan interface{}{args.ClientId: {args.ClinetSeq: repch}}
	kv.mu.Unlock()
	select {
	case <-time.After(500 * time.Millisecond):
		kv.mu.Lock()

		delete(kv.startedmsgs[args.ClientId], args.ClinetSeq)
		kv.mu.Unlock()
		reply.Err = ErrTimeOut
		return

	case reply1 := <-repch:
		kv.mu.Lock()

		delete(kv.startedmsgs[args.ClientId], args.ClinetSeq)
		kv.mu.Unlock()
		if reply1 == nil {
			reply.Err = ErrFail

			return
		}
		reply2 := reply1.(*PutAppendReply)

		reply.Err = reply2.Err
	}
}
func (kv *ShardKV) Modifycfg(shard []int, num int, config shardctrler.Config) {
	op := Op{Opration: "Modifycfg", Shards: shard, Confignum: num, Config: config}
	kv.rf.Start(op)
	//kv.startedmsgs[logindex] = map[int]map[int]chan interface{}{args.ClientId: {args.ClinetSeq: repch}}

}
func (kv *ShardKV) syncNewShare(reply *PullShardReply, shard int) {
	kv.mu.Lock()
	if reply.Confignum == kv.nowconfig.Num {
		op := Op{Opration: "Pull", Data: reply.Data, Seq: reply.Seq, Confignum: reply.Confignum, ClinetSeq: shard}
		_, _, isLeader := kv.rf.Start(op)
		if !isLeader {
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()
}
func (kv *ShardKV) PullShard(args *PullShardArgs, reply *PullShardReply) {
	kv.mu.Lock()
	if rep, ok := kv.bepulling[args.Shard]; ok {
		if rep.Confignum == args.ConfigNum {
			reply.Err = OK
			reply.Confignum = rep.Confignum
			reply.Data = rep.Data
			reply.Seq = rep.Seq
		} else {
			reply.Err = ErrFail
		}
	} else {
		reply.Err = ErrFail
	}
	kv.mu.Unlock()
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.

}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartShardKV() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(GetArgs{})
	labgob.Register(GetReply{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(PutAppendReply{})
	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvMap = make(map[string]string)
	kv.startedmsgs = make(map[int]map[int]chan interface{})
	kv.applyedmsgs = make(map[int]int)
	kv.replys = make(map[int]map[int]interface{})
	kv.sm = shardctrler.MakeClerk(ctrlers)
	kv.shards = make(map[int][]int)
	kv.bepulling = make(map[int]*PullShardReply)
	go kv.Tickerconfig()
	go kv.TickerSnapShot()
	go kv.handleApplyCh()
	go kv.TickerPullShard()
	//go kv.PrintConfig()
	return kv
}

// func (kv *ShardKV) ticker(){
// 	for !kv.killed(){

//		}
//	}
//

func (kv *ShardKV) handleApplyCh() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.SnapshotValid {

			kv.mu.Lock()
			if msg.SnapshotIndex <= kv.applied {
				kv.mu.Unlock()
				continue
			} else {
				kv.applied = msg.SnapshotIndex
				kv.ReadSnapshot(msg.Snapshot)
				kv.mu.Unlock()
			}
		}
		if msg.CommandValid {
			op := msg.Command.(Op)
			clientid := op.ClientId
			clientseq := op.ClinetSeq

			kv.mu.Lock()
			if clientid != 0 && kv.applyedmsgs[clientid] >= clientseq {
				kv.mu.Unlock()
				continue
			}
			kv.applied = msg.CommandIndex
			kv.applyedmsgs[clientid] = clientseq
			switch op.Opration {
			case "Get":
				reply := &GetReply{}
				shard := key2shard(op.Key)
				inshard := false
				for k, v := range kv.shards {
					if k == shard && v[0] == Serving {
						inshard = true
						break
					}
				}
				if !inshard {
					reply.Err = ErrWrongGroup
				} else {
					if value, ok := kv.kvMap[op.Key]; ok {

						reply.Err = OK
						reply.Value = value
					} else {
						reply.Err = ErrNoKey
					}
				}
				if ch, ok := kv.startedmsgs[op.ClientId][op.ClinetSeq]; ok {

					kv.mu.Unlock()

					ch <- reply
					continue
				} else {
					if _, ok := kv.replys[op.ClientId]; ok {
						kv.replys[op.ClientId][op.ClinetSeq] = reply
					} else {
						kv.replys[op.ClientId] = map[int]interface{}{op.ClinetSeq: reply}
					}
				}

			case "Put":
				reply := &PutAppendReply{}
				shard := key2shard(op.Key)
				inshard := false
				for k, v := range kv.shards {
					if k == shard && v[0] == Serving {
						inshard = true
						break
					}
				}
				if !inshard {
					reply.Err = ErrWrongGroup
				} else {
					kv.kvMap[op.Key] = op.Value
					reply.Err = OK
				}

				if ch, ok := kv.startedmsgs[op.ClientId][op.ClinetSeq]; ok {

					kv.mu.Unlock()

					ch <- reply
					continue
				} else {
					if _, ok := kv.replys[op.ClientId]; ok {
						kv.replys[op.ClientId][op.ClinetSeq] = reply
					} else {
						kv.replys[op.ClientId] = map[int]interface{}{op.ClinetSeq: reply}
					}
				}

			case "Append":
				reply := &PutAppendReply{}
				shard := key2shard(op.Key)
				inshard := false

				for k, v := range kv.shards {
					if k == shard && v[0] == Serving {
						inshard = true
						break
					}
				}
				if !inshard {
					reply.Err = ErrWrongGroup
				} else {
					kv.kvMap[op.Key] += op.Value
					reply.Err = OK
				}

				if ch, ok := kv.startedmsgs[op.ClientId][op.ClinetSeq]; ok {
					kv.mu.Unlock()

					ch <- reply
					continue
				} else {
					if _, ok := kv.replys[op.ClientId]; ok {
						kv.replys[op.ClientId][op.ClinetSeq] = reply
					} else {
						kv.replys[op.ClientId] = map[int]interface{}{op.ClinetSeq: reply}
					}
				}
			case "Modifycfg":
				if kv.nowconfig.Num+1 == op.Confignum && kv.nowconfig.Num == kv.preconfig.Num {
					for _, shard := range op.Shards {
						if _, ok := kv.shards[shard]; !ok {
							if op.Confignum == 1 {
								kv.shards[shard] = []int{Serving, op.Confignum}
							} else {
								kv.shards[shard] = []int{Pulling, op.Confignum}
							}

						} else {
							if kv.shards[shard][0] == BePulling {
								kv.shards[shard] = []int{Pulling, op.Confignum}
							}
						}
					}
					shardmap := make(map[int]struct{})
					for _, shard := range op.Shards {
						shardmap[shard] = struct{}{}
					}
					for shard := range kv.shards {
						if _, ok := shardmap[shard]; !ok {
							if kv.shards[shard][0] == Serving {
								kv.shards[shard] = []int{BePulling, op.Confignum}
								pullreply := &PullShardReply{}
								pullreply.Confignum = op.Confignum
								pullreply.Data = make(map[string]string)
								for k, v := range kv.kvMap {
									if key2shard(k) == shard {
										pullreply.Data[k] = v
									}
								}
								pullreply.Seq = make(map[int]int)
								for k, v := range kv.applyedmsgs {
									pullreply.Seq[k] = v
								}
								kv.bepulling[shard] = pullreply
							}
							for kv.shards[shard][0] == Pulling {
								kv.mu.Unlock()
								time.Sleep(100 * time.Millisecond)
								kv.mu.Lock()
							}
						}
					}
					kv.nowconfig = op.Config
					if op.Confignum == 1 {
						kv.preconfig = op.Config
					}
				}

			case "Pull":
				if kv.nowconfig.Num == op.Confignum && kv.nowconfig.Num != kv.preconfig.Num {
					for k, v := range op.Data {
						kv.kvMap[k] = v
					}
					for k, v := range op.Seq {
						if kv.applyedmsgs[k] < v {
							kv.applyedmsgs[k] = v
						}
					}
					kv.shards[op.ClinetSeq] = []int{Serving, op.Confignum}

				}

			}

			kv.mu.Unlock()
		}
	}
}

type Snapshot struct {
	KvMap       map[string]string
	Applyedmsgs map[int]int
	Shard       map[int][]int
	Preconfig   shardctrler.Config
	Nowconfig   shardctrler.Config
	Bepulling   map[int]*PullShardReply
}

func (kv *ShardKV) GetSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	snp := Snapshot{KvMap: kv.kvMap, Applyedmsgs: kv.applyedmsgs, Shard: kv.shards, Preconfig: kv.preconfig, Nowconfig: kv.nowconfig, Bepulling: kv.bepulling}
	if err := e.Encode(snp); err != nil {
		log.Fatalf("encode snapshot error: %v", err)
	}
	return w.Bytes()
}

func (kv *ShardKV) ReadSnapshot(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	snp := Snapshot{}
	if d.Decode(&snp) != nil {
		log.Fatal("read snapshot error")
	}
	kv.kvMap = snp.KvMap
	kv.applyedmsgs = snp.Applyedmsgs
	kv.shards = snp.Shard
	kv.preconfig = snp.Preconfig
	kv.nowconfig = snp.Nowconfig
	kv.bepulling = snp.Bepulling

	// if d.Decode(&kv.kvMap) != nil || d.Decode(&kv.applyedmsgs) != nil || d.Decode(&kv.replys) != nil || d.Decode(&kv.startedmsgs) != nil {
	// 	log.Fatal("read snapshot error")
	// }
	//fmt.Print("read snapshot")
}

func (kv *ShardKV) TickerSnapShot() {
	for !kv.killed() {
		if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
			kv.mu.Lock()
			kv.rf.Snapshot(kv.applied, kv.GetSnapshot())
			kv.mu.Unlock()
		}

	}
}

func (kv *ShardKV) Tickerconfig() {
	for !kv.killed() {
		kv.mu.Lock()
		_, isLeader := kv.rf.GetState()
		if !isLeader {

			kv.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}
		kv.mu.Unlock()
		config := kv.sm.Query(kv.nowconfig.Num + 1)
		kv.mu.Lock()
		if config.Num == kv.nowconfig.Num {
			kv.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if config.Num == kv.nowconfig.Num+1 && kv.nowconfig.Num == kv.preconfig.Num {
			shards := make([]int, 0)
			for i, v := range config.Shards {
				if v == kv.gid {
					shards = append(shards, i)
				}
			}
			fmt.Println("cftmogid:", kv.gid, "me:", kv.me, "config:", kv.nowconfig, "shards:", shards)
			kv.Modifycfg(shards, config.Num, config)
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) TickerPullShard() {
	for !kv.killed() {
		//检测如果没有pull状态 让pre和now相等
		kv.mu.Lock()
		hasPulling := false
		for _, v := range kv.shards {
			if v[0] == Pulling {
				hasPulling = true
				break
			}
		}
		if !hasPulling {
			kv.preconfig = kv.nowconfig
		}
		kv.mu.Unlock()
		_, isLeader := kv.rf.GetState()

		if !isLeader {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		wg := sync.WaitGroup{}
		kv.mu.Lock()
		shards := make(map[int][]int)
		for k, v := range kv.shards {
			shards[k] = v
		}
		kv.mu.Unlock()
		for k, v := range shards {

			if v[0] == Pulling {
				wg.Add(1)
				go kv.PullRpc(k, v[1], &wg)

			}

		}

		wg.Wait()
		time.Sleep(100 * time.Millisecond)
	}
}
func (kv *ShardKV) PullRpc(shard int, configNum int, wg *sync.WaitGroup) {
	args := &PullShardArgs{Shard: shard, ConfigNum: configNum}
	reply := &PullShardReply{}
	kv.mu.Lock()
	servers := kv.preconfig.Groups[kv.preconfig.Shards[shard]]
	kv.mu.Unlock()
	servers_end := make([]*labrpc.ClientEnd, len(servers))
	for i, v := range servers {
		servers_end[i] = kv.make_end(v)
	}
	for i := 0; i < len(servers_end); i++ {
		ok := servers_end[i].Call("ShardKV.PullShard", args, reply)
		if ok && reply.Err == OK {
			kv.syncNewShare(reply, shard)
			break
		} else if ok && reply.Err == ErrWrongLeader {
			continue
		} else if ok && reply.Err == ErrFail {
			continue
		} else if ok && reply.Err == ErrTimeOut {
			continue
		}
	}
	wg.Done()

}

func (kv *ShardKV) PrintConfig() {
	for {
		kv.mu.Lock()

		fmt.Print("gid:", kv.gid, "me:", kv.me, "config:", kv.nowconfig)

		kv.mu.Unlock()
		time.Sleep(1000 * time.Millisecond)
	}

}
