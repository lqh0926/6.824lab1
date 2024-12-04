package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Opration  string
	Key       string
	Value     string
	ClientId  int
	ClinetSeq int
}

func (op *Op) String() string {
	return op.Opration + " " + op.Key + " " + op.Value
}

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	applied      int
	maxraftstate int // snapshot if log grows this big
	// Your definitions here.
	kvMap       map[string]string
	startedmsgs map[int]map[int]chan interface{} //clientid -> clientseq -> repch
	applyedmsgs map[int]int
	replys      map[int]map[int]interface{}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
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

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
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
func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
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

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.

}

func (kv *KVServer) killed() bool {
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
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvMap = make(map[string]string)
	kv.startedmsgs = make(map[int]map[int]chan interface{})
	kv.applyedmsgs = make(map[int]int)
	kv.replys = make(map[int]map[int]interface{})
	go kv.TickerSnapShot()
	go kv.handleApplyCh()
	return kv
}

// func (kv *KVServer) ticker(){
// 	for !kv.killed(){

//		}
//	}
//

func (kv *KVServer) handleApplyCh() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.SnapshotValid {

			kv.mu.Lock()
			if msg.CommandIndex <= kv.applied {
				kv.mu.Unlock()
				continue
			} else {
				kv.applied = msg.CommandIndex
				kv.ReadSnapshot(msg.Snapshot)
				kv.mu.Unlock()
			}
		}
		if msg.CommandValid {
			op := msg.Command.(Op)
			clientid := op.ClientId
			clientseq := op.ClinetSeq

			kv.mu.Lock()
			if kv.applyedmsgs[clientid] >= clientseq {
				kv.mu.Unlock()
				continue
			}
			kv.applied = msg.CommandIndex
			kv.applyedmsgs[clientid] = clientseq
			switch op.Opration {
			case "Get":
				reply := &GetReply{}
				if value, ok := kv.kvMap[op.Key]; ok {

					reply.Err = OK
					reply.Value = value
				} else {
					reply.Err = ErrNoKey
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
				kv.kvMap[op.Key] = op.Value
				reply := &PutAppendReply{}
				reply.Err = OK
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
				kv.kvMap[op.Key] += op.Value
				reply := &PutAppendReply{}
				reply.Err = OK
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

			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) GetSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvMap)
	e.Encode(kv.applyedmsgs)
	e.Encode(kv.replys)
	e.Encode(kv.startedmsgs)
	return w.Bytes()
}

func (kv *KVServer) ReadSnapshot(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.kvMap) != nil || d.Decode(&kv.applyedmsgs) != nil || d.Decode(&kv.replys) != nil || d.Decode(&kv.startedmsgs) != nil {
		log.Fatal("read snapshot error")
	}
}

func (kv *KVServer) TickerSnapShot() {
	for !kv.killed() {
		if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
			kv.mu.Lock()
			kv.rf.Snapshot(kv.applied, kv.GetSnapshot())
			kv.mu.Unlock()
		}
		time.Sleep(100 * time.Millisecond)
	}
}
