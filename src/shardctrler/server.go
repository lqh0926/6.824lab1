package shardctrler

import (
	"container/heap"
	"fmt"
	"sort"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	startedmsgs         map[int]map[int]chan interface{} //clientid -> clientseq -> repch
	applyedmsgs         map[int]int
	replys              map[int]map[int]interface{}
	configs             []Config        // indexed by config num
	bigrootgid2shards   bigrootheadpq   //gid to shards
	smallrootgid2shards smallrootheadpq //gid to shards
}
type gid2shards []shards
type shards struct {
	gid    int
	shards []int
}

func (g gid2shards) Len() int {
	return len(g)
}
func (g gid2shards) Swap(i, j int) {
	g[i], g[j] = g[j], g[i]
}
func (g *gid2shards) Push(x interface{}) {
	*g = append(*g, x.(shards))
}
func (g *gid2shards) Pop() interface{} {
	old := *g
	n := len(old)
	x := old[n-1]
	*g = old[0 : n-1]
	return x
}

type bigrootheadpq struct {
	*gid2shards
}

func (g bigrootheadpq) Less(i, j int) bool {
	if len((*g.gid2shards)[i].shards) > len((*g.gid2shards)[j].shards) {
		return true
	} else if len((*g.gid2shards)[i].shards) == len((*g.gid2shards)[j].shards) {
		return (*g.gid2shards)[i].gid < (*g.gid2shards)[j].gid
	} else {
		return false
	}
}

type smallrootheadpq struct {
	*gid2shards
}

func (g smallrootheadpq) Less(i, j int) bool {
	if len((*g.gid2shards)[i].shards) < len((*g.gid2shards)[j].shards) {
		return true
	} else if len((*g.gid2shards)[i].shards) == len((*g.gid2shards)[j].shards) {
		return (*g.gid2shards)[i].gid < (*g.gid2shards)[j].gid
	} else {
		return false
	}

}

type Op struct {
	// Your data here.
	OprType   string
	Args      interface{} // JoinArgs, LeaveArgs, MoveArgs, QueryArgs
	ClientId  int
	ClinetSeq int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.

	sc.mu.Lock()
	if args.ClientSeq <= sc.applyedmsgs[args.ClientId] {
		if sc.replys[args.ClientId][args.ClientSeq] != nil {

			reply1 := sc.replys[args.ClientId][args.ClientSeq].(*JoinReply)
			//fmt.Println("b"+reply1.Value+"b", reply1.Err)
			reply.Err = reply1.Err
			sc.mu.Unlock()
			//fmt.Print("a")
			return
		}
		//fmt.Print("b")
		sc.mu.Unlock()
		return
	}
	sc.replys[args.ClientId] = make(map[int]interface{})
	op := Op{OprType: "Join", Args: args, ClientId: args.ClientId, ClinetSeq: args.ClientSeq}
	_, _, isLeader := sc.rf.Start(op)
	repch := make(chan interface{})

	if !isLeader {
		//fmt.Println("not leader")
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	//fmt.Print("c")
	if _, ok := sc.startedmsgs[args.ClientId]; !ok {
		sc.startedmsgs[args.ClientId] = make(map[int]chan interface{})
	}
	sc.startedmsgs[args.ClientId][args.ClientSeq] = repch
	//kv.startedmsgs[logindex] = map[int]map[int]chan interface{}{args.ClientId: {args.ClinetSeq: repch}}
	sc.mu.Unlock()
	select {
	case <-time.After(200 * time.Millisecond):

		reply.Err = ErrTimeOut
		sc.mu.Lock()

		delete(sc.startedmsgs[args.ClientId], args.ClientSeq)
		sc.mu.Unlock()
		return

	case reply1 := <-repch:
		sc.mu.Lock()

		delete(sc.startedmsgs[args.ClientId], args.ClientSeq)
		sc.mu.Unlock()
		if reply1 == nil {
			reply.Err = ErrFail
			return
		}
		reply2 := reply1.(*JoinReply)
		reply.Err = reply2.Err

	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	if args.ClientSeq <= sc.applyedmsgs[args.ClientId] {
		if sc.replys[args.ClientId][args.ClientSeq] != nil {

			reply1 := sc.replys[args.ClientId][args.ClientSeq].(*LeaveReply)
			//fmt.Println("b"+reply1.Value+"b", reply1.Err)
			reply.Err = reply1.Err
			sc.mu.Unlock()
			//fmt.Print("a")
			return
		}
		//fmt.Print("b")
		sc.mu.Unlock()
		return
	}
	sc.replys[args.ClientId] = make(map[int]interface{})
	op := Op{OprType: "Leave", Args: args, ClientId: args.ClientId, ClinetSeq: args.ClientSeq}
	_, _, isLeader := sc.rf.Start(op)
	repch := make(chan interface{})

	if !isLeader {
		//fmt.Println("not leader")
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	//fmt.Print("c")
	if _, ok := sc.startedmsgs[args.ClientId]; !ok {
		sc.startedmsgs[args.ClientId] = make(map[int]chan interface{})
	}
	sc.startedmsgs[args.ClientId][args.ClientSeq] = repch
	//kv.startedmsgs[logindex] = map[int]map[int]chan interface{}{args.ClientId: {args.ClinetSeq: repch}}
	sc.mu.Unlock()
	select {
	case <-time.After(200 * time.Millisecond):

		reply.Err = ErrTimeOut
		sc.mu.Lock()

		delete(sc.startedmsgs[args.ClientId], args.ClientSeq)
		sc.mu.Unlock()
		return

	case reply1 := <-repch:
		sc.mu.Lock()

		delete(sc.startedmsgs[args.ClientId], args.ClientSeq)
		sc.mu.Unlock()
		if reply1 == nil {
			reply.Err = ErrFail
			return
		}
		reply2 := reply1.(*LeaveReply)
		reply.Err = reply2.Err

	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	if args.ClientSeq <= sc.applyedmsgs[args.ClientId] {
		if sc.replys[args.ClientId][args.ClientSeq] != nil {

			reply1 := sc.replys[args.ClientId][args.ClientSeq].(*MoveReply)
			//fmt.Println("b"+reply1.Value+"b", reply1.Err)
			reply.Err = reply1.Err
			sc.mu.Unlock()
			//fmt.Print("a")
			return
		}
		//fmt.Print("b")
		sc.mu.Unlock()
		return
	}
	sc.replys[args.ClientId] = make(map[int]interface{})
	op := Op{OprType: "Move", Args: args, ClientId: args.ClientId, ClinetSeq: args.ClientSeq}
	_, _, isLeader := sc.rf.Start(op)
	repch := make(chan interface{})

	if !isLeader {
		//fmt.Println("not leader")
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	//fmt.Print("c")
	if _, ok := sc.startedmsgs[args.ClientId]; !ok {
		sc.startedmsgs[args.ClientId] = make(map[int]chan interface{})
	}
	sc.startedmsgs[args.ClientId][args.ClientSeq] = repch
	//kv.startedmsgs[logindex] = map[int]map[int]chan interface{}{args.ClientId: {args.ClinetSeq: repch}}
	sc.mu.Unlock()
	select {
	case <-time.After(200 * time.Millisecond):

		reply.Err = ErrTimeOut
		sc.mu.Lock()

		delete(sc.startedmsgs[args.ClientId], args.ClientSeq)
		sc.mu.Unlock()
		return

	case reply1 := <-repch:
		sc.mu.Lock()

		delete(sc.startedmsgs[args.ClientId], args.ClientSeq)
		sc.mu.Unlock()
		if reply1 == nil {
			reply.Err = ErrFail
			return
		}
		reply2 := reply1.(*MoveReply)
		reply.Err = reply2.Err

	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	if args.ClientSeq <= sc.applyedmsgs[args.ClientId] {
		if sc.replys[args.ClientId][args.ClientSeq] != nil {

			reply1 := sc.replys[args.ClientId][args.ClientSeq].(*QueryReply)
			//fmt.Println("b"+reply1.Value+"b", reply1.Err)
			reply.Err = reply1.Err
			sc.mu.Unlock()
			//fmt.Print("a")
			return
		}
		//fmt.Print("b")
		sc.mu.Unlock()
		return
	}
	sc.replys[args.ClientId] = make(map[int]interface{})
	op := Op{OprType: "Query", Args: args, ClientId: args.ClientId, ClinetSeq: args.ClientSeq}
	_, _, isLeader := sc.rf.Start(op)
	repch := make(chan interface{})

	if !isLeader {
		//fmt.Println("not leader")
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	//fmt.Print("c")
	if _, ok := sc.startedmsgs[args.ClientId]; !ok {
		sc.startedmsgs[args.ClientId] = make(map[int]chan interface{})
	}
	sc.startedmsgs[args.ClientId][args.ClientSeq] = repch
	//kv.startedmsgs[logindex] = map[int]map[int]chan interface{}{args.ClientId: {args.ClinetSeq: repch}}
	sc.mu.Unlock()
	select {
	case <-time.After(200 * time.Millisecond):

		reply.Err = ErrTimeOut
		sc.mu.Lock()

		delete(sc.startedmsgs[args.ClientId], args.ClientSeq)
		sc.mu.Unlock()
		return

	case reply1 := <-repch:
		sc.mu.Lock()

		delete(sc.startedmsgs[args.ClientId], args.ClientSeq)
		sc.mu.Unlock()
		if reply1 == nil {
			reply.Err = ErrFail
			return
		}
		reply2 := reply1.(*QueryReply)
		reply.Config = reply2.Config
		reply.Err = reply2.Err

	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(&JoinArgs{})
	labgob.Register(JoinReply{})
	labgob.Register(&LeaveArgs{})
	labgob.Register(LeaveReply{})
	labgob.Register(&MoveArgs{})
	labgob.Register(MoveReply{})
	labgob.Register(&QueryArgs{})
	labgob.Register(QueryReply{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.startedmsgs = make(map[int]map[int]chan interface{})
	sc.applyedmsgs = make(map[int]int)
	sc.replys = make(map[int]map[int]interface{})
	var gid2shards1 gid2shards = make([]shards, 0)
	sc.bigrootgid2shards = bigrootheadpq{&gid2shards1}
	gid2shards2 := make([]shards, 0)
	sc.smallrootgid2shards = smallrootheadpq{(*gid2shards)(&gid2shards2)}
	go sc.ApplyMsg()
	//go sc.printconfigticker()
	return sc
}
func (sc *ShardCtrler) ApplyMsg() {
	for {
		msg := <-sc.applyCh
		if msg.CommandValid {
			op := msg.Command.(Op)
			sc.mu.Lock()

			switch op.OprType {
			case "Join":
				args := op.Args.(*JoinArgs)
				if sc.applyedmsgs[args.ClientId] >= args.ClientSeq {
					sc.mu.Unlock()
					continue
				}
				sc.applyedmsgs[op.ClientId] = op.ClinetSeq

				reply := sc.JoinApply(args)

				if ch, ok := sc.startedmsgs[op.ClientId][op.ClinetSeq]; ok {
					sc.mu.Unlock()
					ch <- reply
					continue
				} else {
					if _, ok := sc.replys[op.ClientId]; !ok {
						sc.replys[op.ClientId] = map[int]interface{}{op.ClinetSeq: reply}
					} else {
						sc.replys[op.ClientId][op.ClinetSeq] = reply
					}
				}

			case "Leave":
				args := op.Args.(*LeaveArgs)

				if sc.applyedmsgs[args.ClientId] >= args.ClientSeq {
					sc.mu.Unlock()
					continue
				}

				sc.applyedmsgs[op.ClientId] = op.ClinetSeq
				reply := sc.LeaveApply(args)
				if ch, ok := sc.startedmsgs[op.ClientId][op.ClinetSeq]; ok {
					sc.mu.Unlock()
					ch <- reply
					continue
				} else {
					if _, ok := sc.replys[op.ClientId]; !ok {
						sc.replys[op.ClientId] = map[int]interface{}{op.ClinetSeq: reply}
					} else {
						sc.replys[op.ClientId][op.ClinetSeq] = reply
					}
				}
			case "Move":
				args := op.Args.(*MoveArgs)
				if sc.applyedmsgs[args.ClientId] >= args.ClientSeq {
					sc.mu.Unlock()
					continue
				}
				sc.applyedmsgs[op.ClientId] = op.ClinetSeq
				reply := sc.MoveApply(args)
				if ch, ok := sc.startedmsgs[op.ClientId][op.ClinetSeq]; ok {
					sc.mu.Unlock()
					ch <- reply
					continue
				} else {
					if _, ok := sc.replys[op.ClientId]; !ok {
						sc.replys[op.ClientId] = map[int]interface{}{op.ClinetSeq: reply}
					} else {
						sc.replys[op.ClientId][op.ClinetSeq] = reply
					}
				}
			case "Query":
				args := op.Args.(*QueryArgs)
				if sc.applyedmsgs[args.ClientId] >= args.ClientSeq {
					sc.mu.Unlock()
					continue
				}
				sc.applyedmsgs[op.ClientId] = op.ClinetSeq
				reply := sc.QueryApply(args)
				if ch, ok := sc.startedmsgs[op.ClientId][op.ClinetSeq]; ok {
					sc.mu.Unlock()
					ch <- reply
					continue
				} else {
					if _, ok := sc.replys[op.ClientId]; !ok {
						sc.replys[op.ClientId] = map[int]interface{}{op.ClinetSeq: reply}
					} else {
						sc.replys[op.ClientId][op.ClinetSeq] = reply
					}
				}

			}
			sc.mu.Unlock()
		}
	}
}
func (sc *ShardCtrler) JoinApply(args *JoinArgs) *JoinReply {
	reply := &JoinReply{Err: OK}
	//转为map为有序slice
	groups := make([]struct {
		gid     int
		servers []string
	}, 0, len(args.Servers))

	for gid, servers := range args.Servers {
		groups = append(groups, struct {
			gid     int
			servers []string
		}{gid, servers})
	}

	sort.Slice(groups, func(i, j int) bool {
		return groups[i].gid < groups[j].gid
	})

	for _, group := range groups {
		sc.join_one_group(group.gid, group.servers)
	}

	return reply

}
func (sc *ShardCtrler) join_one_group(gid int, servers []string) {
	if len(sc.configs[len(sc.configs)-1].Groups) == 0 {
		newconfig := Config{}
		newconfig.Num = len(sc.configs)
		newconfig.Groups = make(map[int][]string)
		newconfig.Groups[gid] = servers
		newconfig.Shards = [NShards]int{}
		for i := 0; i < NShards; i++ {
			newconfig.Shards[i] = gid
		}
		sc.configs = append(sc.configs, newconfig)
		temp_shards := make([]int, NShards)
		for i := 0; i < NShards; i++ {
			temp_shards[i] = i
		}
		sc.bigrootgid2shards.Push(shards{gid: gid, shards: temp_shards})
		sc.smallrootgid2shards.Push(shards{gid: gid, shards: temp_shards})
	} else {
		oldconfig := sc.configs[len(sc.configs)-1]
		newconfig := Config{}
		newconfig.Num = len(sc.configs)
		newconfig.Groups = make(map[int][]string)
		for k, v := range oldconfig.Groups {
			newconfig.Groups[k] = v
		}
		newconfig.Groups[gid] = servers
		newg2s := shards{}
		newg2s.gid = gid
		topg2s := heap.Pop(&sc.bigrootgid2shards).(shards)
		for len(topg2s.shards) > len(newg2s.shards) {
			newg2s.shards = append(newg2s.shards, topg2s.shards[0])
			topg2s.shards = topg2s.shards[1:]
			heap.Push(&sc.bigrootgid2shards, topg2s)
			topg2s = heap.Pop(&sc.bigrootgid2shards).(shards)

		}
		heap.Push(&sc.bigrootgid2shards, topg2s)
		heap.Push(&sc.bigrootgid2shards, newg2s)
		for i := 0; i < sc.bigrootgid2shards.Len(); i++ {
			for j := 0; j < len((*sc.bigrootgid2shards.gid2shards)[i].shards); j++ {
				newconfig.Shards[(*sc.bigrootgid2shards.gid2shards)[i].shards[j]] = (*sc.bigrootgid2shards.gid2shards)[i].gid
			}
		}
		sc.configs = append(sc.configs, newconfig)
		sc.smallrootgid2shards = smallrootheadpq{}
		temp := make([]shards, sc.bigrootgid2shards.Len())
		sc.smallrootgid2shards.gid2shards = (*gid2shards)(&temp)
		copy(*(sc.smallrootgid2shards.gid2shards), *(sc.bigrootgid2shards.gid2shards))
		heap.Init(&sc.smallrootgid2shards)
	}
}
func (sc *ShardCtrler) LeaveApply(args *LeaveArgs) *LeaveReply {
	reply := &LeaveReply{Err: OK}
	for _, v := range args.GIDs {
		sc.leave_one_group(v)
	}
	return reply
}
func (sc *ShardCtrler) leave_one_group(gid int) {
	if len(sc.configs[len(sc.configs)-1].Groups) == 1 {
		newconfig := Config{}
		newconfig.Num = len(sc.configs)
		newconfig.Groups = make(map[int][]string)
		newconfig.Shards = [NShards]int{}
		sc.configs = append(sc.configs, newconfig)
		heap.Pop(&sc.bigrootgid2shards)
		heap.Pop(&sc.smallrootgid2shards)
		return
	}
	oldconfig := sc.configs[len(sc.configs)-1]
	newconfig := Config{}
	newconfig.Num = len(sc.configs)
	newconfig.Groups = make(map[int][]string)
	for k, v := range oldconfig.Groups {
		newconfig.Groups[k] = v
	}
	delete(newconfig.Groups, gid)
	newg2s := shards{}
	for i := 0; i < sc.smallrootgid2shards.Len(); i++ {
		if (*sc.smallrootgid2shards.gid2shards)[i].gid == gid {
			newg2s = (*sc.smallrootgid2shards.gid2shards)[i]
			*sc.smallrootgid2shards.gid2shards = append((*sc.smallrootgid2shards.gid2shards)[:i], (*sc.smallrootgid2shards.gid2shards)[i+1:]...)
			heap.Init(&sc.smallrootgid2shards)
			break
		}
	}
	topg2s := heap.Pop(&sc.smallrootgid2shards).(shards)
	for len(newg2s.shards) > 0 {
		topg2s.shards = append(topg2s.shards, newg2s.shards[0])
		heap.Push(&sc.smallrootgid2shards, topg2s)
		newg2s.shards = newg2s.shards[1:]
		topg2s = heap.Pop(&sc.smallrootgid2shards).(shards)
	}
	heap.Push(&sc.smallrootgid2shards, topg2s)
	for i := 0; i < sc.smallrootgid2shards.Len(); i++ {
		for j := 0; j < len((*sc.smallrootgid2shards.gid2shards)[i].shards); j++ {
			newconfig.Shards[(*sc.smallrootgid2shards.gid2shards)[i].shards[j]] = (*sc.smallrootgid2shards.gid2shards)[i].gid
		}
	}
	sc.configs = append(sc.configs, newconfig)
	sc.bigrootgid2shards = bigrootheadpq{}
	temp := make([]shards, sc.smallrootgid2shards.Len())
	sc.bigrootgid2shards.gid2shards = (*gid2shards)(&temp)
	copy(*sc.bigrootgid2shards.gid2shards, *sc.smallrootgid2shards.gid2shards)
	heap.Init(&sc.bigrootgid2shards)

}
func (sc *ShardCtrler) MoveApply(args *MoveArgs) *MoveReply {
	reply := &MoveReply{Err: OK}
	oldconfig := sc.configs[len(sc.configs)-1]
	newconfig := Config{}
	newconfig.Num = len(sc.configs)
	newconfig.Groups = make(map[int][]string)
	for k, v := range oldconfig.Groups {
		newconfig.Groups[k] = v
	}
	newconfig.Shards = oldconfig.Shards
	newconfig.Shards[args.Shard] = args.GID
	//修改一下堆
	for i := 0; i < sc.bigrootgid2shards.Len(); i++ {
		if (*sc.bigrootgid2shards.gid2shards)[i].gid == oldconfig.Shards[args.Shard] {
			for j := 0; j < len((*sc.bigrootgid2shards.gid2shards)[i].shards); j++ {
				if (*sc.bigrootgid2shards.gid2shards)[i].shards[j] == args.Shard {
					(*sc.bigrootgid2shards.gid2shards)[i].shards = append((*sc.bigrootgid2shards.gid2shards)[i].shards[:j], (*sc.bigrootgid2shards.gid2shards)[i].shards[j+1:]...)
					break
				}
			}
		}
		if (*sc.bigrootgid2shards.gid2shards)[i].gid == args.GID {
			(*sc.bigrootgid2shards.gid2shards)[i].shards = append((*sc.bigrootgid2shards.gid2shards)[i].shards, args.Shard)
		}

	}

	heap.Init(&sc.bigrootgid2shards)
	sc.smallrootgid2shards = smallrootheadpq{}
	temp := make([]shards, sc.bigrootgid2shards.Len())
	sc.smallrootgid2shards.gid2shards = (*gid2shards)(&temp)
	copy(*sc.smallrootgid2shards.gid2shards, *sc.bigrootgid2shards.gid2shards)
	heap.Init(&sc.smallrootgid2shards)

	sc.configs = append(sc.configs, newconfig)
	return reply
}
func (sc *ShardCtrler) QueryApply(args *QueryArgs) *QueryReply {
	reply := &QueryReply{Err: OK}
	if args.Num >= len(sc.configs) || args.Num < 0 {
		reply.Config = sc.configs[len(sc.configs)-1]
	} else {
		reply.Config = sc.configs[args.Num]
	}
	return reply
}

func (sc *ShardCtrler) printconfigticker() {
	for {
		time.Sleep(200 * time.Millisecond)
		sc.mu.Lock()
		for i := 0; i < len(sc.configs); i++ {
			fmt.Println(sc.me, " config", i, sc.configs[i])
		}
		sc.mu.Unlock()
	}
}
