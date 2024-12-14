package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut     = "ErrTimeOut"
	ErrFail        = "ErrFail"
	Errrepeated    = "Errrepeated"
	Serving        = 0 // 正常服务中
	Pulling        = 1 // 正在从其他组拉取数据
	BePulling      = 2 // 正在被其他组拉取数据
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int
	ClinetSeq int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId  int
	ClinetSeq int
}

type GetReply struct {
	Err   Err
	Value string
}
type PullShardArgs struct {
	Shard     int
	ConfigNum int
}
type PullShardReply struct {
	Err       Err
	Confignum int
	Data      map[string]string // 数据
	Seq       map[int]int       // 客户端的seq

}
