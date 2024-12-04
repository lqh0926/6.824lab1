package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId int
	seq      int
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
	// You'll have to add code here.
	ck.clientId = int(nrand())
	ck.seq = 1
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	args := GetArgs{Key: key, ClientId: ck.clientId, ClinetSeq: ck.seq}
	ck.seq++
	for {
		//fmt.Println("Get", key, "from", ck.clientId, "seq", ck.seq)
		for i := 0; i < len(ck.servers); i++ {
			//fmt.Println("Get", key, "from", i)
			reply := GetReply{}
			//处理Rpc超时
			var ok bool
			done := make(chan bool)
			go func() {

				ok = ck.servers[i].Call("KVServer.Get", &args, &reply)
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
				//fmt.Println("id:", ck.clientId, "seq:", ck.seq, "value:", reply.Value)
				return reply.Value
			} else if ok && reply.Err == ErrWrongLeader {
				//fmt.Print("id:", ck.clientId, "seq:", ck.seq, "value:", reply.Value)
				//fmt.Println("get wrong leader")
				continue
			} else if ok && reply.Err == ErrNoKey {
				return ""
			} else if ok && reply.Err == ErrFail {
				ck.seq++
				args.ClinetSeq = ck.seq
				continue
			} else if ok && reply.Err == ErrTimeOut {
				//fmt.Print("id:", ck.clientId, "seq:", ck.seq, "value:", reply.Value)
				//fmt.Println("time out")
				continue
			}

		}

	}

}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{Key: key, Value: value, ClientId: ck.clientId, ClinetSeq: ck.seq}
	ck.seq++
	//fmt.Println("PutAppend", key, value, "from", ck.clientId)
	for {
		//fmt.Println("PutAppend", key, value, "from", ck.clientId)
		for i := 0; i < len(ck.servers); i++ {
			reply := PutAppendReply{}

			ok := false
			done := make(chan bool)
			go func() {
				if op == "Put" {
					ok = ck.servers[i].Call("KVServer.Put", &args, &reply)
				}
				if op == "Append" {
					ok = ck.servers[i].Call("KVServer.Append", &args, &reply)
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
				// fmt.Println("id:", ck.clientId, "seq:", ck.seq)
				return
			} else if ok && reply.Err == ErrWrongLeader {
				//fmt.Println("wrong leader")
				continue
			} else if ok && reply.Err == ErrFail {

				args.ClinetSeq = ck.seq
				ck.seq++
				continue
			} else if ok && reply.Err == ErrTimeOut {
				//fmt.Println("time out")
				continue
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
