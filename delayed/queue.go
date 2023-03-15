package delayed

import (
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/keakon/golog/log"
)

const (
	enqueuedKeySuffix = "_enqueued"
	dequeuedKeySuffix = "_dequeued"
	notiKeySuffix     = "_noti"
	idKeySuffix       = "_id"
)

const (
	// KEYS: queue_name, enqueued_key, dequeued_key
	// ARGV: current_timestamp
	dequeueScript = `local task = redis.call('lpop', KEYS[1])
if task == nil then
    return nil
end
local timeout = redis.call('zscore', KEYS[2], task)
redis.call('zadd', KEYS[3], tonumber(ARGV[1]) + timeout, task)
return task`

	// KEYS: queue_name, noti_key, enqueued_key, dequeued_key
	// ARGV: task_data, timeout, prior
	requeueScript = `local deleted = redis.call('zrem', KEYS[4], ARGV[1])
if deleted == 0 then
    return false
end
redis.call('zadd', KEYS[3], ARGV[2], ARGV[1])
if ARGV[3] == '0' then
    redis.call('rpush', KEYS[1], ARGV[1])
else
    redis.call('lpush', KEYS[1], ARGV[1])
end
redis.call('rpush', KEYS[2], '1')
return true`

	// KEYS: queue_name, noti_key, dequeued_key
	// ARGV: current_timestamp, busy_len
	requeueLostScript = `local queue_len = redis.call('llen', KEYS[1])
local noti_len = redis.call('llen', KEYS[2])
local count = queue_len - noti_len
if count > 0 then
    local noti_array = {}
    for i=1,count,1 do
        table.insert(noti_array, '1')
    end
    redis.call('lpush', KEYS[2], unpack(noti_array))
else
    count = 0
end
if queue_len >= tonumber(ARGV[2]) then
    return count
end
local dequeued_tasks = redis.call('zrangebyscore', KEYS[3], 0, ARGV[1])
if #dequeued_tasks == 0 then
    return count
end
if queue_len > 0 then
    local queue = redis.call('lrange', KEYS[1], 0, -1)
    local dequeued_task_dict = {}
    for k, v in pairs(dequeued_tasks) do
        dequeued_task_dict[v] = 1
    end
    for k, v in pairs(queue) do
        dequeued_task_dict[v] = nil
    end
    dequeued_tasks = {}
    for k, v in pairs(dequeued_task_dict) do
        table.insert(dequeued_tasks, k)
    end
    if #dequeued_tasks == 0 then
        return count
    end
end
redis.call('lpush', KEYS[1], unpack(dequeued_tasks))
redis.call('zrem', KEYS[3], unpack(dequeued_tasks))
local noti_array = {}
for i=1,#dequeued_tasks do
    table.insert(noti_array, '1')
end
redis.call('lpush', KEYS[2], unpack(noti_array))
return count + #dequeued_tasks`
)

type Queue struct {
	name           string
	enqueuedKey    string
	dequeuedKey    string
	notiKey        string
	idKey          string
	defaultTimeout uint32 // ms
	dequeueTimeout uint32 // ms, must be larger than 1, redis BLPOP treats timeout equal or less than 0.001 second as 0 (forever)
	requeueTimeout uint32 // ms
	busyLen        uint32

	redis             *redis.Pool
	dequeueScript     *redis.Script
	requeueScript     *redis.Script
	requeueLostScript *redis.Script

	handlers map[string]*Handler
}

func NewQueue(name, redisAddr, redisPassword string, defaultTimeout, dequeueTimeout, requeueTimeout uint32, busyLen uint32) *Queue {
	return &Queue{
		name:              name,
		enqueuedKey:       name + enqueuedKeySuffix,
		dequeuedKey:       name + dequeuedKeySuffix,
		notiKey:           name + notiKeySuffix,
		idKey:             name + idKeySuffix,
		defaultTimeout:    defaultTimeout,
		dequeueTimeout:    dequeueTimeout,
		requeueTimeout:    requeueTimeout,
		busyLen:           busyLen,
		redis:             NewRedisPool(redisAddr, redisPassword),
		dequeueScript:     redis.NewScript(3, dequeueScript),
		requeueScript:     redis.NewScript(4, requeueScript),
		requeueLostScript: redis.NewScript(3, requeueLostScript),
	}
}

func (q *Queue) Clear() error {
	conn := q.redis.Get()
	defer conn.Close()
	_, err := conn.Do("DEL", q.name, q.enqueuedKey, q.dequeuedKey, q.notiKey, q.idKey)
	return err
}

func (q *Queue) Len() (count int, err error) {
	conn := q.redis.Get()
	defer conn.Close()

	return redis.Int(conn.Do("LLEN", q.name))
}

func (q *Queue) Enqueue(task *GoTask) (err error) {
	conn := q.redis.Get()
	defer conn.Close()

	if task.raw.ID == 0 {
		id, err := redis.Int64(conn.Do("INCR", q.idKey))
		if err != nil {
			log.Errorf("enqueue task failed: %v", err)
			return err
		}
		task.raw.ID = uint64(id)
	}

	if len(task.data) == 0 {
		err = task.Serialize()
		if err != nil {
			log.Errorf("serialize task failed: %v", err)
			return
		}
	}

	timeout := task.raw.Timeout
	if timeout == 0 {
		timeout = q.defaultTimeout
	}
	timeout += q.requeueTimeout

	if task.raw.Prior {
		err = conn.Send("LPUSH", q.name, task.data)
	} else {
		err = conn.Send("RPUSH", q.name, task.data)
	}
	if err != nil {
		return
	}

	err = conn.Send("RPUSH", q.notiKey, 1)
	if err != nil {
		return
	}

	_, err = conn.Do("ZADD", q.enqueuedKey, timeout, task.data) // use Do() to combine Send(), Flush() and Receive()
	return
}

func (q *Queue) Dequeue() (task *GoTask, err error) {
	conn := q.redis.Get()
	defer conn.Close()

	reply, err := redis.Values(conn.Do("BLPOP", q.notiKey, float32(q.dequeueTimeout)*0.001)) // convert milliseconds to seconds
	if err != nil {
		if err == redis.ErrNil {
			err = nil
		}
		return
	}

	if len(reply) != 2 {
		return nil, InvalidRedisReplyError
	}

	popped, ok := reply[1].([]uint8)
	if !ok || len(popped) != 1 {
		return nil, InvalidRedisReplyError
	}

	if popped[0] == '1' {
		data, err := redis.Bytes(q.dequeueScript.Do(conn, q.name, q.enqueuedKey, q.dequeuedKey, time.Now().UnixMilli()))
		if err != nil {
			return nil, err
		}
		return DeserializeGoTask(data)
	}
	return
}

func (q *Queue) Release(task *GoTask) (err error) {
	if task.raw.ID == 0 || len(task.data) == 0 { // not been enqueued
		return
	}

	conn := q.redis.Get()
	defer conn.Close()

	err = conn.Send("ZREM", q.enqueuedKey, task.data)
	if err != nil {
		return
	}

	_, err = conn.Do("ZREM", q.dequeuedKey, task.data)
	return
}

func (q *Queue) Requeue(task *GoTask) (ok bool, err error) {
	if task.raw.ID == 0 || len(task.data) == 0 { // not been enqueued
		return
	}

	timeout := task.raw.Timeout
	if timeout == 0 {
		timeout = q.defaultTimeout
	}
	timeout += q.requeueTimeout

	prior := 0
	if task.raw.Prior {
		prior = 1
	}

	conn := q.redis.Get()
	defer conn.Close()

	return redis.Bool(q.requeueScript.Do(conn, q.name, q.notiKey, q.enqueuedKey, q.dequeuedKey, task.data, timeout, prior))
}

func (q *Queue) RequeueLost() (count int, err error) {
	conn := q.redis.Get()
	defer conn.Close()

	return redis.Int(q.requeueLostScript.Do(conn, q.name, q.notiKey, q.dequeuedKey, time.Now().UnixMilli(), q.busyLen))
}
