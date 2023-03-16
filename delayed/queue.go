package delayed

import (
	"github.com/gomodule/redigo/redis"
	"github.com/keakon/golog/log"
)

const (
	idKeySuffix         = "_id"
	notiKeySuffix       = "_noti"
	processingKeySuffix = "_processing"

	defaultDequeueTimeout   uint32 = 1000
	defaultKeepAliveTimeout uint16 = 60
)

const (
	// KEYS: queue_name, processing_key
	// ARGV: worker_id
	dequeueScript = `local task = redis.call('lpop', KEYS[1])
if task == nil then
    return nil
end
redis.call('hset', KEYS[2], ARGV[1], task)
return task`

	// KEYS: queue_name, noti_key, processing_key
	requeueLostScript = `local queue_len = redis.call('llen', KEYS[1])
local noti_len = redis.call('llen', KEYS[2])
local count = queue_len - noti_len
local processing_tasks = redis.call('hgetall', KEYS[3])
for i = 1, #processing_tasks, 2 do
	local worker_id = processing_tasks[i]
	local worker_alive = redis.call('get', worker_id)
	if not worker_alive then
		count = count + 1
		redis.call('rpush', KEYS[1], processing_tasks[i + 1])
		redis.call('hdel', KEYS[3], worker_id)
	end
end
if count > 0 then
    local noti_array = {}
    for i=1,count,1 do
        table.insert(noti_array, '1')
    end
    redis.call('lpush', KEYS[2], unpack(noti_array))
end
return count`
)

type Queue struct {
	workerID         string
	name             string
	idKey            string
	notiKey          string
	processingKey    string
	dequeueTimeout   uint32 // ms, must be larger than 1, redis BLPOP treats timeout equal or less than 0.001 second as 0 (forever)
	KeepAliveTimeout uint16

	redis             *redis.Pool
	dequeueScript     *redis.Script
	requeueScript     *redis.Script
	requeueLostScript *redis.Script

	handlers map[string]*Handler
}

type QueueOption func(*Queue)

func DequeueTimeout(ms uint32) QueueOption {
	return func(q *Queue) {
		if ms > 1 {
			q.dequeueTimeout = ms
		} else {
			q.dequeueTimeout = defaultDequeueTimeout
		}
	}
}

func KeepAliveTimeout(s uint16) QueueOption {
	return func(q *Queue) {
		if s > 1 {
			q.KeepAliveTimeout = s
		} else if s == 0 {
			q.KeepAliveTimeout = defaultKeepAliveTimeout
		}
	}
}

func NewQueue(name string, redisPool *redis.Pool, options ...QueueOption) *Queue {
	queue := &Queue{
		name:              name,
		idKey:             name + idKeySuffix,
		notiKey:           name + notiKeySuffix,
		processingKey:     name + processingKeySuffix,
		dequeueTimeout:    defaultDequeueTimeout,
		KeepAliveTimeout:  defaultKeepAliveTimeout,
		redis:             redisPool,
		dequeueScript:     redis.NewScript(2, dequeueScript),
		requeueLostScript: redis.NewScript(3, requeueLostScript),
	}

	for _, option := range options {
		option(queue)
	}

	return queue
}

func (q *Queue) keepAlive() error {
	conn := q.redis.Get()
	defer conn.Close()

	_, err := conn.Do("SETEX", q.workerID, q.KeepAliveTimeout, 1)
	return err
}

func (q *Queue) die() error {
	conn := q.redis.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", q.workerID)
	return err
}

func (q *Queue) Clear() error {
	conn := q.redis.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", q.name, q.idKey, q.notiKey, q.processingKey, q.workerID)
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

	err = conn.Send("RPUSH", q.name, task.data)
	if err != nil {
		return
	}

	_, err = conn.Do("RPUSH", q.notiKey, 1) // use Do() to combine Send(), Flush() and Receive()
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
		data, err := redis.Bytes(q.dequeueScript.Do(conn, q.name, q.processingKey, q.workerID))
		if err != nil {
			return nil, err
		}
		return DeserializeGoTask(data)
	}
	return
}

func (q *Queue) Release() (err error) {
	conn := q.redis.Get()
	defer conn.Close()

	_, err = conn.Do("HDEL", q.processingKey, q.workerID)
	return
}

func (q *Queue) RequeueLost() (count int, err error) {
	conn := q.redis.Get()
	defer conn.Close()

	return redis.Int(q.requeueLostScript.Do(conn, q.name, q.notiKey, q.processingKey, q.workerID))
}
