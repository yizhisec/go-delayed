package delayed

import (
	"errors"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/keakon/golog/log"
)

const (
	idKeySuffix         = "_id"
	notiKeySuffix       = "_noti"
	processingKeySuffix = "_processing"

	defaultDequeueTimeout   float32 = 1
	defaultKeepAliveTimeout float32 = 60
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

var InvalidRedisReplyError = errors.New("Invalid redis reply")

type Queue struct {
	workerID         string
	name             string
	idKey            string
	notiKey          string
	processingKey    string
	dequeueTimeout   float32 // seconds
	keepAliveTimeout float32 // seconds

	redis             *redis.Pool
	dequeueScript     *redis.Script
	requeueScript     *redis.Script
	requeueLostScript *redis.Script

	handlers map[string]*Handler
}

type QueueOption func(*Queue)

func DequeueTimeout(d time.Duration) QueueOption { // must be larger than 1 ms, redis BLPOP treats timeout equal or less than 0.001 second as 0 (forever)
	return func(q *Queue) {
		if d > time.Millisecond {
			q.dequeueTimeout = float32(d/time.Millisecond) * 0.001
		} else {
			q.dequeueTimeout = defaultDequeueTimeout
		}
	}
}

func KeepAliveTimeout(d time.Duration) QueueOption {
	return func(q *Queue) {
		if d == 0 {
			q.keepAliveTimeout = defaultKeepAliveTimeout
		} else {
			q.keepAliveTimeout = float32(d/time.Millisecond) * 0.001
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
		keepAliveTimeout:  defaultKeepAliveTimeout,
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

	_, err := conn.Do("SETEX", q.workerID, q.keepAliveTimeout, 1)
	if err == nil {
		log.Debugf("Worker %s is alive.", q.workerID)
	}
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

func (q *Queue) Enqueue(task Task) (err error) {
	conn := q.redis.Get()
	defer conn.Close()

	taskID := task.getID()
	if *taskID == 0 {
		id, err := redis.Uint64(conn.Do("INCR", q.idKey))
		if err != nil {
			log.Errorf("Failed to generate task id: %v", err)
			return err
		}
		*taskID = id
	}

	log.Debugf("Enqueuing task %d", *taskID)

	data := task.getData()
	if len(data) == 0 {
		data, err = task.Serialize()
		if err != nil {
			log.Errorf("Failed to serialize task %d: %v", *taskID, err)
			return
		}
	}

	err = conn.Send("RPUSH", q.name, data)
	if err != nil {
		return
	}

	_, err = conn.Do("RPUSH", q.notiKey, 1) // use Do() to combine Send(), Flush() and Receive()
	if err == nil {
		log.Debugf("Enqueued task %d", *taskID)
	}
	return
}

func (q *Queue) Dequeue() (task *GoTask, err error) {
	conn := q.redis.Get()
	defer conn.Close()

	reply, err := redis.Values(conn.Do("BLPOP", q.notiKey, q.dequeueTimeout))
	if err != nil {
		if err == redis.ErrNil {
			err = nil
		}
		return
	}

	if len(reply) != 2 {
		return nil, InvalidRedisReplyError
	}

	popped, ok := reply[1].([]uint8) // reply[0] is the popped key (q.notiKey)
	if !ok || len(popped) != 1 {
		return nil, InvalidRedisReplyError
	}

	if popped[0] == '1' { // redis encodes 1 into '1'
		log.Debugf("Popped a task.")
		var data []byte
		data, err = redis.Bytes(q.dequeueScript.Do(conn, q.name, q.processingKey, q.workerID))
		if err != nil {
			return nil, err
		}
		task, err = DeserializeGoTask(data)
		if err == nil {
			log.Debugf("Dequeued task %d.", task.raw.ID)
		}
		return
	} else {
		return nil, InvalidRedisReplyError
	}
}

func (q *Queue) Release() (err error) {
	conn := q.redis.Get()
	defer conn.Close()

	log.Debugf("Releasing the task of worker %s.", q.workerID)
	_, err = conn.Do("HDEL", q.processingKey, q.workerID)
	if err == nil {
		log.Debugf("Released the task of worker %s.", q.workerID)
	}
	return
}

func (q *Queue) RequeueLost() (count int, err error) {
	conn := q.redis.Get()
	defer conn.Close()

	count, err = redis.Int(q.requeueLostScript.Do(conn, q.name, q.notiKey, q.processingKey, q.workerID))
	if count > 0 {
		if count == 1 {
			log.Debugf("Requeued 1 lost task.")
		} else {
			log.Debugf("Requeued %d lost tasks.", count)
		}
	}
	return
}
