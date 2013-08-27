package beam

import (
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"io"
	"path"
)

var (
	ErrNilConnectionPool   = errors.New("Connection pool cannot be nil")
	ErrInvalidResposeType  = errors.New("Invalid response type")
	ErrStreamAlreadyExists = errors.New("Stream already exists")
)

type Job struct {
	Id         int
	Name       string
	Args       []string
	Env        []string
	ExitStatus int

	pool    *redis.Pool
	streams map[string]*redisStream
}

type Message struct {
	Id   string
	Body []byte
}

// Create and register new job
func NewJob(pool *redis.Pool, jobName string, args ...string) (*Job, error) {
	if pool == nil {
		return nil, ErrNilConnectionPool
	}

	// Get a new id for the job
	id, err := redis.Int(send(pool, "RPUSH", "/jobs", jobName))
	if err != nil {
		return nil, err
	}
	id = id - 1

	return &Job{
		Id:      id,
		Name:    jobName,
		Args:    args,
		pool:    pool,
		streams: make(map[string]*redisStream),
	}, nil
}

func (j *Job) Start() error {
	Debugf("Start job: %d", j.Id)

	// Setup the job
	if err := j.writeArgs(); err != nil {
		return err
	}
	if err := j.writeEnv(); err != nil {
		return err
	}

	// Start the job event loop
	go func() {
		if err := j.watch(); err != nil {
			panic(err)
		}
	}()
	return nil
}

func (j *Job) OpenRead(name string) (io.ReadCloser, error) {
	if _, exists := j.streams[name]; exists {
		return nil, ErrStreamAlreadyExists
	}

	s := NewReadStream(j.keyParts("streams", "out"))
	j.streams[name] = s
	return s, nil
}

func (j *Job) OpenWrite(name string) (io.WriteCloser, error) {
	if _, exists := j.streams[name]; exists {
		return nil, ErrStreamAlreadyExists
	}

	s := NewWriteStream(j.pool, j.keyParts("streams", "in"), name)
	j.streams[name] = s
	return s, nil
}

func (j *Job) watch() error {
	conn := j.pool.Get()
	defer conn.Close()
	for {
		msg, err := j.popMessage(conn)
		if err != nil {
			return err
		}

		// Check if job has ended
		if msg.Id == "x" {
			for _, stream := range j.streams {
				if err := stream.Close(); err != nil {
					return err
				}
			}
		}

		// If we have a stream write to it, if not we discard the message
		if stream, ok := j.streams[msg.Id]; ok {
			stream.stream <- msg.Body
		}
	}
	return nil
}

func (j *Job) popMessage(conn redis.Conn) (*Message, error) {
	reply, err := redis.MultiBulk(conn.Do("BLPOP", j.keyParts("out"), DEFAULTTIMEOUT))
	if err != nil {
		return nil, err
	}
	return parseMessage(reply)
}

func (j *Job) Wait() error {
	Debugf("Wait job: %d", j.Id)

	isComplete, err := j.isComplete()
	if err != nil {
		return err
	}
	if isComplete {
		return nil
	}

	if _, err := send(j.pool, "BLPOP", j.keyParts("wait"), DEFAULTTIMEOUT); err != nil {
		return err
	}
	Debugf("Job complete: %d", j.Id)
	return nil
}

func (j *Job) isComplete() (bool, error) {
	// Check if job is already finished
	status, err := send(j.pool, "GET", j.keyParts("status"))
	if err != nil {
		return false, fmt.Errorf("Error getting job status: %s", err)
	}

	if status != nil {
		Debugf("Status of job is already set. Returning.")
		// FIXME: always returning success for now
		return true, nil
	}
	return false, nil
}

func (j *Job) Close() error {
	Debugf("Close job: %d", j.Id)
	return nil
}

func (j *Job) writeArgs() error {
	if len(j.Args) > 0 {
		args := append([]interface{}{j.keyParts("args")}, asInterfaceSlice(j.Args)...)
		if _, err := send(j.pool, "RPUSH", args...); err != nil {
			return err
		}
	}
	return nil
}

func (j *Job) writeEnv() error {
	if len(j.Env) > 0 {
		env := append([]interface{}{j.keyParts("env")}, asInterfaceSlice(splitEnv(j.Env))...)
		if _, err := send(j.pool, "HMSET", env...); err != nil {
			return err
		}
	}
	return nil
}

func (j *Job) keyParts(parts ...string) string {
	base := []string{fmt.Sprintf("/jobs/%d", j.Id)}
	return path.Join(append(base, parts...)...)
}
