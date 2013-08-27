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
	streamer   *streamer
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
		Id:       id,
		Name:     jobName,
		Args:     args,
		streamer: NewStreamer(pool, fmt.Sprintf("/jobs/%d/streams/out", id), fmt.Sprintf("/jobs/%d/streams/in", id)).(*streamer),
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

	// Signal job is ready for processing
	if _, err := send(j.streamer.pool, "RPUSH", "/jobs/start", j.Id); err != nil {
		return err
	}
	return nil
}

func (j *Job) OpenRead(name string) (io.ReadCloser, error) {
	return j.streamer.OpenRead(name)
}

func (j *Job) OpenWrite(name string) (io.WriteCloser, error) {
	return j.streamer.OpenWrite(name)
}

func (j *Job) watch() error {
	conn := j.streamer.pool.Get()
	defer conn.Close()
	for {
		msg, err := j.popMessage(conn)
		if err != nil {
			return err
		}

		// Check if job has ended
		if msg.Id == "x" {
			if err := j.streamer.Close(); err != nil {
				return err
			}
		}
		j.streamer.writeMessage(msg)
	}
	return nil
}

func (j *Job) popMessage(conn redis.Conn) (*Message, error) {
	reply, err := redis.MultiBulk(conn.Do("BLPOP", j.streamer.ReadKey, DEFAULTTIMEOUT))
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

	if _, err := send(j.streamer.pool, "BLPOP", j.keyParts("wait"), DEFAULTTIMEOUT); err != nil {
		return err
	}
	Debugf("Job complete: %d", j.Id)
	return nil
}

func (j *Job) isComplete() (bool, error) {
	// Check if job is already finished
	status, err := send(j.streamer.pool, "GET", j.keyParts("status"))
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

func (j *Job) writeArgs() error {
	if len(j.Args) > 0 {
		args := append([]interface{}{j.keyParts("args")}, asInterfaceSlice(j.Args)...)
		if _, err := send(j.streamer.pool, "RPUSH", args...); err != nil {
			return err
		}
	}
	return nil
}

func (j *Job) writeEnv() error {
	if len(j.Env) > 0 {
		env := append([]interface{}{j.keyParts("env")}, asInterfaceSlice(splitEnv(j.Env))...)
		if _, err := send(j.streamer.pool, "HMSET", env...); err != nil {
			return err
		}
	}
	return nil
}

func (j *Job) keyParts(parts ...string) string {
	base := []string{fmt.Sprintf("/jobs/%d", j.Id)}
	return path.Join(append(base, parts...)...)
}
