package beam

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"path"
)

const (
	DEFAULTTIMEOUT = 0 // Wait forever
)

type Client struct {
	pool *redis.Pool
}

type Job struct {
	Id         int
	Name       string
	Args       []string
	Env        []string
	Streams    *Streamer
	ExitStatus int

	client      *Client
	exitError   error
	exitSuccess chan bool
	exitFailure chan bool
}

func NewClient(connector Connector) (*Client, error) {
	return &Client{pool: newConnectionPool(connector, 10)}, nil
}

func (c *Client) NewJob(name string, args ...string) (*Job, error) {
	id, err := redis.Int(c.send("RPUSH", "/jobs", name))
	if err != nil {
		return nil, err
	}

	job := &Job{
		Id:     id - 1,
		Name:   name,
		Args:   args,
		client: c,
	}
	job.Streams = NewStreamer(c.pool, fmt.Sprintf("%s/streams/out", job.key()), fmt.Sprintf("%s/streams/in", job.key()))
	return job, nil
}

func (c *Client) Close() error {
	return c.pool.Close()
}

func (j *Job) Start() error {
	Debugf("Starting job: %d", j.Id)

	client := j.client
	// Send job arguments
	if len(j.Args) > 0 {
		args := append([]interface{}{fmt.Sprintf("%s/args", j.key())}, asInterfaceSlice(j.Args)...)
		if _, err := client.send("RPUSH", args...); err != nil {
			return err
		}
	}

	// Send environment vars
	if len(j.Env) > 0 {
		env := append([]interface{}{fmt.Sprintf("%s/env", j.key())}, asInterfaceSlice(splitEnv(j.Env))...)
		if _, err := client.send("HMSET", env...); err != nil {
			return err
		}
	}

	j.exitFailure, j.exitSuccess = make(chan bool), make(chan bool)

	// Send start job
	if _, err := client.send("RPUSH", fmt.Sprintf("/jobs/start"), j.Id); err != nil {
		return err
	}

	return nil
}

func (j *Job) PopMessage(conn redis.Conn) ([]byte, error) {
	var reply []interface{}
	reply, err := redis.MultiBulk(conn.Do("BLPOP", path.Join(j.key(), "out"), DEFAULTTIMEOUT))
	if err != nil {
		return []byte{}, err
	}
	return reply[1].([]byte), nil
}

func (j *Job) Watch() error {
	conn := j.client.pool.Get()
	defer conn.Close()
	for {
		msg, err := j.PopMessage(conn)
		if err != nil {
			return err
		}
		fmt.Printf("[%s] %s", j.Id, msg)
	}
	return nil
}


// Wait for the job to succeed or fail
func (j *Job) Wait() error {
	conn := j.client.pool.Get()
	// Check if the job is already finished
	status, err := conn.Do("GET", path.Join(j.key(), "status"))
	if err != nil {
		return fmt.Errorf("Error getting job status: %s", err)
	}
	if status != nil {
		Debugf("Status of job is already set. Returning.")
		// FIXME: always returning success for now
		return nil
	}
	Debugf("Job has not yet exited. Waiting.")
	reply, err := redis.MultiBulk(conn.Do("BLPOP", fmt.Sprintf("%s/wait", j.key()), DEFAULTTIMEOUT))
	Debugf("Job complete: %d (%s", j.Id, reply)
	return err
}

func (j *Job) key() string {
	return fmt.Sprintf("/jobs/%d", j.Id)
}

func (c *Client) send(name string, args ...interface{}) (interface{}, error) {
	conn := c.pool.Get()
	defer conn.Close()

	return conn.Do(name, args...)
}
