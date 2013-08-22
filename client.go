package beam

import (
	"bytes"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"strconv"
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
	args := append([]interface{}{fmt.Sprintf("%s/args", j.key())}, asInterfaceSlice(j.Args)...)
	if _, err := client.send("RPUSH", args...); err != nil {
		return err
	}

	// Send environment vars
	env := append([]interface{}{fmt.Sprintf("%s/env", j.key())}, asInterfaceSlice(splitEnv(j.Env))...)
	if _, err := client.send("HMSET", env...); err != nil {
		return err
	}

	j.exitFailure, j.exitSuccess = make(chan bool), make(chan bool)
	// Start waiting for exit
	go func() {
		Debugf("Waiting for job: %d", j.Id)

		reply, err := redis.MultiBulk(client.send("BLPOP", fmt.Sprintf("%s/wait", j.key()), DEFAULTTIMEOUT))
		Debugf("Job complete: %d", j.Id)
		if err != nil {
			j.exitError = err
			j.exitFailure <- true
			return
		}

		status := reply[1].([]byte)
		// Status will be empty for success
		if len(status) == 0 {
			j.exitSuccess <- true
			return
		}

		buffer := bytes.NewBuffer(status)
		code, err := strconv.Atoi(buffer.String())
		if err != nil {
			j.exitError = err
			j.exitFailure <- true
			return
		}
		j.ExitStatus = code
		j.exitFailure <- true
	}()

	// Send start job
	if _, err := client.send("RPUSH", fmt.Sprintf("/jobs/start"), j.Id); err != nil {
		return err
	}

	return nil
}

// Wait for the job to succeed or fail
func (j *Job) Wait() error {
	defer close(j.exitFailure)
	defer close(j.exitSuccess)
	defer Debugf("Job wait complete")

	Debugf("Waiting for job to complete")
	switch {
	case <-j.exitSuccess:
		Debugf("Shutting down job streams - success")
		return j.Streams.Shutdown()
	case <-j.exitFailure:
		Debugf("Shutting down job streams - failue")
		j.Streams.Shutdown()
		return j.exitError
	}
	return nil
}

func (j *Job) key() string {
	return fmt.Sprintf("/jobs/%d", j.Id)
}

func (c *Client) send(name string, args ...interface{}) (interface{}, error) {
	conn := c.pool.Get()
	defer conn.Close()

	return conn.Do(name, args...)
}
