package beam

import (
	"bytes"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"strconv"
)

const (
	DEFALTPOOLSIZE = 5
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
	Streams    Streamer
	ExitStatus int

	client      *Client
	exitError   error
	exitSuccess chan bool
	exitFailure chan bool
}

func NewClient(connector Connector) (*Client, error) {
	return &Client{
		pool: redis.NewPool(func() (redis.Conn, error) {
			conn, err := connector.Connect()
			if err != nil {
				return nil, err
			}
			return redis.NewConn(conn, 0, 0), nil
		}, DEFALTPOOLSIZE),
	}, nil
}

func (c *Client) NewJob(name string, args ...string) *Job {
	//FIXME: Should we get the id for the job here?
	return &Job{
		Name:   name,
		Args:   args,
		client: c,
	}
}

func (c *Client) Close() error {
	return c.pool.Close()
}

func (j *Job) Start() error {
	client := j.client
	id, err := redis.Int(client.send("RPUSH", "/jobs", j.Name))
	if err != nil {
		return err
	}

	j.Id = id - 1
	// Send job arguments
	args := append([]interface{}{fmt.Sprintf("%s/args", j.key())}, asInterfaceSlice(j.Args)...)
	_, err = client.send("RPUSH", args...)
	if err != nil {
		return err
	}

	// Send environment vars
	env := append([]interface{}{fmt.Sprintf("%s/env", j.key())}, asInterfaceSlice(splitEnv(j.Env))...)
	_, err = client.send("HMSET", env...)
	if err != nil {
		return err
	}

	// Send start job
	_, err = client.send("RPUSH", fmt.Sprintf("/jobs/start"), j.Id)
	if err != nil {
		return err
	}

	j.exitFailure, j.exitSuccess = make(chan bool), make(chan bool)
	// Start waiting for exit
	go func() {
		reply, err := client.send("BLPOP", fmt.Sprintf("%s/wait", j.key()), DEFAULTTIMEOUT)
		if err != nil {
			j.exitError = err
			j.exitFailure <- true
			return
		}
		buffer := bytes.NewBuffer(reply.([]interface{})[1].([]byte))

		code, err := strconv.Atoi(buffer.String())
		if err != nil {
			j.exitError = err
			j.exitFailure <- true
			return
		}
		j.ExitStatus = code
		j.exitSuccess <- true
	}()
	return nil
}

// Wait for the job to succeed or fail
func (j *Job) Wait() error {
	defer close(j.exitFailure)
	defer close(j.exitSuccess)

	switch {
	case <-j.exitSuccess:
		return nil
	case <-j.exitFailure:
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
