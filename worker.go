package beam

import (
	"os"
	"net"
	"fmt"
	"path"
	"github.com/garyburd/redigo/redis"
)

type Connector interface {
	Connect() (net.Conn, error)
}

type Worker struct {
	transport Connector
	Prefix string	// The prefix for all redis keys
	handlers map[string]JobHandler
}

// NewWorker initializes a new beam worker.
func NewWorker(transport Connector, prefix string) *Worker{
	return &Worker{
		transport: transport,
		Prefix: prefix,
		handlers: make(map[string]JobHandler),
	}
}

// RegisterJob exposes the function <h> as a remote job to be invoked by clients
// under the name <name>.
func (w *Worker) RegisterJob(name string, h JobHandler) {
	w.handlers[name] = h
}

// ServeJob is the server's default job handler. It is called every time a new job is created.
// It looks up a handler registered at <name>, and calls it with the same arguments. If no handler
// is registered, it returns an error.
func (w *Worker) ServeJob(name string, args []string, env map[string]string, streams *Streamer, db DB) error {
	h, exists := w.handlers[name]
	if !exists {
		return fmt.Errorf("No such job: %s", name)
	}
	return h(name, args, env, streams, db)
}

// A JobHandler is a function which can be invoked as a job by beam clients.
// The API for invoking jobs resembles that of unix processes:
//  - A job is invoked under a certain <name>.
//  - It may receive arguments as a string array (<args>).
//  - It may receive an environment as a map of key-value pairs (<env>).
//  - It may read from, and write to, streams of binary data. (<streams>).
//  - It returns value which can either indicate "success" or a variety of error conditions.
//
// Additionally, a job may modify the server's database, which is shared with all other jobs.
// This is similar to how multiple unix processes share access to the same filesystem.
//
type JobHandler func(name string, args []string, env map[string]string, streams *Streamer, db DB) error


// Work runs an infinite loop, watching its database for new requests, starting job as requested,
// moving stream data back and forth, and updating job status as it changes.
func (w *Worker) Work() error {
	conn, err := w.Connect()
	if err != nil {
		return err
	}
	defer conn.Close()
	for {
		Debugf("Waiting for job")
		// Get the list of current jobs
		// Wait for next start event
		vals, err := redis.Values(conn.Do("BLPOP", w.KeyPath("start"), "0"))
		if err != nil {
			return err
		}
		var id string
		if _, err := redis.Scan(vals[1:], &id); err != nil {
			return err
		}
		Debugf("Received instruction to start job %s", id)
		// Acquire lock on the job
		acquired, err := redis.Bool(conn.Do("SETNX", w.KeyPath(id, "worker"), "me"))
		if err != nil {
			return err
		}
		Debugf("Acquiring lock for job %s... -> %s", id, acquired)
		// FIXME: set a dead man's switch with TTL & a periodic refresh
		if acquired {
			Debugf("Spawning goroutine for job %s", id)
			go func(id string) {
				if err := w.startJob(id); err != nil {
					fmt.Fprintf(os.Stderr, "Error starting job %s: %s\n", id, err)
				}
			}(id)
		}
	}
}


// Connect opens a new redis connection using the worker's transport, and returns it.
func (w *Worker) Connect() (redis.Conn, error) {
	Debugf("Opening new connection")
	conn, err := w.transport.Connect()
	if err != nil {
		return nil, err
	}
	Debugf("Connection successful")
	return redis.NewConn(conn, 0, 0), nil
}


func (w *Worker) startJob(id string) error {
	conn, err := w.Connect()
	if err != nil {
		return err
	}
	Debugf("Connected")
	defer conn.Close()
	// Get job name
	name, err := redis.String(conn.Do("GET", w.KeyPath(id)))
	if err != nil {
		return err
	}
	Debugf("Job name = %s", name)

	// Get all arguments
	argsVals, err := redis.Values(conn.Do("LRANGE", w.KeyPath(id, "args"), "0", "-1"))
	if err != nil {
		return err
	}
	args := make([]string, len(argsVals))
	for i := range args {
		if _, err := redis.Scan(argsVals[i:], &args[i]); err != nil {
			return err
		}
	}
	Debugf("Job arguments = %v", args)

	// Get env
	envVals, err := redis.Values(conn.Do("HGETALL", w.KeyPath(id, "env")))
	if err != nil {
		return err
	}
	env := make(map[string]string)
	for len(envVals) > 0 {
		var (k, v string)
		envVals, err = redis.Scan(envVals, &k, &v)
		if err != nil {
			return err
		}
		env[k] = v
	}
	Debugf("Job env = %v", env)

	// Setup streams
	streams := NewStreamer(conn, w.KeyPath(id, "in"), w.KeyPath(id, "out"))
	err = w.ServeJob(name, args, env, streams, w)
	var status string
	if err == nil {
		status = ""
	} else {
		status = err.Error()
	}
	// Set the status and notify the client
	if _, err := conn.Do("SET", w.KeyPath(id, "status"), status); err != nil {
		return err
	}
	if _, err := conn.Do("RPUSH", w.KeyPath(id, "wait"), status); err != nil {
		return err
	}
	return nil
}

func (w *Worker) KeyPath(parts ...string) string {
	parts = append([]string{w.Prefix}, parts...)
	return path.Join(parts...)
}
