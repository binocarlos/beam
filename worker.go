package beam

import (
	"net"
	"fmt"
	"github.com/garyburd/redigo/redis"
)

type Worker struct {
	redigo.Conn
	Prefix string	// The prefix for all redis keys
	handlers map[string]JobHandler
	jobs	[]*Job
}

// NewWorker initializes a new beam worker.
func NewWorker(conn net.Conn, prefix string) *Worker{
	return &Worker{
		Conn: redis.NewConn(conn, 0, 0),
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
func (w *Worker) ServeJob(name string, args []string, env map[string]string, streams Streamer, db DB) error {
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
type JobHandler func(name string, args []string, env map[string]string, streams Streamer, db DB) error

// Work runs an infinite loop, watching its database for new requests, starting job as requested,
// moving stream data back and forth, and updating job status as it changes.
func (w *Worker) Work() error {
	return nil
}
