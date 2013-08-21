package beam

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"os"
	"runtime"
	"strings"
)

// Debug function, if the debug flag is set, then display. Do nothing otherwise
// If Docker is in damon mode, also send the debug info on the socket
func Debugf(format string, a ...interface{}) {
	if os.Getenv("DEBUG") != "" {

		// Retrieve the stack infos
		_, file, line, ok := runtime.Caller(1)
		if !ok {
			file = "<unknown>"
			line = -1
		} else {
			file = file[strings.LastIndex(file, "/")+1:]
		}

		fmt.Fprintf(os.Stderr, fmt.Sprintf("[%d] [debug] %s:%d %s\n", os.Getpid(), file, line, format), a...)
	}
}

// Take an array of "KEY=VALUE" pairs and split the elements on '=' into a array
func splitEnv(env []string) []string {
	out := make([]string, len(env)*2)
	var i int

	for _, pair := range env {
		parts := strings.Split(pair, "=")
		out[i] = parts[0]
		out[i+1] = parts[1]
		i += 2
	}
	return out
}

func asInterfaceSlice(value []string) []interface{} {
	out := make([]interface{}, len(value))
	for i, v := range value {
		out[i] = v
	}
	return out
}

func newConnectionPool(connector Connector, size int) *redis.Pool {
	return redis.NewPool(func() (redis.Conn, error) {
		conn, err := connector.Connect()
		if err != nil {
			return nil, err
		}
		return redis.NewConn(conn, 0, 0), nil
	}, size)
}
