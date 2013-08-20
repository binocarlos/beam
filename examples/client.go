package main

import (
	"fmt"
	"github.com/dotcloud/beam"
	"net"
)

type conn struct {
}

func (c *conn) Connect() (net.Conn, error) {
	return net.Dial("tcp", ":6379")
}

func main() {
	connector := &conn{}
	client, err := beam.NewClient(connector)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	job := client.NewJob("exec", "ls", "-la")
	job.Env = []string{"DEBUG=1"}
	//job.Streams.WriteTo(os.Stdout, "stdout")
	//job.Streams.WriteTo(os.Stderr, "stderr")
	defer job.Streams.Close()

	if err := job.Start(); err != nil {
		panic(err)
	}

	fmt.Println(job.Id)

	if err := job.Wait(); err != nil {
		panic(err)
	}
}
