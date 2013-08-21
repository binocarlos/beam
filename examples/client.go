package main

import (
	"fmt"
	"github.com/dotcloud/beam"
	"io"
	"os"
)

func main() {
	client, err := beam.NewClient(&beam.NetTransport{"tcp", ":6379"})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	job, err := client.NewJob("exec", "ls", "-la")
	if err != nil {
		panic(err)
	}
	job.Env = []string{"DEBUG=1"}

	go func() {
		r := job.Streams.OpenRead("stdout")
		if _, err := io.Copy(os.Stdout, r); err != nil {
			panic(err)
		}
	}()
	if err := job.Start(); err != nil {
		panic(err)
	}

	fmt.Println(job.Id)

	if err := job.Wait(); err != nil {
		panic(err)
	}
}
