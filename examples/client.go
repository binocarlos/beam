package main

import (
	"fmt"
	"github.com/dotcloud/beam"
	"io"
	"os"
	"os/exec"
)

func JobExec(name string, args []string, env map[string]string, streams *beam.Streamer, db beam.DB) error {
	var (
		cmdName string
		cmdArgs []string
	)
	if len(args) >= 1 {
		cmdName = args[0]
	} else {
		return fmt.Errorf("Not enough arguments")
	}
	if len(args) > 1 {
		cmdArgs = args[1:]
	}
	p := exec.Command(cmdName, cmdArgs...)

	out := streams.OpenWrite("stdout")
	err := streams.OpenWrite("stderr")
	defer out.Close()
	defer err.Close()

	p.Stdout = out
	p.Stderr = err

	return p.Run()
}

func worker() {
	worker := beam.NewWorker(&beam.NetTransport{"tcp", ":6379"}, "/jobs")
	worker.RegisterJob("exec", JobExec)

	worker.Work()
}

func main() {
	go worker()

	client, err := beam.NewClient(&beam.NetTransport{"tcp", ":6379"})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	job, err := client.NewJob("exec", os.Args[1:]...)
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

	go func() {
		e := job.Streams.OpenRead("stderr")
		if _, err := io.Copy(os.Stderr, e); err != nil {
			panic(err)
		}
	}()

	if err := job.Start(); err != nil {
		panic(err)
	}

	if err := job.Wait(); err != nil {
		panic(err)
	}
}
