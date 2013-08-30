package main

import (
	"fmt"
	"github.com/dotcloud/beam"
	"github.com/dotcloud/docker/term"
	"github.com/kr/pty"
	"io"
	"os"
	"os/exec"
	"syscall"
)

var winsize *term.Winsize

func worker() {
	worker := beam.NewWorker(&beam.NetTransport{"tcp", ":6379"}, "/jobs")
	worker.RegisterJob("tty", func(name string, args []string, env map[string]string, streams beam.Streamer, db beam.DB) error {
		p := exec.Command("bash")

		master, slave, err := pty.Open()
		if err != nil {
			return err
		}
		if slave == nil {
			return fmt.Errorf("tty is nil")
		}

		term.SetWinsize(master.Fd(), winsize)

		p.Stdout = slave
		p.Stderr = slave
		p.Stdin = slave
		p.SysProcAttr = &syscall.SysProcAttr{Setctty: true, Setsid: true}

		go streams.ReadFrom(master, "stdout")
		go streams.ReadFrom(master, "stderr")
		go streams.WriteTo(master, "stdin")

		return p.Run()
	})
	worker.Work()
}

func createJob(client *beam.Client) *beam.Job {
	job, err := client.NewJob("tty")
	if err != nil {
		panic(err)
	}

	master, slave, err := pty.Open()
	if err != nil {
		panic(err)
	}

	_, err = term.SetRawTerminal(master.Fd())
	if err != nil {
		panic(err)
	}

	go job.WriteTo(slave, "stdout")
	go job.WriteTo(slave, "stderr")
	go job.ReadFrom(slave, "stdin")

	go io.Copy(os.Stdout, master)
	go io.Copy(os.Stderr, master)
	go io.Copy(master, os.Stdin)

	return job
}

func main() {
	go worker()

	state, err := term.SetRawTerminal(os.Stdin.Fd())
	if err != nil {
		panic(err)
	}
	defer term.RestoreTerminal(os.Stdin.Fd(), state)

	winsize, _ = term.GetWinsize(os.Stdin.Fd())
	client, err := beam.NewClient(&beam.NetTransport{"tcp", ":6379"})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	job := createJob(client)

	if err := job.Start(); err != nil {
		panic(err)
	}
	if err := job.Wait(); err != nil {
		panic(err)
	}
}
