package beam

import (
	"bufio"
	"io"
)

type Channel struct {
	stream                   chan []byte
	buffer                   []byte
	isClosed                 bool
	SendEmptyResponseOnClose bool
	wait                     chan bool
}

func NewChannel(stream chan []byte, sendEmptyResponseOnClose bool) (*Channel, chan bool) {
	var wait chan bool
	if !sendEmptyResponseOnClose {
		wait = make(chan bool)
	}

	return &Channel{stream, make([]byte, 0), false, sendEmptyResponseOnClose, wait}, wait
}

func (c *Channel) Read(p []byte) (n int, err error) {
	maxWrite := len(p)

	bs := len(c.buffer)
	if bs > 0 {
		if maxWrite < bs {
			bs = maxWrite
		}
		n = copy(p, c.buffer[:bs])
		c.buffer = c.buffer[n:]
		maxWrite = maxWrite - n
		if maxWrite == 0 {
			return
		}
	}

	for chunk := range c.stream {
		cs := len(chunk)
		if maxWrite < cs {
			cs = maxWrite
		}
		n += copy(p, chunk[:cs])
		c.buffer = chunk[n:]
		return
	}

	err = io.EOF
	go func() {
		c.wait <- true
	}()
	return
}

func (c *Channel) Write(p []byte) (int, error) {
	if c.isClosed {
		return 0, io.ErrClosedPipe
	}

	c.stream <- p
	return len(p), nil
}

func (c *Channel) ReadFrom(r io.Reader) (int64, error) {
	var n int64
	if wt, ok := r.(io.WriterTo); ok {
		return wt.WriteTo(c)
	}

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		b := scanner.Bytes()

		i, err := c.Write(append(b, '\n'))
		n += int64(i)
		if err != nil {
			return n, err
		}
		if err := scanner.Err(); err != nil {
			return n, err
		}
	}
	return n, nil
}

func (c *Channel) Close() error {
	if c.SendEmptyResponseOnClose {
		c.Write([]byte(""))
	}
	c.isClosed = true
	close(c.stream)
	return nil
}
