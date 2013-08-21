package beam

import (
	"bytes"
	"io"
)

type redisReader struct {
	c      <-chan []byte
	name   string
	buffer bytes.Buffer
}

type redisWriter struct {
	c    chan<- []byte
	name string
}

func newRedisReader(c chan []byte, key string) io.Reader {
	return &redisReader{c, key, bytes.Buffer{}}
}

func newRedisWriter(c chan []byte, key string) io.Writer {
	return &redisWriter{c, key}
}

func (r *redisReader) Read(p []byte) (int, error) {
	var n int
	for msg := range r.c {
		i, err := r.buffer.Write(msg)
		if err != nil {
			Debugf("Failed to write to buffer. Size: %d Err: %s", i, err)
			return i, err
		}
		n += i
		if n > len(p) {
			break
		}
	}
	return r.buffer.Read(p)
}

func (w *redisWriter) Write(p []byte) (int, error) {
	w.c <- formatMessage(w.name, p)
	return len(p), nil
}

func formatMessage(name string, msg []byte) []byte {
	return append([]byte(name), msg...)
}
