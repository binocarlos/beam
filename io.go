package beam

import (
	"bytes"
	"io"
)

type redisReader struct {
	c        <-chan []byte
	name     string
	buffer   bytes.Buffer
	isClosed bool
}

type redisWriter struct {
	c        chan<- []byte
	name     string
	isClosed bool
}

func newRedisReader(c chan []byte, key string) io.ReadCloser {
	return &redisReader{c, key, bytes.Buffer{}, false}
}

func newRedisWriter(c chan []byte, key string) io.WriteCloser {
	return &redisWriter{c, key, false}
}

func (r *redisReader) Read(p []byte) (int, error) {
	if r.isClosed && r.buffer.Len() == 0 {
		return 0, io.EOF
	}
	for msg := range r.c {
		i, err := r.buffer.Write(msg)
		if err != nil {
			Debugf("Failed to write to buffer. Size: %d Err: %s", i, err)
			return i, err
		}
		return r.buffer.Read(p)
	}
	return r.buffer.Read(p)
}

func (r *redisReader) Close() error {
	r.isClosed = true
	return nil
}

func (w *redisWriter) Write(p []byte) (int, error) {
	if w.isClosed {
		return 0, io.ErrClosedPipe
	}
	w.c <- formatMessage(w.name, p)
	return len(p), nil
}

func (w *redisWriter) Close() error {
	w.isClosed = true
	w.c <- formatMessage(w.name, []byte(""))
	return nil
}

func formatMessage(name string, msg []byte) []byte {
	return append([]byte(name+":"), msg...)
}
