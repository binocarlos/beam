package beam

import (
	"bufio"
	"errors"
	"github.com/garyburd/redigo/redis"
	"io"
)

var (
	ErrWriteOnReadonlyStream = errors.New("Cannot write on readonly stream")
	ErrReadOnWriteonlyStream = errors.New("Cannot read on writeonly stream")
)

type redisStream struct {
	Key      string
	Name     string
	pool     *redis.Pool
	wait     chan bool
	stream   chan []byte
	isClosed bool
}

func NewWriteStream(pool *redis.Pool, key, name string) *redisStream {
	return &redisStream{
		Key:  key,
		Name: name,
		pool: pool,
	}
}

func NewReadStream(name string) *redisStream {
	return &redisStream{
		Name:   name,
		stream: make(chan []byte, 1024),
		wait:   make(chan bool, 1),
	}
}

func (s *redisStream) Write(p []byte) (int, error) {
	if s.pool == nil {
		return 0, ErrWriteOnReadonlyStream
	}
	if s.isClosed {
		return 0, io.ErrClosedPipe
	}
	msg := append([]byte(s.Name+":"), p...)
	if _, err := send(s.pool, "RPUSH", s.Key, msg); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (s *redisStream) Read(p []byte) (int, error) {
	if s.stream == nil {
		return 0, ErrReadOnWriteonlyStream
	}
	if s.isClosed {
		return 0, io.EOF
	}

	// Read on chunk and write
	for msg := range s.stream {
		n := len(msg)
		copy(p[:n], msg)

		return n, nil
	}

	s.wait <- true
	s.isClosed = true

	return 0, io.EOF
}

func (s *redisStream) ReadFrom(r io.Reader) (int64, error) {
	var n int64
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		b := scanner.Bytes()

		i, err := s.Write(append(b, '\n'))
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

func (s *redisStream) Close() error {
	// If readonly stream wait until buffer is flushed
	if s.stream != nil {
		close(s.stream)
		<-s.wait
	}
	// Write terminating message
	if s.pool != nil {
		msg := []byte("-" + s.Name + ":")
		if _, err := send(s.pool, "RPUSH", s.Key, msg); err != nil {
			return err
		}
	}
	s.isClosed = true
	return nil
}
