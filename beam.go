// Beam is a protocol and library for service-oriented communication,
// with an emphasis on real-world patterns, simplicity and not reinventing the wheel.
//
// See http://github.com/dotcloud/beam.

package beam

import (
	"github.com/garyburd/redigo/redis"
	"io"
)

type DB interface {
}

type streamer struct {
	WriteKey string
	ReadKey  string
	pool     *redis.Pool
	streams  map[string]*redisStream
}

type Message struct {
	Id   string
	Body []byte
}

func NewStreamer(pool *redis.Pool, writeKey, readKey string) Streamer {
	return &streamer{
		WriteKey: writeKey,
		ReadKey:  readKey,
		pool:     pool,
		streams:  make(map[string]*redisStream),
	}
}

func (s *streamer) OpenRead(name string) (io.ReadCloser, error) {
	if _, exists := s.streams[name]; exists {
		return nil, ErrStreamAlreadyExists
	}

	rs := NewReadStream(name)
	s.streams[name] = rs

	return rs, nil
}

func (s *streamer) OpenWrite(name string) (io.WriteCloser, error) {
	if _, exists := s.streams[name]; exists {
		return nil, ErrStreamAlreadyExists
	}

	rs := NewWriteStream(s.pool, s.WriteKey, name)
	s.streams[name] = rs

	return rs, nil
}

func (s *streamer) Close() error {
	for _, stream := range s.streams {
		if err := stream.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (s *streamer) writeMessage(msg *Message) {
	if stream, exists := s.streams[msg.Id]; exists {
		stream.stream <- msg.Body
	}
}
