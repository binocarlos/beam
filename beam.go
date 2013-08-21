// Beam is a protocol and library for service-oriented communication,
// with an emphasis on real-world patterns, simplicity and not reinventing the wheel.
//
// See http://github.com/dotcloud/beam.

package beam

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"io"
)

var (
	ErrInvalidResposeType = errors.New("Invalid response type")
)

type DB interface {
}

type streamingContext struct {
	name    string
	message []byte
	err     error
}

type Streamer struct {
	conn   redis.Conn
	InKey  string
	OutKey string

	readers  map[string]chan []byte
	handlers map[string]chan []byte
}

func NewStreamer(conn redis.Conn, in, out string) *Streamer {
	streamer := &Streamer{
		conn:     conn,
		InKey:    in,
		OutKey:   out,
		readers:  make(map[string]chan []byte),
		handlers: make(map[string]chan []byte),
	}
	e := streamer.start()
	go func() {
		for err := range e {
			Debugf("Error: %s", err)
		}
	}()

	return streamer
}

// OpenRead returns a read-only interface to receive data on the stream <name>.
// If the stream hasn't been open for read access before, it is advertised as such to the peer.
func (s *Streamer) OpenRead(name string) io.Reader {
	c := make(chan []byte, 100)
	s.handlers[name] = c
	return newRedisReader(c, name)
}

func (s *Streamer) OpenWrite(name string) io.Writer {
	return nil
}

func newListener(s *Streamer, name string, c chan []byte) {
	for msg := range c {
		if handler, ok := s.handlers[name]; ok {
			handler <- msg
		}
	}
	if handler, ok := s.handlers[name]; ok {
		close(handler)
	}
}

func (s *Streamer) start() chan error {
	e := make(chan error)
	go func() {
		for {
			reply, err := redis.MultiBulk(s.conn.Do("BLPOP", s.OutKey, DEFAULTTIMEOUT))
			if err != nil {
				e <- err
				continue
			}
			b := reply[1].([]byte)

			Debugf("Received message %s", string(b))
			parts := bytes.SplitN(b, []byte(":"), 2)
			if len(parts) < 2 {
				e <- ErrInvalidResposeType
				continue
			}
			name := string(parts[0])
			msg := parts[1]
			if len(msg) == 0 {
				for _, c := range s.readers {
					close(c)
				}
				break
			}
			c, ok := s.readers[name]
			if !ok {
				c = make(chan []byte, 100)
				s.readers[name] = c

				go newListener(s, name, c)
			}
			c <- msg
		}
	}()

	//	go func() {
	//		for msg := range s.readChan {
	//			_, err := s.conn.Do("RPUSH", s.InKey, msg)
	//			if err != nil {
	//				e <- err
	//				continue
	//			}
	//		}
	//	}()
	return e
}

// ReadFrom opens a read-only interface on the stream <name>, and copies data
// to that interface from <src> until EOF or error.
// The return value n is the number of bytes read.
// Any error encountered during the write is also returned.
func (s *Streamer) ReadFrom(src io.Reader, name string) (int64, error) {
	return 0, nil
}

// OpenWrite returns a write-only interface to send data on the stream <name>.
// If the stream hasn't been open for write access before, it is advertised as such to the peer.
func (s *Streamer) OpenWrite(name string) io.Writer {
	return nil
}

// WriteTo opens a write-only interface on the stream <name>, and copies data
// from that interface to <dst> until there's no more data to write or when an error occurs.
// The return value n is the number of bytes written.
// Any error encountered during the write is also returned.
func (s *Streamer) WriteTo(dst io.Writer, name string) (int64, error) {
	return 0, fmt.Errorf("Not implemented")
}

// OpenReadWrite returns a read-write interface to send and receive on the stream <name>.
// If the stream hasn't been open for read or write access before, it is advertised as such to the peer.
func (s *Streamer) OpenReadWrite(name string) io.ReadWriter {
	return nil
}

// Close closes the stream <name>. All future reads will return io.EOF, and writes will return
// io.ErrClosedPipe
func (s *Streamer) Close(name string) {

}

// Shutdown waits until all streams with read access are closed and
// all WriteTo and ReadFrom operations are completed,
// then it stops accepting remote messages for its streams,
// then it returns.
func (s *Streamer) Shutdown() error {
	return fmt.Errorf("Not implemented")
}
