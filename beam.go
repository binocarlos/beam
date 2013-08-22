// Beam is a protocol and library for service-oriented communication,
// with an emphasis on real-world patterns, simplicity and not reinventing the wheel.
//
// See http://github.com/dotcloud/beam.

package beam

import (
	"bytes"
	"errors"
	"github.com/garyburd/redigo/redis"
	"io"
	"sync"
)

var (
	ErrInvalidResposeType = errors.New("Invalid response type")
)

type DB interface {
}

type Streamer struct {
	pool          *redis.Pool
	InKey         string
	OutKey        string
	isReadsClosed bool
	group         sync.WaitGroup
	mutex         sync.Mutex

	consumers     map[string]chan []byte
	handlers      map[string]chan []byte
	producerCount int
	producer      chan []byte
	errs          chan error
	waits         []chan bool
}

func NewStreamer(pool *redis.Pool, in, out string) *Streamer {
	streamer := &Streamer{
		pool:      pool,
		InKey:     in,
		OutKey:    out,
		consumers: make(map[string]chan []byte),
		handlers:  make(map[string]chan []byte),
		producer:  make(chan []byte, 1024),
		mutex:     sync.Mutex{},
		waits:     make([]chan bool, 0),
	}
	streamer.start()
	go func() {
		for err := range streamer.errs {
			Debugf("Error: %s", err)
		}
	}()
	return streamer
}

func newListener(s *Streamer, name string, c chan []byte) {
	Debugf("Starting listener for: %s", name)
	s.group.Add(1)
	defer s.group.Done()

	for msg := range c {
		for {
			if reader, ok := s.handlers[name]; ok {
				reader <- msg
				break
			} else if s.isReadsClosed {
				Debugf("Discarding message for: %s", name)
				break
			}
		}
	}
	if reader, ok := s.handlers[name]; ok {
		close(reader)
	}
}

func (s *Streamer) start() {
	s.errs = make(chan error, 1024)
	go func() {
		s.group.Add(1)
		defer s.group.Done()

		conn := s.pool.Get()
		defer conn.Close()

		Debugf("Reading from: %s", s.OutKey)
		for {
			reply, err := redis.MultiBulk(conn.Do("BLPOP", s.OutKey, DEFAULTTIMEOUT))
			if err != nil {
				s.errs <- err
				continue
			}
			b := reply[1].([]byte)

			parts := bytes.SplitN(b, []byte(":"), 2)
			if len(parts) < 2 {
				s.errs <- ErrInvalidResposeType
				continue
			}
			name := string(parts[0])
			msg := parts[1]
			if len(msg) == 0 {
				s.mutex.Lock()
				if c, ok := s.consumers[name]; ok {
					close(c)
					delete(s.consumers, name)
				}
				if len(s.consumers) == 0 {
					s.mutex.Unlock()
					break
				}
				s.mutex.Unlock()
				continue
			}
			c, ok := s.consumers[name]
			if !ok {
				s.mutex.Lock()
				c = make(chan []byte, 1024)
				s.consumers[name] = c

				go newListener(s, name, c)
				s.mutex.Unlock()
			}
			c <- msg
		}
		for _, c := range s.consumers {
			close(c)
		}
		s.isReadsClosed = true
		s.group.Done()
	}()

	go func() {
		s.group.Add(1)
		defer s.group.Done()

		conn := s.pool.Get()
		defer conn.Close()

		Debugf("Writing to: %s", s.InKey)
		for msg := range s.producer {
			if _, err := conn.Do("RPUSH", s.InKey, msg); err != nil {
				s.errs <- err
			}
		}
	}()
}

// OpenRead returns a read-only interface to receive data on the stream <name>.
// If the stream hasn't been open for read access before, it is advertised as such to the peer.
func (s *Streamer) OpenRead(name string) io.ReadCloser {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, ok := s.consumers[name]; !ok {
		cs := make(chan []byte, 1024)
		s.consumers[name] = cs
		go newListener(s, name, cs)

	}
	c := make(chan []byte, 1024)
	s.handlers[name] = c
	channel, wait := NewChannel(c, false)
	if wait != nil {
		s.waits = append(s.waits, wait)
	}
	return channel
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
func (s *Streamer) OpenWrite(name string) io.WriteCloser {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.producerCount++
	c := make(chan []byte)
	go func() {
		s.group.Add(1)
		defer s.group.Done()

		for msg := range c {
			s.producer <- formatMessage(name, msg)
		}
		s.producerCount--
		if s.producerCount == 0 {
			close(s.producer)
		}
	}()
	channel, wait := NewChannel(c, true)
	if wait != nil {
		s.waits = append(s.waits, wait)
	}
	return channel
}

func formatMessage(name string, msg []byte) []byte {
	return append([]byte(name+":"), msg...)
}

// WriteTo opens a write-only interface on the stream <name>, and copies data
// from that interface to <dst> until there's no more data to write or when an error occurs.
// The return value n is the number of bytes written.
// Any error encountered during the write is also returned.
func (s *Streamer) WriteTo(dst io.Writer, name string) (int64, error) {
	return 0, nil
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
	close(s.errs)
	s.group.Wait()
	// Wait for all chan streams to end
	for _, w := range s.waits {
		<-w
	}
	return nil
}
