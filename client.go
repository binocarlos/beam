package beam

import (
	"github.com/garyburd/redigo/redis"
)

const (
	DEFAULTTIMEOUT = 0 // Wait forever
)

type Client struct {
	pool *redis.Pool
}

func NewClient(connector Connector) (*Client, error) {
	return &Client{pool: newConnectionPool(connector, 10)}, nil
}

func (c *Client) NewJob(name string, args ...string) (*Job, error) {
	//job.Streams = NewStreamer(c.pool, fmt.Sprintf("%s/streams/out", job.key()), fmt.Sprintf("%s/streams/in", job.key()))
	return NewJob(c.pool, name, args...)
}

func (c *Client) Close() error {
	return c.pool.Close()
}
