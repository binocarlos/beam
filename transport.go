package beam

import (
	"net"
)

type NetTransport struct {
	Network string
	Address string
}

func (t *NetTransport) Connect() (net.Conn, error) {
	return net.Dial(t.Network, t.Address)
}
