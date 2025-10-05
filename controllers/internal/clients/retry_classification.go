package clients

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
)

func retryableError(err error, retryTimeout bool) bool {
	switch {
	case err == nil:
		return false
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return false
	}

	switch err {
	case io.EOF, io.ErrUnexpectedEOF:
		return true
	}

	if err != nil && err.Error() == "redis: connection pool timeout" {
		return true
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return retryTimeout
		}
		return true
	}

	msg := err.Error()
	switch {
	case msg == "ERR max number of clients reached":
		return true
	case strings.HasPrefix(msg, "LOADING "):
		return true
	case strings.HasPrefix(msg, "READONLY "):
		return true
	case strings.HasPrefix(msg, "MASTERDOWN "):
		return true
	case strings.HasPrefix(msg, "CLUSTERDOWN "):
		return true
	case strings.HasPrefix(msg, "TRYAGAIN "):
		return true
	}

	return false
}
