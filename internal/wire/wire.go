// Package wire provides protobuf message framing for the stalker protocol.
//
// Messages are length-delimited using protobuf's standard varint encoding.
// This allows efficient streaming of variable-length messages over TCP.
package wire

import (
	"bufio"
	"fmt"
	"io"
	"sync"

	"github.com/xtxerr/stalker/config"
	"github.com/xtxerr/stalker/internal/errors"
	pb "github.com/xtxerr/stalker/internal/proto"
	"google.golang.org/protobuf/encoding/protodelim"
)

// Reader reads length-delimited protobuf envelopes from an io.Reader.
// It is safe for concurrent use.
type Reader struct {
	r  *bufio.Reader
	mu sync.Mutex
}

// NewReader creates a Reader wrapping the given io.Reader.
func NewReader(r io.Reader) *Reader {
	return &Reader{r: bufio.NewReader(r)}
}

// Read reads and unmarshals the next envelope.
// Returns an error if the message exceeds MaxMessageSize.
func (r *Reader) Read() (*pb.Envelope, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	env := &pb.Envelope{}
	opts := protodelim.UnmarshalOptions{
		MaxSize: config.DefaultMaxMessageSize,
	}
	if err := opts.UnmarshalFrom(r.r, env); err != nil {
		return nil, fmt.Errorf("read envelope: %w", err)
	}
	return env, nil
}

// Writer writes length-delimited protobuf envelopes to an io.Writer.
// It is safe for concurrent use.
type Writer struct {
	w  io.Writer
	mu sync.Mutex
}

// NewWriter creates a Writer wrapping the given io.Writer.
func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w}
}

// Write marshals and writes an envelope with length prefix.
func (w *Writer) Write(env *pb.Envelope) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := protodelim.MarshalTo(w.w, env); err != nil {
		return fmt.Errorf("write envelope: %w", err)
	}
	return nil
}

// Conn combines Reader and Writer for bidirectional communication.
type Conn struct {
	*Reader
	*Writer
}

// NewConn creates a Conn from an io.ReadWriter (e.g., net.Conn).
func NewConn(rw io.ReadWriter) *Conn {
	return &Conn{
		Reader: NewReader(rw),
		Writer: NewWriter(rw),
	}
}

// =============================================================================
// Error Envelope Helpers
// =============================================================================

// NewError creates an error envelope with the given request ID, error code, and message.
// Error codes should be from the errors package (errors.Code*).
func NewError(id uint64, code int32, msg string) *pb.Envelope {
	return &pb.Envelope{
		Id: id,
		Payload: &pb.Envelope_Error{
			Error: &pb.Error{
				Code:    code,
				Message: msg,
			},
		},
	}
}

// NewErrorFromErr creates an error envelope from a Go error.
// It automatically maps the error to the appropriate wire code using errors.ErrorToCode.
func NewErrorFromErr(id uint64, err error) *pb.Envelope {
	code := errors.ErrorToCode(err)
	return NewError(id, code, err.Error())
}

// NewErrorf creates an error envelope with a formatted message.
func NewErrorf(id uint64, code int32, format string, args ...interface{}) *pb.Envelope {
	return NewError(id, code, fmt.Sprintf(format, args...))
}
