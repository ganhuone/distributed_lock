package concurrency

import (
	"context"
	"time"

	"go.uber.org/zap"

	v3 "go.etcd.io/etcd/client/v3"
)

const defaultSessionTTL = 60

type Session struct {
	client *v3.Client
	opts   *sessionOptions
	id     v3.LeaseID

	ctx    context.Context
	cancel context.CancelFunc
	donec  <-chan struct{}
}

func NewSession(client *v3.Client, opts ...SessionOption) (*Session, error) {
	lg := client.GetLogger()

	ops := &sessionOptions{
		ttl: defaultSessionTTL,
		ctx: client.Ctx(),
	}

	for _, opt := range opts {
		opt(ops, lg)
	}

	id := ops.leaseID

	if id == v3.NoLease {
		resp, err := client.Grant(ops.ctx, int64(ops.ttl))
		if err != nil {
			return nil, err
		}
		id = resp.ID
	}

	ctx, cancel := context.WithCancel(ops.ctx)
	keepAlive, err := client.KeepAlive(ctx, id)
	if err != nil || keepAlive == nil {
		cancel()
		return nil, err
	}

	donec := make(chan struct{})
	s := &Session{
		client: client,
		opts:   ops,
		id:     id,
		ctx:    ctx,
		cancel: cancel,
		donec:  donec,
	}

	go func() {
		defer func() {
			close(donec)
			cancel()
		}()

		for range keepAlive {

		}
	}()

	return s, nil
}

func (s *Session) Client() *v3.Client {
	return s.client
}

func (s *Session) Lease() v3.LeaseID {
	return s.id
}

func (s *Session) Ctx() context.Context {
	return s.ctx
}

func (s *Session) Done() <-chan struct{} {
	return s.donec
}

func (s *Session) Orphan() {
	s.cancel()
	<-s.donec
}

func (s *Session) Close() error {
	s.Orphan()

	ctx, cancel := context.WithTimeout(s.opts.ctx, time.Duration(s.opts.ttl)*time.Second)
	_, err := s.client.Revoke(ctx, s.id)
	cancel()
	return err
}

type sessionOptions struct {
	ttl     int
	leaseID v3.LeaseID
	ctx     context.Context
}

type SessionOption func(*sessionOptions, *zap.Logger)

func WithTTL(ttl int) SessionOption {
	return func(so *sessionOptions, lg *zap.Logger) {
		if ttl > 0 {
			so.ttl = ttl
		} else {
			lg.Warn("WithTTL(): TTL should be > 0, preserving current TTL", zap.Int64("current-session-ttl", int64(so.ttl)))
		}
	}
}

func WithLease(leaseID v3.LeaseID) SessionOption {
	return func(so *sessionOptions, _ *zap.Logger) {
		so.leaseID = leaseID
	}
}

func WithContext(ctx context.Context) SessionOption {
	return func(so *sessionOptions, _ *zap.Logger) {
		so.ctx = ctx
	}
}
