package redis_lock

import "time"

const (
	DefaultIdleTimeoutSeconds = 10
	DefaultMaxActive          = 100
	DefaultMaxIdle            = 20
	DefaultLockExpireSeconds  = 30
	WatchDogWorkStepSeconds   = 10
)

type ClientOptions struct {
	maxIdle            int
	idleTimeoutSeconds int
	maxActive          int
	wait               bool

	network  string
	address  string
	password string
}

type ClientOption func(c *ClientOptions)

func WithMaxIdle(maxIdle int) ClientOption {
	return func(c *ClientOptions) {
		c.maxIdle = maxIdle
	}
}

func WithIdleTimeoutSeconds(idleTimeoutSeconds int) ClientOption {
	return func(c *ClientOptions) {
		c.idleTimeoutSeconds = idleTimeoutSeconds
	}
}

func WithMaxActive(maxActive int) ClientOption {
	return func(c *ClientOptions) {
		c.maxActive = maxActive
	}
}

func WithWaitMode() ClientOption {
	return func(c *ClientOptions) {
		c.wait = true
	}
}

func repairClient(c *ClientOptions) {
	if c.maxIdle < 0 {
		c.maxIdle = DefaultMaxIdle
	}

	if c.idleTimeoutSeconds < 0 {
		c.idleTimeoutSeconds = DefaultIdleTimeoutSeconds
	}

	if c.maxActive < 0 {
		c.maxActive = DefaultMaxActive
	}
}

type LockOptions struct {
	isBlock             bool
	blockWaitingSeconds int64
	expireSeconds       int64
	watchDogMode        bool
}

type LockOption func(l *LockOptions)

func WithBlock() LockOption {
	return func(l *LockOptions) {
		l.isBlock = true
	}
}

func WithBlockWaitingSeconds(blockWaitingSeconds int64) LockOption {
	return func(l *LockOptions) {
		l.blockWaitingSeconds = blockWaitingSeconds
	}
}

func WithExpireSeconds(expireSeconds int64) LockOption {
	return func(l *LockOptions) {
		l.expireSeconds = expireSeconds
	}
}

func repairLock(l *LockOptions) {
	if l.isBlock && l.blockWaitingSeconds <= 0 {
		l.blockWaitingSeconds = 5
	}

	if l.expireSeconds > 0 {
		return
	}

	l.expireSeconds = DefaultLockExpireSeconds
	l.watchDogMode = true
}

type RedLockOptions struct {
	singleNodesTimeout time.Duration
	expireDuration     time.Duration
}

type RedLockOption func(r *RedLockOptions)

func WithSingleNodesTimeout(singleNodesTimeout time.Duration) RedLockOption {
	return func(r *RedLockOptions) {
		r.singleNodesTimeout = singleNodesTimeout
	}
}

func WithRedLockExpireDuration(expireDuration time.Duration) RedLockOption {
	return func(r *RedLockOptions) {
		r.expireDuration = expireDuration
	}
}

func repairRedLock(r *RedLockOptions) {
	if r.singleNodesTimeout <= 0 {
		r.singleNodesTimeout = DefaultSingleLockTimeout
	}
}

type SingleNodeConf struct {
	Network  string
	Address  string
	Password string
	Opts     []ClientOption
}
