package redis_lock

import (
	"context"
	"errors"
	"fmt"
	"redis_lock/utils"
	"sync/atomic"
	"time"

	"github.com/gomodule/redigo/redis"
)

const RedisLockKeyPrefix = "REDIS_LOCK_PREFIX"

var ErrLockAcquiredByOthers = errors.New("lock is acquired by others")

var ErrNil = redis.ErrNil

func IsRetryableErr(err error) bool {
	return errors.Is(err, ErrLockAcquiredByOthers)
}

type RedisLock struct {
	LockOptions
	key    string
	token  string
	client LockClient

	runningDog int32
	stopDog    context.CancelFunc
}

func NewRedisLock(key string, client LockClient, opts ...LockOption) *RedisLock {
	r := RedisLock{
		key:    key,
		token:  utils.GetProcessAndGoroutineIDStr(),
		client: client,
	}

	for _, opt := range opts {
		opt(&r.LockOptions)
	}

	repairLock(&r.LockOptions)

	return &r
}

func (r *RedisLock) Lock(ctx context.Context) (err error) {
	defer func() {
		if err != nil {
			return
		}

		r.watchDog(ctx)
	}()

	err = r.tryLock(ctx)
	if err == nil {
		return nil
	}

	if !r.isBlock {
		return err
	}

	if !IsRetryableErr(err) {
		return err
	}

	err = r.blockingLock(ctx)
	return

}

func (r *RedisLock) blockingLock(ctx context.Context) error {
	timeoutCh := time.After(time.Duration(r.blockWaitingSeconds) * time.Second)

	ticker := time.NewTicker(time.Duration(50) * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		select {
		case <-ctx.Done():
			return fmt.Errorf("lock failed, ctx timeout, err: %w", ctx.Err())
		case <-timeoutCh:
			return fmt.Errorf("block waiting time out err: %w", ErrLockAcquiredByOthers)
		default:
		}

		err := r.tryLock(ctx)
		if err == nil {
			return nil
		}

		if !IsRetryableErr(err) {
			return err
		}

	}
	return nil
}

func (r *RedisLock) tryLock(ctx context.Context) error {
	reply, err := r.client.SetNEX(ctx, r.getLockKey(), r.token, r.expireSeconds)
	if err != nil {
		return err
	}

	if reply != 1 {
		return fmt.Errorf("reply: %d, err: %w", reply, ErrLockAcquiredByOthers)
	}

	return nil
}

func (r *RedisLock) Unlock(ctx context.Context) error {
	defer func() {
		if r.stopDog != nil {
			r.stopDog()
		}
	}()

	keysAndArgs := []interface{}{r.getLockKey(), r.token}
	reply, err := r.client.Eval(ctx, LuaCheckAndDeleteDistributionLock, 1, keysAndArgs)
	if err != nil {
		return err
	}

	if ret, _ := reply.(int64); ret != 1 {
		return errors.New("can not unlock without ownership of lock")
	}

	return nil
}

func (r *RedisLock) getLockKey() string {
	return RedisLockKeyPrefix + r.key
}

func (r *RedisLock) watchDog(ctx context.Context) {
	if !r.watchDogMode {
		return
	}

	for !atomic.CompareAndSwapInt32(&r.runningDog, 0, 1) {

	}

	ctx, r.stopDog = context.WithCancel(ctx)

	go func() {
		defer func() {
			atomic.StoreInt32(&r.runningDog, 0)
		}()

		r.runWatchDog(ctx)
	}()

}

func (r *RedisLock) runWatchDog(ctx context.Context) {
	ticker := time.NewTicker(WatchDogWorkStepSeconds * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		select {
		case <-ctx.Done():
			return
		default:
		}

		_ = r.DelayExpire(ctx, WatchDogWorkStepSeconds+5)
	}
}

func (r *RedisLock) DelayExpire(ctx context.Context, expireSeconds int64) error {
	keysAndArgs := []interface{}{r.getLockKey(), r.token, expireSeconds}

	reply, err := r.client.Eval(ctx, LuaCheckAndExpireDistributionLock, 1, keysAndArgs)
	if err != nil {
		return err
	}

	if ret, _ := reply.(int64); ret != 1 {
		return errors.New("can not expire lock without ownership of lock")
	}

	return nil
}
