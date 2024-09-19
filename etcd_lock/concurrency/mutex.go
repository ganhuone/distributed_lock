package concurrency

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	v3 "go.etcd.io/etcd/client/v3"
)

// ErrLocked is returned by TryLock when Mutex is already locked by another session.
var ErrLocked = errors.New("mutex: Locked by another session")
var ErrSessionExpired = errors.New("mutex: session is expired")
var ErrLockReleased = errors.New("mutex: lock has already been released")

type Mutex struct {
	s *Session

	pfx   string
	myKey string
	myRev int64
	hdr   *pb.ResponseHeader
}

func NewMutex(s *Session, pfx string) *Mutex {
	return &Mutex{
		s:     s,
		pfx:   pfx + "/",
		myKey: "",
		myRev: -1,
		hdr:   nil,
	}
}

func (m *Mutex) TryLock(ctx context.Context) error {
	resp, err := m.tryAcquire(ctx)
	if err != nil {
		return err
	}

	ownerKey := resp.Responses[1].GetResponseRange().Kvs
	if len(ownerKey) == 0 || ownerKey[0].CreateRevision == m.myRev {
		m.hdr = resp.Header
		return nil
	}

	client := m.s.Client()
	if _, err := client.Delete(ctx, m.myKey); err != nil {
		return err
	}
	m.myKey = "\x00"
	m.myRev = -1
	return ErrLocked

}

func (m *Mutex) Lock(ctx context.Context) error {
	resp, err := m.tryAcquire(ctx)
	if err != nil {
		return err
	}

	ownerKey := resp.Responses[1].GetResponseRange().Kvs
	if len(ownerKey) == 0 || ownerKey[0].CreateRevision == m.myRev {
		m.hdr = resp.Header
		return nil
	}

	client := m.s.Client()
	werr := waitDeletes(ctx, client, m.pfx, m.myRev-1)

	if werr != nil {
		m.Unlock(client.Ctx())
		return werr
	}

	gresp, werr := client.Get(ctx, m.myKey)
	if werr != nil {
		m.Unlock(client.Ctx())
		return werr
	}

	if len(gresp.Kvs) == 0 {
		return ErrSessionExpired
	}
	m.hdr = gresp.Header

	return nil
}

func (m *Mutex) tryAcquire(ctx context.Context) (*v3.TxnResponse, error) {
	s := m.s
	client := m.s.client
	m.myKey = fmt.Sprintf("%s%x", m.pfx, s.Lease())
	cmp := v3.Compare(v3.CreateRevision(m.myKey), "=", 0)

	put := v3.OpPut(m.myKey, "", v3.WithLease(s.Lease()))

	get := v3.OpGet(m.myKey)

	getOwner := v3.OpGet(m.pfx, v3.WithFirstCreate()...)

	resp, err := client.Txn(ctx).If(cmp).Then(put, getOwner).Else(get, getOwner).Commit()
	if err != nil {
		return nil, err
	}

	m.myRev = resp.Header.Revision
	if !resp.Succeeded {
		m.myRev = resp.Responses[0].GetResponseRange().Kvs[0].CreateRevision
	}
	return resp, nil

}

func (m *Mutex) Unlock(ctx context.Context) error {
	if m.myKey == "" || m.myRev <= 0 || m.myKey == "\x00" {
		return ErrLockReleased
	}

	if !strings.HasPrefix(m.myKey, m.pfx) {
		return fmt.Errorf("invalid key %q, it should have prefix %q", m.myKey, m.pfx)
	}

	client := m.s.Client()
	if _, err := client.Delete(ctx, m.myKey); err != nil {
		return err
	}

	m.myKey = "\x00"
	m.myRev = -1
	return nil
}

func (m *Mutex) IsOwner() v3.Cmp {
	return v3.Compare(v3.CreateRevision(m.myKey), "=", m.myRev)
}

func (m *Mutex) Key() string { return m.myKey }

func (m *Mutex) Header() *pb.ResponseHeader { return m.hdr }

type lockerMutex struct{ *Mutex }

func (lm *lockerMutex) Lock() {
	client := lm.s.Client()
	if err := lm.Mutex.Lock(client.Ctx()); err != nil {
		panic(err)
	}
}
func (lm *lockerMutex) Unlock() {
	client := lm.s.Client()
	if err := lm.Mutex.Unlock(client.Ctx()); err != nil {
		panic(err)
	}
}

// NewLocker creates a sync.Locker backed by an etcd mutex.
func NewLocker(s *Session, pfx string) sync.Locker {
	return &lockerMutex{NewMutex(s, pfx)}
}
