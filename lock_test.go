package mogul

import (
	"testing"
	"time"

	"fmt"

	"context"
	"math/rand"
	"sync"

	"github.com/pascaldekloe/goe/verify"
	"sync/atomic"
)

func TestSingleNode(t *testing.T) {
	session := initDB(t)
	defer clearDB(t, session)

	var m MutexCreator = New(session.DB(database).C(collection), session.DB(database).C("tasks"))

	l := m.NewMutex("Key", "routine1")

	feed := []struct {
		duration time.Duration
		success  bool
		lock     *Mutex
	}{
		0: {
			time.Hour * 1,
			true,
			l,
		},
		1: {
			time.Millisecond * 1000,
			false,
			m.NewMutex("Key", "routine2"),
		},
		2: {
			time.Millisecond * 1000,
			true,
			l,
		},
	}

	for i, v := range feed {
		got, _ := v.lock.TryLock(v.duration)
		verify.Values(t, fmt.Sprintf("%d : Outcome of lock was not ok", i), got, v.success)
		defer v.lock.Unlock()
	}

}

func TestMultipleRoutines(t *testing.T) {
	session := initDB(t)
	defer clearDB(t, session)

	var m MutexCreator = New(session.DB(database).C(collection), session.DB(database).C("tasks"))

	var wg sync.WaitGroup

	hits := 0

	for i := 1; i <= 10; i++ {
		wg.Add(1)
		user := fmt.Sprintf("User#%d", i)
		go func() {
			time.Sleep(time.Microsecond * time.Duration(rand.Float32()*10000))
			l := m.NewMutex("Multiple", user)
			if got, _ := l.TryLock(time.Hour); got {
				hits++

			}
			wg.Done()
		}()
	}
	wg.Wait()

	verify.Values(t, "Should get single value", hits, 1)
}

func TestManyLocks(t *testing.T) {
	session := initDB(t)
	defer clearDB(t, session)

	var m MutexCreator = New(session.DB(database).C(collection), session.DB(database).C("tasks"))

	var wg sync.WaitGroup

	var hits int32

	for i := 1; i <= 100; i++ {
		wg.Add(1)
		id := fmt.Sprintf("Multiple%d", i)
		go func() {
			time.Sleep(time.Microsecond * time.Duration(rand.Float32()*10000))
			l := m.NewMutex(id, "host")
			if got, err := l.TryLock(time.Hour); got {
				defer l.Unlock()
				atomic.AddInt32(&hits, 1)

			} else if err != nil {
				t.Errorf("Fail %s", err)
			}
			wg.Done()
		}()
	}
	wg.Wait()

	verify.Values(t, "Should get one value per iteration", hits, int32(100))
}

func TestExtendTime(t *testing.T) {
	session := initDB(t)
	defer clearDB(t, session)

	var m MutexCreator = New(session.DB(database).C(collection), session.DB(database).C("tasks"))

	l := m.NewMutex("Reclaim after deadline", "host")

	l.TryLock(time.Minute)
	l.TryLock(time.Minute * 5)

	if l.doc.ExpiresAtUtc.Sub(time.Now().UTC()) < time.Minute {
		t.Error("Deadline should be extended")
	}

	l.Unlock()
}

func TestMutex_IsExpired(t *testing.T) {
	session := initDB(t)
	defer clearDB(t, session)

	var m MutexCreator = New(session.DB(database).C(collection), session.DB(database).C("tasks"))

	l := m.NewMutex("Key", "routine1")

	l.TryLock(time.Millisecond * 100)
	verify.Values(t, "Not expired at start", l.IsExpired(), false)
	time.Sleep(time.Millisecond * 100)
	verify.Values(t, "Not expired at start", l.IsExpired(), true)
}

func TestWorkWithLockTwice(t *testing.T) {
	session := initDB(t)
	defer clearDB(t, session)

	var m MutexCreator = New(session.DB(database).C(collection), session.DB(database).C("tasks"))

	l := m.NewMutex("Reclaim after deadline", "host")
	next := m.NewMutex("Reclaim after deadline", "second host")

	timeFrame := time.Millisecond * 100

	if got, _ := l.TryLock(timeFrame); got {
		deadline := time.Now().Add(timeFrame)

		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		defer cancel()

		select {
		case <-ctx.Done():
			break

		}

		got, _ := next.TryLock(timeFrame)
		verify.Values(t, "Failed to get lock after expiration", got, true)
	} else {
		t.Errorf("Fail")
	}

}
