package mogul

import (
	"testing"
	"time"

	"fmt"

	"context"
	"math/rand"
	"sync"

	"github.com/pascaldekloe/goe/verify"
	"gopkg.in/mgo.v2"
	"sync/atomic"
)

func TestSingleNode(t *testing.T) {
	session := initDB(t)
	defer clearDB(t, session)

	l := New("Key", "routine1", session)

	feed := []struct {
		duration time.Duration
		success  bool
		lock     *Mutex
	}{
		0: {
			time.Millisecond * 10000,
			true,
			l,
		},
		1: {
			time.Millisecond * 1000,
			false,
			New("Key", "routine2", session),
		},
		2: {
			time.Millisecond * 1000,
			false,
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

	var wg sync.WaitGroup

	hits := 0

	for i := 1; i <= 10; i++ {
		wg.Add(1)
		go func() {
			time.Sleep(time.Microsecond * time.Duration(rand.Float32()*10000))
			l := New("Multiple", "host", session)
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

	var wg sync.WaitGroup

	var hits int32

	for i := 1; i <= 100; i++ {
		wg.Add(1)
		id := fmt.Sprintf("Multiple%d", i)
		go func() {
			time.Sleep(time.Microsecond * time.Duration(rand.Float32()*10000))
			l := New(id, "host", session)
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

func TestWorkWithLockTwice(t *testing.T) {
	session := initDB(t)
	defer clearDB(t, session)

	l := New("Reclaim after deadline", "host", session)

	timeFrame := time.Millisecond * 100

	deadline := time.Now().Add(timeFrame)
	if got, _ := l.TryLock(timeFrame); got {

		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		defer cancel()

		select {
		case <-ctx.Done():
			break

		}

		got, _ := l.TryLock(timeFrame)
		verify.Values(t, "Failed to get lock after expiration", got, true)
	} else {
		t.Errorf("Fail")
	}

}

func initDB(t *testing.T) *mgo.Session {
	uri := "mongodb://localhost/"
	s, err := mgo.Dial(uri)
	if err != nil {
		t.Fatalf("Connect to %q: %s", uri, err)
	}
	s.SetSafe(&mgo.Safe{FSync: true})

	s.DB(*database).C(*collection).DropCollection()

	return s
}

func clearDB(t *testing.T, s *mgo.Session) {

	s.DB(*database).C(*collection).DropCollection()
	s.Close()

}
