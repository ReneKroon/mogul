package mogul_test

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/ReneKroon/mogul"
	"github.com/globalsign/mgo"
)

var database = "mogul"
var collection = "locks"
var tasks = "tasks"

func ExampleManager_TryLock() {
	session := initDB()
	defer clearDB(session)

	var m mogul.MutexCreator = mogul.New(session.DB(database).C(collection), session.DB(database).C(tasks))

	var wg sync.WaitGroup

	hits := 0

	// Lets started some goroutines that sleep for a while and then try to increment a counter.
	// If the lock is acquired by one routine, the others will fail and not increment.
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

	fmt.Println(hits)
}

func initDB() *mgo.Session {
	uri := "mongodb://localhost/"
	s, _ := mgo.Dial(uri)

	s.SetSafe(&mgo.Safe{FSync: true})

	s.DB(database).C(collection).DropCollection()

	return s
}

func clearDB(s *mgo.Session) {

	s.DB(database).C(collection).DropCollection()
	s.DB(database).C(tasks).DropCollection()
	s.Close()

}
