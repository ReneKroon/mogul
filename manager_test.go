package mogul

import (
	"github.com/pascaldekloe/goe/verify"
	"gopkg.in/mgo.v2"
	"testing"
	"time"
)

var database = "mogul"
var collection = "locks"
var tasks = "tasks"

func TestInsertRetrieveTask(t *testing.T) {

	session := initDB(t)
	defer clearDB(t, session)

	var m TaskHandler = New(session.DB(database).C(collection), session.DB(database).C(tasks))

	payload := []byte("barfbarf")
	user := "testUser"
	name := "firstTask"

	err := m.Add(name, payload)
	if err != nil {
		t.Fatal(err)
	}

	task, err := m.Next(user, nil)

	if err != nil {
		t.Fatal(err)
	}

	verify.Values(t, "Should have gotten single task back out", task.Name, name)
	verify.Values(t, "Should have payload back out", task.Data, payload)
	verify.Values(t, "Should have been assigned to me", *task.User, user)

}

func TestCompleteTask(t *testing.T) {
	session := initDB(t)
	defer clearDB(t, session)

	var m TaskHandler = New(session.DB(database).C(collection), session.DB(database).C(tasks))

	payload := []byte("barfbarf")
	user := "testUser"
	name := "firstTask"

	err := m.Add(name, payload)
	if err != nil {
		t.Fatal(err)
	}

	task, err := m.Next(user, nil)

	if err != nil {
		t.Fatal(err)
	}

	task.Complete()
	task, err = m.Next(user, nil)

	if task != nil {
		t.Fatalf("Got task %v where none should have been found", task)
	}

	if err != mgo.ErrNotFound {
		t.Fatal("Nothing should have been found")
	}

}

func TestFailTask(t *testing.T) {
	session := initDB(t)
	defer clearDB(t, session)

	var m TaskHandler = New(session.DB(database).C(collection), session.DB(database).C(tasks))

	payload := []byte("barfbarf")
	user := "testUser"
	name := "firstTask"

	err := m.Add(name, payload)
	if err != nil {
		t.Fatal(err)
	}

	lease := time.Hour
	task, err := m.Next(user, &lease)

	if err != nil {
		t.Fatal(err)
	}

	task.Failed()
	task, err = m.Next("newUser", nil)

	if err != nil {
		t.Fatal(err)
	}

	var noLease *time.Time

	verify.Values(t, "Should have gotten single task back out", task.Name, name)
	verify.Values(t, "Should have payload back out", task.Data, payload)
	verify.Values(t, "Should have been assigned to me", *task.User, "newUser")
	verify.Values(t, "Should have no expires value", task.ExpiresAtUtc, noLease)

}

func TestClaimAfterExpirationWithNewExpiration(t *testing.T) {
	session := initDB(t)
	defer clearDB(t, session)

	var m TaskHandler = New(session.DB(database).C(collection), session.DB(database).C(tasks))

	payload := []byte("barfbarf")
	user := "testUser"
	name := "firstTask"

	err := m.Add(name, payload)
	if err != nil {
		t.Fatal(err)
	}

	lease := time.Microsecond
	_, err = m.Next(user, &lease)

	time.Sleep(time.Millisecond)
	lease2 := time.Hour
	task, err := m.Next("u2", &lease2)

	if err != nil {
		t.Fatal(err)
	}

	verify.Values(t, "Should have gotten single task back out", task.Name, name)
	verify.Values(t, "Should have payload back out", task.Data, payload)
	verify.Values(t, "Should have been assigned to me", *task.User, "u2")
	if !task.ExpiresAtUtc.UTC().After(time.Now().UTC()) {
		t.Fatal("Expiration is in future")
	}

}

func TestClaimAfterExpirationWithoutNewExpiration(t *testing.T) {
	session := initDB(t)
	defer clearDB(t, session)

	var m TaskHandler = New(session.DB(database).C(collection), session.DB(database).C(tasks))

	payload := []byte("barfbarf")
	user := "testUser"
	name := "firstTask"

	err := m.Add(name, payload)
	if err != nil {
		t.Fatal(err)
	}

	lease := time.Microsecond
	_, err = m.Next(user, &lease)

	time.Sleep(time.Millisecond)
	task, err := m.Next("u2", nil)

	if err != nil {
		t.Fatal(err)
	}

	var noLease *time.Time

	verify.Values(t, "Should have gotten single task back out", task.Name, name)
	verify.Values(t, "Should have payload back out", task.Data, payload)
	verify.Values(t, "Should have been assigned to me", *task.User, "u2")
	verify.Values(t, "Should have no expires value", task.ExpiresAtUtc, noLease)
}

func initDB(t *testing.T) *mgo.Session {
	uri := "mongodb://localhost/"
	s, err := mgo.Dial(uri)
	if err != nil {
		t.Fatalf("Connect to %q: %s", uri, err)
	}
	s.SetSafe(&mgo.Safe{FSync: true})

	s.DB(database).C(collection).DropCollection()

	return s
}

func clearDB(t *testing.T, s *mgo.Session) {

	s.DB(database).C(collection).DropCollection()
	s.DB(database).C(tasks).DropCollection()
	s.Close()

}
