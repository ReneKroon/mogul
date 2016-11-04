// mogul - distributed lock via mongoDB
//
// Copyright 2016 - Rene Kroon <kroon.r.w@gmail.com>
//

package mogul

import (
	"flag"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var (
	database   = flag.String("mogul.dbname", "mogul", "database to store locks, defaults to mogul")
	collection = flag.String("mogul.collection", "locks", "collection to use for lock objects, defaults to locks")
)

// Mutex is the object used for locking
type Mutex struct {
	doc        document
	collection *mgo.Collection
}

type document struct {
	Name         string    `bson:"_id"`
	User         string    `bson:"user"`
	ExpiresAtUtc time.Time `bson:"expires"`
}

// Use the new function to combine a mongo session with your lock's name and user
// to obtain a new mutex, which is not locked yet
func New(name string, user string, session *mgo.Session) *Mutex {

	clone := session.Copy()
	clone.SetSafe(&mgo.Safe{WMode: "majority"})
	clone.SetMode(mgo.Strong, false)

	return &Mutex{collection: clone.DB(*database).C(*collection), doc: document{Name: name, User: user}}

}

// Trylock will claim a lock if it is available. It also returns true when you already hold the lock.
// This extends the duration if you already hold the lock.
func (m *Mutex) TryLock(atMost time.Duration) (bool, error) {

	now := time.Now().UTC()
	until := now.Add(atMost)

	m.doc.ExpiresAtUtc = until

	selector := bson.M{"$or": []bson.M{m.identityClause(), bson.M{"$and": []bson.M{bson.M{"_id": m.doc.Name}, bson.M{"expires": bson.M{"$lt": now}}}}}}
	_, err := m.collection.Upsert(selector, &m.doc)

	found := 0
	if err == nil {
		found, err = m.collection.Find(m.identityClause()).Count()
	}

	return found > 0, err
}

// Unlock frees the lock, removing the corresponding record in the database
func (m *Mutex) Unlock() error {
	return m.collection.Remove(m.identityClause())
}

// IsExpired will return true when your lock time has expired
func (m *Mutex) IsExpired() bool {
	return m.doc.ExpiresAtUtc.Before(time.Now().UTC())
}

func (m *Mutex) identityClause() bson.M {
	return bson.M{"$and": []bson.M{bson.M{"_id": m.doc.Name}, bson.M{"user": m.doc.User}}}
}
