// Package mogul provides distributed locking and task handling via mongodb.
//
// Using mongo documents we can synchronize and do work over a number of nodes.
// The typical usecase would be to run a cron job or scheduler on all nodes and then perform
// task creation on a single node using a lock. These tasks can then be efficiently executed
// on all nodes that are alive.
package mogul

import (
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"math/rand"
	"time"
)

type Manager struct {
	locks *mgo.Collection
	tasks *mgo.Collection
}

// New creates a new manager to perform locking and task management. You can cast it to
// TaskHandler or MutexCreator to limit the behaviour to one of the two subjects.
func New(locks *mgo.Collection, tasks *mgo.Collection) *Manager {

	clone := locks.Database.Session.Clone()
	clone.SetSafe(&mgo.Safe{WMode: "majority"})
	clone.SetMode(mgo.Strong, false)

	clone2 := tasks.Database.Session.Clone()
	clone2.SetSafe(&mgo.Safe{WMode: "majority"})
	clone2.SetMode(mgo.Strong, false)

	index := mgo.Index{
		Name: "taskDistribution",
		Key:  []string{"$2d:location"},
		Bits: 26,
	}

	clone2.DB(tasks.Database.Name).C(tasks.Name).EnsureIndex(index)

	return &Manager{
		locks: clone.DB(locks.Database.Name).C(locks.Name),
		tasks: clone2.DB(tasks.Database.Name).C(tasks.Name),
	}
}

// Use the new function to combine a mongo session with your lock's name and user
// to obtain a new mutex, which is not locked yet
func (m *Manager) NewMutex(name string, user string) *Mutex {

	return &Mutex{collection: m.locks, doc: lock{Name: name, User: user}}

}

// Add a task, with a name (should be unique for different jobs) and some payload for the consumer to base his/her work on.
func (m *Manager) Add(name string, data []byte) error {
	// https://gist.github.com/icchan/bd42095afd8305594778
	err := m.tasks.Insert(&Task{Name: name, Data: data, Doc: meta{Location: []float64{rand.Float64(), rand.Float64()}}})
	return err
}

// Next picks the next available task. You can give a lease time after which
// the job is back up for grabs for other consumers if its not completed.
// next task is selected via random points in a 2D array that are assigned to all tasks:
// http://stackoverflow.com/questions/2824157/random-record-from-mongodb at bottom
func (m *Manager) Next(user string, leaseTime *time.Duration) (*Task, error) {

	var result Task

	now := time.Now().UTC()

	unclaimed := bson.M{"user": bson.M{"$exists": false}}
	expired := bson.M{"expires": bson.M{"$lt": now}}
	unclaimedOrExpired := bson.M{"$or": []bson.M{unclaimed, expired}}

	nearest := bson.M{"location": bson.M{"$near": []float64{rand.Float64(), rand.Float64()}}}

	clause := bson.M{"$and": []bson.M{nearest, unclaimedOrExpired}}

	updates := bson.M{"user": user}
	updateClause := bson.M{}
	if leaseTime != nil {
		updates["expires"] = time.Now().UTC().Add(*leaseTime)
		updateClause = bson.M{"$set": updates}
	} else {
		updateClause = bson.M{"$set": updates, "$unset": bson.M{"expires": ""}}
	}

	_, err := m.tasks.Find(clause).Apply(mgo.Change{
		Update:    updateClause,
		ReturnNew: true,
	}, &result)

	if err != nil {
		return nil, err
	}

	result.collection = m.tasks
	return &result, nil
}
