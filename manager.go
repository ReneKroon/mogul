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

// Add a task, with a name (should be unique) and some payload for the consumer to base his/her work on.
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

	clause := bson.M{"location": bson.M{"$near": []float64{rand.Float64(), rand.Float64()}}}
	clause["user"] = bson.M{"$exists": false}
	//clause["expires"]

	updates := bson.M{"user": user}
	if leaseTime != nil {
		updates["expires"] = time.Now().UTC().Add(*leaseTime)
	}

	_, err := m.tasks.Find(clause).Apply(mgo.Change{
		Update:    bson.M{"$set": updates},
		ReturnNew: true,
	}, &result)

	if err != nil {
		return nil, err
	}

	return &result, nil
}

// If the task has been completed succesfully we remove it from the database.
func (m *Manager) Complete(t *Task) error {

	return m.tasks.Remove(t.identity())
}

// If the task failed we remove our claim on the task, to make it available again.
// The job will be run again. If you don't want this then call Complete.
func (m *Manager) Failed(t *Task) error {
	fields := bson.M{"user" : ""}
	fields["expires"] = ""
	unset := bson.M{"$unset": fields}

	return m.tasks.Update(t.identity(), unset)
}

