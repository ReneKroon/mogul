// mogul - distributed lock & task management via mongoDB
//
// Copyright 2016 - Rene Kroon <kroon.r.w@gmail.com>
//

package mogul

import (
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

// Task is the entity we work with to regulate jobs. It consists of a name and a payload.
// If a task is claimed the user and optinal expiresAtUtc will be filled.
type Task struct {
	Name         string          `bson:"_id"`
	User         *string         `bson:"user,omitempty"`
	Data         []byte          `bson:"task"`
	ExpiresAtUtc *time.Time      `bson:"expires,omitempty"`
	Doc          meta            `bson:",inline"`
	collection   *mgo.Collection `bson:"-"`
}

// If the task has been completed succesfully we remove it from the database.
func (t *Task) Complete() error {
	return t.collection.Remove(t.identity())
}

// If the task failed we remove our claim on the task, to make it available again.
// The job will be run again. If you don't want this then call Complete.
func (t *Task) Failed() error {
	fields := bson.M{"user": ""}
	fields["expires"] = ""
	unset := bson.M{"$unset": fields}

	return t.collection.Update(t.identity(), unset)
}

// You can cast the Manager object to a TaskHandler to have a dedicated object
// for modifying tasks.
type TaskHandler interface {
	Add(name string, data []byte) error
	Next(user string, leaseTime *time.Duration) (*Task, error)
}

type meta struct {
	Location []float64 `bson:"location"`
}

type geoJson struct {
	Type        string    `json:"-"`
	Coordinates []float64 `json:"coordinates"`
}

func (t *Task) identity() bson.M {
	fields := bson.M{"_id": t.Name}
	fields["user"] = t.User

	return fields
}
