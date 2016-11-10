// mogul - distributed lock & task management via mongoDB
//
// Copyright 2016 - Rene Kroon <kroon.r.w@gmail.com>
//

package mogul

import (
	"time"
	"gopkg.in/mgo.v2/bson"
)

// Task is the entity we work with
type Task struct {
	Name         string     `bson:"_id"`
	User         *string    `bson:"user,omitempty"`
	Data         []byte     `bson:"task"`
	ExpiresAtUtc *time.Time `bson:"expires,omitempty"`
	Doc          meta       `bson:",inline"`
}

type TaskHandler interface {
	Add(name string, data []byte) error
	Next(user string, leaseTime *time.Duration) (*Task, error)
	Complete(*Task) error
	Failed(*Task) error
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