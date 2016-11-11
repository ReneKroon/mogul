package mogul_test

import (
	"fmt"
	"github.com/ReneKroon/mogul"
)

func ExampleManager_Add() {

	session := initDB()
	defer clearDB(session)

	var m mogul.TaskHandler = mogul.New(session.DB(database).C(collection), session.DB(database).C(tasks))

	payload := []byte("barfbarf")
	user := "testUser"
	name := "firstTask"

	// create a task
	m.Add(name, payload)

	// the only task we just created should pop
	task, _ := m.Next(user, nil)

	fmt.Println(task)
}
