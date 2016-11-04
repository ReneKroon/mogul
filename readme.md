# Mogul - locking over nodes via mongoDB
[![GoDoc](http://godoc.org/github.com/ReneKroon/mogul?status.png)](http://godoc.org/github.com/ReneKroon/mogul) 
[![Build Status](https://travis-ci.org/ReneKroon/mogul.svg?branch=master)](https://travis-ci.org/ReneKroon/mogul)

This packages gives you some functionality to set a global lock for a specified duration. 
Afterwards the lock is up for grabs again. Make sure to use an unique identifier for each
gorouting on each host for the user parameter.

The package uses mongo's atomic handling of documents. A document in the locks collection will 
automatically represent an atomic entity which can be claimed if it does not exists, or when the 
associated lock has expired.


``` Go

func myWorkForSingleNode() {
..

	lock := New("ImportantLock", "user#1", session)
	
	if got, _ := l.TryLock(timeFrame); got {
	    defer lock.Unlock()
	
	    // Do important stuff
	
	}
..
}

```
