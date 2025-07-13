package server

import (
	"fmt"
	"sync/atomic"
)

var verboseSvc = false
var svcCount int64 = 0

type ChanSvc chan func()

func SvcSync[T any](s ChanSvc, code func() (T, error)) (T, error) {
	result := make(chan bool)
	var value T
	var err error
	Svc(s, func() {
		value, err = code()
		result <- true
	})
	<-result
	return value, err
}

func Svc(s ChanSvc, code func()) {
	go func() { // using a goroutine so the channel won't block
		if verboseSvc {
			count := atomic.AddInt64(&svcCount, 1)
			fmt.Printf("@@ QUEUE SVC %d\n", count)
			s <- func() {
				fmt.Printf("@@ START SVC %d [%d]\n", count, atomic.LoadInt64(&svcCount))
				code()
				fmt.Printf("@@ END SVC %d [%d]\n", count, atomic.LoadInt64(&svcCount))
			}
		} else {
			s <- code
		}
	}()
}

// Run a service. Close the channel to stop it.
func RunSvc(s ChanSvc) {
	go func() {
		for {
			cmd, ok := <-s
			if !ok {
				break
			}
			cmd()
		}
	}()
}
