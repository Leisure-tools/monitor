package monitor

import (
	"fmt"
	"sync/atomic"
)

var verboseSvc = false
var svcCount int64 = 0

type ChanSvc struct {
	Activity   string
	Owner      ServiceOwner
	Channel    chan func()
	invocation uint64
}

type ServiceOwner interface {
	ShutdownService(c *ChanSvc)
}

type SvcRunner interface {
	Svc(code func(), wrap ...bool)
}

func (s *ChanSvc) String() string {
	return fmt.Sprint("Service: ", s.Owner)
}

func CriticalSvcSync[T any](r SvcRunner, code func() (T, error)) (T, error) {
	return SvcSync(r, code, true, true)
}

func WrappedSvcSync[T any](r SvcRunner, code func() (T, error)) (T, error) {
	return SvcSync(r, code, true)
}

func UnwrappedSvcSync[T any](r SvcRunner, code func() (T, error)) (T, error) {
	return SvcSync(r, code)
}

func SvcSync[T any](r SvcRunner, code func() (T, error), wrap ...bool) (T, error) {
	result := make(chan bool)
	var value T
	var err error

	r.Svc(func() {
		value, err = code()
		result <- true
	}, wrap...)
	<-result
	return value, err
}

func (s *ChanSvc) Svc(code func(), wrap ...bool) {
	go func() { // using a goroutine so the channel won't block
		if len(wrap) > 0 && wrap[0] {
			defer func() {
				if e := frecovery(s.Activity); e != nil {
					if len(wrap) > 1 && wrap[1] {
						s.Shutdown()
					}
				}
			}()
		}
		if verboseSvc {
			count := atomic.AddInt64(&svcCount, 1)
			fmt.Printf("@@ QUEUE SVC %d\n", count)
			s.Channel <- func() {
				fmt.Printf("@@ START SVC %d [%d]\n", count, atomic.LoadInt64(&svcCount))
				code()
				fmt.Printf("@@ END SVC %d [%d]\n", count, atomic.LoadInt64(&svcCount))
			}
		} else {
			s.Channel <- code
		}
	}()
}

// Run a service. Close the channel to stop it.
func NewSvc(owner ServiceOwner) *ChanSvc {
	s := &ChanSvc{
		Owner:   owner,
		Channel: make(chan func()),
	}
	go func() {
		for {
			cmd, ok := <-s.Channel
			if !ok {
				break
			}
			s.invocation++
			cmd()
		}
	}()
	return s
}

// verify the service is currently running this code
func (s *ChanSvc) Verify(i uint64) {
	if i != s.invocation {
		abort("Bad invocation, service %v expected %d but got %d", s, s.invocation, i)
	}
}

// safely shutdown the service
func (s *ChanSvc) Shutdown() {
	s.Svc(func() {
		close(s.Channel)
		s.Owner.ShutdownService(s)
	})
}
