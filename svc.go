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
	Svc        chan func()
	invocation uint64
}

type ServiceOwner interface {
	ShutdownService(svc *ChanSvc)
}

func (s *ChanSvc) String() string {
	return fmt.Sprint("Service: ", s.Owner)
}

func WrappedSvcSync[T any](s *ChanSvc, code func() (T, error)) (T, error) {
	return SvcSync(s, true, code)
}

func UnwrappedSvcSync[T any](s *ChanSvc, code func() (T, error)) (T, error) {
	return SvcSync(s, false, code)
}

func SvcSync[T any](s *ChanSvc, wrap bool, code func() (T, error)) (T, error) {
	result := make(chan bool)
	var value T
	var err error

	Svc(s, false, func() {
		if wrap {
			defer func() {
				if e := frecovery(s.Activity); e != nil {
					err = e
					s.Owner.ShutdownService(s)
				}
			}()
		}
		value, err = code()
		result <- true
	})
	<-result
	return value, err
}

func Svc(s *ChanSvc, wrap bool, code func()) {
	go func() { // using a goroutine so the channel won't block
		if wrap {
			defer func() {
				if e := frecovery(s.Activity); e != nil {
					s.Shutdown()
				}
			}()
		}
		if verboseSvc {
			count := atomic.AddInt64(&svcCount, 1)
			fmt.Printf("@@ QUEUE SVC %d\n", count)
			s.Svc <- func() {
				fmt.Printf("@@ START SVC %d [%d]\n", count, atomic.LoadInt64(&svcCount))
				code()
				fmt.Printf("@@ END SVC %d [%d]\n", count, atomic.LoadInt64(&svcCount))
			}
		} else {
			s.Svc <- code
		}
	}()
}

// Run a service. Close the channel to stop it.
func NewSvc(owner ServiceOwner) *ChanSvc {
	s := &ChanSvc{
		Owner: owner,
		Svc:   make(chan func()),
	}
	go func() {
		for {
			cmd, ok := <-s.Svc
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
	Svc(s, false, func() {
		close(s.Svc)
		s.Owner.ShutdownService(s)
	})
}
