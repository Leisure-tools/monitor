package monitor

import (
	"errors"
	"fmt"
	"iter"
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

func Collect[T SvcRunner, Result any](runners iter.Seq[T], code func(T, func(Result) bool)) iter.Seq[Result] {
	results := make(chan Result)
	counts := make(chan int)
	errorChan := make(chan error)
	count := 0
	expected := 0
	for runner := range runners {
		count++
		runner.Svc(func() {
			count := 0
			defer func() {
				// recover from writing to a dead counts or error chan
				defer func() { recover() }()
				if err := recover(); err != nil {
					e, ok := err.(error)
					if !ok {
						e = fmt.Errorf("Error in service: %v", e)
					}
					errorChan <- e
				}
				counts <- count
			}()
			live := true
			code(runner, func(item Result) bool {
				if live {
					func() {
						defer func() {
							if err := recover(); err != nil {
								live = false
							}
						}()
						results <- item
						count++
					}()
				}
				return live
			})
		})
	}
	return func(yield func(item Result) bool) {
		//processed := 0
	loop:
		for count > 0 || expected > 0 {
			select {
			case c := <-counts:
				expected += c
				count--
			case r := <-results:
				//processed++
				expected--
				if !yield(r) {
					break loop
				}
			}
		}
		//if processed > 0 {
		//	fmt.Printf("PROCESSED %d ITEMS\n", processed)
		//}
		close(errorChan)
		close(results)
		close(counts)
		errs := make([]error, 0, count)
		for err := range errorChan {
			errs = append(errs, err)
		}
		if len(errs) > 0 {
			panic(fmt.Errorf("Service errors: %w", errors.Join(errs...)))
		}
	}
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
