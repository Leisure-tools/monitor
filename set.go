package monitor

import (
	"fmt"
	"sort"
	"strings"
)

type Set[T comparable] map[T]bool

func NewSet[T comparable](elements ...T) Set[T] {
	l := len(elements)
	if l == 0 {
		l = 8
	}
	result := make(Set[T], l)
	for _, item := range elements {
		result[item] = true
	}
	return result
}

func (s Set[T]) ToSlice() []T {
	items := make([]T, len(s))
	i := 0
	for item := range s {
		items[i] = item
		i += 1
	}
	return items
}

func (s Set[T]) Merge(s2 Set[T]) Set[T] {
	if s == nil && len(s2) > 0 {
		// this won't alter the receiver but the returned set will at least be correct
		s = NewSet[T]()
	}
	for k, v := range s2 {
		s[k] = v
	}
	return s
}

func (s Set[T]) Copy() Set[T] {
	return Set[T]{}.Merge(s)
}

func (s Set[T]) Union(s2 Set[T]) Set[T] {
	if len(s) == 0 {
		return s2
	} else if len(s2) == 0 {
		return s
	}
	return s.Copy().Merge(s2)
}

func (s Set[T]) Complement(s2 Set[T]) Set[T] {
	return s.Copy().Subtract(s2)
}

func (s Set[T]) Subtract(s2 Set[T]) Set[T] {
	for item := range s2 {
		delete(s, item)
	}
	return s
}

func (s Set[T]) Add(els ...T) Set[T] {
	if s == nil {
		return NewSet(els...)
	}
	for _, el := range els {
		s[el] = true
	}
	return s
}

func (s Set[T]) Remove(els ...T) Set[T] {
	for _, el := range els {
		delete(s, el)
	}
	return s
}

func (s Set[T]) Has(els ...T) bool {
	for _, el := range els {
		if !s[el] {
			return false
		}
	}
	return true
}

func (s Set[T]) Contains(s2 Set[T]) bool {
	if len(s) < len(s2) {
		return false
	}
	for item := range s2 {
		if !s.Has(item) {
			return false
		}
	}
	return true
}

func (s Set[T]) Intersection(s2 Set[T]) Set[T] {
	result := make(Set[T], 0)
	if len(s2) < len(s) {
		s, s2 = s2, s
	}
	for el := range s {
		if s2.Has(el) {
			result.Add(el)
		}
	}
	return result
}

func (s Set[T]) Intersects(s2 Set[T]) bool {
	if len(s2) < len(s) {
		s, s2 = s2, s
	}
	for item := range s {
		if s2.Has(item) {
			return true
		}
	}
	return false
}

func (s Set[T]) IsEmpty() bool {
	return len(s) == 0
}

func (s Set[T]) String() string {
	sl := s.ToSlice()
	var f func(i, j int) bool
	var strs []string
	sortStrings := false

	switch x := any(sl).(type) {
	case []int:
		f = func(i, j int) bool { return x[i] < x[j] }
	case []int8:
		f = func(i, j int) bool { return x[i] < x[j] }
	case []int16:
		f = func(i, j int) bool { return x[i] < x[j] }
	case []int32:
		f = func(i, j int) bool { return x[i] < x[j] }
	case []int64:
		f = func(i, j int) bool { return x[i] < x[j] }
	case []uint:
		f = func(i, j int) bool { return x[i] < x[j] }
	case []uint8:
		f = func(i, j int) bool { return x[i] < x[j] }
	case []uint16:
		f = func(i, j int) bool { return x[i] < x[j] }
	case []uint32:
		f = func(i, j int) bool { return x[i] < x[j] }
	case []uint64:
		f = func(i, j int) bool { return x[i] < x[j] }
	case []uintptr:
		f = func(i, j int) bool { return x[i] < x[j] }
	case []float32:
		f = func(i, j int) bool { return x[i] < x[j] }
	case []float64:
		f = func(i, j int) bool { return x[i] < x[j] }
	default:
		// defer sorting until after string conversion
		sortStrings = true
	}
	if f != nil {
		sort.Slice(sl, f)
	}
	strs = make([]string, len(sl))
	for i, v := range sl {
		strs[i] = fmt.Sprint(v)
	}
	if sortStrings {
		sort.Strings(strs)
	}
	return fmt.Sprintf("Set[%s]", strings.Join(strs, ", "))
}
