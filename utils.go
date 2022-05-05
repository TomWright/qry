package qry

import (
	"strings"
)

func genericMap[A, B any](target []A, mapFn func(A) B) []B {
	res := make([]B, len(target))
	for k, v := range target {
		res[k] = mapFn(v)
	}
	return res
}

type stringer interface {
	String() string
}

func genericJoin[T stringer](elems []T, separator string) string {
	return strings.Join(
		genericMap(elems, func(a T) string {
			return a.String()
		}),
		separator,
	)
}
