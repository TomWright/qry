package qry_test

import (
	"fmt"
	"github.com/go-test/deep"
	"testing"
)

func checkDiff(t *testing.T, exp any, got any) bool {
	return checkDiffMsg(t, exp, got, "")
}

func checkDiffMsg(t *testing.T, exp any, got any, message string) bool {
	if message != "" {
		message += "\n"
	}
	if diffs := deep.Equal(exp, got); len(diffs) > 0 {
		diffsMsg := ""
		for _, diff := range diffs {
			diffsMsg += fmt.Sprintf("- %s\n", diff)
		}
		t.Errorf("%sexpected:\n%v\ngot:\n%v\n%s", message, exp, got, diffsMsg)
		return false
	}
	return true
}
