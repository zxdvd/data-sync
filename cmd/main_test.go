package main

import (
	"os"
	"testing"
)

func TestM(t *testing.T) {
	t.Log("test main function")
	os.Args = []string{"", "--config", "testdata/tasks.yml"}
	main()
}
