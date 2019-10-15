package sherlockio

import (
	"fmt"
	"os"
	"testing"
)

var (
	filename = "sherlock"
)

func TestSlowLogs(t *testing.T) {

	fmt.Println("Testing slow logs...")

	logging = true
	if !IsSlowLogsEnabled() {
		t.Error("Logging is disabled by default. Not expected")
	}

	defer os.Remove(filename)
	logDir = "/tmp"
	OverrideSettings(filename, ".", true)

	err := LogEvents(&IOEvent{
		Namespace: "testns",
		Name:      "testname",
		Duration:  float64(2000),
		Query:     "sum(metric_name{_namespace=\"testindex\"",
		Mode:      "read",
		Store:     "127.0.0.1:8090",
	})
	if err != nil {
		t.Error(fmt.Sprintf("Failed: %s", err.Error()))
	}
}
