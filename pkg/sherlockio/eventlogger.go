package sherlockio

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/natefinch/lumberjack.v2"
)

type IOEvent struct {
	Namespace string
	Name      string
	Duration  float64
	Query     string
	Component string
	Mode      string
	Store     string
}

var (
	fileName = "thanos"
	logDir   = "/var/log"

	logging = true

	l lumberjack.Logger

	reportingThreshold float64 = 1000

	slowCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "slow_logs",
			Help: "Number of times slow logs are reported",
		},
		[]string{"ns", "name"})

	failedLogs = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "slow_log_write_failed",
		Help: "Number of times slow logs failed",
	})
)

func registerSlowLogMetrics() {
	prometheus.MustRegister(slowCount)
	prometheus.MustRegister(failedLogs)
}

func init() {
	flag := os.Getenv("LOG_FILE")
	if flag != "" {
		fileName = flag
	}

	flag = os.Getenv("LOG_DIR")
	if flag != "" {
		logDir = flag
	}

	if _, err := os.Stat(logDir); os.IsNotExist(err) {
		logging = false
		return
	} else {
		registerSlowLogMetrics()
	}

	flag = os.Getenv("SLOW_LOG_THRESHOLD")
	if flag != "" {
		threshold, err := strconv.ParseUint(flag, 0, 64)
		if err == nil {
			reportingThreshold = float64(threshold)
		}
	}

	// initialize lumberjack
	l = lumberjack.Logger{
		Filename:   fmt.Sprintf("%s/%s", logDir, fileName),
		MaxSize:    500, // megabytes
		MaxBackups: 2,
		MaxAge:     3, //days
	}
	log.SetOutput(&l)

	handleEvents()
}

func handleEvents() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP)
	go func() {
		for {
			<-c
			l.Rotate()
		}
	}()

	// Stop logger
	gracefulStop := make(chan os.Signal)
	signal.Notify(gracefulStop, syscall.SIGTERM)
	go func() {
		<-gracefulStop
		l.Close()
	}()
}

func IsSlowLogsEnabled() bool {
	return logging
}

func OverrideSettings(fn string, dirName string, enabled bool) {
	fileName = fn
	logDir = dirName
	logging = enabled

	// Close existing handle
	l.Close()

	// initialize lumberjack
	l = lumberjack.Logger{
		Filename:   fmt.Sprintf("%s/%s", logDir, fileName),
		MaxSize:    500, // megabytes
		MaxBackups: 3,
		MaxAge:     3, //days
	}
	log.SetOutput(&l)
	handleEvents()
}

func writeFile(obj *IOEvent) error {
	payload := fmt.Sprintf("[%s] [%s] [%v] [%s] [%s] [%s] [query]\n", obj.Namespace, obj.Name, obj.Duration, url.QueryEscape(obj.Query), obj.Store, obj.Mode)
	_, err := l.Write([]byte(payload))
	return err
}

func LogEvents(event *IOEvent) error {
	if logging == false {
		return nil
	}

	if event.Duration < reportingThreshold {
		return nil
	} else {
		slowCount.WithLabelValues(event.Namespace, event.Name).Inc()
	}

	err := writeFile(event)
	if err != nil {
		failedLogs.Inc()
	}

	return err
}
