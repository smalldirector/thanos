package v1

import (
	"net/http"
	"strconv"
	"time"

	"github.com/NYTimes/gziphandler"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/sherlockio"
	"github.com/thanos-io/thanos/pkg/tracing"
	"github.corp.ebay.com/sherlockio/egress-ebay/cmd/egress/common"
	"github.corp.ebay.com/sherlockio/egress-ebay/cmd/egress/thanos"

	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
)

func newMetrics() *Metrics {
	m := &Metrics{}
	secBuckets := []float64{.05, .1, .25, .5, 1, 2.5, 5, 10, 20, 30, 60}
	m.egressQueryRateLimitCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "egress_query_rate_limit_counter",
		Help: "Number of times rate limit is encountered during query",
	},
		[]string{"handler"},
	)
	m.holdLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "request_hold_latency",
		Help:    "Amount of time request spent in hold queue",
		Buckets: secBuckets,
	},
		[]string{"handler"},
	)
	m.queryLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "query_latency",
		Help:    "thanos query_range latency",
		Buckets: secBuckets,
	}, []string{"code"})
	m.completeQueryLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "query_latency_complete",
		Help:    "thanos query_range latency",
		Buckets: secBuckets,
	}, []string{"code"})

	prometheus.MustRegister(m.egressQueryRateLimitCounter)
	prometheus.MustRegister(m.holdLatency)
	prometheus.MustRegister(m.queryLatency)
	prometheus.MustRegister(m.completeQueryLatency)

	return m
}

type Metrics struct {
	egressQueryRateLimitCounter *prometheus.CounterVec
	holdLatency                 *prometheus.HistogramVec
	queryLatency                *prometheus.HistogramVec
	completeQueryLatency        *prometheus.HistogramVec
}

func Default(ins extpromhttp.InstrumentationMiddleware, s *thanos.EgressV1, tracer opentracing.Tracer, m *Metrics) func(name string, f ApiFunc) http.HandlerFunc {

	c := thanos.ParseFlags()
	logger := common.GetLoggerWithClass(common.QUERIER, c)

	p := func(name string, f ApiFunc) http.HandlerFunc {
		hf := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			// Define new span
			span, _ := tracing.StartSpan(r.Context(), "sherlockio_default_handler")
			defer span.Finish()

			SetCORS(w)

			start := time.Now()
			err := s.Rl.GetQuota(r.Context())
			defer s.Rl.ReleaseQuota(err)
			if err != nil {
				m.egressQueryRateLimitCounter.WithLabelValues(name).Inc()
				span.SetTag("error", true)
				span.SetTag("quota", "read quota exceeded")
				span.LogKV("event", "quota_exceeded", "request", r.URL.String())
				http.Error(w, http.StatusText(http.StatusTooManyRequests), http.StatusTooManyRequests)
				return
			}
			m.holdLatency.WithLabelValues(name).Observe(time.Since(start).Seconds())

			authSuccess := s.ApplyAuthentication(w, r, tracer, span.Context())
			if !authSuccess {
				span.SetTag("error", true)
				span.LogKV("event", "Authentication failed", "request", r.URL.String())
				http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
				return
			}

			s.PopulateMandatoryParams(r)

			sTime := time.Now()
			code := http.StatusOK
			data, warnings, errf := f(r)
			latency := time.Since(sTime).Seconds()
			m.queryLatency.WithLabelValues(strconv.Itoa(code)).Observe(latency)

			sTime = time.Now()
			if errf != nil {
				code = RespondError(w, errf, data)
			} else if data != nil {
				Respond(w, data, warnings)
			} else {
				w.WriteHeader(http.StatusNoContent)
			}
			completeLatency := time.Since(sTime).Seconds()
			m.queryLatency.WithLabelValues(strconv.Itoa(code)).Observe(latency)

			sherlockio.LogEvents(&sherlockio.IOEvent{
				Duration: float64((latency + completeLatency) * 1000),
				Query:    r.URL.RawQuery,
				Mode:     "query_range",
				Store:    r.Host,
			})
		})
		reg := prometheus.DefaultRegisterer.(*prometheus.Registry)
		handler := common.HandlerMetricWrapper(reg, name, hf)
		return ins.NewHandler(name, tracing.HTTPMiddleware(tracer, name, logger, gziphandler.GzipHandler(handler)))
	}

	return p
}

func Overload(ins extpromhttp.InstrumentationMiddleware, s *thanos.EgressV1, tracer opentracing.Tracer, m *Metrics) func(name string, f ApiFunc) http.HandlerFunc {

	c := thanos.ParseFlags()
	logger := common.GetLoggerWithClass(common.QUERIER, c)
	qh := sherlockio.NewQueryHandler(*c, logger, tracer, s.Discovery, s.Auth)

	p := func(name string, f ApiFunc) http.HandlerFunc {
		hf := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer s.PanicRecover(w, name)

			// Define new span
			span, _ := tracing.StartSpan(r.Context(), "sherlockio_remote_read_handler")
			defer span.Finish()

			SetCORS(w)

			start := time.Now()
			err := s.Rl.GetQuota(r.Context())
			defer s.Rl.ReleaseQuota(err)
			if err != nil {
				m.egressQueryRateLimitCounter.WithLabelValues(name).Inc()
				span.SetTag("error", true)
				span.SetTag("quota", "read quota exceeded")
				span.LogKV("event", "quota_exceeded", "request", r.URL.String())
				http.Error(w, http.StatusText(http.StatusTooManyRequests), http.StatusTooManyRequests)
				return
			}
			m.holdLatency.WithLabelValues(name).Observe(time.Since(start).Seconds())

			authSuccess := s.ApplyAuthentication(w, r, tracer, span.Context())
			if !authSuccess {
				span.SetTag("error", true)
				span.LogKV("event", "Authentication failed", "request", r.URL.String())
				http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
				return
			}

			sTime := time.Now()
			contentType := r.Header.Get("Content-Type")
			if contentType == "application/x-protobuf" {
				qh.HandleReadRequest(w, r, span.Context())
			} else {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			latency := float64(time.Since(sTime) / time.Millisecond)
			sherlockio.LogEvents(&sherlockio.IOEvent{
				Duration: latency,
				Query:    r.URL.String(),
				Mode:     "remote_read",
			})

		})
		reg := prometheus.DefaultRegisterer.(*prometheus.Registry)
		handler := common.HandlerMetricWrapper(reg, name, hf)
		return ins.NewHandler(name, tracing.HTTPMiddleware(tracer, name, logger, gziphandler.GzipHandler(handler)))
	}
	return p
}
