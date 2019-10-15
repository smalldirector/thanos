package sherlockio

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http/httptrace"
	"os"
	"sort"
	"strconv"

	"github.corp.ebay.com/sherlockio/egress-ebay/cmd/util"

	"github.com/prometheus/prometheus/prompb"
	"github.com/opentracing/opentracing-go"
	"github.corp.ebay.com/sherlockio/egress-ebay/cmd/egress/health"

	"github.com/go-kit/kit/log/level"
	s "github.corp.ebay.com/sherlockio/commons/sanitize"
	"github.corp.ebay.com/sherlockio/egress-ebay/cmd/egress/auth"
	"github.corp.ebay.com/sherlockio/egress-ebay/cmd/egress/common"
	"github.corp.ebay.com/sherlockio/egress-ebay/cmd/egress/discovery"
	d "github.corp.ebay.com/sherlockio/egress-ebay/cmd/egress/discovery"

	"github.corp.ebay.com/sherlockio/lookup-store/contracts"

	"net/http"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
)

type QueryHandler struct {
	config     common.EgressConfig
	logger     log.Logger
	reg        *prometheus.Registry
	tracer     opentracing.Tracer
	mux        *http.ServeMux
	httpClient *http.Client

	auth      *auth.Auth
	rlQuota   *auth.RlQuota
	discovery d.IDiscovery
}

type requestContext struct {
	index     int
	namespace string
	name      string
	query     *prompb.Query
	stores    []*contracts.StoreHandle

	encodedReq []byte
	httpReq    []*http.Request
}

type errorContext struct {
	index          int
	store          string
	responseStatus int
	err            error
}

type responseContext struct {
	reqContext *requestContext

	resp *http.Response
	data []byte
	qr   []*prompb.QueryResult
	err  []*errorContext

	latency       float64
	tsdbLatency   float64
	decodeLatency float64
}

type queryStats struct {
	rcs  []*responseContext
	size int
	errs []*errorContext
}

var (
	secBuckets        = []float64{.05, .1, .25, .5, 1, 2.5, 5, 10, 20, 30, 60}
	msBuckets         = []float64{0.01, 0.025, 0.05, 0.1}
	preProcessLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "read_pre_process_latency",
		Help:    "Pre process latency",
		Buckets: msBuckets,
	}, []string{"code"})
	copyLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "copy_latency",
		Help:    "copy latency",
		Buckets: msBuckets,
	})
	readLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "read_latency",
		Help:    "query latency",
		Buckets: secBuckets,
	}, []string{"reqCnt", "code"})
	readLatencyComplete = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "read_latency_complete",
		Help:    "complete query latency",
		Buckets: secBuckets,
	}, []string{"reqCnt", "code"})
	sortDedupLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "read_sort_dedup_latency",
		Help:    "latency to sort/dedup",
		Buckets: secBuckets,
	})
	tsdbFetchLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "tsdb_read_latency",
		Help:    "tsdb read latency",
		Buckets: secBuckets,
	}, []string{"code"})

	responseSize = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "query_response_size",
		Help: "Size of query response",
	})
	seriesRead = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "series_read",
		Help: "Number of series read and returned",
	})
	totalSeriesRead = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "total_series_read",
		Help: "Number of series read from tsdb",
	})
	dataPointsRead = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "data_points_read",
		Help: "Number of datapoints read",
	})
	totalDataPointsRead = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "total_data_points_read",
		Help: "Number of datapoints read from tsdb",
	})
	seriesReadByNamespace = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "series_read_by_namespace",
		Help: "Number of series read by namespace",
	}, []string{"ns"})

	namespaceReadLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "namespace_read_latency",
		Help:    "namespace_read_latency",
		Buckets: prometheus.ExponentialBuckets(0.4, 3, 5), // 400ms to 32s
	}, []string{"ns", "tier", "code", "series"})
	granularReadLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "granular_read_latency",
		Help:    "granular_read_latency",
		Buckets: prometheus.ExponentialBuckets(0.4, 3, 5), // 400ms to 32s
	}, []string{"reqCnt", "storeCnt", "qwindow", "tier", "code", "series"})
	shardReadLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "shard_read_latency",
		Help:    "shard_read_latency",
		Buckets: prometheus.ExponentialBuckets(0.4, 3, 5), // 400ms to 32s
	}, []string{"shard", "partition", "code"})
	storeDiscoverLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "store_discover_latency",
		Help:    "store discovery latency",
		Buckets: msBuckets,
	})
	storesDiscoveryError = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "stores_discovery_errors",
			Help: "Number of times getting stores list for query failed",
		},
	)
	noStoresFetched = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "none_store_discovery",
			Help: "Number of times 0 stores were obtained",
		},
	)
	noStoresHealthy = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "none_store_healthy",
			Help: "Number of times 0 stores were healthy",
		},
	)
)

func registerMetrics() {
	prometheus.MustRegister(preProcessLatency)
	prometheus.MustRegister(copyLatency)
	prometheus.MustRegister(readLatency)
	prometheus.MustRegister(readLatencyComplete)
	prometheus.MustRegister(sortDedupLatency)
	prometheus.MustRegister(tsdbFetchLatency)
	prometheus.MustRegister(storeDiscoverLatency)

	prometheus.MustRegister(responseSize)
	prometheus.MustRegister(seriesRead)
	prometheus.MustRegister(totalSeriesRead)
	prometheus.MustRegister(dataPointsRead)
	prometheus.MustRegister(totalDataPointsRead)
	prometheus.MustRegister(seriesReadByNamespace)

	prometheus.MustRegister(namespaceReadLatency)
	prometheus.MustRegister(granularReadLatency)
	prometheus.MustRegister(shardReadLatency)

	prometheus.MustRegister(storesDiscoveryError)
	prometheus.MustRegister(noStoresFetched)
	prometheus.MustRegister(noStoresHealthy)

}

func NewQueryHandler(config common.EgressConfig, logger log.Logger, tracer opentracing.Tracer, d discovery.IDiscovery, a *auth.Auth) *QueryHandler {
	qh := &QueryHandler{config: config, logger: logger, tracer: tracer, auth: a, discovery: d, reg: prometheus.DefaultRegisterer.(*prometheus.Registry)}
	qh.httpClient = &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: config.Http.Options.MaxIdleConns,
		},
		Timeout: time.Duration(config.Http.Options.QueryTimeout) * time.Second,
	}
	registerMetrics()
	return qh
}

func (qh *QueryHandler) HandleReadRequest(w http.ResponseWriter, r *http.Request, parent opentracing.SpanContext) {
	// decode request
	req, err := decodeReadRequest(r)
	if err != nil {
		level.Info(qh.logger).Log("BadReqeust", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	querySpan := qh.tracer.StartSpan(
		"sherlockio_remote_read",
		opentracing.ChildOf(parent),
	)
	defer querySpan.Finish()
	ctx := opentracing.ContextWithSpan(r.Context(), querySpan)

	reqContexts, errContexts := qh.requestContexts(req, querySpan)
	if len(reqContexts) == 0 && len(errContexts) > 0 {
		http.Error(w, errContexts[0].err.Error(), errContexts[0].responseStatus)
		return
	}

	var qs *queryStats
	sTime := time.Now()
	if len(reqContexts) == 0 {
		qs = qh.emptyResponse(w, r, len(req.Queries), ctx, querySpan)
	} else if len(reqContexts) == 1 && len(reqContexts[0].stores) == 1 {
		qs = qh.singleStoreRead(w, r, reqContexts[0], ctx, querySpan)
	} else {
		qs = qh.multiStoreRead(w, r, reqContexts, ctx, querySpan)
	}
	latency := time.Since(sTime).Seconds()
	go reportMetrics(reqContexts, qs, latency)
}

func (qh *QueryHandler) requestContexts(req *prompb.ReadRequest, span opentracing.Span) ([]*requestContext, []*errorContext) {
	now := time.Now()
	sTime := now

	reqContexts := make([]*requestContext, 0, len(req.Queries))
	errContexts := make([]*errorContext, 0, len(req.Queries))
	for index, q := range req.Queries {
		namespace, namespaceEquals, name, nameEquals := GetNamespaceName(q.Matchers)
		if !validateRemoteReadRequest(namespace, namespaceEquals, name, nameEquals) {
			errContexts = append(errContexts, &errorContext{index: index, err: errors.New("namespace and name missing in query"), responseStatus: http.StatusBadRequest})
			continue
		}
		level.Info(qh.logger).Log("readRequest", fmt.Sprintf("name: %s, namespace: %s, dims: %v", name, namespace, q.Matchers))
		if q.StartTimestampMs == 0 {
			q.StartTimestampMs = now.Add(time.Duration(-1)*time.Hour).Unix() * 1000
		}
		if q.EndTimestampMs == 0 {
			q.EndTimestampMs = now.Unix() * 1000
		}
		sanitize(q.Matchers)

		stores, err := qh.stores(namespace, name, nil, q.StartTimestampMs, q.EndTimestampMs)
		if err != nil {
			errContexts = append(errContexts, &errorContext{index: index, err: err, responseStatus: http.StatusInternalServerError})
			continue
		}
		if len(stores) == 0 {
			continue
		}

		level.Info(qh.logger).Log("readRequest", fmt.Sprintf("num stores queried: %d", len(stores)))

		encodedReq, err := encodeReadRequest(&prompb.ReadRequest{Queries: []*prompb.Query{q}})
		if err != nil {
			errContexts = append(errContexts, &errorContext{index: index, err: err, responseStatus: http.StatusBadRequest})
			continue
		}

		tess_cluster := os.Getenv("tess_cluster")
		span.SetTag("ns", namespace)
		span.SetTag("name", name)
		span.SetTag("tess_cluster", tess_cluster)

		reqContexts = append(reqContexts, &requestContext{index: index, query: q, stores: stores, namespace: namespace, name: name, encodedReq: encodedReq})
	}

	queryPreProcessTime := time.Since(sTime).Seconds()
	if len(reqContexts) == 0 && len(errContexts) > 0 {
		preProcessLatency.WithLabelValues(strconv.Itoa(errContexts[0].responseStatus)).Observe(queryPreProcessTime)
	} else {
		preProcessLatency.WithLabelValues("200").Observe(queryPreProcessTime)
	}
	span.LogKV("pre_process", queryPreProcessTime, "q", len(req.Queries), "rCtx", len(reqContexts), "errCtx", len(errContexts))
	level.Info(qh.logger).Log("pre_process", queryPreProcessTime, "q", len(req.Queries), "rCtx", len(reqContexts), "errCtx", len(errContexts))

	return reqContexts, errContexts
}

func (qh *QueryHandler) emptyResponse(w http.ResponseWriter, r *http.Request, queryCnt int, ctx context.Context, span opentracing.Span) *queryStats {

	qr := make([]*prompb.QueryResult, queryCnt)
	pr := &prompb.ReadResponse{Results: qr}
	data, err := encodeReadResponse(pr, &r.Header)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return nil
	}
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Encoding", "snappy")
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Header().Set("Content-Length", strconv.Itoa(int(len(data))))
	w.Write(data)

	span.LogKV("emptyResponse", "0", "latency", 0)
	return nil
}

func (qh *QueryHandler) singleStoreRead(w http.ResponseWriter, r *http.Request, reqContext *requestContext, ctx context.Context, span opentracing.Span) *queryStats {
	sTime := time.Now()
	ip := *reqContext.stores[0].Ip
	port := int(reqContext.stores[0].DataPort)
	level.Debug(qh.logger).Log("remote_read_endpoint", ip+":"+strconv.Itoa(port))
	span.LogKV(fmt.Sprintf("store-%d", 0), ip)

	httpReq, err := newReadHttpRequest(ip, int(port), bytes.NewReader(reqContext.encodedReq), ctx, span)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return &queryStats{rcs: nil, size: 0, errs: []*errorContext{{index: 0, err: err, responseStatus: http.StatusInternalServerError}}}
	}

	resp := qh.doTsdbRead(httpReq, reqContext, reqContext.stores[0], span.Context())
	latency := time.Since(sTime).Seconds()
	readLatency.WithLabelValues("1", strconv.Itoa(resp.StatusCode)).Observe(latency)

	sTime = time.Now()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return &queryStats{rcs: nil, size: 0, errs: []*errorContext{{index: 0, err: err, responseStatus: http.StatusInternalServerError}}}
	}

	cdata := make([]byte, len(data))
	copy(cdata, data)
	copyDuration := time.Since(sTime).Seconds()
	copyLatency.Observe(copyDuration)

	w.WriteHeader(resp.StatusCode)
	w.Header().Set("Content-Encoding", "snappy")
	w.Header().Set("Content-Type", "application/x-protobuf")
	size := resp.ContentLength
	w.Header().Set("Content-Length", strconv.Itoa(int(size)))
	io.Copy(w, bytes.NewReader(cdata))
	responseSize.Add(float64(len(data)))

	span.LogKV("singleStoreRead", "0", "latency", latency, "copyLatency", copyDuration)
	return &queryStats{rcs: []*responseContext{{reqContext: reqContext, resp: resp, data: data, latency: latency, tsdbLatency: latency, decodeLatency: 0}}, size: int(size), errs: nil}
}

func (qh *QueryHandler) multiStoreRead(w http.ResponseWriter, r *http.Request, reqContexts []*requestContext, ctx context.Context, span opentracing.Span) *queryStats {
	sTime := time.Now()
	ch := make(chan *responseContext, len(reqContexts))
	for index, reqContext := range reqContexts {
		go qh.processRequestContext(ch, index, reqContext, ctx, span)
	}
	rcs := make([]*responseContext, 0, len(reqContexts))
	qrs := make([]*prompb.QueryResult, len(reqContexts))
	errs := make([]*errorContext, 0, len(reqContexts))
	resultLength := 0
	for i := 0; i < len(reqContexts); i++ {
		rc := <-ch
		rcs = append(rcs, rc)
		if rc.err != nil {
			errs = append(errs, rc.err...)
		}
		if rc.qr == nil {
			continue
		}
		resultLength += len(rc.qr)
		if len(rc.qr) > 0 {
			qrs[i] = rc.qr[0]
		}
	}
	if resultLength == 0 && len(errs) > 0 {
		http.Error(w, errs[0].err.Error(), errs[0].responseStatus)
		return &queryStats{rcs: nil, size: 0, errs: errs}
	}
	latency := time.Since(sTime).Seconds()
	reqCnt := "1"
	if len(reqContexts) > 1 {
		reqCnt = "2"
	}
	readLatency.WithLabelValues(reqCnt, "200").Observe(latency)

	sTime = time.Now()
	pr := &prompb.ReadResponse{Results: qrs}
	data, err := encodeReadResponse(pr, &r.Header)
	encodeLatency := time.Since(sTime).Seconds()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return &queryStats{rcs: nil, size: 0, errs: []*errorContext{{index: 0, err: err, responseStatus: http.StatusInternalServerError}}}
	}
	span.LogKV("multiStoreRead", "0", "latency", latency, "encodeLatency", encodeLatency, "resultsLength", len(qrs))
	level.Info(qh.logger).Log("multiStoreRead", "0", "latency", latency, "encodeLatency", encodeLatency)

	w.Header().Set("Content-Encoding", "snappy")
	w.Header().Set("Content-Type", "application/x-protobuf")
	size := len(data)
	w.Header().Set("Content-Length", strconv.Itoa(size))
	w.Write(data)

	responseSize.Add(float64(size))
	return &queryStats{rcs: rcs, size: len(data), errs: nil}
}

func (qh *QueryHandler) processRequestContext(rcChan chan *responseContext, index int, reqContext *requestContext, ctx context.Context, querySpan opentracing.Span) {

	totalDataPointCount := 0
	totalTimeSeriesCount := 0

	sTime := time.Now()
	rcCh := make(chan *responseContext, len(reqContext.stores))
	for index, store := range reqContext.stores {
		go qh.doTsdbReadAndDecode(rcCh, index, store, reqContext, ctx, querySpan)
	}
	aggTS := make([]*prompb.TimeSeries, 0)
	errs := make([]*errorContext, 0, len(reqContext.stores))
	rcs := make([]*responseContext, 0, len(reqContext.stores))
	for i := 0; i < len(reqContext.stores); i++ {
		rc := <-rcCh
		rcs = append(rcs, rc)
		if rc.err != nil {
			errs = append(errs, rc.err...)
		}
		if rc.qr == nil {
			continue
		}
		for _, result := range rc.qr {
			aggTS = append(aggTS, result.Timeseries...)
			totalTimeSeriesCount += len(result.Timeseries)
			for _, ts := range result.Timeseries {
				totalDataPointCount += len(ts.Samples)
			}
		}
	}
	tsdbLatency := maxLatency(rcs, "tsdbLatency")
	decodeLatency := maxLatency(rcs, "decodeLatency")
	latency := time.Since(sTime).Seconds()

	sTime = time.Now()
	if len(aggTS) > 1 && len(reqContext.stores) > 1 {
		aggTS = sortMerge(aggTS, qh.config.Http.Options.ReplicaLabel)
	}

	sortDedupLatency.Observe(time.Since(sTime).Seconds())
	totalSeriesRead.Add(float64(totalTimeSeriesCount))
	totalDataPointsRead.Add(float64(totalDataPointCount))

	rc := &responseContext{reqContext: reqContext, latency: latency, tsdbLatency: tsdbLatency, decodeLatency: decodeLatency, err: errs}
	if len(aggTS) > 0 {
		rc.qr = []*prompb.QueryResult{{Timeseries: aggTS}}
	}
	querySpan.LogKV("query", index, "latency", latency, "maxTsdbLatency", tsdbLatency, "maxDecodeLatency", decodeLatency, "sort_dedup_time", time.Since(sTime).Seconds(), "resultsLength", len(rc.qr))
	rcChan <- rc
}

func maxLatency(rcs []*responseContext, condition string) float64 {
	latency := float64(0)
	switch condition {
	case "tsdbLatency":
		for _, rc := range rcs {
			if latency < rc.tsdbLatency {
				latency = rc.tsdbLatency
			}
		}
	case "decodeLatency":
		for _, rc := range rcs {
			if latency < rc.decodeLatency {
				latency = rc.decodeLatency
			}
		}
	case "totalLatency":
		for _, rc := range rcs {
			if latency < rc.latency {
				latency = rc.latency
			}
		}
	default:
		panic("invalid condition")
	}

	return latency

}

func (qh *QueryHandler) doTsdbReadAndDecode(rcCh chan *responseContext, index int, store *contracts.StoreHandle, reqContext *requestContext, ctx context.Context, querySpan opentracing.Span) {
	sTime := time.Now()
	level.Debug(qh.logger).Log("remote_read_endpoint", *store.Ip+":"+strconv.Itoa(int(store.DataPort)))
	querySpan.LogKV(fmt.Sprintf("store-%d", index), *store.Ip)

	httpReq, err := newReadHttpRequest(*store.Ip, int(store.DataPort), bytes.NewReader(reqContext.encodedReq), ctx, querySpan)
	if err != nil {
		rcCh <- &responseContext{err: []*errorContext{{index: index, responseStatus: http.StatusInternalServerError, err: err}}}
		return
	}
	resp := qh.doTsdbRead(httpReq, reqContext, store, querySpan.Context())

	tsdbLatency := time.Since(sTime).Seconds()
	if resp.StatusCode/100 != 2 {
		rcCh <- &responseContext{latency: tsdbLatency, tsdbLatency: tsdbLatency, decodeLatency: 0, err: []*errorContext{{index: index, responseStatus: resp.StatusCode, err: errors.New(resp.Status)}}}
		querySpan.LogKV("tsdbFail", *store.Ip, "tsdbLatency", tsdbLatency)
		return
	}

	sTime = time.Now()
	pr, err := decodeReadResponse(resp)

	decodeLatency := time.Since(sTime).Seconds()
	totalLatency := tsdbLatency + decodeLatency
	rc := &responseContext{latency: totalLatency, tsdbLatency: tsdbLatency, decodeLatency: decodeLatency}
	if err != nil {
		rc.err = []*errorContext{{index: index, responseStatus: http.StatusInternalServerError, err: err}}
		rc.qr = nil
	} else {
		rc.err = nil
		rc.qr = pr.Results
	}
	querySpan.LogKV("tsdbSuccess", *store.Ip, "tsdbLatency", tsdbLatency, "decodeLatency", decodeLatency, "totalLatency", totalLatency, "resultsLength", len(rc.qr))
	rcCh <- rc
}

func (qh *QueryHandler) doTsdbRead(req *http.Request, reqContext *requestContext, store *contracts.StoreHandle, spanCxt opentracing.SpanContext) *http.Response {
	querySpan := qh.tracer.StartSpan(
		"sherlockio_promtsdb_remote_read",
		opentracing.ChildOf(spanCxt),
	)
	defer querySpan.Finish()
	reqPayload := fmt.Sprintf("%v", reqContext.query)
	querySpan.LogKV("host", req.Host)
	querySpan.LogKV("payload", reqPayload)

	sTime := time.Now()

	req = req.WithContext(req.Context())
	resp, err := qh.httpClient.Do(req)
	if err != nil {
		return get500Response(err)
	}
	readLatency := time.Since(sTime).Seconds()
	querySpan.LogKV("readLatency", readLatency, "size", resp.ContentLength, "status", resp.Status)
	shardReadLatency.WithLabelValues(strconv.Itoa(int(store.Shard)), store.Cluster, strconv.Itoa(resp.StatusCode)).Observe(readLatency)

	sTime = time.Now()
	LogEvents(&IOEvent{
		Namespace: reqContext.namespace,
		Name:      reqContext.name,
		Duration:  readLatency,
		Query:     reqPayload,
		Mode:      "remote_store_read",
		Store:     req.Host,
	})
	logLatency := time.Since(sTime).Seconds()
	querySpan.LogKV("logLatency", logLatency)

	tsdbFetchLatency.WithLabelValues(strconv.Itoa(resp.StatusCode)).Observe(readLatency + logLatency)
	return resp
}

func get500Response(err error) *http.Response {
	resp := &http.Response{}
	resp.StatusCode = 500
	resp.Status = err.Error()
	return resp
}

func (qh *QueryHandler) stores(namespace string, name string, labels map[string]util.Matchers, StartTimestampMs int64, EndTimestampMs int64) ([]*contracts.StoreHandle, error) {
	stores, err := qh.discovery.StoresList(namespace, name, nil, StartTimestampMs, EndTimestampMs)
	if err != nil {
		storesDiscoveryError.Inc()
		level.Error(qh.logger).Log("store.discovery.error", err)
		return nil, err
	}
	if len(stores) == 0 {
		noStoresFetched.Inc()
		level.Info(qh.logger).Log("no.stores.fetched", 0)
		return []*contracts.StoreHandle{}, nil
	}

	rm := health.ReplicaManager()
	if !qh.config.HealthStatsConfig.DisableReplicaManager {
		stores = rm.ManagedStores(name, stores, StartTimestampMs, EndTimestampMs)
		if len(stores) == 0 {
			noStoresHealthy.Inc()
			return nil, errors.New("healthy store empty")
		}
	}

	stores = ignoreStoresWithoutDataPort(stores)
	if len(stores) == 0 {
		return nil, errors.New("healthy store empty")
	}

	return stores, nil
}

func newReadHttpRequest(ip string, port int, body io.Reader, ctx context.Context, span opentracing.Span) (*http.Request, error) {
	trace := NewClientTrace(span)
	ctx = httptrace.WithClientTrace(ctx, trace)
	httpReq, err := http.NewRequest("POST", strings.Join([]string{"http://", ip, ":", strconv.Itoa(port), common.Remote_Read_URI}, ""), body)
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("X-Prometheus-Remote-Read-Version", "0.1.0")
	httpReq.Header.Set("Accept-Encoding", "snappy")
	httpReq = httpReq.WithContext(ctx)
	return httpReq, nil
}

func validateRemoteReadRequest(namespace string, namespaceEquals bool, name string, nameEquals bool) bool {
	if namespace == "" || name == "" || !namespaceEquals || !nameEquals {
		return false
	}
	return true
}

func decodeReadRequest(r *http.Request) (*prompb.ReadRequest, error) {
	buf, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	if r.Header.Get("Content-Encoding") == "snappy" {
		buf, err = snappy.Decode(nil, buf)
		if err != nil {
			return nil, err
		}
	}

	req := &prompb.ReadRequest{}
	if err := proto.Unmarshal(buf, req); err != nil {
		return nil, err
	}

	if len(req.Queries) == 0 {
		err = errors.New("no queries in the request")
		return nil, err
	}
	return req, nil
}

func encodeReadRequest(r *prompb.ReadRequest) ([]byte, error) {
	buf, err := proto.Marshal(r)
	if err != nil {
		return nil, err
	}
	encoded := snappy.Encode(nil, buf)
	return encoded, nil
}

func decodeReadResponse(resp *http.Response) (*prompb.ReadResponse, error) {
	buf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	decomp, err := snappy.Decode(nil, buf)
	if err != nil {
		return nil, err
	}

	pr := &prompb.ReadResponse{}
	if err := proto.Unmarshal(decomp, pr); err != nil {
		return nil, err
	}
	return pr, nil
}

func encodeReadResponse(pr *prompb.ReadResponse, h *http.Header) ([]byte, error) {
	data, err := proto.Marshal(pr)
	if err != nil {
		return nil, err
	}

	data = snappy.Encode(nil, data)
	return data, nil
}

func sanitize(matchers []*prompb.LabelMatcher) {
	for _, l := range matchers {
		if l.Type == prompb.LabelMatcher_EQ || l.Type == prompb.LabelMatcher_NEQ {
			if l.Name == "__name__" {
				l.Value = s.Name(l.Value)
			} else {
				l.Name = s.Label(l.Name)
			}

		}
	}
}

func GetNamespaceName(matchers []*prompb.LabelMatcher) (namespace string, namespaceEquals bool, name string, nameEquals bool) {
	for _, l := range matchers {
		switch l.Name {
		case "__name__":
			if l.Type == prompb.LabelMatcher_EQ || l.Type == prompb.LabelMatcher_NEQ {
				name = l.Value
				namespaceEquals = true
			}
		case "_namespace_":
			if l.Type == prompb.LabelMatcher_EQ || l.Type == prompb.LabelMatcher_NEQ {
				namespace = l.Value
				nameEquals = true
			}
		}
	}
	return namespace, namespaceEquals, name, nameEquals
}

// the aggTS array has to have at least 2 entries to execute this method
// Assuming the aggTS Labels slice is already sorted on Name
func sortMerge(aggTS []*prompb.TimeSeries, dedupString string) []*prompb.TimeSeries {
	if dedupString != "" {
		for idx, ts := range aggTS {
			//sort the labels for each series, and push dedup string to the end and remove it
			sort.Slice(ts.Labels, func(i, j int) bool {
				if ts.Labels[i].Name == dedupString {
					return false
				}
				if ts.Labels[j].Name == dedupString {
					return true
				}
				return ts.Labels[i].Name < ts.Labels[j].Name
			})
			if aggTS[idx].Labels[len(ts.Labels)-1].Name == dedupString {
				aggTS[idx].Labels = aggTS[idx].Labels[:len(ts.Labels)-1]
			}
		}
	}
	//After this the series with same series will be next to each other
	sort.Slice(aggTS, func(i, j int) bool {
		return CompareLabels(aggTS[i].Labels, aggTS[j].Labels) < 0
	})

	// combine the series with same labels into a single series
	newTS := make([]*prompb.TimeSeries, 0, len(aggTS))
	mergedTS := aggTS[0]
	for idx := 1; idx < len(aggTS); idx++ {
		if CompareLabels(mergedTS.Labels, aggTS[idx].Labels) == 0 {
			mergedTS.Samples = merge(mergedTS.Samples, aggTS[idx].Samples)
		} else {
			newTS = append(newTS, mergedTS)
			mergedTS = aggTS[idx]
		}
	}
	newTS = append(newTS, mergedTS)
	return newTS
}

// CompareLabels compares two sets of labels.
func CompareLabels(a, b []prompb.Label) int {
	l := len(a)
	if len(b) < l {
		l = len(b)
	}
	for i := 0; i < l; i++ {
		if d := strings.Compare(a[i].Name, b[i].Name); d != 0 {
			return d
		}
		if d := strings.Compare(a[i].Value, b[i].Value); d != 0 {
			return d
		}
	}
	// If all labels so far were in common, the set with fewer labels comes first.
	return len(a) - len(b)
}

func merge(a, b []prompb.Sample) []prompb.Sample {
	ret := make([]prompb.Sample, 0, len(a)+len(b))
	i, j, k := 0, 0, 0
	for i < len(a) && j < len(b) {
		if a[i].Timestamp == b[j].Timestamp {
			ret = append(ret, a[i])
			i++
			j++
		} else if a[i].Timestamp < b[j].Timestamp {
			ret = append(ret, a[i])
			i++
		} else {
			ret = append(ret, b[j])
			j++
		}
	}
	for ; i < len(a); i++ {
		ret = append(ret, a[i])
		k++
	}
	for ; j < len(b); j++ {
		ret = append(ret, b[j])
		k++
	}
	return ret
}

func ignoreStoresWithoutDataPort(stores []*contracts.StoreHandle) []*contracts.StoreHandle {
	newStores := make([]*contracts.StoreHandle, 0, len(stores))
	for _, store := range stores {
		if store.DataPort != 0 {
			newStores = append(newStores, store)
		}
	}
	return newStores
}

func NewClientTrace(span opentracing.Span) *httptrace.ClientTrace {
	trace := &clientTrace{span: span}
	return &httptrace.ClientTrace{
		DNSStart: trace.dnsStart,
		DNSDone:  trace.dnsDone,
	}
}

type clientTrace struct {
	span opentracing.Span
}

func (h *clientTrace) dnsStart(info httptrace.DNSStartInfo) {
	h.span.LogKV("event", "DNS start", "host", info.Host)
}

func (h *clientTrace) dnsDone(httptrace.DNSDoneInfo) {
	h.span.LogKV("event", "DNS done")
}