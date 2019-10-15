package sherlockio

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"runtime/debug"
	"strconv"
	"time"
)

func reportMetrics(rqs []*requestContext, qs *queryStats, latency float64) {
	defer PanicRecover()

	if len(rqs) == 0 || qs == nil {
		return
	}

	rcs := qs.rcs
	err := qs.errs
	if rcs == nil && err == nil {
		panic("rcs and err are nil")
	}
	code := "200"
	if err != nil {
		code = strconv.Itoa(err[0].responseStatus)
	}
	reqCnt := "1"
	if len(rqs) > 1 {
		reqCnt = "2"
	}
	readLatencyComplete.WithLabelValues(reqCnt, code).Observe(latency)

	if rcs == nil {
		return
	}

	dpCnt := 0
	qWindowInMillis := int64(0)
	storeCnt := 0
	totalSeriesCnt := 0
	allTier := ""
	for _, rc := range rcs {
		code = "200"
		seriesCnt := 0
		storeCnt += len(rc.reqContext.stores)
		qStart := rc.reqContext.query.StartTimestampMs
		qEnd := rc.reqContext.query.EndTimestampMs
		tier := getTierBucket(qStart, qEnd)
		allTier = mergeTier(allTier, tier)
		if rc.qr == nil && rc.data == nil && len(rc.err) > 0 {
			code = strconv.Itoa(rc.err[0].responseStatus)
		} else if rc.qr == nil && rc.data != nil {
			pr, err := decodeReadResponse(&http.Response{Body: ioutil.NopCloser(bytes.NewReader(rc.data))})
			if err != nil {
				code = "500"
			} else {
				rc.qr = pr.Results
			}
		}
		if rc.qr != nil {
			for _, qr := range rc.qr {
				seriesCnt += len(qr.Timeseries)
				for _, ts := range qr.Timeseries {
					dpCnt += len(ts.Samples)
				}
			}
		}

		totalSeriesCnt += seriesCnt
		seriesBkt := getSeriesBucket(seriesCnt)
		namespaceReadLatency.WithLabelValues(rc.reqContext.namespace, tier, code, seriesBkt).Observe(rc.latency)
		seriesReadByNamespace.WithLabelValues(rc.reqContext.namespace).Add(float64(seriesCnt))

		if qWindowInMillis < (qEnd - qStart) {
			qWindowInMillis = qEnd - qStart
		}
	}
	storeBkt := getStoreBucket(storeCnt)
	qWindowBucket := getQueryWindowBucket(qWindowInMillis)
	seriesBkt := getSeriesBucket(totalSeriesCnt)

	seriesRead.Add(float64(totalSeriesCnt))
	dataPointsRead.Add(float64(dpCnt))

	granularReadLatency.WithLabelValues(reqCnt, storeBkt, qWindowBucket, allTier, code, seriesBkt).Observe(latency)
}

func getStoreBucket(storeCnt int) string {
	storeBkt := "1"
	if storeCnt > 1 {
		storeBkt = "2"
	}
	return storeBkt
}

func getSeriesBucket(seriesCnt int) string {
	seriesBkt := "0"
	switch {
	case seriesCnt == 0:
		seriesBkt = "0"
	case seriesCnt < 10000:
		seriesBkt = "1t10k"
	case seriesCnt <= 50000:
		seriesBkt = "10kt50k"
	case seriesCnt <= 100000:
		seriesBkt = "50kt100k"
	case seriesCnt > 100000:
		seriesBkt = "100ktInf"
	}
	return seriesBkt
}

func getTierBucket(qStart int64, qEnd int64) string {
	tier := "hot"
	now := time.Now()
	hotTierStartInMs := now.Add(-1*time.Duration(216)*time.Hour).Unix() * 1000
	longTermEndInMs := now.Add(-1*time.Duration(24)*time.Hour).Unix() * 1000

	if qStart > hotTierStartInMs {
		tier = "hot"
	} else if qStart < hotTierStartInMs && qEnd < longTermEndInMs {
		tier = "cold"
	} else {
		tier = "both"
	}

	return tier
}

func mergeTier(tier string, allTier string) string {
	if allTier != tier {
		if allTier == "" {
			allTier = tier
		} else if tier == "cold" && allTier == "hot" {
			allTier = "both"
		} else if tier == "hot" && allTier == "cold" {
			allTier = "both"
		} else if tier == "both" {
			allTier = tier
		}
	}
	return allTier
}

func getQueryWindowBucket(qWindow int64) string {
	if qWindow < 1000*60*10 { // 10m
		return "10m"
	} else if qWindow < 1000*60*60 { // 1h
		return "10mt1h"
	} else if qWindow < 1000*60*60*24 { // 1d
		return "1ht1d"
	} else {
		return "1dtinf"
	}

}

func PanicRecover() {
	r := recover()
	var err error
	if r != nil {
		switch t := r.(type) {
		case string:
			err = errors.New(t)
		case error:
			err = t
		default:
			err = errors.New("Unknown error")
		}
		fmt.Println("panic", err, "stack", debug.Stack())
	}
}