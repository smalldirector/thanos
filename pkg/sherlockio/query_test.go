package sherlockio

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oklog/run"
	"github.corp.ebay.com/sherlockio/egress-ebay/cmd/egress/health"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"

	"github.com/prometheus/prometheus/prompb"
	"github.com/thanos-io/thanos/pkg/tracing"
	"github.com/opentracing/opentracing-go"
	"github.corp.ebay.com/sherlockio/egress-ebay/cmd/egress/auth"
	"github.corp.ebay.com/sherlockio/egress-ebay/cmd/egress/common"
	"github.corp.ebay.com/sherlockio/egress-ebay/cmd/egress/discovery"
)

func HandleReadRequest(t *testing.T) {

}

func DoSingleStoreRead(t *testing.T) {
	runtime.GOMAXPROCS(6)
	query := prompb.Query{Matchers: []prompb.LabelMatcher{{Type: prompb.LabelMatcher_EQ, Name: "_namespace_", Value: "thanostest"},
		{Type: prompb.LabelMatcher_EQ, Name: "__name__", Value: "testmetric0"},
	}}
	readReq := &prompb.ReadRequest{Queries: []prompb.Query{query}}
	pr := GetResponse(readReq)
	assert.True(t, len(pr.Results) > 0)
	fmt.Println(len(pr.Results))
	for _, qr := range pr.Results {
		assert.True(t, len(qr.Timeseries) > 0)
		fmt.Println("series: ", len(qr.Timeseries))
	}
}

func MultipleStoreRead(t *testing.T) {
	runtime.GOMAXPROCS(6)
	query1 := prompb.Query{Matchers: []prompb.LabelMatcher{{Type: prompb.LabelMatcher_EQ, Name: "_namespace_", Value: "thanostest"},
		{Type: prompb.LabelMatcher_EQ, Name: "__name__", Value: "testmetric0"},
	}}
	query2 := prompb.Query{Matchers: []prompb.LabelMatcher{{Type: prompb.LabelMatcher_EQ, Name: "_namespace_", Value: "thanostest"},
		{Type: prompb.LabelMatcher_EQ, Name: "__name__", Value: "testmetric1"},
	}}
	readReq := &prompb.ReadRequest{Queries: []prompb.Query{query1, query2}}
	pr := GetResponse(readReq)
	fmt.Println(len(pr.Results))
	for index, qr := range pr.Results {
		assert.True(t, len(qr.Timeseries) > 0)
		fmt.Println("index", index, "series: ", len(qr.Timeseries))
	}
}

func GetResponse(req *prompb.ReadRequest) *prompb.ReadResponse {
	config, err := common.ParseConfigFile("config.yml", "storage.yml", "")
	logger := common.GetLogger("")
	tracer := opentracing.GlobalTracer()
	sd, err := discovery.NewDiscovery(*config, tracer)
	if err != nil {
		common.ExitIfError(err, logger, "cannot init discovery")
	}

	a, err := auth.NewAuthHandler(*config, logger, tracer)
	if err != nil {
		panic(err)
	}
	_, err = health.NewReplicaStateManager(&run.Group{}, config, opentracing.GlobalTracer())
	if err != nil {
		panic(err)
	}

	protoReq, err := proto.Marshal(req)
	if err != nil {
		panic(err)
	}
	encoded := snappy.Encode(nil, protoReq)

	qh := NewQueryHandler(*config, logger, tracer, sd, a)
	httpReq, err := http.NewRequest("POST", strings.Join([]string{"http://test:", common.Remote_Read_URI}, ""), bytes.NewReader(encoded))
	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Set("X-Prometheus-Remote-Read-Version", "0.1.0")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("Accept-Encoding", "snappy")
	if err != nil {
		panic(err)
	}

	span, _ := tracing.StartSpan(httpReq.Context(), "sherlockio_remote_read_handler")
	defer span.Finish()
	rw := httptest.NewRecorder()
	qh.HandleReadRequest(rw, httpReq, span.Context())
	resp := rw.Result()
	pr, err := decodeReadResponse(resp)
	if err != nil {
		panic(err)
	}

	return pr
}

func TestUseSnappy(t *testing.T) {
	h := http.Header{"Accept-Encoding": []string{"snappy"}}
	fmt.Println(useSnappy(&h))
}