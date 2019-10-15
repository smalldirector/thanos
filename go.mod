module github.com/thanos-io/thanos

require (
	cloud.google.com/go v0.44.1
	github.com/Azure/azure-storage-blob-go v0.7.0
	github.com/NYTimes/gziphandler v1.1.1
	github.com/armon/go-metrics v0.0.0-20190430140413-ec5e00d3c878
	github.com/cespare/xxhash v1.1.0
	github.com/codahale/hdrhistogram v0.0.0-20161010025455-3a0bb77429bd // indirect
	github.com/fatih/structtag v1.0.0
	github.com/fortytw2/leaktest v1.3.0
	github.com/fsnotify/fsnotify v1.4.7
	github.com/go-kit/kit v0.9.0
	github.com/gogo/protobuf v1.3.0
	github.com/golang/snappy v0.0.1
	github.com/googleapis/gax-go v2.0.2+incompatible
	github.com/gophercloud/gophercloud v0.3.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.1-0.20191002090509-6af20e3a5340
	github.com/hashicorp/golang-lru v0.5.3
	github.com/leanovate/gopter v0.2.4
	github.com/lovoo/gcloud-opentracing v0.3.0
	github.com/mattn/go-ieproxy v0.0.0-20190805055040-f9202b1cfdeb // indirect; Pinned for FreeBSD support.
	github.com/mattn/go-runewidth v0.0.4 // indirect
	github.com/miekg/dns v1.1.19
	github.com/minio/minio-go/v6 v6.0.27-0.20190529152532-de69c0e465ed
	github.com/mozillazg/go-cos v0.12.0
	github.com/mwitkow/go-conntrack v0.0.0-20190716064945-2f068394615f
	github.com/oklog/run v1.0.0
	github.com/oklog/ulid v1.3.1
	github.com/olekukonko/tablewriter v0.0.1
	github.com/opentracing-contrib/go-stdlib v0.0.0-20190519235532-cf7a6c988dc9
	github.com/opentracing/basictracer-go v1.0.0
	github.com/opentracing/opentracing-go v1.1.0
	github.com/pkg/errors v0.8.1
	github.com/prometheus/client_golang v1.1.0
	github.com/prometheus/client_model v0.0.0-20190812154241-14fe0d1b01d4
	github.com/prometheus/common v0.7.0
	github.com/prometheus/prometheus v2.13.0+incompatible // v1.8.2 is misleading as Prometheus does not have v2 module.
	github.com/stretchr/testify v1.4.0
	github.com/uber-go/atomic v1.4.0 // indirect
	github.com/uber/jaeger-client-go v2.16.0+incompatible
	github.com/uber/jaeger-lib v2.2.0+incompatible
	github.corp.ebay.com/sherlockio/commons v0.0.0-20191009222316-2dfaa7e5646a
	github.corp.ebay.com/sherlockio/egress-ebay v1.0.7
	github.corp.ebay.com/sherlockio/lookup-store v0.0.0-20190430223704-22aab020fcda
	github.corp.ebay.com/sherlockio/prom-common-lib v0.0.3-0.20191014084944-4351a44cbee9
	go.elastic.co/apm v1.5.0
	go.elastic.co/apm/module/apmot v1.5.0
	go.uber.org/automaxprocs v1.2.0
	golang.org/x/crypto v0.0.0-20191002192127-34f69633bfdc // indirect
	golang.org/x/oauth2 v0.0.0-20190604053449-0f29369cfe45
	golang.org/x/sync v0.0.0-20190423024810-112230192c58
	golang.org/x/text v0.3.2
	google.golang.org/api v0.11.0
	google.golang.org/genproto v0.0.0-20190819201941-24fa4b261c55
	google.golang.org/grpc v1.22.1
	gopkg.in/alecthomas/kingpin.v2 v2.2.6
	gopkg.in/fsnotify.v1 v1.4.7
	gopkg.in/natefinch/lumberjack.v2 v2.0.0-20150622162204-20b71e5b60d7
	gopkg.in/yaml.v2 v2.2.2
)

// We want to replace the client-go version with a specific commit hash,
// so that we don't get errors about being incompatible with the Go proxies.
// See https://github.com/thanos-io/thanos/issues/1415
replace (
	github.com/prometheus/prometheus => github.corp.ebay.com/Sherlock/prometheus v0.0.0-20191012024804-4b0cd9c56adb
	github.corp.ebay.com/sherlockio/egress-ebay v1.0.7 => ../egress-ebay
	github.corp.ebay.com/sherlockio/prom-common-lib v0.0.2 => github.corp.ebay.com/sherlockio/prom-common-lib v0.0.3-0.20191014084944-4351a44cbee9
	golang.org/x/sys => golang.org/x/sys v0.0.0-20190412213103-97732733099d // v0.0.0-20190425145619-16072639606e (multiple-value "golang.org/x/sys/windows".GetCurrentProcess() in single-value context) Required to build properly on windows for github.com/elastic/go-sysinfo.
	k8s.io/api => k8s.io/api v0.0.0-20190620084959-7cf5895f2711
	k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.0.0-20190620085554-14e95df34f1f
	k8s.io/apimachinery => k8s.io/apimachinery v0.0.0-20190612205821-1799e75a0719
	k8s.io/client-go => k8s.io/client-go v0.0.0-20190620085101-78d2af792bab
	k8s.io/code-generator => k8s.io/code-generator v0.0.0-20190612205613-18da4a14b22b
	k8s.io/klog => k8s.io/klog v0.3.1
	k8s.io/kube-openapi => k8s.io/kube-openapi v0.0.0-20190228160746-b3a7cee44a30
	k8s.io/kubernetes => k8s.io/kubernetes v1.14.3
)

go 1.13
