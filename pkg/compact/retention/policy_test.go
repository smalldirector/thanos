package retention

import (
	"context"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore/inmem"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestPolicyAssigner_Assign(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	logger := log.NewNopLogger()
	confContentYaml := mockPolicyConfigsYaml(t)
	pcs, err := ParsePolicyConfigsFromYaml(confContentYaml)
	testutil.Ok(t, err)
	policies, err := pcs.ToPolicies()
	testutil.Ok(t, err)
	pa := NewPolicyAssigner(logger, inmem.NewBucket(), 30 * time.Second,  policies)
	activeMeta := mockMeta(t, 0, 100, 200)
	pg := pa.Assign(ctx, activeMeta)
	testutil.Assert(t, pg.isDefaultPolicyActive, "assign policies")
	testutil.Assert(t, len(pg.active) == 3, "assign policies")
	testutil.Assert(t, len(pg.inactive) == 0, "assign policies")
	now := time.Now().UnixNano() / int64(time.Millisecond)
	staleMeta := mockMeta(t, 0, now - int64(32 * 24 * time.Hour) / int64(time.Millisecond), now - int64(29 * 24 * time.Hour) / int64(time.Millisecond))
	pg = pa.Assign(ctx, staleMeta)
	testutil.Assert(t, !pg.isDefaultPolicyActive, "assign policies")
	testutil.Assert(t, len(pg.active) == 1, "assign policies")
	testutil.Assert(t, len(pg.inactive) == 2, "assign policies")
}

func TestPolicyConfigs_ToPolicies(t *testing.T) {
	confContentYaml := mockPolicyConfigsYaml(t)
	pcs, err := ParsePolicyConfigsFromYaml(confContentYaml)
	testutil.Ok(t, err)
	policies, err := pcs.ToPolicies()
	testutil.Ok(t, err)
	testutil.Assert(t, len(policies) == 3, "parse policies")
	for _, v := range policies {
		for _, p := range v {
			if len(p.lset) == 0 {
				testutil.Assert(t, p.isDefaultPolicy(), "parse policies")
			} else {
				testutil.Assert(t, !p.isDefaultPolicy(), "parse policies")
			}
		}
	}
}

func TestParsePolicyConfigsFromYaml(t *testing.T) {
	confContentYaml := mockPolicyConfigsYaml(t)
	pcs, err := ParsePolicyConfigsFromYaml(confContentYaml)
	testutil.Ok(t, err)
	testutil.Assert(t, len(pcs.Configs) == 3, "parse policy configs from Yaml")
}

func mockPolicyConfigsYaml(t *testing.T) []byte {
	var b strings.Builder
	b.WriteString("policies:\n")
	b.WriteString("  - expr: \"{}\"\n")
	b.WriteString("    retentions:\n")
	b.WriteString("      res-raw: 30d\n")
	b.WriteString("      res-5m: 90d\n")
	b.WriteString("      res-1h: 180d\n")
	b.WriteString("  - expr: metric_name{key=\"value1\"}\n")
	b.WriteString("    retentions:\n")
	b.WriteString("      res-raw: 45d\n")
	b.WriteString("      res-5m: 180d\n")
	b.WriteString("      res-1h: 360d\n")
	b.WriteString("  - expr: metric_name{key=\"value2\"}\n")
	b.WriteString("    retentions:\n")
	b.WriteString("      res-raw: 15d\n")
	b.WriteString("      res-5m: 30d\n")
	b.WriteString("      res-1h: 90d\n")
	return []byte(b.String())
}

func mockMeta(t *testing.T, resolution, minTime, maxTime int64) *metadata.Meta {
	meta := metadata.Meta{}
	meta.Version = metadata.MetaVersion1
	meta.BlockMeta = tsdb.BlockMeta{
		ULID:    ulid.MustNew(ulid.Now(), rand.New(rand.NewSource(time.Now().UnixNano()))),
		MinTime: minTime,
		MaxTime: maxTime,
	}
	labels := make(map[string]string)
	labels["shard"] = "shard"
	labels["replica"] = "replica"
	meta.Thanos = metadata.Thanos{
		Labels: labels,
		Downsample: metadata.ThanosDownsample{
			Resolution: resolution,
		},
		Source: metadata.TestSource,
	}
	return &meta
}