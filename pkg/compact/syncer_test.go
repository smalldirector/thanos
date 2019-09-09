package compact

import (
	"context"
	"github.com/prometheus/prometheus/tsdb/labels"
	"io/ioutil"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/inmem"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestMetaSyncer_Sync(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	logger := log.NewNopLogger()
	reg := prometheus.NewRegistry()
	bkt := mockObjectStoreBucket(t, ctx, logger)

	syncer := NewMetaSyncer(logger, reg, bkt, 0, 1, 0)

	metas, err := syncer.Sync(ctx)
	testutil.Ok(t, err)
	testutil.Assert(t, len(metas) == 2, "meta syncer failure")
}

func mockObjectStoreBucket(t *testing.T, ctx context.Context, logger log.Logger) objstore.Bucket {
	dataDir, err := ioutil.TempDir("", "thanos-dedup-bucket")
	testutil.Ok(t, err)
	bkt := inmem.NewBucket()

	id0 := createBlock(t, ctx, dataDir, "r0")
	err = objstore.UploadDir(ctx, logger, bkt, filepath.Join(dataDir, id0.String()), id0.String())
	testutil.Ok(t, err)

	id1 := createBlock(t, ctx, dataDir, "r1")
	err = objstore.UploadDir(ctx, logger, bkt, filepath.Join(dataDir, id1.String()), id1.String())
	testutil.Ok(t, err)

	return bkt
}

func createBlock(t *testing.T, ctx context.Context, dataDir string, replica string) ulid.ULID {
	globalConfigs := make(map[string]string)
	globalConfigs["shard"] = "s0"
	globalConfigs["replica"] = replica

	var lset []labels.Labels
	if replica == "r0" {
		lset = []labels.Labels{
			{{Name: "a", Value: "1"}},
			{{Name: "a", Value: "2"}},
			{{Name: "a", Value: "3"}},
			{{Name: "a", Value: "4"}},
			{{Name: "b", Value: "1"}},
		}
	} else {
		lset = []labels.Labels{
			{{Name: "a", Value: "1"}},
			{{Name: "a", Value: "2"}},
			{{Name: "a", Value: "3"}},
			{{Name: "b", Value: "1"}},
			{{Name: "b", Value: "2"}},
		}
	}
	id, err := testutil.CreateBlock(ctx, dataDir, lset, 100, 0, 1000, labels.FromMap(globalConfigs), 0)
	testutil.Ok(t, err)
	return id
}
