package retention

import (
	"context"
	"fmt"
	"github.com/thanos-io/thanos/pkg/compact"
	"os"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/objstore"
)

func ApplyRetentionPolicy(ctx context.Context, logger log.Logger, confContentYaml []byte, reg prometheus.Registerer,
	bkt objstore.Bucket, blockSyncTimeout time.Duration, syncer *compact.MetaSyncer, dir string) error {
	if err := initWorkspace(dir); err != nil {
		return err
	}
	pcs, err := ParsePolicyConfigsFromYaml(confContentYaml)
	if err != nil {
		return err
	}
	level.Info(logger).Log("msg", "starting to apply retention", "policies", fmt.Sprint(pcs))
	policies, err := pcs.ToPolicies()
	if err != nil {
		return err
	}
	keeper := NewBucketKeeper(logger, reg, bkt, syncer, blockSyncTimeout, dir, policies)
	if err := keeper.Handle(ctx); err != nil {
		return err
	}
	level.Info(logger).Log("msg", "retention apply done")
	return nil
}

func initWorkspace(dir string) error {
	if err := os.RemoveAll(dir); err != nil {
		return errors.Wrap(err, "clean up the retention temporary directory")
	}
	if err := os.MkdirAll(dir, 0777); err != nil {
		return errors.Wrap(err, "create the retention temporary directory")
	}
	return nil
}
