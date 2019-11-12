package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/compact/retention"
	"github.com/thanos-io/thanos/pkg/extflag"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/runutil"
	"gopkg.in/alecthomas/kingpin.v2"
)

func registerRetention(m map[string]setupFunc, app *kingpin.Application, name string) {
	cmd := app.Command(name, "continuously dedup blocks in an object store bucket")

	dataDir := cmd.Flag("data-dir", "Data directory in which to cache blocks and process deduplication.").
		Default("./data").String()

	consistencyDelay := modelDuration(cmd.Flag("consistency-delay", "Minimum age of fresh (non-dedup) blocks before they are being processed.").
		Default("30m"))

	blockSyncConcurrency := cmd.Flag("block-sync-concurrency", "Number of goroutines to use when syncing block metadata from object storage.").
		Default("20").Int()

	blockSyncTimeout := modelDuration(cmd.Flag("block-sync-timeout", "Timeout duration when syncing block from object storage. 0s - disables this timeout").Default("0s"))

	objStoreConfig := regCommonObjStoreFlags(cmd, "", true)

	retentionPolicyConfig := regRetentionPolicyConfig(cmd)

	m[name] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ bool) error {
		return runRetention(g, logger, reg, *dataDir, time.Duration(*consistencyDelay), *blockSyncConcurrency, time.Duration(*blockSyncTimeout), objStoreConfig, retentionPolicyConfig, name)
	}
}

func runRetention(g *run.Group, logger log.Logger, reg *prometheus.Registry, dataDir string, consistencyDelay time.Duration, blockSyncConcurrency int, blockSyncTimeout time.Duration, objStoreConfig *extflag.PathOrContent, retentionPolicyConfig *extflag.PathOrContent, component string) error {
	confContentYaml, err := objStoreConfig.Content()
	if err != nil {
		return err
	}

	bkt, err := client.NewBucket(logger, confContentYaml, reg, component)
	if err != nil {
		if bkt != nil {
			runutil.CloseWithLogOnErr(logger, bkt, "bucket client")
		}
		return err
	}

	policyConfigYaml, err := retentionPolicyConfig.Content()
	if err != nil {
		runutil.CloseWithLogOnErr(logger, bkt, "bucket client")
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	retentionDir := filepath.Join(dataDir, "retention")

	if err := os.RemoveAll(retentionDir); err != nil {
		cancel()
		return errors.Wrap(err, "clean working downsample directory")
	}
	syncer := compact.NewMetaSyncer(logger, reg, bkt, consistencyDelay, blockSyncConcurrency, blockSyncTimeout)
	g.Add(func() error {
		defer runutil.CloseWithLogOnErr(logger, bkt, "bucket client")

		return retention.ApplyRetentionPolicy(ctx, logger, policyConfigYaml, reg, bkt, blockSyncTimeout, syncer, retentionDir)
	}, func(error) {
		cancel()
	})
	return nil
}

func regRetentionPolicyConfig(cmd *kingpin.CmdClause) *extflag.PathOrContent {
	help := fmt.Sprintf("YAML file that contains policy configuration <policy.config-yaml>.")
	return extflag.RegisterPathOrContent(cmd, fmt.Sprintf("retention.policy"), help, true)
}