package compact

import (
	"context"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"
)

// MetaSyncer fetches all the blocks' metadata information for given bucket
type MetaSyncer struct {
	logger               log.Logger
	bkt                  objstore.Bucket
	consistencyDelay     time.Duration
	blockSyncConcurrency int
	blockSyncTimeout     time.Duration

	metrics   *metaSyncerMetrics
	mtx       sync.Mutex
	blocksMtx sync.Mutex
}

type metaSyncerMetrics struct {
	syncMetas        *prometheus.CounterVec
	syncMetaFailures *prometheus.CounterVec
	syncMetaDuration *prometheus.HistogramVec
}

func newMetaSyncerMetrics(reg prometheus.Registerer) *metaSyncerMetrics {
	var m metaSyncerMetrics

	m.syncMetas = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_dedup_sync_meta_total",
		Help: "Total number of sync meta operations.",
	}, []string{"bucket"})
	m.syncMetaFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_dedup_sync_meta_failures",
		Help: "Total number of failed sync meta operations.",
	}, []string{"bucket", "block"})
	m.syncMetaDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "thanos_dedup_sync_meta_duration_seconds",
		Help: "Time it took to sync meta files.",
		Buckets: []float64{
			0.25, 0.6, 1, 2, 3.5, 5, 7.5, 10, 15, 30, 60, 100, 200, 500,
		},
	}, []string{"bucket"})

	reg.MustRegister(m.syncMetas, m.syncMetaFailures, m.syncMetaDuration)

	return &m
}

// NewMetaSyncer return a new MetaSyncer with given bucket information
func NewMetaSyncer(logger log.Logger, reg prometheus.Registerer, bkt objstore.Bucket, consistencyDelay time.Duration,
	blockSyncConcurrency int, blockSyncTimeout time.Duration) *MetaSyncer {
	return &MetaSyncer{
		logger:               logger,
		bkt:                  bkt,
		consistencyDelay:     consistencyDelay,
		blockSyncConcurrency: blockSyncConcurrency,
		blockSyncTimeout:     blockSyncTimeout,
		metrics:              newMetaSyncerMetrics(reg),
	}
}

func (s *MetaSyncer) Sync(ctx context.Context) ([]*metadata.Meta, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	var wg sync.WaitGroup
	defer wg.Wait()

	blocks := make(map[ulid.ULID]*metadata.Meta)
	metaIdsChan := make(chan ulid.ULID)
	errChan := make(chan error, s.blockSyncConcurrency)

	var syncCtx context.Context
	var cancel context.CancelFunc
	if s.blockSyncTimeout.Seconds() > 0 {
		syncCtx, cancel = context.WithTimeout(ctx, s.blockSyncTimeout)
	} else {
		syncCtx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	for i := 0; i < s.blockSyncConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range metaIdsChan {
				s.blocksMtx.Lock()
				_, seen := blocks[id]
				s.blocksMtx.Unlock()
				if seen {
					continue
				}
				s.metrics.syncMetas.WithLabelValues(s.bkt.Name()).Inc()
				begin := time.Now()
				meta, err := s.download(syncCtx, id)
				s.metrics.syncMetaDuration.WithLabelValues(s.bkt.Name()).Observe(time.Since(begin).Seconds())
				if err != nil {
					errChan <- err
					s.metrics.syncMetaFailures.WithLabelValues(s.bkt.Name(), id.String()).Inc()
					return
				}
				if meta == nil {
					continue
				}
				s.blocksMtx.Lock()
				blocks[id] = meta
				s.blocksMtx.Unlock()
			}
		}()
	}

	remote := map[ulid.ULID]struct{}{}

	err := s.bkt.Iter(ctx, "", func(name string) error {
		id, ok := block.IsBlockDir(name)
		if !ok {
			return nil
		}

		remote[id] = struct{}{}

		select {
		case <-ctx.Done():
		case metaIdsChan <- id:
		}
		return nil
	})

	close(metaIdsChan)

	if err != nil {
		return nil, Retry(errors.Wrapf(err, "sync block metas from bucket %s", s.bkt.Name()))
	}

	wg.Wait()
	close(errChan)

	if err := <-errChan; err != nil {
		return nil, Retry(errors.Wrapf(err, "download block metas from bucket %s", s.bkt.Name()))
	}

	result := make([]*metadata.Meta, 0)
	for id, b := range blocks {
		if _, ok := remote[id]; ok {
			result = append(result, b)
		}
	}
	return result, nil
}

func (s *MetaSyncer) download(ctx context.Context, id ulid.ULID) (*metadata.Meta, error) {
	meta, err := block.DownloadMeta(ctx, s.logger, s.bkt, id)
	if err != nil {
		return nil, Retry(errors.Wrapf(err, "downloading block meta %s", id))
	}
	if ulid.Now()-id.Time() < uint64(s.consistencyDelay/time.Millisecond) {
		level.Debug(s.logger).Log("msg", "block is too fresh for now", "consistency-delay", s.consistencyDelay, "block", id)
		return nil, nil
	}
	level.Debug(s.logger).Log("msg", "downloaded block meta", "block", id)
	return &meta, nil
}
