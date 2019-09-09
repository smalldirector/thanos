package retention

import (
	"context"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/labels"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/compact/dedup"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/objstore"
)

type seriesEvaluator struct {
}

// evaluate policies on the series and see whether it should be purged or not.
// return true means the series should be purged
// return false means the series should not be purged
func (e *seriesEvaluator) Evaluate(lset labels.Labels, pg *policyGroup) (bool, error) {
	if len(pg.inactive) == 0 {
		return true, nil
	}
	m := lset.Map()
	if pg.isDefaultPolicyActive {
		// if default policy is active, check whether there is matched inactive policy or not
		// if find matched inactive policy, do not purge the series at this time. If not, purge it in this round.
		for _, p := range pg.inactive {
			matched, err := e.isMatched(p.lset.Map(), m)
			if err != nil {
				return false, err
			}
			if matched {
				return false, nil
			}
		}
		return true, nil
	}
	// three steps to detect the series should be purged or not when default policy is not active
	// step 1: check whether there is any matched active policy
	// step 2: if find matched active policy, check whether there is any matched inactive policy
	// step 3: if find matched inactive policy, do not purge the series at this time. If not, purge it in this round.
	foundMatchedActivePolicy := false
	for _, p := range pg.active {
		matched, err := e.isMatched(p.lset.Map(), m)
		if err != nil {
			return false, err
		}
		if matched {
			foundMatchedActivePolicy = true
			break
		}
	}
	if !foundMatchedActivePolicy {
		return false, nil
	}
	foundMatchedInActivePolicy := false
	for _, p := range pg.inactive {
		// don't compare the regular policy with default policy
		// otherwise it means we have no way set retention time shorter than the one defined in default policy
		if p.isDefaultPolicy() {
			continue
		}
		matched, err := e.isMatched(p.lset.Map(), m)
		if err != nil {
			return false, err
		}
		if matched {
			foundMatchedInActivePolicy = true
			break
		}
	}
	return !foundMatchedInActivePolicy, nil
}

// compare lset from policy and series
func (e *seriesEvaluator) isMatched(plset map[string]string, slset map[string]string) (bool, error) {
	matched := true
	for k, v := range plset {
		if _, ok := slset[k]; !ok {
			matched = false
			break
		}
		ok, err := regexp.MatchString(v, slset[k])
		if err != nil {
			return false, err
		}
		if !ok {
			matched = false
			break
		}
	}
	return matched, nil
}

type bucketKeeper struct {
	logger log.Logger
	bkt    objstore.Bucket

	syncer  *compact.MetaSyncer
	keeper  *blockKeeper
	metrics *bucketKeeperMetrics
}

type bucketKeeperMetrics struct {
	handledBlocks        *prometheus.CounterVec
	handledBlockFailures *prometheus.CounterVec
}

func newBucketKeeperMetrics(reg prometheus.Registerer) *bucketKeeperMetrics {
	var m bucketKeeperMetrics

	m.handledBlocks = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_retention_handled_blocks_total",
		Help: "Total number of blocks handled in the retention process.",
	}, []string{"bucket"})
	m.handledBlockFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_retention_handled_block_failures",
		Help: "Total number of failed blocks.",
	}, []string{"bucket", "block"})

	reg.MustRegister(m.handledBlocks)

	return &m
}

func NewBucketKeeper(logger log.Logger, reg prometheus.Registerer, bkt objstore.Bucket, syncer *compact.MetaSyncer,
	blockSyncTimeout time.Duration, dir string, polices map[resType][]*policy) *bucketKeeper {
	return &bucketKeeper{
		logger:  logger,
		bkt:     bkt,
		syncer:  syncer,
		keeper:  NewBlockKeeper(logger, bkt, dir, blockSyncTimeout, polices),
		metrics: newBucketKeeperMetrics(reg),
	}
}

func (k *bucketKeeper) Handle(ctx context.Context) error {
	metas, err := k.syncer.Sync(ctx)
	if err != nil {
		return err
	}
	for _, meta := range metas {
		handled, err := k.keeper.Handle(ctx, meta)
		if err != nil {
			k.metrics.handledBlockFailures.WithLabelValues(k.bkt.Name(), meta.ULID.String()).Inc()
			return errors.Wrapf(err, "failed to handle block: %s", meta.ULID.String())
		}
		if handled {
			k.metrics.handledBlocks.WithLabelValues(k.bkt.Name()).Inc()
		}
	}
	return nil
}

type blockKeeper struct {
	logger  log.Logger
	bkt     objstore.Bucket
	dir     string
	timeout time.Duration

	pa        *policyAssigner
	evaluator *seriesEvaluator
}

func NewBlockKeeper(logger log.Logger, bkt objstore.Bucket, dir string, timeout time.Duration, polices map[resType][]*policy) *blockKeeper {
	return &blockKeeper{
		logger:    logger,
		bkt:       bkt,
		dir:       dir,
		timeout:   timeout,
		pa:        NewPolicyAssigner(logger, bkt, timeout, polices),
		evaluator: &seriesEvaluator{},
	}
}

func (bk *blockKeeper) Handle(ctx context.Context, b *metadata.Meta) (bool, error) {
	// get active policies for target block
	pg := bk.pa.Assign(ctx, b)
	if pg == nil {
		return false, nil
	}
	// if no policies defined, keep the block forever
	if len(pg.inactive) == 0 && len(pg.active) == 0 {
		return false, nil
	}
	// if no inactive policies, delete remote block directly
	if len(pg.inactive) == 0 {
		// clean old block
		if err := bk.cleanRemote(ctx, b.ULID); err != nil {
			return true, err
		}
		level.Info(bk.logger).Log("msg", "deleted out-dated block", "block", b.ULID, "minTime", b.MinTime, "maxTime", b.MaxTime)
		return true, nil
	}
	// if no active policies, no need to do action on the block
	if len(pg.active) == 0 {
		level.Debug(bk.logger).Log("msg", "no active policies found, skip it", "block", b.ULID, "minTime", b.MinTime, "maxTime", b.MaxTime)
		return false, nil
	}
	// download block
	if err := bk.download(ctx, b.ULID); err != nil {
		return true, err
	}
	// apply policies on the block
	newId, hasDataLeft, hasDataPurged, err := bk.apply(b, pg)
	if err != nil {
		return true, err
	}

	// upload the new block and log applied policies
	if hasDataLeft {
		var tid *ulid.ULID
		if hasDataPurged {
			tid = newId
			// upload new block
			if err := bk.upload(ctx, newId); err != nil {
				return true, err
			}
		} else {
			tid = &b.ULID
		}
		// log applied policies on target block
		if err := bk.log(ctx, tid, pg); err != nil {
			level.Warn(bk.logger).Log("msg", "failed to log applied policies", "block", b.ULID,"err", err)
		}
	}

	// remove remote block
	if !hasDataLeft || hasDataPurged {
		if err := bk.cleanRemote(ctx, b.ULID); err != nil {
			return true, err
		}
		level.Info(bk.logger).Log("msg", "deleted out-dated block", "block", b.ULID, "minTime", b.MinTime, "maxTime", b.MaxTime)
	}
	// remove local blocks
	if newId != nil {
		bk.cleanLocal(*newId)
	}
	bk.cleanLocal(b.ULID)
	return true, nil
}

func (bk *blockKeeper) apply(b *metadata.Meta, pg *policyGroup) (*ulid.ULID, bool, bool, error) {
	hasDataLeft := true
	hasDataPurged := false

	br, err := dedup.NewBlockReader(bk.logger, b.Thanos.Downsample.Resolution, filepath.Join(bk.dir, b.ULID.String()))
	if err != nil {
		if err := br.Close(); err != nil {
			level.Warn(bk.logger).Log("msg", "failed to close block reader", "err", err)
		}
		return nil, hasDataLeft, hasDataPurged, err
	}

	defer func() {
		if err := br.Close(); err != nil {
			level.Warn(bk.logger).Log("msg", "failed to close block reader", "err", err)
		}
	}()

	symbols, changed, err := bk.rebuildSymbols(br.IndexReader(), pg)
	if err != nil {
		return nil, hasDataLeft, hasDataPurged, err
	}
	if !changed {
		level.Debug(bk.logger).Log("msg", "no matched policies found, skip it", "block", b.ULID, "minTime", b.MinTime, "maxTime", b.MaxTime)
		return nil, hasDataLeft, hasDataPurged, nil
	}

	newMeta := NewBlockMeta(*b)
	bw, err := downsample.NewStreamedBlockWriter(filepath.Join(bk.dir, newMeta.ULID.String()), symbols, bk.logger, *newMeta)
	if err != nil {
		return nil, false, false, err
	}

	postings := br.Postings()
	ir := br.IndexReader()

	var lset labels.Labels
	var chks []chunks.Meta

	for postings.Next() {
		lset = lset[:0]
		chks = chks[:0]
		if err := ir.Series(postings.At(), &lset, &chks); err != nil {
			return nil, hasDataLeft, hasDataPurged, err
		}
		matched, err := bk.evaluator.Evaluate(lset, pg)
		if err != nil {
			return nil, false, false, err
		}
		if matched {
			hasDataPurged = true
			continue
		}
		// populate chunks
		for i := range chks {
			chks[i].Chunk, err = br.ChunkReader().Chunk(chks[i].Ref)
			if err != nil {
				return nil, hasDataLeft, hasDataPurged, err
			}
		}
		if err := bw.WriteSeries(lset, chks); err != nil {
			return nil, hasDataLeft, hasDataPurged, err
		}
		hasDataLeft = true
	}

	if err := bw.Close(); err != nil {
		return nil, hasDataLeft, hasDataPurged, err
	}

	return &newMeta.ULID, hasDataLeft, hasDataPurged, nil
}

func (bk *blockKeeper) rebuildSymbols(ir tsdb.IndexReader, pg *policyGroup) (map[string]struct{}, bool, error) {
	isSymbolsChanged := false
	symbols := make(map[string]struct{})
	rawPostings, err := ir.Postings(index.AllPostingsKey())
	if err != nil {
		return symbols, isSymbolsChanged, err
	}
	postings := ir.SortedPostings(rawPostings)
	var lset labels.Labels
	var chks []chunks.Meta
	for postings.Next() {
		lset = lset[:0]
		chks = chks[:0]
		if err := ir.Series(postings.At(), &lset, &chks); err != nil {
			return symbols, isSymbolsChanged, err
		}
		matched, err := bk.evaluator.Evaluate(lset, pg)
		if err != nil {
			return symbols, isSymbolsChanged, err
		}
		if matched {
			isSymbolsChanged = true
			continue
		}
		for _, label := range lset {
			symbols[label.Name] = struct{}{}
			symbols[label.Value] = struct{}{}
		}
	}
	return symbols, isSymbolsChanged, nil
}

func (bk *blockKeeper) log(ctx context.Context, id *ulid.ULID, pg *policyGroup) error {
	blockDir := filepath.Join(bk.dir, id.String())
	meta := NewAppliedPolicyMeta(pg)
	if err := WriteMetaFile(bk.logger, blockDir, meta); err != nil {
		level.Warn(bk.logger).Log("msg", "write policy file failure", "block", id)
		return err
	}
	if err := UploadPolicyFile(ctx, bk.logger, bk.timeout, bk.dir, bk.bkt, *id); err != nil {
		level.Warn(bk.logger).Log("msg", "uploaded policy file failure", "block", id)
		return err
	}
	level.Debug(bk.logger).Log("msg", "logged applied policies", "block", id)
	return nil
}

func (bk *blockKeeper) download(ctx context.Context, id ulid.ULID) error {
	if err := DownloadBlock(ctx, bk.logger, bk.timeout, bk.dir, bk.bkt, id); err != nil {
		return compact.Retry(err)
	}
	level.Debug(bk.logger).Log("msg", "downloaded block", "block", id)
	return nil
}

func (bk *blockKeeper) upload(ctx context.Context, id *ulid.ULID) error {
	if id == nil {
		return nil
	}
	if err := UploadBlock(ctx, bk.logger, bk.timeout, bk.dir, bk.bkt, *id); err != nil {
		return compact.Retry(err)
	}
	level.Debug(bk.logger).Log("msg", "uploaded block", "block", id)
	return nil
}

func (bk *blockKeeper) cleanRemote(ctx context.Context, id ulid.ULID) error {
	if err := DeleteRemoteBlock(ctx, bk.timeout, bk.bkt, id); err != nil {
		return compact.Retry(err)
	}
	level.Debug(bk.logger).Log("msg", "deleted remote block", "block", id)
	return nil
}

func (bk *blockKeeper) cleanLocal(id ulid.ULID) {
	if err := DeleteLocalBlock(bk.dir, id); err != nil {
		level.Warn(bk.logger).Log("msg", "delete local block failure", "block", id)
	} else {
		level.Debug(bk.logger).Log("msg", "deleted local block", "block", id)
	}
}

func NewBlockMeta(base metadata.Meta) *metadata.Meta {
	newMeta := base
	newMeta.ULID = ulid.MustNew(ulid.Now(), rand.New(rand.NewSource(time.Now().UnixNano())))
	return &newMeta
}

func DownloadBlock(ctx context.Context, logger log.Logger, timeout time.Duration, dir string, bkt objstore.Bucket, id ulid.ULID) error {
	_ = DeleteLocalBlock(dir, id)
	blockDir := filepath.Join(dir, id.String())
	workCtx, cancel := TimeoutContext(ctx, timeout)
	defer cancel()
	err := block.Download(workCtx, logger, bkt, id, blockDir)
	if err != nil {
		return errors.Wrapf(err, "download block %s", id)
	}
	return nil
}

func UploadBlock(ctx context.Context, logger log.Logger, timeout time.Duration, dir string, bkt objstore.Bucket, id ulid.ULID) error {
	blockDir := filepath.Join(dir, id.String())
	workCtx, cancel := TimeoutContext(ctx, timeout)
	defer cancel()
	if err := block.Upload(workCtx, logger, bkt, blockDir); err != nil {
		return errors.Wrapf(err, "upload block %s", id)
	}
	return nil
}

func UploadPolicyFile(ctx context.Context, logger log.Logger, timeout time.Duration, dir string, bkt objstore.Bucket, id ulid.ULID) error {
	blockDir := filepath.Join(dir, id.String())
	workCtx, cancel := TimeoutContext(ctx, timeout)
	defer cancel()
	if err := objstore.UploadFile(workCtx, logger, bkt, path.Join(blockDir, AppliedPolicyMetaFile), path.Join(id.String(), AppliedPolicyMetaFile)); err != nil {
		return errors.Wrap(err, "upload applied policy file")
	}
	return nil
}

func DeleteLocalBlock(dir string, id ulid.ULID) error {
	blockDir := filepath.Join(dir, id.String())
	if err := os.RemoveAll(blockDir); err != nil {
		return errors.Wrapf(err, "delete local block %s", id)
	}
	return nil
}

func DeleteRemoteBlock(ctx context.Context, timeout time.Duration, bkt objstore.Bucket, id ulid.ULID) error {
	workCtx, cancel := TimeoutContext(ctx, timeout)
	defer cancel()
	if err := block.Delete(workCtx, bkt, id); err != nil {
		return errors.Wrapf(err, "delete remote block %s", id)
	}
	return nil
}

func TimeoutContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	var workCtx context.Context
	var cancel context.CancelFunc
	if timeout.Seconds() > 0 {
		workCtx, cancel = context.WithTimeout(ctx, timeout)
	} else {
		workCtx, cancel = context.WithCancel(ctx)
	}
	return workCtx, cancel
}
